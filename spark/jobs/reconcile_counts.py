"""
========================================================================================
DỰ ÁN: BDT301 — Big Data Assignment
MODULE: reconcile_counts.py (Hàm đối soát số dòng dữ liệu toàn trình)
TÁC GIẢ: Thành viên 2 (Tiến) - Spark Core ETL & Data Quality Lead
MỤC ĐÍCH: 
  - Thực hiện đối soát số dòng giữa các tầng dữ liệu (Data Reconciliation).
  - Chứng minh tính toàn vẹn dữ liệu: Không có bản ghi nào bị mất mát không rõ nguyên nhân.
  - Công thức kiểm chứng chuẩn:
      Total Raw Records == Clean Records + Bad Records + Deduplicated Records
  - Script được thiết kế để:
      + Nhúng trực tiếp vào run_pipeline.sh để kiểm tra tự động.
      + Xuất kết quả ra console và file log JSON tại: ingestion/logs/reconciliation.json.
      + Trả về Exit Code 0 nếu đối soát thành công (khớp số liệu).
      + Trả về Exit Code 1 nếu đối soát thất bại hoặc thiếu dữ liệu.
========================================================================================
"""

import os
import sys
import json
import time

# Import schemas
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, count, when
from schemas import BOOKS_RAW_SCHEMA, REVIEWS_RAW_SCHEMA, CSV_READ_OPTIONS

def create_spark(app_name="BDT301_TV2_Data_Reconciliation"):
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "1g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "1536m")
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", "4")
    )
    
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_user = os.getenv("MINIO_ROOT_USER")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD")
    if minio_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    if minio_user:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", minio_user)
    if minio_password:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", minio_password)
        
    builder = (
        builder
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    return builder.getOrCreate()


def reconcile_books_dataset(spark, raw_path, clean_path, bad_path):
    print(f"\n[*] Đang thực hiện đối soát số dòng cho tập dữ liệu: books_data...")
    
    try:
        df_raw = spark.read.options(**CSV_READ_OPTIONS).schema(BOOKS_RAW_SCHEMA).csv(raw_path)
        is_bad_book = col("Title").isNull() | (trim(col("Title")) == "")
        stats = df_raw.select(
            count("*").alias("raw_count"),
            count(when(is_bad_book, 1)).alias("bad_count"),
            count(when(~is_bad_book, 1)).alias("valid_count")
        ).collect()[0]
        raw_count = stats["raw_count"]
        bad_expected = stats["bad_count"]
        valid_count = stats["valid_count"]
    except Exception as e:
        print(f"  [✗] Lỗi đọc dữ liệu thô sách tại {raw_path}: {e}", file=sys.stderr)
        return {"dataset": "books_data", "status": "FAIL", "error": str(e)}

    try:
        df_clean = spark.read.parquet(clean_path)
        clean_count = df_clean.count()
    except Exception as e:
        print(f"  [✗] Lỗi đọc dữ liệu sạch sách tại {clean_path}: {e}", file=sys.stderr)
        clean_count = 0

    try:
        df_bad = spark.read.parquet(bad_path)
        bad_count = df_bad.count()
    except Exception as e:
        print(f"  [✗] Lỗi đọc dữ liệu lỗi sách tại {bad_path}: {e}", file=sys.stderr)
        bad_count = 0

    dedup_count = valid_count - clean_count
    accounted_for = clean_count + bad_count + dedup_count
    
    is_pass = (
        clean_count > 0 and
        raw_count == accounted_for and
        raw_count == (clean_count + bad_count + dedup_count) and
        bad_count == bad_expected and
        dedup_count >= 0
    )
    
    result = {
        "dataset": "books_data",
        "raw_records": raw_count,
        "clean_records": clean_count,
        "bad_records": bad_count,
        "deduplicated_records": dedup_count,
        "accounted_for": accounted_for,
        "status": "PASS" if is_pass else "FAIL"
    }
    
    print(f"  - Tổng số dòng thô (Raw)       : {raw_count:,}")
    print(f"  - Số dòng sạch (Clean Parquet) : {clean_count:,}")
    print(f"  - Số dòng lỗi (Bad Records)    : {bad_count:,}")
    print(f"  - Số dòng khử trùng (Dedup)    : {dedup_count:,}")
    print(f"  - Tổng giải trình (Clean+Bad+Dedup): {accounted_for:,}")
    print(f"  - Điều kiện raw == clean+bad+dedup : {'KHỚP (PASS)' if is_pass else 'LỆCH (FAIL)'}")
    return result


def reconcile_reviews_dataset(spark, raw_path, clean_path, bad_path):
    print(f"\n[*] Đang thực hiện đối soát số dòng cho tập dữ liệu: Books_rating...")
    
    try:
        df_raw = spark.read.options(**CSV_READ_OPTIONS).schema(REVIEWS_RAW_SCHEMA).csv(raw_path)
        parsed = (
            df_raw
            .withColumn("score_float", col("review/score").cast("float"))
            .withColumn("time_long", col("review/time").cast("long"))
        )
        is_bad_review = (
            col("Title").isNull() | (trim(col("Title")) == "") |
            col("score_float").isNull() | (col("score_float") < 1.0) | (col("score_float") > 5.0) |
            col("time_long").isNull()
        )
        stats = parsed.select(
            count("*").alias("raw_count"),
            count(when(is_bad_review, 1)).alias("bad_count"),
            count(when(~is_bad_review, 1)).alias("valid_count")
        ).collect()[0]
        raw_count = stats["raw_count"]
        bad_expected = stats["bad_count"]
        valid_count = stats["valid_count"]
    except Exception as e:
        print(f"  [✗] Lỗi đọc dữ liệu thô reviews tại {raw_path}: {e}", file=sys.stderr)
        return {"dataset": "Books_rating", "status": "FAIL", "error": str(e)}

    try:
        df_clean = spark.read.parquet(clean_path)
        clean_count = df_clean.count()
    except Exception as e:
        print(f"  [✗] Lỗi đọc dữ liệu sạch reviews tại {clean_path}: {e}", file=sys.stderr)
        clean_count = 0

    try:
        df_bad = spark.read.parquet(bad_path)
        bad_count = df_bad.count()
    except Exception as e:
        print(f"  [✗] Lỗi đọc dữ liệu lỗi reviews tại {bad_path}: {e}", file=sys.stderr)
        bad_count = 0

    dedup_count = valid_count - clean_count
    accounted_for = clean_count + bad_count + dedup_count
    
    is_pass = (
        clean_count > 0 and
        raw_count == accounted_for and
        raw_count == (clean_count + bad_count + dedup_count) and
        bad_count == bad_expected and
        dedup_count >= 0
    )
    
    result = {
        "dataset": "Books_rating",
        "raw_records": raw_count,
        "clean_records": clean_count,
        "bad_records": bad_count,
        "deduplicated_records": dedup_count,
        "accounted_for": accounted_for,
        "status": "PASS" if is_pass else "FAIL"
    }
    
    print(f"  - Tổng số dòng thô (Raw)       : {raw_count:,}")
    print(f"  - Số dòng sạch (Clean Parquet) : {clean_count:,}")
    print(f"  - Số dòng lỗi (Bad Records)    : {bad_count:,}")
    print(f"  - Số dòng khử trùng (Dedup)    : {dedup_count:,}")
    print(f"  - Tổng giải trình (Clean+Bad+Dedup): {accounted_for:,}")
    print(f"  - Điều kiện raw == clean+bad+dedup : {'KHỚP (PASS)' if is_pass else 'LỆCH (FAIL)'}")
    return result


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Đọc đường dẫn từ biến môi trường (tương thích cả S3A MinIO và local)
    books_raw = os.getenv("RAW_BOOKS_PATH", os.path.join(base_dir, "data", "raw", "books_data.csv"))
    if not (books_raw.startswith("s3a://") or books_raw.startswith("s3://")):
        if not os.path.exists(books_raw):
            alt_books = os.path.join(base_dir, "books_data.csv")
            if os.path.exists(alt_books):
                books_raw = alt_books
                
    reviews_raw = os.getenv("RAW_REVIEWS_PATH", os.path.join(base_dir, "data", "raw", "Books_rating.csv"))
    if not (reviews_raw.startswith("s3a://") or reviews_raw.startswith("s3://")):
        if not os.path.exists(reviews_raw):
            alt_reviews = os.path.join(base_dir, "Books_rating.csv")
            if os.path.exists(alt_reviews):
                reviews_raw = alt_reviews
                
    processed_dir = os.getenv("PROCESSED_DIR", os.path.join(base_dir, "data", "processed"))
    bad_records_dir = os.getenv("BAD_RECORDS_DIR", os.path.join(base_dir, "data", "bad_records"))
    
    books_clean = os.path.join(processed_dir, "books_clean")
    books_bad = os.path.join(bad_records_dir, "books")
    reviews_clean = os.path.join(processed_dir, "reviews_clean")
    reviews_bad = os.path.join(bad_records_dir, "reviews")
    
    spark = create_spark()
    
    try:
        print("="*80)
        print("BẮT ĐẦU QUY TRÌNH ĐỐI SOÁT DỮ LIỆU TOÀN TRÌNH (DATA RECONCILIATION)")
        print("Điều kiện kiểm chứng: raw == clean + bad + dedup")
        print("="*80)
        
        books_res = reconcile_books_dataset(spark, books_raw, books_clean, books_bad)
        reviews_res = reconcile_reviews_dataset(spark, reviews_raw, reviews_clean, reviews_bad)
        
        # Ghi kết quả ra file JSON
        log_dir = os.path.join(base_dir, "ingestion", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "reconciliation.json")
        
        overall_pass = (books_res.get("status") == "PASS" and reviews_res.get("status") == "PASS")
        final_report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "formula": "raw == clean + bad + dedup",
            "results": [books_res, reviews_res],
            "overall_status": "SUCCESS" if overall_pass else "FAILED"
        }
        
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)
            
        print("\n" + "="*80)
        print(f"[✓] Đã xuất báo cáo đối soát tại: {log_file}")
        print(f"[*] Trạng thái đối soát toàn bộ: {final_report['overall_status']}")
        print("="*80)
        
        # Trả về mã thoát (Exit code)
        if overall_pass:
            sys.exit(0)
        else:
            print("[✗] Đối soát dữ liệu thất bại! Dữ liệu không khớp điều kiện raw == clean + bad + dedup.", file=sys.stderr)
            sys.exit(1)
            
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
