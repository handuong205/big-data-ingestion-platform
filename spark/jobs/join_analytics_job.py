"""
========================================================================================
DỰ ÁN: BDT301 — Big Data Assignment
MODULE: join_analytics_job.py (PySpark Job 2a - Join Analytics)
TÁC GIẢ: Thành viên 3 (TV3) - Spark Analytics & Serving Layer Lead
MỤC ĐÍCH:
  - Join bảng reviews sạch với bảng books sạch theo khoá chuẩn hoá `title_norm`.
  - Thực thi các câu hỏi cần thông tin sách:
      Q1: Điểm trung bình và số review theo thể loại × năm.
      Q2: Top tác giả và nhà xuất bản (nhóm có >= N review).
      Q6: Phân nhóm người đánh giá theo User_id.
  - Ghi kết quả vào PostgreSQL (Serving Layer) qua JDBC.
  - Đo và lưu tỷ lệ khớp join vào bảng dq_report + reports/join_match.json.

CHẠY:
  ./run_pipeline.sh --only join_analytics
  hoặc:
  spark-submit spark/jobs/join_analytics_job.py

BIẾN MÔI TRƯỜNG:
  PROCESSED_DIR   (mặc định <repo>/data/processed)
  MIN_REVIEWS_Q2  (mặc định 30)
  POSTGRES_*      (bắt buộc, xem .env)
========================================================================================
"""

import json
import os
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, desc, round as spark_round, trim

# Cho phép `from serving import ...` khi chạy bằng spark-submit.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from serving import write_metrics_to_postgres, write_to_postgres  # noqa: E402


def create_spark_session():
    """SparkSession tối ưu cho join + aggregate, có ghi event log làm minh chứng."""
    event_log_dir = os.getenv(
        "SPARK_EVENT_LOG_DIR",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "event-logs",
        ),
    )
    os.makedirs(event_log_dir, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName("BDT301_TV3_Job2a_Join_Analytics")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1536m"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_log_dir)
    )

    # Tương thích MinIO/S3A nếu dữ liệu processed được đọc từ object storage.
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


def path_exists(path):
    """Kiểm tra path local hoặc S3A mà không cần Hadoop FileSystem API."""
    if path.startswith("s3a://") or path.startswith("s3://"):
        return True
    return os.path.exists(path)


# Dấu phân cách giữa các phần tử của list gốc: "', '" hoặc "\", \"".
# Không split theo dấu phẩy trần vì tên thể loại có thể chứa phẩy
# (ví dụ "Body, Mind & Spirit").
LIST_DELIMITER = r"['\"]\s*,\s*['\"]"


def explode_list_column(df, source_col, target_col):
    """
    Tách cột dạng "Fiction', 'History" hoặc "Author A', 'Author B" thành nhiều dòng.

    Job 1 chỉ bóc cặp ngoặc [] ở đầu/cuối, chưa tách từng giá trị, nên Job 2a
    phải explode để Q1/Q2 không gộp sai nhiều thể loại/tác giả vào một nhóm.
    Chuỗi được bóc thêm ngoặc kép/đơn còn sót trước khi split.
    """
    cleaned = F.regexp_replace(
        trim(col(source_col)),
        r"^[\[\]'\"\s]+|[\[\]'\"\s]+$",
        "",
    )
    result = (
        df.filter(col(source_col).isNotNull() & (trim(col(source_col)) != ""))
        .withColumn("__value", F.explode(F.split(cleaned, LIST_DELIMITER)))
        .withColumn(target_col, trim(col("__value")))
        .filter(col(target_col) != "")
        .drop("__value")
    )
    if source_col != target_col:
        result = result.drop(source_col)
    return result


def solve_q1_category_year(joined):
    """Q1: điểm trung bình + số review theo thể loại × năm."""
    categories = explode_list_column(joined, "main_category", "main_category")
    return (
        categories.filter(col("review_year").isNotNull())
        .groupBy("main_category", "review_year")
        .agg(
            F.count("*").alias("total_reviews"),
            F.countDistinct("title_norm").alias("total_books"),
            spark_round(F.avg("score"), 2).alias("avg_score"),
        )
        .orderBy("main_category", "review_year")
    )


def solve_q2_authors(joined, min_reviews):
    """Q2a: tác giả có >= min_reviews review."""
    authors = explode_list_column(joined, "main_author", "main_author")
    return (
        authors.groupBy("main_author")
        .agg(
            F.count("*").alias("total_reviews"),
            F.countDistinct("title_norm").alias("total_books"),
            spark_round(F.avg("score"), 2).alias("avg_score"),
            spark_round(F.avg("helpful_votes"), 2).alias("avg_helpful_votes"),
        )
        .filter(col("total_reviews") >= min_reviews)
        .orderBy(desc("total_reviews"), desc("avg_score"))
    )


def solve_q2_publishers(joined, min_reviews):
    """Q2b: nhà xuất bản có >= min_reviews review."""
    return (
        joined.filter(col("publisher").isNotNull() & (trim(col("publisher")) != ""))
        .withColumn("publisher", trim(col("publisher")))
        .groupBy("publisher")
        .agg(
            F.count("*").alias("total_reviews"),
            F.countDistinct("title_norm").alias("total_books"),
            spark_round(F.avg("score"), 2).alias("avg_score"),
        )
        .filter(col("total_reviews") >= min_reviews)
        .orderBy(desc("total_reviews"), desc("avg_score"))
    )


def solve_q6_reviewer_segments(joined):
    """
    Q6: phân nhóm người đánh giá theo User_id.

    User_id thiếu khoảng 17% (412.905 / 2.430.902 review sạch). Mức thiếu này
    vẫn chấp nhận được nên câu hỏi nâng cao này được thực hiện, nhưng chỉ tính
    trên ~83% review có User_id; kết quả không đại diện cho review ẩn danh.
    Tính trên từng user trước, rồi gộp theo dải số review/user. Cách tính
    avg_score và avg_helpful_votes có trọng số theo số review (không phải
    trung bình cộng của các trung bình).
    """
    per_user = (
        joined.filter(col("user_id").isNotNull() & (trim(col("user_id")) != ""))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("review_count"),
            F.sum("score").alias("score_sum"),
            F.sum("helpful_votes").alias("helpful_sum"),
        )
    )

    banded = per_user.withColumn(
        "review_count_band",
        F.when(col("review_count") == 1, "1. 1 review")
        .when(col("review_count") <= 5, "2. 2-5 reviews")
        .when(col("review_count") <= 20, "3. 6-20 reviews")
        .otherwise("4. >20 reviews"),
    )

    return (
        banded.groupBy("review_count_band")
        .agg(
            F.count("user_id").alias("total_users"),
            F.sum("review_count").alias("total_reviews"),
            spark_round(F.sum("score_sum") / F.sum("review_count"), 2).alias("avg_score"),
            spark_round(
                F.sum("helpful_sum") / F.sum("review_count"), 2
            ).alias("avg_helpful_votes"),
        )
        .orderBy("review_count_band")
    )


def save_join_report(processed_dir, report):
    """Lưu tỷ lệ khớp join ra JSON để dùng trong báo cáo."""
    reports_dir = os.getenv("REPORTS_DIR")
    if not reports_dir:
        if processed_dir.startswith("s3a://") or processed_dir.startswith("s3://"):
            print("[i] Bỏ qua join_match.json: processed_dir nằm trên S3A và chưa đặt REPORTS_DIR.")
            return
        reports_dir = os.path.join(os.path.dirname(processed_dir), "reports")
    os.makedirs(reports_dir, exist_ok=True)
    report_path = os.path.join(reports_dir, "join_match.json")
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    print(f"[*] Đã lưu tỷ lệ khớp join tại: {report_path}")


def main():
    started = time.time()
    min_reviews_q2 = int(os.getenv("MIN_REVIEWS_Q2", "30"))

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    processed_dir = os.getenv("PROCESSED_DIR", os.path.join(base_dir, "data", "processed"))
    reviews_path = os.path.join(processed_dir, "reviews_clean")
    books_path = os.path.join(processed_dir, "books_clean")

    spark = create_spark_session()
    joined = None
    try:
        for path in (reviews_path, books_path):
            if not path_exists(path):
                raise FileNotFoundError(
                    f"[X] Không tìm thấy dữ liệu sạch tại: {path}. "
                    f"Chạy clean_data_job.py (Job 1) trước."
                )

        print(f"[*] Đọc reviews sạch: {reviews_path}")
        reviews = spark.read.parquet(reviews_path)
        print(f"[*] Đọc books sạch:   {books_path}")
        books = spark.read.parquet(books_path)

        reviews_count = reviews.count()
        if reviews_count == 0:
            raise RuntimeError(f"[X] reviews_clean rỗng: {reviews_path}")

        # Bảng sách là bảng chiều nhỏ: khử trùng theo title_norm để join không bị fan-out.
        books_dim = (
            books.select("title_norm", "main_author", "publisher", "main_category")
            .filter(col("title_norm").isNotNull())
            .dropDuplicates(["title_norm"])
            .cache()
        )
        books_count = books_dim.count()
        print(f"[*] Reviews: {reviews_count:,} dòng | Books (distinct title_norm): {books_count:,}")

        joined = (
            reviews.select(
                "title_norm", "review_year", "score", "helpful_votes", "user_id"
            )
            .join(F.broadcast(books_dim), on="title_norm", how="inner")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        matched_reviews = joined.count()
        matched_books = joined.select("title_norm").distinct().count()
        review_match_pct = matched_reviews / reviews_count * 100 if reviews_count else 0.0
        book_match_pct = matched_books / books_count * 100 if books_count else 0.0

        print("\n" + "=" * 72)
        print("TỶ LỆ KHỚP JOIN (title_norm)")
        print("=" * 72)
        print(f"  Reviews khớp : {matched_reviews:,}/{reviews_count:,} ({review_match_pct:.2f}%)")
        print(f"  Books khớp   : {matched_books:,}/{books_count:,} ({book_match_pct:.2f}%)")
        if matched_reviews > reviews_count:
            print("  [!] CẢNH BÁO: matched > reviews, kiểm tra trùng title_norm trong books_clean.")

        report = {
            "reviews_total": reviews_count,
            "reviews_matched": matched_reviews,
            "reviews_match_pct": round(review_match_pct, 2),
            "books_total": books_count,
            "books_matched": matched_books,
            "books_match_pct": round(book_match_pct, 2),
            "min_reviews_q2": min_reviews_q2,
        }
        save_join_report(processed_dir, report)

        # ------------------------------------------------------------ Q1
        print("\n[*] Q1: điểm trung bình + số review theo thể loại × năm...")
        q1 = solve_q1_category_year(joined)
        print(f"    -> {q1.count():,} nhóm thể loại × năm")
        write_to_postgres(q1, "agg_category_year")

        # ------------------------------------------------------------ Q2
        print(f"\n[*] Q2: tác giả / nhà xuất bản có >= {min_reviews_q2} review...")
        q2_authors = solve_q2_authors(joined, min_reviews_q2)
        print(f"    -> {q2_authors.count():,} tác giả")
        write_to_postgres(q2_authors, "agg_author")

        q2_publishers = solve_q2_publishers(joined, min_reviews_q2)
        print(f"    -> {q2_publishers.count():,} nhà xuất bản")
        write_to_postgres(q2_publishers, "agg_publisher")

        # ------------------------------------------------------------ Q6
        print("\n[*] Q6: phân nhóm người đánh giá theo User_id...")
        q6 = solve_q6_reviewer_segments(joined)
        print(f"    -> {q6.count():,} nhóm người đánh giá")
        write_to_postgres(q6, "agg_reviewer_segment")

        # ------------------------------------------------------ dq_report
        write_metrics_to_postgres(
            spark,
            [
                ("join_reviews_total", reviews_count, "Số review sạch đưa vào join"),
                ("join_reviews_matched", matched_reviews, "Số review khớp được sách"),
                ("join_reviews_match_pct", round(review_match_pct, 2), "Tỷ lệ khớp review (%)"),
                ("join_books_total", books_count, "Số sách distinct theo title_norm"),
                ("join_books_matched", matched_books, "Số sách có ít nhất 1 review khớp"),
                ("join_books_match_pct", round(book_match_pct, 2), "Tỷ lệ khớp sách (%)"),
            ],
        )

        print(f"\n[OK] Job 2a hoàn tất trong {time.time() - started:.1f} giây.")
    finally:
        if joined is not None:
            joined.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
