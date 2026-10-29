"""
========================================================================================
DỰ ÁN: BDT301 — Big Data Assignment
MODULE: review_analytics_job.py (PySpark Job 2b - Review Analytics)
TÁC GIẢ: Thành viên 2 (Tiến) - Spark Core ETL & Review Analytics Lead
MỤC ĐÍCH: 
  - Thực thi Job 2b giải quyết các câu hỏi nghiệp vụ chỉ cần sử dụng bảng Reviews sạch:
      1. Câu hỏi Q3: Tương quan Giá / Độ dài review với Điểm đánh giá và Độ hữu ích.
      2. Câu hỏi Q4: Trích xuất các Review hữu ích nhất mỗi năm sử dụng Window Function.
      3. Câu hỏi Q5: Xác định các đầu sách có lượng review lớn nhưng điểm trung bình thấp
                     (những tựa sách gây thất vọng hoặc tranh cãi nhất).
  - Đảm bảo tiêu chuẩn Rubric: Thực hiện $\\ge 2$ phép tính tổng hợp (Aggregations) phức tạp
    kết hợp với Window Function.
  - Đồng bộ kết quả sang PostgreSQL (Serving Layer) qua JDBC với cơ chế Idempotent (overwrite).
========================================================================================
"""

import os
import sys
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, round as spark_round, avg, row_number, trim,
    when, desc, asc, min as spark_min, max as spark_max
)
from pyspark.sql import functions as F
# Dùng chung JDBC writer với Job 2a: truncate=true để giữ DDL/index của
# postgres/init.sql thay vì DROP TABLE như mode=overwrite mặc định.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from serving import write_to_postgres  # noqa: E402

def create_spark_session(app_name="BDT301_TV2_Job2b_Review_Analytics"):
    """
    Khởi tạo SparkSession tối ưu cho việc tính toán phân tích và kết nối JDBC.
    """
    event_log_dir = os.getenv("SPARK_EVENT_LOG_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "event-logs"))
    os.makedirs(event_log_dir, exist_ok=True)
    
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "1g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "1536m")
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", "8")
        # Ghi nhận Spark Event Log làm minh chứng thực thi
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_log_dir)
    )

    # Tương thích với MinIO qua giao thức S3A nếu có
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


def solve_q3_price_and_length_correlation(df_reviews, base_dir):
    """
    CÂU HỎI Q3: Phân tích tương quan Giá sách / Độ dài review với Điểm đánh giá và Độ hữu ích.
    - Chia dữ liệu thành các dải độ dài review:
        + Ngắn (< 100 ký tự)
        + Vừa (100 - 300 ký tự)
        + Dài (300 - 600 ký tự)
        + Rất dài (> 600 ký tự)
    - Nếu có trường Giá (price), chia các khoảng giá: <$10, $10-$20, $20-$50, >$50.
    - Tính toán: avg_score, avg_helpful_votes, total_reviews.
    """
    print("\n" + "="*80)
    print("THỰC THI CÂU HỎI Q3: TƯƠNG QUAN ĐỘ DÀI REVIEW & GIÁ VS ĐIỂM SỐ")
    print("="*80)
    
    # 1. Phân tích theo Dải độ dài bài viết (Áp dụng cho 100% dữ liệu)
    q3_length_df = (
        df_reviews
        .withColumn(
            "length_band",
            when(col("review_length") < 100, "1. Short (< 100 chars)")
            .when((col("review_length") >= 100) & (col("review_length") < 300), "2. Medium (100-300 chars)")
            .when((col("review_length") >= 300) & (col("review_length") < 600), "3. Long (300-600 chars)")
            .otherwise("4. Very Long (> 600 chars)")
        )
        .groupBy("length_band")
        .agg(
            count("*").alias("total_reviews"),
            spark_round(avg("score"), 2).alias("avg_score"),
            spark_round(avg("helpful_votes"), 2).alias("avg_helpful_votes"),
            spark_round(avg("review_length"), 0).alias("avg_length")
        )
        .orderBy("length_band")
    )
    
    print("\n[Kết quả Q3: Phân tích theo độ dài review]")
    q3_length_df.show(truncate=False)
    write_to_postgres(q3_length_df, "agg_review_length_band")
    
    # 2. Phân tích theo Dải giá sách (cho các bản ghi có giá hợp lệ)
    q3_price_df = (
        df_reviews
        .filter(col("price").isNotNull() & (col("price") > 0))
        .withColumn(
            "price_band",
            when(col("price") < 10, "1. Under $10")
            .when((col("price") >= 10) & (col("price") < 20), "2. $10 - $20")
            .when((col("price") >= 20) & (col("price") < 50), "3. $20 - $50")
            .otherwise("4. Over $50")
        )
        .groupBy("price_band")
        .agg(
            count("*").alias("total_reviews"),
            spark_round(avg("score"), 2).alias("avg_score"),
            spark_round(avg("helpful_votes"), 2).alias("avg_helpful_votes"),
            spark_round(avg("price"), 2).alias("avg_actual_price")
        )
        .orderBy("price_band")
    )
    
    print("\n[Kết quả Q3: Phân tích theo khoảng giá sách]")
    q3_price_df.show(truncate=False)
    write_to_postgres(q3_price_df, "agg_price_band")


def solve_q4_top_helpful_reviews(df_reviews, base_dir, top_n=10):
    """
    CÂU HỎI Q4: Trích xuất các bài review hữu ích nhất mỗi năm sử dụng Window Function.
    - Kỹ thuật nâng cao: Áp dụng Window.partitionBy("review_year").orderBy(desc("helpful_votes"))
    - Lọc Top N đánh giá có nhiều lượt bình chọn hữu ích nhất cho từng năm.
    """
    print("\n" + "="*80)
    print(f"THỰC THI CÂU HỎI Q4: TOP {top_n} REVIEW HỮU ÍCH NHẤT MỖI NĂM (WINDOW FUNCTION)")
    print("="*80)
    
    # 1. Khai báo Window Specification phân vùng theo năm và sắp xếp theo lượt vote hữu ích
    window_spec = Window.partitionBy("review_year").orderBy(desc("helpful_votes"), desc("score"))
    
    # 2. Đánh số thứ tự hạng (ranking) cho từng review trong từng năm
    ranked_reviews = (
        df_reviews
        .filter(col("review_year").isNotNull() & (col("helpful_votes") > 0))
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= top_n)
        .select(
            col("review_year"),
            col("rank"),
            col("title"),
            col("user_id"),
            col("profile_name"),
            col("score"),
            col("helpful_votes"),
            col("total_votes"),
            col("helpfulness_ratio"),
            col("review_summary")
        )
        .orderBy("review_year", "rank")
    )
    
    print(f"\n[Kết quả Q4: Top {top_n} review hữu ích nhất mỗi năm]")
    ranked_reviews.show(15, truncate=35)
    write_to_postgres(ranked_reviews, "top_helpful_reviews")


def solve_q5_controversial_or_disappointing_books(df_reviews, base_dir, min_reviews=30):
    """
    CÂU HỎI Q5: Tìm các đầu sách nhận được lượng thảo luận lớn nhưng điểm trung bình thấp.
    - Điều kiện: Có từ min_reviews lượt đánh giá trở lên.
    - Sắp xếp tăng dần theo điểm trung bình để tìm những sách gây tranh cãi hoặc thất vọng nhất.
    """
    print("\n" + "="*80)
    print(f"THỰC THI CÂU HỎI Q5: CÁC ĐẦU SÁCH NHIỀU REVIEW NHƯNG ĐIỂM THẤP (>= {min_reviews} REVIEWS)")
    print("="*80)
    
    q5_df = (
        df_reviews
        .filter(col("title").isNotNull() & (trim(col("title")) != ""))
        .groupBy("title", "title_norm")
        .agg(
            count("score").alias("total_reviews"),
            spark_round(avg("score"), 2).alias("avg_score"),
            spark_round(avg("helpful_votes"), 1).alias("avg_helpful_votes"),
            spark_min("score").alias("min_score"),
            spark_max("score").alias("max_score")
        )
        # Lọc các đầu sách có lượng tương tác đáng kể
        .filter(col("total_reviews") >= min_reviews)
        # Sắp xếp theo điểm thấp nhất trước
        .orderBy(asc("avg_score"), desc("total_reviews"))
    )
    
    print("\n[Kết quả Q5: Top 10 cuốn sách gây thất vọng / tranh cãi nhất]")
    q5_df.show(10, truncate=40)
    write_to_postgres(q5_df, "book_summary")

def solve_q7_activity_heatmap(df_reviews):
    """Q7: số review và điểm TB theo tháng × thứ trong tuần (1 = Chủ nhật, theo Spark)."""
    day_names = F.array(*[F.lit(d) for d in (
        "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")])
    heatmap = (
        df_reviews
        .filter(col("review_month").isNotNull() & col("review_day_of_week").isNotNull())
        .groupBy("review_month", col("review_day_of_week").alias("day_of_week"))
        .agg(
            count("*").alias("total_reviews"),
            spark_round(avg("score"), 2).alias("avg_score"),
        )
        .withColumn("month_name", F.date_format(
            F.make_date(F.lit(2000), col("review_month"), F.lit(1)), "MMMM"))
        .withColumn("day_name", F.element_at(day_names, col("day_of_week")))
        .select("review_month", "month_name", "day_of_week", "day_name",
                "total_reviews", "avg_score")
        .orderBy("review_month", "day_of_week")
    )
    write_to_postgres(heatmap, "agg_activity_heatmap")

def main():
    start_time = time.time()
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Đọc đường dẫn từ biến môi trường PROCESSED_DIR
    processed_dir = os.getenv("PROCESSED_DIR", os.path.join(base_dir, "data", "processed"))
    clean_reviews_dir = os.path.join(processed_dir, "reviews_clean")
    
    spark = create_spark_session()
    
    try:
        # Bắt buộc phải có dữ liệu sạch từ Job 1, không tự động chuyển sang dữ liệu sample
        if not (clean_reviews_dir.startswith("s3a://") or clean_reviews_dir.startswith("s3://")):
            if not os.path.exists(clean_reviews_dir):
                raise FileNotFoundError(
                    f"[✗] LỖI: Chưa tìm thấy dữ liệu Parquet đã làm sạch tại: {clean_reviews_dir}. "
                    f"Vui lòng chạy clean_data_job.py trước!"
                )
        
        print(f"[*] Đang nạp dữ liệu sạch từ: {clean_reviews_dir}")
        df_reviews = spark.read.parquet(clean_reviews_dir)
        review_count = df_reviews.count()
        if review_count == 0:
            raise RuntimeError(f"[✗] LỖI: Dữ liệu reviews tại {clean_reviews_dir} không có bản ghi nào!")
            
        print(f"[*] Tổng số bản ghi review nạp vào phân tích: {review_count:,}")
        
        # 1. Thực thi Câu hỏi Q3
        solve_q3_price_and_length_correlation(df_reviews, base_dir)
        
        # 2. Thực thi Câu hỏi Q4 (Window Function)
        solve_q4_top_helpful_reviews(df_reviews, base_dir, top_n=10)
        
        # 3. Thực thi Câu hỏi Q5
        solve_q5_controversial_or_disappointing_books(df_reviews, base_dir, min_reviews=20)

        # 4. Thực thi Câu hỏi Q7 (Activity Heatmap)
        solve_q7_activity_heatmap(df_reviews)
        
        elapsed = time.time() - start_time
        print(f"\n[✓] Hoàn tất toàn bộ PySpark Job 2b trong {elapsed:.2f} giây!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
