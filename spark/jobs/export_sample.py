"""
========================================================================================
DỰ ÁN: BDT301 — Big Data Assignment
MODULE: export_sample.py
TÁC GIẢ: Thành viên 2 (Tiến) - Spark Core ETL & Data Quality Lead
MỤC ĐÍCH: 
  - Trích xuất tập dữ liệu mẫu Parquet (~10,000 dòng đánh giá và ~1,000 dòng sách sạch).
  - Tên cột và kiểu dữ liệu được chốt chuẩn hóa 100% để:
      + Bàn giao ngay cho Thành viên 3 (viết trước Job 2a - Join & Aggregations).
      + Bàn giao ngay cho Thành viên 4 (dựng sớm Dashboard Metabase trên dữ liệu mẫu).
========================================================================================
"""

import os
import sys

# Thêm đường dẫn để import schemas
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, lower, trim, regexp_replace, regexp_extract, length,
    from_unixtime, to_date, year, month, dayofweek, when
)
from schemas import BOOKS_RAW_SCHEMA, REVIEWS_RAW_SCHEMA, CSV_READ_OPTIONS

def create_spark():
    """Khởi tạo SparkSession cục bộ cho việc xuất mẫu dữ liệu"""
    return (
        SparkSession.builder
        .appName("BDT301_TV2_Export_Sample")
        .master(os.getenv("SPARK_MASTER_URL", "local[2]"))
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def clean_books_sample(df_books, limit_rows=1000):
    """
    Làm sạch và chuẩn hóa bảng sách mẫu:
    - Loại bỏ dấu ngoặc vuông và nháy đơn ở cột authors và categories.
    - Trích xuất năm xuất bản từ publishedDate.
    - Tạo khóa chuẩn hóa title_norm (viết thường, bỏ ký tự lạ).
    - Khử trùng lặp theo title_norm.
    """
    print(f"[*] Đang chuẩn hóa {limit_rows} dòng sách mẫu...")
    
    cleaned = (
        df_books
        # 1. Lọc bỏ các dòng tiêu đề rỗng
        .filter(col("Title").isNotNull() & (trim(col("Title")) != ""))
        
        # 2. Tạo khóa join chuẩn hóa (lowercase, strip whitespace and punctuation)
        .withColumn("title_norm", lower(trim(regexp_replace(col("Title"), "[^a-zA-Z0-9 ]", ""))))
        
        # 3. Làm sạch mảng authors (Ví dụ: "['Julie Strain']" -> "Julie Strain")
        .withColumn("main_author", regexp_replace(col("authors"), r"^\[\'?|\'?\]$", ""))
        
        # 4. Làm sạch mảng categories (Ví dụ: "['Comics & Graphic Novels']" -> "Comics & Graphic Novels")
        .withColumn("main_category", regexp_replace(col("categories"), r"^\[\'?|\'?\]$", ""))
        
        # 5. Trích xuất năm 4 chữ số từ chuỗi ngày xuất bản (publishedDate) an toàn với regexp_extract
        .withColumn(
            "published_year",
            when(
                length(regexp_extract(col("publishedDate"), r"(\d{4})", 1)) == 4,
                regexp_extract(col("publishedDate"), r"(\d{4})", 1).cast("int")
            ).otherwise(None)
        )
        
        # 6. Ép kiểu trường ratingsCount: cast sang float trước rồi mới sang int (vì dữ liệu có dạng '5.0')
        .withColumn("ratings_count", col("ratingsCount").cast("float").cast("int"))
        
        # 7. Khử trùng lặp trên tiêu đề chuẩn hóa
        .dropDuplicates(["title_norm"])
        
        # 8. Chọn lọc các cột chuẩn hóa bàn giao cho nhóm
        .select(
            col("Title").alias("title"),
            col("title_norm"),
            col("main_author"),
            col("publisher"),
            col("published_year"),
            col("main_category"),
            col("ratings_count"),
            col("description"),
            col("image")
        )
        .limit(limit_rows)
    )
    return cleaned


def clean_reviews_sample(df_reviews, limit_rows=10000):
    """
    Làm sạch và chuẩn hóa bảng đánh giá mẫu:
    - Tách cột review/helpfulness thành helpful_votes và total_votes.
    - Chuyển review/time sang timestamp và trích year, month, day_of_week.
    - Ép kiểu price và score, loại điểm không hợp lệ ngoài [1, 5].
    - Tính độ dài văn bản review_length.
    """
    print(f"[*] Đang chuẩn hóa {limit_rows} dòng đánh giá mẫu...")
    
    cleaned = (
        df_reviews
        # 1. Lọc điều kiện cơ bản: Tiêu đề có thật, điểm hợp lệ từ 1.0 đến 5.0
        .filter(
            col("Title").isNotNull() & (trim(col("Title")) != "") &
            col("review/score").isNotNull() &
            (col("review/score").cast("float") >= 1.0) &
            (col("review/score").cast("float") <= 5.0)
        )
        
        # 2. Tạo khóa join chuẩn hóa tương ứng với bảng sách
        .withColumn("title_norm", lower(trim(regexp_replace(col("Title"), "[^a-zA-Z0-9 ]", ""))))
        
        # 3. Tách chuỗi hữu ích dạng "7/7"
        .withColumn("helpful_votes", split(col("review/helpfulness"), "/")[0].cast("int"))
        .withColumn("total_votes", split(col("review/helpfulness"), "/")[1].cast("int"))
        
        # 4. Tính tỷ lệ hữu ích (tránh chia cho 0)
        .withColumn(
            "helpfulness_ratio",
            when(col("total_votes") > 0, col("helpful_votes") / col("total_votes")).otherwise(None)
        )
        
        # 5. Ép kiểu điểm số và giá sách
        .withColumn("score", col("review/score").cast("float"))
        .withColumn("price", col("Price").cast("float"))
        
        # 6. Chuyển đổi timestamp và trích xuất thành phần ngày tháng
        .withColumn("review_timestamp", from_unixtime(col("review/time").cast("long")).cast("timestamp"))
        .withColumn("review_year", year(col("review_timestamp")))
        .withColumn("review_month", month(col("review_timestamp")))
        .withColumn("review_day_of_week", dayofweek(col("review_timestamp")))
        
        # 7. Tính độ dài văn bản review
        .withColumn("review_length", length(col("review/text")))
        
        # 8. Khử trùng lặp đánh giá của cùng user trên cùng đầu sách
        .dropDuplicates(["User_id", "title_norm", "review/time"])
        
        # 9. Lựa chọn các cột tiêu chuẩn bàn giao
        .select(
            col("Id").alias("book_id"),
            col("Title").alias("title"),
            col("title_norm"),
            col("price"),
            col("User_id").alias("user_id"),
            col("profileName").alias("profile_name"),
            col("score"),
            col("helpful_votes"),
            col("total_votes"),
            col("helpfulness_ratio"),
            col("review_timestamp"),
            col("review_year"),
            col("review_month"),
            col("review_day_of_week"),
            col("review_length"),
            col("review/summary").alias("review_summary"),
            col("review/text").alias("review_text")
        )
        .limit(limit_rows)
    )
    return cleaned


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Ưu tiên tìm trong thư mục chuẩn data/raw/
    books_file = os.path.join(base_dir, "data", "raw", "books_data.csv")
    if not os.path.exists(books_file):
        books_file = os.path.join(base_dir, "books_data.csv")
        
    reviews_file = os.path.join(base_dir, "data", "raw", "Books_rating.csv")
    if not os.path.exists(reviews_file):
        reviews_file = os.path.join(base_dir, "Books_rating.csv")
    
    output_dir = os.path.join(base_dir, "data", "sample")
    os.makedirs(output_dir, exist_ok=True)
    
    spark = create_spark()
    
    try:
        # Đọc dữ liệu sách thô và xuất mẫu
        df_books_raw = spark.read.options(**CSV_READ_OPTIONS).schema(BOOKS_RAW_SCHEMA).csv(books_file)
        sample_books = clean_books_sample(df_books_raw, limit_rows=1000)
        
        books_out = os.path.join(output_dir, "sample_books.parquet")
        sample_books.write.mode("overwrite").parquet(books_out)
        print(f"[✓] Đã xuất mẫu sách thành công: {books_out}")
        
        # Đọc dữ liệu review thô và xuất mẫu
        df_reviews_raw = spark.read.options(**CSV_READ_OPTIONS).schema(REVIEWS_RAW_SCHEMA).csv(reviews_file)
        sample_reviews = clean_reviews_sample(df_reviews_raw, limit_rows=10000)
        
        reviews_out = os.path.join(output_dir, "sample_reviews.parquet")
        sample_reviews.write.mode("overwrite").parquet(reviews_out)
        print(f"[✓] Đã xuất mẫu review thành công: {reviews_out}")
        
        # In schema và 5 dòng tiêu biểu để kiểm tra
        print("\n--- SCHEMA BẢNG SÁCH MẪU ---")
        sample_books.printSchema()
        print("\n--- 3 DÒNG TIÊU BIỂU BẢNG SÁCH ---")
        sample_books.show(3, truncate=40)
        
        print("\n--- SCHEMA BẢNG REVIEW MẪU ---")
        sample_reviews.printSchema()
        print("\n--- 3 DÒNG TIÊU BIỂU BẢNG REVIEW ---")
        sample_reviews.show(3, truncate=40)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
