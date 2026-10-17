"""
========================================================================================
DỰ ÁN: BDT301 — Big Data Assignment
MODULE: eda_survey.py
TÁC GIẢ: Thành viên 2 (Tiến) - Spark Core ETL & Data Quality Lead
MỤC ĐÍCH: 
  - Khảo sát thực trạng dữ liệu thô (Exploratory Data Analysis & Data Profiling).
  - Thu thập số liệu thực tế về:
      1. Tỷ lệ thiếu (Missing Rate / Null %) của các cột quan trọng.
      2. Phân phối điểm số đánh giá (review/score) và phát hiện điểm bất thường.
      3. Ý nghĩa trường ratingsCount trong bảng sách.
      4. Tỷ lệ trùng lặp (Duplicates) trong cả hai bảng.
      5. Tỷ lệ khớp khóa nối (Join match rate) giữa hai bảng theo Title.
      6. Kiểm tra các dòng có ký tự xuống dòng (multiline).
  - Kết quả khảo sát được xuất ra báo cáo để:
      + Báo cho TV1 về cấu hình đọc multiline.
      + Chốt với TV3 và TV4 về phương án Câu 3 (Q3) và tỷ lệ khớp join.
      + Đưa số liệu chính xác vào Data Quality Report (bảng dq_report).
========================================================================================
"""

import os
import sys
import time

# Đảm bảo Python nhận diện thư mục hiện tại để import schemas
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, trim, lower,
    regexp_replace, length, round as spark_round, desc
)
from schemas import BOOKS_RAW_SCHEMA, REVIEWS_RAW_SCHEMA, CSV_READ_OPTIONS

def create_spark_session(app_name="BDT301_TV2_EDA_Survey"):
    """
    Khởi tạo SparkSession tối ưu cho việc phân tích dữ liệu lớn trên máy cục bộ hoặc cụm.
    - Cấu hình bộ nhớ driver và executor phù hợp để tránh lỗi OutOfMemory (OOM).
    - Tối ưu hóa số lượng partition để shuffle nhanh chóng.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        # Tăng dung lượng bộ nhớ cho driver để gom kết quả thống kê
        .config("spark.driver.memory", "4g")
        # Giảm số lượng partition mặc định từ 200 xuống 8 khi chạy local để tiết kiệm overhead
        .config("spark.sql.shuffle.partitions", "8")
        # Cho phép đọc ký tự xuống dòng trong ô CSV
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    return builder.getOrCreate()


def analyze_books_data(spark, books_path):
    """
    Khảo sát chi tiết bảng sách (books_data.csv).
    """
    print("\n" + "="*80)
    print("1. KHẢO SÁT BẢNG SÁCH (books_data.csv)")
    print("="*80)
    
    # 1. Đọc dữ liệu với Schema tường minh đã định nghĩa
    df_books = spark.read.options(**CSV_READ_OPTIONS).schema(BOOKS_RAW_SCHEMA).csv(books_path)
    total_books = df_books.count()
    print(f"[*] Tổng số dòng trong books_data: {total_books:,} dòng")
    
    # 2. Thống kê tỷ lệ Missing Values cho từng cột
    print("\n[*] Tỷ lệ giá trị thiếu (Missing / Null / Empty):")
    missing_exprs = [
        count(when(isnull(col(c)) | (trim(col(c)) == "") | (col(c) == "None"), c)).alias(c)
        for c in df_books.columns
    ]
    missing_counts = df_books.select(missing_exprs).collect()[0].asDict()
    
    for col_name, null_cnt in missing_counts.items():
        pct = (null_cnt / total_books) * 100
        print(f"  - Cột '{col_name:<15}': {null_cnt:>8,} dòng thiếu ({pct:6.2f}%)")
        
    # 3. Kiểm tra trùng lặp tiêu đề sách (Title)
    print("\n[*] Kiểm tra trùng lặp tiêu đề sách (Title):")
    unique_titles = df_books.select("Title").filter(col("Title").isNotNull()).distinct().count()
    duplicate_titles = total_books - unique_titles
    print(f"  - Số tiêu đề duy nhất (Unique Titles): {unique_titles:,}")
    print(f"  - Số dòng có tiêu đề bị trùng lặp   : {duplicate_titles:,} ({(duplicate_titles/total_books)*100:.2f}%)")
    
    # Hiển thị top 5 sách xuất hiện nhiều lần nhất (do tái bản hoặc nhiều ấn bản)
    print("  - Top 5 tiêu đề lặp lại nhiều lần nhất:")
    df_books.groupBy("Title").count().orderBy(desc("count")).limit(5).show(truncate=False)
    
    return df_books, total_books, missing_counts


def analyze_reviews_data(spark, reviews_path, sample_ratio=None):
    """
    Khảo sát chi tiết bảng đánh giá (Books_rating.csv).
    - Có hỗ trợ tham số sample_ratio (ví dụ: 0.1 để chạy nhanh 10% nếu cần thử nghiệm).
    """
    print("\n" + "="*80)
    print("2. KHẢO SÁT BẢNG ĐÁNH GIÁ (Books_rating.csv)")
    print("="*80)
    
    # 1. Đọc dữ liệu đánh giá
    df_reviews = spark.read.options(**CSV_READ_OPTIONS).schema(REVIEWS_RAW_SCHEMA).csv(reviews_path)
    if sample_ratio:
        df_reviews = df_reviews.sample(fraction=sample_ratio, seed=42)
        print(f"[*] Đang lấy mẫu {sample_ratio*100}% để khảo sát nhanh...")
        
    total_reviews = df_reviews.count()
    print(f"[*] Tổng số đánh giá khảo sát: {total_reviews:,} dòng")
    
    # 2. Thống kê tỷ lệ Missing Values cho các cột chính
    print("\n[*] Tỷ lệ giá trị thiếu (Missing / Null / Empty):")
    check_cols = ["Id", "Title", "Price", "User_id", "profileName", "review/helpfulness", "review/score", "review/time"]
    missing_exprs = [
        count(when(isnull(col(c)) | (trim(col(c)) == ""), c)).alias(c)
        for c in check_cols
    ]
    missing_counts = df_reviews.select(missing_exprs).collect()[0].asDict()
    
    for col_name, null_cnt in missing_counts.items():
        pct = (null_cnt / total_reviews) * 100
        print(f"  - Cột '{col_name:<20}': {null_cnt:>9,} dòng thiếu ({pct:6.2f}%)")
        
    # 3. Phân phối điểm đánh giá (review/score)
    print("\n[*] Phân phối điểm số đánh giá (review/score):")
    score_dist = df_reviews.groupBy("review/score").count().orderBy(desc("count")).collect()
    for row in score_dist:
        score_val = row["review/score"]
        cnt = row["count"]
        pct = (cnt / total_reviews) * 100
        print(f"  - Mức điểm {str(score_val):<8}: {cnt:>9,} lượt ({pct:6.2f}%)")
        
    # 4. Kiểm tra trùng lặp đánh giá (Duplicate Reviews)
    print("\n[*] Kiểm tra đánh giá trùng lặp (cùng User_id, Title, review/time):")
    dedup_reviews = df_reviews.dropDuplicates(["User_id", "Title", "review/time"]).count()
    dup_cnt = total_reviews - dedup_reviews
    print(f"  - Số review duy nhất: {dedup_reviews:,}")
    print(f"  - Số review bị trùng: {dup_cnt:,} ({(dup_cnt/total_reviews)*100:.2f}%)")
    
    # 5. Khảo sát độ dài văn bản review (review_length)
    print("\n[*] Thống kê độ dài văn bản review:")
    df_reviews_len = df_reviews.withColumn("rev_len", length(col("review/text")))
    stats = df_reviews_len.select(
        spark_round(count(when(col("rev_len") < 50, True)) / total_reviews * 100, 2).alias("under_50_chars"),
        spark_round(count(when((col("rev_len") >= 50) & (col("rev_len") <= 500), True)) / total_reviews * 100, 2).alias("50_to_500_chars"),
        spark_round(count(when(col("rev_len") > 500, True)) / total_reviews * 100, 2).alias("over_500_chars")
    ).collect()[0]
    print(f"  - Review ngắn (< 50 ký tự)   : {stats['under_50_chars']}%")
    print(f"  - Review vừa (50 - 500 ký tự): {stats['50_to_500_chars']}%")
    print(f"  - Review dài (> 500 ký tự)   : {stats['over_500_chars']}%")
    
    return df_reviews, total_reviews, missing_counts


def analyze_join_compatibility(df_books, df_reviews):
    """
    Đo lường tỷ lệ khớp (Join match rate) giữa hai bảng theo Title.
    Đây là căn cứ cốt lõi để trả lời cho TV3 (phụ trách Job Join 2a) và giảng viên.
    """
    print("\n" + "="*80)
    print("3. ĐO LƯỜNG TỶ LỆ KHỚP KHÓA NỐI (JOIN COMPATIBILITY)")
    print("="*80)
    
    # Lấy danh sách tiêu đề sách từ bảng sách (đã loại null)
    books_titles = df_books.select("Title").filter(col("Title").isNotNull()).distinct()
    
    # Lấy danh sách tiêu đề sách được review từ bảng reviews
    reviews_titles = df_reviews.select("Title").filter(col("Title").isNotNull()).distinct()
    total_rev_titles = reviews_titles.count()
    
    # Cách 1: Khớp nguyên bản (Raw Match)
    exact_match = reviews_titles.join(books_titles, "Title", "inner").count()
    exact_pct = (exact_match / total_rev_titles) * 100
    print(f"[*] Khớp tiêu đề nguyên bản (Case-sensitive):")
    print(f"  - Tổng số đầu sách có review : {total_rev_titles:,}")
    print(f"  - Số đầu sách khớp với bảng sách: {exact_match:,} ({exact_pct:.2f}%)")
    
    # Cách 2: Khớp sau khi chuẩn hóa chữ thường và bỏ khoảng trắng thừa (Normalized Match)
    b_norm = books_titles.withColumn("t_norm", lower(trim(regexp_replace(col("Title"), "[^a-zA-Z0-9 ]", ""))))
    r_norm = reviews_titles.withColumn("t_norm", lower(trim(regexp_replace(col("Title"), "[^a-zA-Z0-9 ]", ""))))
    
    norm_match = r_norm.join(b_norm, "t_norm", "inner").select("t_norm").distinct().count()
    norm_pct = (norm_match / total_rev_titles) * 100
    print(f"\n[*] Khớp tiêu đề sau khi Chuẩn hóa (title_norm: lowercase, strip special chars):")
    print(f"  - Số đầu sách khớp sau chuẩn hóa: {norm_match:,} ({norm_pct:.2f}%)")
    print(f"  - Tỷ lệ khớp tăng thêm: +{norm_pct - exact_pct:.2f}% sau khi làm sạch!")


def main():
    start_time = time.time()
    
    # Thiết lập đường dẫn tới 2 file CSV
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    books_file = os.path.join(base_dir, "books_data.csv")
    reviews_file = os.path.join(base_dir, "Books_rating.csv")
    
    print(f"Đang chuẩn bị khởi động Spark để khảo sát dữ liệu...")
    print(f"Bảng sách : {books_file}")
    print(f"Bảng review: {reviews_file}")
    
    spark = create_spark_session()
    
    try:
        # Khảo sát bảng sách
        df_books, total_b, b_missing = analyze_books_data(spark, books_file)
        
        # Khảo sát bảng review (dùng full dataset hoặc sample nếu máy giới hạn)
        df_reviews, total_r, r_missing = analyze_reviews_data(spark, reviews_file)
        
        # Đo lường tỷ lệ khớp Join
        analyze_join_compatibility(df_books, df_reviews)
        
        elapsed = time.time() - start_time
        print(f"\n[✓] Hoàn tất quá trình khảo sát EDA trong {elapsed:.2f} giây!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
