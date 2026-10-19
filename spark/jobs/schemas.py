"""
========================================================================================
DỰ ÁN: BDT301 — Big Data Assignment
MODULE: schemas.py
TÁC GIẢ: Thành viên 2 (Tiến) - Spark Core ETL & Data Quality Lead
MỤC ĐÍCH: 
  - Khai báo Schema tường minh (Explicit Schema) cho hai tập dữ liệu thô:
      1. books_data.csv: Metadata của các đầu sách
      2. Books_rating.csv: Đánh giá và nhận xét của người dùng
  - Lý do KHÔNG sử dụng inferSchema=True:
      + inferSchema quét dữ liệu 2 lần (lần 1 xác định kiểu, lần 2 load dữ liệu) làm tăng gấp đôi I/O.
      + inferSchema dễ suy diễn sai kiểu dữ liệu (ví dụ: timestamp Unix thành long đơn thuần).
      + Sử dụng StringType() cho tầng thô để nạp an toàn, không bị crash nếu gặp ô dữ liệu dị thường.
========================================================================================
"""

from pyspark.sql.types import StructType, StructField, StringType

# --------------------------------------------------------------------------------------
# 1. SCHEMA CHO BẢNG SÁCH (books_data.csv)
# Kích thước: ~181.3 MB, 212,405 dòng
# --------------------------------------------------------------------------------------
BOOKS_RAW_SCHEMA = StructType([
    # Tiêu đề sách: Khóa định danh chính để liên kết (join) với bảng review
    StructField("Title", StringType(), True),
    
    # Mô tả tóm tắt nội dung cuốn sách
    StructField("description", StringType(), True),
    
    # Danh sách tác giả dưới dạng chuỗi mảng Python: ví dụ: "['Julie Strain']"
    StructField("authors", StringType(), True),
    
    # Đường link ảnh bìa sách từ Google Books API
    StructField("image", StringType(), True),
    
    # Đường link xem trước sách trên Google Books
    StructField("previewLink", StringType(), True),
    
    # Nhà xuất bản cuốn sách (A&C Black, iUniverse,...)
    StructField("publisher", StringType(), True),
    
    # Ngày/năm xuất bản: định dạng không đồng nhất (1996, 2005-01-01, 2005-02,...)
    StructField("publishedDate", StringType(), True),
    
    # Link thông tin chi tiết của sách
    StructField("infoLink", StringType(), True),
    
    # Thể loại sách dưới dạng chuỗi mảng: ví dụ: "['Comics & Graphic Novels']"
    StructField("categories", StringType(), True),
    
    # Số lượng đánh giá do Google Books ghi nhận (cần khảo sát so với số review thực tế)
    StructField("ratingsCount", StringType(), True)
])

# --------------------------------------------------------------------------------------
# 2. SCHEMA CHO BẢNG ĐÁNH GIÁ (Books_rating.csv)
# Kích thước: ~2.85 GB, ~3 triệu dòng
# --------------------------------------------------------------------------------------
REVIEWS_RAW_SCHEMA = StructType([
    # Mã định danh sách hoặc ASIN (ví dụ: "1882931173", "0826414346")
    StructField("Id", StringType(), True),
    
    # Tiêu đề sách: Trùng với Title trong books_data (khóa join tự nhiên)
    StructField("Title", StringType(), True),
    
    # Giá sách tại thời điểm review (chú ý: tỷ lệ thiếu rất cao, cần khảo sát)
    StructField("Price", StringType(), True),
    
    # Mã định danh người dùng Amazon (ví dụ: "A30TK6U7DNS82R")
    StructField("User_id", StringType(), True),
    
    # Tên hiển thị hồ sơ của người dùng (ví dụ: 'Kevin Killian')
    StructField("profileName", StringType(), True),
    
    # Tỷ lệ hữu ích của review dạng "helpful/total" (ví dụ: "7/7", "10/11", "0/0")
    StructField("review/helpfulness", StringType(), True),
    
    # Điểm đánh giá: thang điểm từ 1.0 đến 5.0 (ví dụ: "4.0", "5.0")
    StructField("review/score", StringType(), True),
    
    # Thời điểm đánh giá: số giây Unix Timestamp (ví dụ: "940636800" = 23/10/1999)
    StructField("review/time", StringType(), True),
    
    # Tiêu đề ngắn gọn của bài review (ví dụ: "Really Enjoyed It")
    StructField("review/summary", StringType(), True),
    
    # Nội dung văn bản chi tiết của bài review (có thể dài hàng nghìn ký tự)
    StructField("review/text", StringType(), True)
])

# --------------------------------------------------------------------------------------
# 3. CẤU HÌNH TÙY CHỌN ĐỌC CSV AN TOÀN TRONG SPARK
# --------------------------------------------------------------------------------------
CSV_READ_OPTIONS = {
    # File có dòng tiêu đề (header)
    "header": "true",
    
    # Dấu bao bọc chuỗi (quote character là dấu nháy kép ")
    "quote": "\"",
    
    # Ký tự escape (trong CSV chuẩn, dấu ngoặc kép được escape bằng "")
    "escape": "\"",
    
    # Cho phép đọc các ô có ký tự xuống dòng bên trong dấu nháy kép
    "multiLine": "true",
    
    # Bỏ qua khoảng trắng ở đầu/cuối ô dữ liệu
    "ignoreLeadingWhiteSpace": "true",
    "ignoreTrailingWhiteSpace": "true",
    
    # Chế độ xử lý dòng lỗi: PERMISSIVE sẽ gán null cho cột lỗi thay vì làm sập job
    "mode": "PERMISSIVE"
}
