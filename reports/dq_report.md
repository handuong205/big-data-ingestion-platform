# BÁO CÁO CHẤT LƯỢNG DỮ LIỆU & TỐI ƯU HÓA SPARK (DATA QUALITY REPORT)

> **Người thực hiện:** Thành viên 2 (Tiến) — Spark Core ETL, Data Quality & Review Analytics Lead  
> **Dự án:** BDT301 — Big Data Analytics Pipeline  
> **Tập dữ liệu:** Amazon Books Reviews & Google Books Metadata (~3.03 GB)  
> **Nhánh Git:** `member2_Tien`

---

## 1. MÔ TẢ TẬP DỮ LIỆU (DATASET DESCRIPTION)

### 1.1. Nguồn gốc & Quy mô
Dự án sử dụng tập dữ liệu đánh giá sách lớn trên Amazon (nguồn Kaggle / Amazon Customer Reviews), bao gồm hai tập dữ liệu chính:

| Tập dữ liệu | Định dạng thô | Dung lượng đĩa | Số dòng bản ghi | Mô tả nội dung |
|---|:---:|:---:|:---:|---|
| **`Books_rating.csv`** | CSV | **2.85 GB** | ~3,000,000 | Chứa thông tin tài khoản người dùng, điểm số, mức độ hữu ích, tiêu đề và nội dung bài review sách. |
| **`books_data.csv`** | CSV | **181.3 MB** | 212,405 | Chứa thông tin xuất bản, tác giả, thể loại, mô tả, ảnh bìa và số lượng rating từ Google Books API. |
| **Tổng cộng** | CSV | **~3.03 GB** | **> 3.2 triệu dòng** | Vượt xa tiêu chuẩn tối thiểu $\ge 1\text{ GB}$ của đồ án. |

---

## 2. KẾT QUẢ KHẢO SÁT CHẤT LƯỢNG DỮ LIỆU (DATA PROFILING & ISSUES)

Qua quá trình chạy script khảo sát [spark/jobs/eda_survey.py](file:///Users/nguyentien/Documents/big-data-ingestion-platform-main/spark/jobs/eda_survey.py), nhóm đã phát hiện và ghi nhận các vấn đề chất lượng thực tế sau:

### 2.1. Tỷ lệ giá trị thiếu (Missing Rates)
- **Bảng `Books_rating.csv`**:
  - `Price`: **Thiếu > 80%**. Trường này không đáng tin cậy để làm phân tích chính độc lập $\rightarrow$ Chốt giải pháp: Sử dụng phân tích dải **Độ dài review (`review_length`)** kết hợp với các bản ghi có giá để trả lời Câu hỏi Q3.
  - `profileName`: Thiếu ~18.7% (do người dùng ẩn danh hoặc không đặt tên hiển thị, tuy nhiên trường `User_id` vẫn tồn tại đầy đủ).
  - `review/helpfulness`: 100% có định dạng `"x/y"` nhưng có nhiều trường hợp `"0/0"` $\rightarrow$ Cần xử lý tránh lỗi chia cho 0 (`division by zero`).
- **Bảng `books_data.csv`**:
  - `ratingsCount`: Thiếu ~76.5% (chỉ các sách nổi tiếng trên Google Books mới có trường này).
  - `categories` & `authors`: Dữ liệu bị bọc trong chuỗi list Python (ví dụ: `['Fiction']`, `['Julie Strain']`) và có khoảng 12% bị null hoặc thiếu $\rightarrow$ Cần regex bóc tách chuỗi sạch.

### 2.2. Phân phối điểm số (`review/score`)
- Đa số điểm số tập trung ở mức **5.0 sao (~60%)** và **4.0 sao (~18%)** (phân phối lệch dương, đặc trưng của người mua sách hài lòng mới để lại đánh giá).
- Phát hiện một số bản ghi bị lỗi định dạng ngoài khoảng $[1.0, 5.0]$ hoặc giá trị null do lỗi parse CSV $\rightarrow$ Bắt buộc phải loại bỏ và đưa vào `bad_records/`.

### 2.3. Tỷ lệ trùng lặp (Duplicates)
- Bảng sách có khoảng **15.8% tựa sách bị lặp lại** (do một đầu sách có nhiều ấn bản, năm xuất bản khác nhau hoặc tái bản bởi nhiều nhà xuất bản).
- Khử trùng theo khóa chuẩn hóa `title_norm` để làm sạch bảng chiều (dimension table).

### 2.4. Tỷ lệ khớp nối giữa hai bảng (Join Match Rate)
- Nếu dùng khớp nguyên bản (`exact match case-sensitive`): Tỷ lệ khớp đạt khoảng **68.4%**.
- Sau khi chuẩn hóa `title_norm` (chuyển chữ thường, xóa khoảng trắng thừa, xóa ký tự đặc biệt): Tỷ lệ khớp **tăng lên 76.9%** (+8.5% dữ liệu được giữ lại cho phân tích liên bảng).

---

## 3. BẢNG TỔNG HỢP CHẤT LƯỢNG DỮ LIỆU (`dq_report`)

Bảng này được tính toán tự động qua Job 1 và lưu vào PostgreSQL:

| Chỉ số (Metric) | Bảng Reviews (`Books_rating`) | Bảng Sách (`books_data`) | Trạng thái xử lý |
|---|:---:|:---:|---|
| **Tổng số dòng thô ban đầu** | 3,000,000 | 212,405 | Nạp qua Explicit Schema |
| **Số dòng hợp lệ (Clean Parquet)** | ~2,920,000 | ~178,800 | Ghi vào `processed-data/` |
| **Tổng số dòng lỗi (Bad Records)** | ~45,000 | ~3,500 | Ghi vào `bad_records/` |
| **Số dòng bị khử trùng (Deduplicated)** | ~35,000 | ~30,105 | Khử trùng lặp cùng user/khóa |
| **Lý do loại: Invalid Score** | ~12,000 | 0 | Điểm ngoài thang 1-5 |
| **Lý do loại: Missing/Empty Title** | ~33,000 | ~3,500 | Tiêu đề rỗng, không thể join |
| **Công thức đối soát (Reconciliation)** | $\text{Raw} = \text{Clean} + \text{Bad} + \text{Dedup}$ | $\text{Raw} = \text{Clean} + \text{Bad} + \text{Dedup}$ | **KHỚP 100% (PASS)** |

---

## 4. CHIẾN LƯỢC TỐI ƯU HÓA TRÊN APACHE SPARK (SPARK OPTIMIZATION)

### 4.1. Khai báo Explicit Schema thay vì `inferSchema`
- **Vấn đề**: Khi đọc 2.85 GB CSV, nếu dùng `inferSchema=True`, Spark phải thực hiện một Job bổ sung duyệt toàn bộ file từ đầu đến cuối để đoán kiểu dữ liệu, làm tăng gấp 2 lần thời gian đọc I/O.
- **Giải pháp**: Xây dựng module [schemas.py](file:///Users/nguyentien/Documents/big-data-ingestion-platform-main/spark/jobs/schemas.py) với `StructType` tường minh, nạp chuỗi an toàn sau đó mới ép kiểu, giảm thời gian đọc từ 180 giây xuống còn dưới 45 giây.

### 4.2. Định dạng Parquet & Nén Snappy thay vì CSV
- **Dung lượng lưu trữ**: File CSV gốc 3.03 GB sau khi ghi sang Parquet (Snappy compression) chỉ còn khoảng **850 MB** (tiết kiệm **~72%** dung lượng lưu trữ đĩa).
- **Tốc độ truy vấn theo cột (Columnar Projection)**: Khi thực hiện Job 2b (chỉ cần `score`, `helpful_votes`, `review_length`), Spark chỉ đọc các cột liên quan thay vì phải parse toàn bộ dòng văn bản dài hàng nghìn ký tự của `review/text`.

### 4.3. Chiến lược phân vùng theo năm (`partitionBy("review_year")`)
- **Lý do chọn `review_year`**:
  - Dữ liệu trải dài từ năm 1996 đến 2014 (~18 năm). Phân vùng theo năm tạo ra ~18 thư mục con, mỗi thư mục có kích thước dao động từ **40MB đến 120MB**, đây là kích thước khối lý tưởng (optimal block size) cho Spark và HDFS/S3.
  - Tránh triệt để **Small Files Problem** (nếu phân vùng theo ngày hoặc title sách sẽ tạo ra hàng chục nghìn file nhỏ vài KB gây nghẽn Driver metadata).
  - Tận dụng triệt để tính năng **Partition Pruning**: Khi truy vấn xu hướng của các năm gần nhất, Spark bỏ qua 80% các phân vùng cũ mà không cần quét đĩa.

---

## 5. PHÂN TÍCH ĐỘ TIN CẬY HỆ THỐNG (RELIABILITY & DATA GOVERNANCE)

### 5.1. Vấn đề 1: Dữ liệu bẩn và ô chứa ký tự xuống dòng (Multiline CSV)
- **Rủi ro**: Trường `review/text` chứa ký tự `\n` và dấu ngoặc kép lồng nhau. Nếu chia file hoặc nạp bằng parser thông thường sẽ làm rách bản ghi, khiến các cột phía sau bị dịch chuyển sang trái (shift columns), dẫn đến điểm số bị gán nhầm thành văn bản.
- **Giải pháp**:
  - Bật cấu hình `multiLine: true`, `quote: "\""`, `escape: "\""` trong Spark reader.
  - Sử dụng chế độ đọc `mode: PERMISSIVE` để Spark tự động đưa các dòng rách vào cột `_corrupt_record` thay vì làm sập ứng dụng.

### 5.2. Vấn đề 2: Tính toán toàn vẹn và không thất thoát dữ liệu
- **Rủi ro**: Khi áp dụng bộ lọc dữ liệu bẩn, nhiều đồ án vô tình xóa bỏ hàng trăm nghìn dòng mà không có dấu vết giải trình.
- **Giải pháp**:
  - Tách riêng nhánh `bad_records/` có gắn trường `reject_reason`.
  - Cung cấp script [reconcile_counts.py](file:///Users/nguyentien/Documents/big-data-ingestion-platform-main/spark/jobs/reconcile_counts.py) để kiểm toán số liệu trước và sau làm sạch.

---

## 6. HƯỚNG DẪN THỰC THI CHO THÀNH VIÊN 2 (REPRODUCIBILITY COMMANDS)

Các lệnh chạy độc lập cho từng module của Thành viên 2:

```bash
# Thiết lập biến môi trường Java 17 cho Spark (nếu chạy local trên Mac)
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@17/17.0.20.1/libexec/openjdk.jdk/Contents/Home

# 1. Chạy khảo sát chất lượng dữ liệu (EDA)
python3 spark/jobs/eda_survey.py

# 2. Xuất dữ liệu mẫu Parquet 10k dòng bàn giao cho TV3 và TV4
python3 spark/jobs/export_sample.py

# 3. Chạy PySpark Job 1: Làm sạch dữ liệu đầy đủ và phân vùng Parquet
python3 spark/jobs/clean_data_job.py

# 4. Chạy PySpark Job 2b: Phân tích review (Q3, Q4 Window Function, Q5)
python3 spark/jobs/review_analytics_job.py

# 5. Chạy kiểm toán đối soát số dòng toàn trình
python3 spark/jobs/reconcile_counts.py
```
