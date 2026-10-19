# BÁO CÁO CHẤT LƯỢNG DỮ LIỆU & TỐI ƯU HÓA SPARK (DATA QUALITY REPORT)

> **Người thực hiện:** Thành viên 2 (Tiến) — Spark Core ETL, Data Quality & Review Analytics Lead
> **Dự án:** BDT301 — Big Data Analytics Pipeline
> **Tập dữ liệu:** Amazon Books Reviews & Google Books Metadata
> **Nhánh Git:** `member2_Tien`
> **Ghi chú:** Các số liệu dưới đây đã được đối chiếu với lần chạy pipeline đầy đủ
> ngày 19/10/2026 (`logs/pipeline_20261019_155339.log`,
> `ingestion/logs/reconciliation.json`).

---

## 1. MÔ TẢ TẬP DỮ LIỆU (DATASET DESCRIPTION)

### 1.1. Nguồn gốc & Quy mô

Dự án sử dụng tập dữ liệu đánh giá sách trên Amazon (Kaggle:
`mohamedbakhet/amazon-books-reviews`), gồm hai bảng:

| Tập dữ liệu | Định dạng thô | Dung lượng | Số dòng | Mô tả |
|---|:---:|:---:|:---:|---|
| `books_data.csv` | CSV | 181.348.853 bytes (173 MiB) | 212.404 | Metadata sách từ Google Books: tác giả, NXB, thể loại, ngày xuất bản, mô tả, ảnh bìa. |
| `Books_rating.csv` | CSV | 2.859.504.349 bytes (2,66 GiB) | 3.000.000 | Review của người dùng: điểm, thời gian, độ hữu ích, nội dung review. |
| **Tổng** | CSV | **3.040.853.202 bytes (~3,04 GB)** | **3.212.404** | Vượt yêu cầu tối thiểu ≥ 1 GB. |

Sau khi làm sạch, dữ liệu Parquet còn **1,6 GB** (books 78 MB + reviews 1,5 GB),
tiết kiệm khoảng **47%** so với CSV gốc.

---

## 2. KẾT QUẢ KHẢO SÁT CHẤT LƯỢNG DỮ LIỆU

Nguồn: `spark/jobs/eda_survey.py` và thống kê lại trên dữ liệu thô ngày 19/10/2026.

### 2.1. Tỷ lệ giá trị thiếu

**`Books_rating.csv` (3.000.000 dòng):**

| Cột | Tỷ lệ thiếu | Ghi chú |
|---|:---:|---|
| `Price` | **83,96%** | Không đủ tin cậy để phân tích giá độc lập → Q3 dùng thêm dải độ dài review. |
| `User_id` | **18,73%** | Q6 chỉ tính trên 81,27% review có User_id. |
| `profileName` | **18,73%** | Cùng nhóm review thiếu User_id. |
| `review/helpfulness` | 0% | 100% đúng định dạng `x/y`; **29,52%** là `0/0` → phải tránh chia cho 0. |

**`books_data.csv` (212.404 dòng):**

| Cột | Tỷ lệ thiếu |
|---|:---:|
| `ratingsCount` | **76,58%** |
| `categories` | **19,40%** |
| `authors` | **14,79%** |
| `publishedDate` | **11,91%** |

### 2.2. Phân phối điểm số (`review/score`)

| Điểm | Tỷ lệ |
|:---:|:---:|
| 5 | **60,24%** |
| 4 | 19,52% |
| 3 | 8,48% |
| 2 | 5,04% |
| 1 | 6,72% |

Phân phối lệch dương, đặc trưng của review sách Amazon.

### 2.3. Trùng lặp

- `Title` trong `books_data.csv`: **0% trùng chính xác**.
- Sau khi chuẩn hoá `title_norm`, **5.338 dòng bị gộp** (2,51%) do khác hoa/thường,
  dấu câu hoặc tiêu đề gần giống (ví dụ nhiều biến thể của "The Hobbit").
- Reviews bị loại do trùng `(User_id, title_norm, review_time)`: **568.890 dòng**
  (18,96% của 3.000.000).

### 2.4. Tỷ lệ khớp nối giữa hai bảng (Join Match Rate)

Đo thực tế trên dữ liệu sạch:

| Cách khớp | Theo số review | Theo title distinct |
|---|:---:|:---:|
| `Title` nguyên bản, so trên dữ liệu thô (trước dedup sách) | **99,99%** | **98,27%** |
| `Title` nguyên bản, so với `books_clean` (sau dedup) | **97,47%** | — |
| Sau chuẩn hoá `title_norm` | **100,00%** | **100,00%** |

Việc Job 1 khử trùng sách theo `title_norm` làm mất một số biến thể tiêu đề, nên
khớp exact trên `books_clean` giảm còn 97,47%; chuẩn hoá `title_norm` đưa tỷ lệ
khớp trở lại 100%.

Lý do tỷ lệ cao: `books_data.csv` của bộ Kaggle này là metadata được thu thập cho
đúng tập sách có review, không phải catalogue độc lập rộng hơn. Vì vậy **không
được khái quát** tỷ lệ 100% cho bộ dữ liệu khác.

### 2.5. Bất thường thời gian

- 20 review có `review_year = 1969` (timestamp `1969-12-31 23:59:59`), không hợp lệ
  với dữ liệu Amazon (thực tế 1995–2013).
- 20 dòng này hiện vẫn nằm trong `reviews_clean` vì Job 1 chỉ loại timestamp null.
  Cần lọc thêm `review_year >= 1995` nếu muốn loại bỏ hoàn toàn.

---

## 3. BẢNG TỔNG HỢP CHẤT LƯỢNG DỮ LIỆU (`dq_report`)

Kết quả Job 1 + đối soát (`ingestion/logs/reconciliation.json`):

| Chỉ số | Reviews | Books |
|---|:---:|:---:|
| Tổng dòng thô | 3.000.000 | 212.404 |
| Dòng sạch (Clean Parquet) | **2.430.902** | **207.065** |
| Dòng lỗi (Bad Records) | **208** | **1** |
| Dòng bị khử trùng (Deduplicated) | **568.890** | **5.338** |
| Tổng giải trình (Clean + Bad + Dedup) | 3.000.000 | 212.404 |
| Trạng thái đối soát | **PASS** | **PASS** |

Lý do loại:

- Books: 1 dòng `MISSING_OR_EMPTY_TITLE`.
- Reviews: 208 dòng `MISSING_OR_EMPTY_TITLE`.

Công thức đối soát:

```text
raw = clean + bad + deduplicated
```

Lưu ý: bảng PostgreSQL `dq_report` hiện chứa **6 metric join** do Job 2a ghi
(`join_reviews_total`, `join_reviews_matched`, `join_reviews_match_pct`,
`join_books_total`, `join_books_matched`, `join_books_match_pct`). Các số liệu
row-count ở bảng trên nằm trong `reconciliation.json` và báo cáo này; nếu muốn
hiển thị trên Metabase thì cần bổ sung bước ghi các metric đó vào `dq_report`.

---

## 4. CHIẾN LƯỢC TỐI ƯU HÓA TRÊN APACHE SPARK

### 4.1. Khai báo Explicit Schema thay vì `inferSchema`

- `inferSchema=True` phải quét toàn bộ 3 GB CSV thêm một lần để đoán kiểu, làm
  tăng I/O và thời gian đọc.
- `spark/jobs/schemas.py` khai báo `StructType` tường minh, đọc toàn bộ cột dạng
  chuỗi rồi mới ép kiểu sau, tránh crash khi gặp ô dị thường.
- Số liệu thời gian đọc chi tiết không tách riêng trong log; tổng thời gian Job 1
  trên dữ liệu đầy đủ là **777 giây**.

### 4.2. Parquet + Snappy thay vì CSV

- CSV gốc: **3,04 GB** → Parquet: **1,6 GB**, tiết kiệm khoảng **47%**.
- Parquet cho phép column pruning: Job 2a/2b chỉ đọc các cột cần thiết
  (`title_norm`, `score`, `helpful_votes`, `review_year`, `user_id`) thay vì
  parse toàn bộ `review/text`.

### 4.3. Phân vùng theo năm (`partitionBy("review_year")`)

- Dữ liệu sạch có **20 phân vùng**: 1969 (20 dòng bất thường) và 1995–2013.
- Kích thước mỗi phân vùng từ **131 KB đến 133 MB**; phần lớn 16–133 MB.
- Partition pruning giúp các truy vấn theo năm chỉ đọc phân vùng liên quan.
- Không phân vùng theo ngày hoặc theo sách để tránh small-files problem.

---

## 5. PHÂN TÍCH ĐỘ TIN CẬY HỆ THỐNG

### 5.1. Dữ liệu bẩn và ô chứa ký tự xuống dòng (Multiline CSV)

- `review/text` có thể chứa `\n` và dấu ngoặc kép lồng nhau.
- Giải pháp: `multiLine=true`, `quote='"'`, `escape='"'`, `mode=PERMISSIVE`.
- Không tách file CSV thủ công; Spark đọc trực tiếp để tránh làm rách bản ghi.

### 5.2. Toàn vẹn và không thất thoát dữ liệu

- Dòng lỗi được ghi vào `data/bad_records/` kèm `reject_reason`.
- `reconcile_counts.py` kiểm toán `raw = clean + bad + dedup`; cả hai bảng đều
  **PASS**.
- Dedup review theo `(User_id, title_norm, review_time)` loại 568.890 dòng; cần
  ghi rõ con số này khi báo cáo để không gây cảm giác mất dữ liệu vô căn cứ.

### 5.3. Rủi ro còn lại

- 18,73% review thiếu `User_id` → Q6 chỉ đại diện cho 81,27% review.
- 20 review năm 1969 chưa được lọc.
- Chuẩn hoá `title_norm` gộp 5.338 dòng sách; các ấn bản khác nhau có thể bị tính
  chung (ví dụ nhiều biến thể "The Hobbit" vẫn tách riêng, nhưng một số biến thể
  khác bị gộp).

---

## 6. HƯỚNG DẪN THỰC THI

```bash
# 1. Khảo sát chất lượng dữ liệu (EDA)
python spark/jobs/eda_survey.py

# 2. Xuất dữ liệu mẫu Parquet 10k dòng cho TV3 và TV4
python spark/jobs/export_sample.py

# 3. Job 1: làm sạch dữ liệu đầy đủ, ghi Parquet phân vùng
python spark/jobs/clean_data_job.py

# 4. Job 2b: phân tích review (Q3, Q4 window function, Q5)
python spark/jobs/review_analytics_job.py

# 5. Đối soát số dòng toàn trình
python spark/jobs/reconcile_counts.py
```

Trong Docker, dùng `./run_pipeline.sh` để chạy đúng thứ tự và ghi log.
