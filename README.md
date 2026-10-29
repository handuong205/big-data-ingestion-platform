# BDT301 — Big Data Assignment

Dự án xây dựng pipeline Big Data sử dụng Docker Compose, MinIO, Apache Spark, PostgreSQL và Metabase.

## Tổng quan hệ thống

| Service | Image / nguồn build | Vai trò |
|---|---|---|
| Apache Spark | `apache/spark:3.5.6-python3` + jar S3A/JDBC (`spark/Dockerfile`) | Xử lý dữ liệu phân tán |
| MinIO | Build tại máy từ source (`minio/Dockerfile`) | Lưu trữ dữ liệu CSV thô (Raw Layer) |
| PostgreSQL | `postgres:16.15` | Lưu kết quả phân tích (Serving Layer) |
| Metabase | `metabase/metabase:v0.63.20.2` | Trực quan hóa dữ liệu |

Spark được cấu hình theo mô hình Master và hai Worker trong `docker-compose.yml`.

## Kiến trúc dữ liệu

```text
2 CSV source files (books_data.csv, Books_rating.csv)
        |
        v
Python Ingestion
        |
        v
MinIO Raw Layer — raw-data/raw/books/, raw-data/raw/reviews/
        |
        v
Apache Spark Job 1 — làm sạch
        |
        v
MinIO Processed Layer — processed-data/processed/ (Parquet),
                        processed-data/bad_records/
        |
        v
Apache Spark Job 2a, 2b — Analytics
        |
        v
PostgreSQL (Serving Layer)
        |
        v
Metabase Dashboard
```

## Phạm vi Data Infrastructure & Ingestion

- Khởi chạy và quản lý các service bằng Docker Compose.
- Build MinIO từ source tại máy.
- Thu nạp các file CSV vào MinIO Raw Layer.
- Ghi nhận thời gian, dung lượng và checksum trong quá trình ingestion.
- Chuẩn bị hạ tầng để tích hợp Spark ETL, analytics và dashboard.

## Cấu trúc repository tham khảo

```text
Assignment/
├── docker-compose.yml
├── run_pipeline.sh              # chạy toàn bộ pipeline bằng một lệnh
├── .env.example
├── README.md
├── SETUP.md
├── minio/
│   └── Dockerfile
├── data/                        # không commit
│   ├── raw/                     # CSV tải từ Kaggle
│   ├── processed/               # chỉ dùng khi chạy Spark local; pipeline ghi lên MinIO
│   ├── bad_records/             # như trên
│   └── reports/                 # join_match.json của Job 2a
├── ingestion/
│   ├── requirements.txt
│   ├── download_kaggle_dataset.py
│   ├── ingest.py                # nạp CSV vào MinIO
│   └── logs/
├── spark/
│   ├── Dockerfile
│   ├── jobs/
│   └── event-logs/
├── postgres/
│   └── init.sql
├── logs/                        # log của run_pipeline.sh
└── reports/
```

Không commit file `.env`, mật khẩu hoặc dữ liệu CSV lên GitHub.

## Bắt đầu nhanh

1. Cài Docker Desktop (Compose v2), Git (kèm Git Bash) và Python 3.10+.
2. Clone repository, chuyển sang branch `dev`.
3. `copy .env.example .env`, điền mật khẩu và `KAGGLE_API_TOKEN`.
4. `python -m pip install -r ingestion/requirements.txt`
5. Trong **Git Bash**, tại thư mục gốc:

   ```bash
   ./run_pipeline.sh
   ```

   Một lệnh chạy toàn bộ: dựng cụm Docker → tải dữ liệu Kaggle → nạp vào MinIO → làm sạch → phân tích → ghi PostgreSQL → đối soát số dòng.

**Lưu ý về MinIO:** MinIO được build tại máy từ source. Không chạy `docker compose pull` thông thường. Chi tiết từng bước, kiểm tra kết quả và xử lý lỗi nằm trong [`SETUP.md`](SETUP.md).

## Nguồn dữ liệu & Thư viện bên ngoài

### 1. Tập dữ liệu gốc (Kaggle Dataset)
- **Tên dataset:** [Amazon Books Reviews](https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews)
- **Dung lượng thô:** ~3 GB CSV (> 3,2 triệu bản ghi).
- **Các tệp tin thành phần:**
  - `books_data.csv` (212.404 dòng): Chứa siêu dữ liệu sách (`Title`, `authors`, `categories`, `publisher`, `publishedDate`,...).
  - `Books_rating.csv` (3.000.000 dòng): Chứa thông tin đánh giá độc giả (`Id`, `Title`, `Price`, `User_id`, `review/score`, `review/time`, `review/helpfulness`, `review/text`,...).
- **Cơ chế thu thập:** Tự động tải qua `kagglehub` trong `ingestion/download_kaggle_dataset.py` thông qua API Token được cấu hình trong `.env`.

### 2. Thư viện & Trình kết nối bên ngoài (External Dependencies & Drivers)
- **Apache Spark Containers:**
  - Base Image: `apache/spark:3.5.6-python3`
  - Hadoop-AWS Connector: `hadoop-aws-3.3.4.jar`
  - AWS SDK: `aws-java-sdk-bundle-1.12.262.jar` (tương thích giao thức S3A kết nối MinIO)
  - PostgreSQL JDBC Driver: `postgresql-42.6.0.jar`
- **Python Host & Ingestion:**
  - `boto3==1.34.84` (MinIO S3 Client API)
  - `kagglehub==0.2.7` (Tải dữ liệu tự động)
  - `python-dotenv==1.0.1` (Nạp biến môi trường)

---

## Bắt đầu nhanh & Thứ tự thực thi

1. Cài Docker Desktop (Compose v2), Git (kèm Git Bash) và Python 3.10+.
2. Clone repository, chuyển sang branch `dev` (hoặc nhánh tính năng):
   ```bash
   git clone https://github.com/handuong205/big-data-ingestion-platform.git
   cd big-data-ingestion-platform
   git checkout dev
   ```
3. Tạo file môi trường: `copy .env.example .env` (trên Windows) hoặc `cp .env.example .env`, điền mật khẩu và `KAGGLE_API_TOKEN`.
4. Cài thư viện Ingestion:
   ```bash
   python -m pip install -r ingestion/requirements.txt
   ```
5. Trong **Git Bash**, tại thư mục gốc chạy một lệnh duy nhất:
   ```bash
   ./run_pipeline.sh
   ```

Toàn bộ pipeline sẽ tự động thực thi tuần tự:
```
Dựng cụm Docker ──> Tải Kaggle CSV ──> Nạp MinIO Raw ──> Job 1 Làm sạch (Parquet) ──> Job 2a & 2b Phân tích ──> Ghi Serving Postgres ──> Đối soát số dòng
```

---

## Kết quả kỳ vọng sau khi chạy Pipeline (Expected Outputs)

Sau khi `run_pipeline.sh` thực thi thành công, hệ thống cam kết đạt các chỉ số đo đạc thực nghiệm:

| Chỉ số / Bảng Serving | Kết quả đo đạc thực tế | Ý nghĩa kiểm định |
|---|---|---|
| **Thời gian chạy toàn bộ** | ~1.475 giây (~24,6 phút) | Đo đạc trên máy tính cá nhân 16 GB RAM |
| **Bảng sách sạch (`books_clean`)** | **207.065 dòng** | Đã loại 1 dòng rỗng và khử 5.338 dòng trùng `title_norm` |
| **Bảng review sạch (`reviews_clean`)** | **2.430.902 dòng** | Đã loại 208 dòng lỗi và khử 568.890 dòng trùng lặp |
| **Bản ghi có `User_id`** | **2.017.997 dòng (83,01%)** | Phân bổ thành 4 tầng hoạt động trong `agg_reviewer_segment` |
| **Bản ghi khuyết `User_id`** | **412.905 dòng (16,99%)** | Được theo dõi riêng biệt, tránh bóp méo mô hình gợi ý |
| **Tỷ lệ khớp Join (`title_norm`)** | **100,00%** | Broadcast Join thành công giữa review sạch và sách sạch |
| **Đối soát số dòng (Reconciliation)** | **100,00% PASSED** | $\text{Raw} = \text{Clean} + \text{Bad Records} + \text{Trùng lặp}$ |

---

## Tái lập kết quả độc lập bằng `queries.sql` (Không phụ thuộc GUI)

Nhằm đảm bảo tính tái lập (Reproducibility) phục vụ hội đồng chấm đồ án, toàn bộ kết quả phân tích (Q1 đến Q6 và Heatmap chu kỳ) được lưu trữ độc lập trong file [`queries.sql`](queries.sql).

Thực hiện kiểm thử tái lập ngay trên terminal mà không cần cấu hình Metabase:

```bash
# Thực thi trực tiếp vào container PostgreSQL
docker compose exec -T postgres psql -U postgres -d amazon_books -f queries.sql
```

File tích hợp sẵn truy vấn kiểm tra toàn vẹn Checksum tự động:
- Trả về: **`PASSED: DATA MATCHES 100% PRODUCTION BENCHMARK (2,017,997 / 412,905)`**
- Thời gian thực thi toàn bộ các câu hỏi: **< 0,5 giây** nhờ hệ thống chỉ mục (Indexes) tối ưu.

---

## Các giao diện dịch vụ cục bộ

Các URL dưới đây hoạt động khi các container đang chạy:

| Dịch vụ | URL | Thông tin đăng nhập mặc định |
|---|---|---|
| MinIO Console | http://localhost:9001 | Cấu hình trong `.env` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`) |
| Spark Master UI | http://localhost:8080 | Xem DAG, Job, Stage và trạng thái 2 Worker |
| Metabase BI | http://localhost:3000 | Kết nối cơ sở dữ liệu Serving `amazon_books` |
| PostgreSQL | `localhost:5432` | User: `postgres`, Database: `amazon_books` |

---

## Tài liệu kỹ thuật dự án

- **[`FINAL_REPORT.md`](FINAL_REPORT.md)** — **Báo cáo tổng hợp đồ án nhóm 4 người (5–8 trang chuẩn học thuật)**: Kiến trúc, Data Quality, tối ưu PySpark, Phân tích nghiệp vụ sâu và Đối soát hệ thống.
- **[`docs_tv4_insight_and_problem.md`](docs_tv4_insight_and_problem.md)** — **Báo cáo chuyên sâu của TV4**: Đặt vấn đề nghiệp vụ, phân tích 4 yếu tố (Fact, Insight, Recommendation, Limitation) cho từng câu hỏi, và bảng đóng góp TV4.
- **[`queries.sql`](queries.sql)** — Toàn bộ truy vấn SQL Serving Layer chuẩn ANSI PostgreSQL 16 (Q1–Q6, Heatmap, thẻ KPI).
- **[`reports/tv3_analytics_serving.md`](reports/tv3_analytics_serving.md)** — Báo cáo tầng Serving Layer, cấu trúc DDL và độ trễ truy vấn (TV3).
- **[`reports/dq_report.md`](reports/dq_report.md)** — Báo cáo kiểm định chất lượng dữ liệu và đối soát dòng (TV2).
- **[`SETUP.md`](SETUP.md)** — Hướng dẫn chi tiết từng bước cài đặt, cấu hình `.env`, xử lý sự cố môi trường và chạy pipeline trên máy sạch.

