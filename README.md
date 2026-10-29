# BDT301 — Big Data Assignment

Dự án xây dựng pipeline Big Data phân tích review sách Amazon, sử dụng Docker Compose, MinIO, Apache Spark, PostgreSQL và Metabase.

## Tổng quan hệ thống

| Service | Image / nguồn build | Vai trò |
|---|---|---|
| Apache Spark | `apache/spark:3.5.6-python3` + jar S3A/JDBC (`spark/Dockerfile`) | Xử lý dữ liệu phân tán (1 Master + 2 Worker) |
| MinIO | Build tại máy từ source (`minio/Dockerfile`) | Object storage: Raw Layer (`raw-data`) và Processed Layer (`processed-data`) |
| PostgreSQL | `postgres:16.15` | Lưu bảng tổng hợp (Serving Layer) |
| Metabase | `metabase/metabase:v0.63.20.2` | Trực quan hóa dữ liệu |

Sơ đồ kiến trúc chi tiết: [`reports/system_architecture_diagram.png`](reports/system_architecture_diagram.png).

## Kiến trúc dữ liệu

```text
2 CSV source files (books_data.csv, Books_rating.csv)
        |
        v
Python Ingestion (ingest.py: checksum, đếm dòng, không nạp trùng)
        |
        v
MinIO Raw Layer — raw-data/raw/books/, raw-data/raw/reviews/
        |
        v
Apache Spark Job 1 — làm sạch, kiểm tra chất lượng
        |
        v
MinIO Processed Layer — processed-data/processed/ (Parquet, partition theo review_year)
                        processed-data/bad_records/
        |
        v
Apache Spark Job 2a (join sách × review), Job 2b (phân tích review)
        |
        v
PostgreSQL (Serving Layer, bảng tổng hợp + index)
        |
        v
Dashboard / queries.sql
```

## Cấu trúc repository

```text
Assignment/
├── docker-compose.yml
├── run_pipeline.sh              # chạy toàn bộ pipeline bằng một lệnh
├── queries.sql                  # truy vấn kết quả Q1–Q6 trên PostgreSQL
├── FINAL_REPORT.md              # báo cáo tổng hợp
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
│   ├── ingest.py                # nạp CSV vào MinIO, tạo bucket
│   └── logs/                    # ingestion.jsonl, reconciliation.json
├── spark/
│   ├── Dockerfile
│   ├── jobs/
│   │   ├── schemas.py           # schema tường minh cho 2 file CSV
│   │   ├── eda_survey.py        # khảo sát chất lượng dữ liệu
│   │   ├── export_sample.py     # xuất mẫu Parquet 10k dòng
│   │   ├── clean_data_job.py    # Job 1: làm sạch
│   │   ├── review_analytics_job.py  # Job 2b: Q3, Q4, Q5
│   │   ├── join_analytics_job.py    # Job 2a: Q1, Q2, Q6
│   │   ├── serving.py           # JDBC writer dùng chung
│   │   └── reconcile_counts.py  # đối soát số dòng
│   └── event-logs/              # Spark event log (bằng chứng thực thi)
├── postgres/
│   └── init.sql                 # DDL + index của Serving Layer
├── logs/                        # log của run_pipeline.sh
└── reports/                     # báo cáo thành viên, sơ đồ, ảnh minh chứng
```

Không commit file `.env`, mật khẩu hoặc dữ liệu CSV lên GitHub.

## Bắt đầu nhanh & thứ tự thực thi

1. Cài Docker Desktop (Compose v2), Git (kèm Git Bash) và Python 3.10+.
2. Clone repository và chuyển sang branch `dev`:
   ```bash
   git clone https://github.com/handuong205/big-data-ingestion-platform.git
   cd big-data-ingestion-platform
   git checkout dev
   ```
3. Tạo file môi trường: `copy .env.example .env` (Windows) hoặc `cp .env.example .env`, rồi điền mật khẩu và `KAGGLE_API_TOKEN`.
4. Cài thư viện cho các script chạy trên máy host:
   ```bash
   python -m pip install -r ingestion/requirements.txt
   ```
5. Trong **Git Bash**, tại thư mục gốc:
   ```bash
   ./run_pipeline.sh
   ```

Script chạy lần lượt 7 bước và dừng ngay khi một bước lỗi:

```text
infra → download → ingest → clean → review_analytics → join_analytics → reconcile
```

| Bước | Việc làm |
|---|---|
| `infra` | Dựng cụm Docker, tạo bucket `raw-data` và `processed-data`, áp dụng `postgres/init.sql` |
| `download` | Tải 2 file CSV từ Kaggle (bỏ qua nếu đã có trong `data/raw/`) |
| `ingest` | Nạp CSV lên MinIO Raw Layer, kiểm tra SHA-256 và số dòng |
| `clean` | Spark Job 1: ghi Parquet sạch và bad_records lên MinIO Processed Layer |
| `review_analytics` | Spark Job 2b: Q3, Q4, Q5 → PostgreSQL |
| `join_analytics` | Spark Job 2a: Q1, Q2, Q6 + tỷ lệ khớp join → PostgreSQL |
| `reconcile` | Đối soát `raw = clean + bad + dedup` cho cả 2 bảng |

Chạy lại từ một bước: `./run_pipeline.sh --from clean`; chạy một bước: `./run_pipeline.sh --only ingest`; xem danh sách bước: `./run_pipeline.sh --list`.

**Lưu ý về MinIO:** MinIO được build tại máy từ source, không chạy `docker compose pull`. Chi tiết từng bước, kiểm tra kết quả và xử lý lỗi nằm trong [`SETUP.md`](SETUP.md).

## Nguồn dữ liệu & thư viện bên ngoài

### 1. Tập dữ liệu gốc
- **Dataset:** [Amazon Books Reviews](https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews) (Kaggle, tác giả Mohamed Bakhet).
- **Dung lượng:** khoảng 3 GB CSV, tổng cộng 3.212.404 bản ghi.
  - `books_data.csv` (181 MB, 212.404 dòng): thông tin sách (`Title`, `authors`, `categories`, `publisher`, `publishedDate`, ...).
  - `Books_rating.csv` (2,86 GB, 3.000.000 dòng): review của độc giả (`Id`, `Title`, `Price`, `User_id`, `review/score`, `review/time`, `review/helpfulness`, `review/text`, ...).
- **Cách lấy dữ liệu:** tải tự động bằng `kagglehub` trong `ingestion/download_kaggle_dataset.py`, dùng API token trong `.env`.

### 2. Thư viện và driver
- **Spark containers** (`spark/Dockerfile`):
  - Base image: `apache/spark:3.5.6-python3` (Hadoop 3.3.4)
  - `hadoop-aws-3.3.4.jar` và `aws-java-sdk-bundle-1.12.262.jar`: kết nối MinIO qua S3A
  - `postgresql-42.7.4.jar`: PostgreSQL JDBC driver
- **Python trên máy host** (`ingestion/requirements.txt`, chưa ghim phiên bản):
  - `boto3`: S3 client để nạp dữ liệu và tạo bucket trên MinIO
  - `kagglehub[pandas-datasets]`: tải dataset
  - `python-dotenv`: nạp biến môi trường từ `.env`

## Kết quả kỳ vọng sau khi chạy pipeline

Số liệu đo trên dữ liệu đầy đủ:

| Chỉ số | Kết quả | Ghi chú |
|---|---|---|
| Thời gian chạy | ~25 phút cho lần chạy đầu (gồm tải và nạp dữ liệu); ~10 phút khi chạy lại từ `clean` | Phụ thuộc cấu hình máy |
| `books_clean` | 207.065 dòng | Loại 1 dòng lỗi, bỏ 5.338 dòng trùng `title_norm` |
| `reviews_clean` | 2.430.902 dòng | Loại 208 dòng lỗi, bỏ 568.890 dòng trùng |
| Review có `User_id` | 2.017.997 dòng (83,01%) | Dùng cho Q6 (`agg_reviewer_segment`) |
| Review thiếu `User_id` | 412.905 dòng (16,99%) | Không tính vào Q6 |
| Tỷ lệ khớp join (`title_norm`) | 100% review, 100% sách | Lưu trong `data/reports/join_match.json` và bảng `dq_report` |
| Đối soát số dòng | PASS cho cả 2 bảng | Lưu trong `ingestion/logs/reconciliation.json` |

Các bảng được tạo trong PostgreSQL:

| Bảng | Câu hỏi | Job |
|---|---|---|
| `agg_category_year` | Q1: điểm và số review theo thể loại × năm | 2a |
| `agg_author`, `agg_publisher` | Q2: top tác giả, nhà xuất bản (≥ 30 review) | 2a |
| `agg_price_band`, `agg_review_length_band` | Q3: giá và độ dài review so với điểm | 2b |
| `top_helpful_reviews` | Q4: top 10 review hữu ích nhất mỗi năm | 2b |
| `book_summary` | Q5: sách có nhiều review điểm thấp | 2b |
| `agg_reviewer_segment` | Q6: phân nhóm người đánh giá | 2a |
| `agg_activity_heatmap` | Heatmap: mật độ review theo Tháng × Thứ trong tuần (84 điểm dữ liệu) | 2b |
| `dq_report` | Chỉ số chất lượng join | 2a |

## Tái lập kết quả bằng `queries.sql`

[`queries.sql`](queries.sql) chứa các truy vấn trả lời Q1–Q6. Chạy trực tiếp trên PostgreSQL, không cần giao diện:

```bash
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < queries.sql
```

File được đẩy vào container qua stdin, nên không cần mount `queries.sql`. User và database lấy theo `POSTGRES_USER` và `POSTGRES_DB` trong `.env`.

> **Lưu ý:** Bảng heatmap `agg_activity_heatmap` đã được tích hợp hoàn chỉnh vào Spark Job 2b (`review_analytics_job.py`) và Serving Layer, cung cấp đầy đủ 84 điểm dữ liệu (12 tháng × 7 ngày trong tuần) phục vụ trực quan hóa Heatmap trên Metabase.

## Các giao diện dịch vụ cục bộ

Các URL dưới đây hoạt động khi container đang chạy:

| Dịch vụ | URL | Đăng nhập / ghi chú |
|---|---|---|
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` trong `.env` |
| MinIO API | http://localhost:9000 | Endpoint S3 |
| Spark Master UI | http://localhost:8080 | Trạng thái 2 Worker và các ứng dụng đã chạy |
| Spark Worker UI | http://localhost:8081, http://localhost:8082 | |
| Spark Application UI | http://localhost:4040 | Chỉ có khi một job đang chạy: DAG, Job, Stage |
| Metabase | http://localhost:3000 | Khi thêm database: host `postgres`, port `5432`, database/user/password theo `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` trong `.env` |
| PostgreSQL | `localhost:5432` | `POSTGRES_USER` / `POSTGRES_PASSWORD`, database `POSTGRES_DB` |

## Tài liệu dự án

- [`SETUP.md`](SETUP.md) — cài đặt, cấu hình `.env`, chạy pipeline, kiểm tra kết quả và xử lý lỗi.
- [`FINAL_REPORT.md`](FINAL_REPORT.md) — báo cáo tổng hợp: kiến trúc, chất lượng dữ liệu, xử lý Spark, kết quả phân tích và trade-off.
- [`docs_tv4_insight_and_problem.md`](docs_tv4_insight_and_problem.md) — đặt vấn đề nghiệp vụ và phân tích insight cho từng câu hỏi (TV4).
- [`reports/dq_report.md`](reports/dq_report.md) — báo cáo chất lượng dữ liệu và đối soát số dòng (TV2).
- [`reports/tv3_analytics_serving.md`](reports/tv3_analytics_serving.md) — Job 2a, thiết kế Serving Layer và độ trễ truy vấn (TV3).
- [`docs.md`](docs.md) — phân công công việc 4 thành viên.
