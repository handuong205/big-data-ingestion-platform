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

## Các giao diện cục bộ

Các URL dưới đây áp dụng khi cổng tương ứng được ánh xạ trong `docker-compose.yml`.

| Dịch vụ | URL |
|---|---|
| MinIO Console | http://localhost:9001 |
| MinIO API | http://localhost:9000 |
| Spark Master UI | http://localhost:8080 |
| Spark Worker 1 UI | http://localhost:8081 |
| Spark Worker 2 UI | http://localhost:8082 |
| Metabase | http://localhost:3000 |

## Tài liệu

- **[SETUP.md](SETUP.md)** — yêu cầu môi trường, cấu hình `.env`, chạy pipeline, kiểm tra kết quả, kết nối Metabase và xử lý lỗi.
