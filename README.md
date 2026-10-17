# BDT301 — Big Data Assignment

Dự án xây dựng pipeline Big Data sử dụng Docker Compose, MinIO, Apache Spark, PostgreSQL và Metabase.

## Tổng quan hệ thống

| Service | Image / nguồn build | Vai trò |
|---|---|---|
| Apache Spark | `apache/spark:3.5.6` | Xử lý dữ liệu phân tán |
| MinIO | Build tại máy từ source | Lưu trữ dữ liệu CSV thô (Raw Layer) |
| PostgreSQL | `postgres:16` | Lưu kết quả phân tích |
| Metabase | `metabase/metabase:latest` | Trực quan hóa dữ liệu |

Spark được cấu hình theo mô hình Master và hai Worker trong `docker-compose.yml`.

## Kiến trúc dữ liệu

```text
2 CSV source files (books_data.csv, Books_rating.csv)
        |
        v
Python Ingestion
        |
        v
MinIO — raw-data/raw/books/, raw-data/raw/reviews/
        |
        v
Apache Spark — ETL / Analytics
        |
        v
PostgreSQL
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
├── .env.example
├── README.md
├── SETUP.md
├── minio/
│   └── Dockerfile
├── data/
│   └── raw/
├── ingestion/
│   ├── requirements.txt
│   ├── ingest.py
│   └── logs/
├── spark/
│   ├── jobs/
│   └── event-logs/
├── postgres/
└── reports/
```

Cấu trúc thực tế có thể thay đổi theo code hiện tại của nhóm. Không commit file `.env`, mật khẩu hoặc dữ liệu CSV dung lượng lớn lên GitHub.

## Bắt đầu nhanh

1. Cài Docker Desktop/ Docker Engine với Compose v2 và Git.
2. Clone repository.
3. Tạo file `.env` dựa trên `.env.example` nếu repository có cung cấp; nếu không, xem hướng dẫn trong `SETUP.md`.
4. Tại thư mục chứa `docker-compose.yml`, chạy:

   ```powershell
   docker compose config
   docker compose up -d --build
   docker compose ps
   ```

**Lưu ý về MinIO:** MinIO được build tại máy từ source. Tránh chạy `docker compose pull` thông thường nếu lệnh đó cố pull image MinIO từ registry. Chi tiết cài đặt, ingestion, kiểm tra và xử lý lỗi nằm trong [`SETUP.md`](SETUP.md).

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

- **[SETUP.md](SETUP.md)** — yêu cầu môi trường, cấu hình `.env`, build và chạy Docker, thiết lập Python, ingestion, kiểm tra và xử lý lỗi.
