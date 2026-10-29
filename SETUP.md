# SETUP — BDT301 Big Data Assignment

Hướng dẫn cài đặt và chạy toàn bộ pipeline từ một máy sạch. Xem [`README.md`](README.md) để biết tổng quan dự án.

## 1. Yêu cầu môi trường

| Thành phần | Ghi chú |
|---|---|
| Docker Desktop (Compose v2) | Cấp tối thiểu **8 GB RAM** cho Docker (máy 16 GB dùng mặc định là đủ) |
| Git kèm **Git Bash** | `run_pipeline.sh` phải chạy bằng Git Bash, không chạy bằng PowerShell |
| Python 3.10+ | Chạy script tải dữ liệu và ingestion trên máy host |
| Ổ đĩa trống ~10 GB | Dataset ~3 GB, bản sao trên MinIO, Parquet, Docker image |
| Internet | GitHub, Docker Hub, Maven Central, Kaggle |

Kiểm tra:

```powershell
docker info
docker compose version
git --version
python --version
```

Nếu `docker info` báo lỗi kết nối, mở Docker Desktop và chờ Engine khởi động.

## 2. Lấy source code

```powershell
git clone <repository-url>
cd <ten-thu-muc-repository>
git checkout dev
```

## 3. Tạo file `.env`

```powershell
copy .env.example .env
```

Mở `.env` và sửa:

| Biến | Giá trị |
|---|---|
| `MINIO_ROOT_PASSWORD`, `POSTGRES_PASSWORD` | Mật khẩu mạnh (MinIO yêu cầu ≥ 8 ký tự) |
| `KAGGLE_API_TOKEN` | Lấy tại kaggle.com → Settings → API → *Create New Token* |

Các biến còn lại giữ nguyên. Các biến `SPARK_MASTER_URL`, `MINIO_ENDPOINT`, `POSTGRES_HOST`, `RAW_*_PATH`... trong `.env` là giá trị cho script chạy **trên máy host**; bên trong container Spark, `docker-compose.yml` ghi đè bằng giá trị nội bộ (`spark://spark-master:7077`, `http://minio:9000`, `postgres`, `s3a://raw-data/raw/...`).

Không commit `.env` (đã có trong `.gitignore`).

## 4. Cài thư viện Python

```powershell
python -m pip install -r ingestion/requirements.txt
python -c "import boto3, dotenv, kagglehub; print('Dependencies OK')"
```

## 5. Chạy pipeline

Mở **Git Bash** tại thư mục gốc project:

```bash
./run_pipeline.sh
```

Script chạy lần lượt 7 bước, dừng ngay khi một bước lỗi và ghi log vào `logs/pipeline_<thời gian>.log`:

| Bước | Việc thực hiện | Thời gian (lần đầu) |
|---|---|---|
| `infra` | Build image MinIO (từ source) và Spark (kèm jar S3A + JDBC) nếu chưa có, khởi động cụm, chờ MinIO/Postgres healthy và đủ 2 Spark worker | 5–15 phút |
| `download` | Tải dataset từ Kaggle về `data/raw/` (bỏ qua nếu đã có) | Tùy mạng |
| `ingest` | Nạp 2 file CSV lên MinIO, kiểm tra SHA-256 và số dòng | ~2–3 phút |
| `clean` | Spark Job 1: làm sạch, ghi Parquet vào MinIO `processed-data/processed/`, bản ghi lỗi vào `processed-data/bad_records/` | ~6 phút |
| `review_analytics` | Spark Job 2b: phân tích review, ghi vào PostgreSQL | ~2 phút |
| `join_analytics` | Spark Job 2a: join sách với review, ghi Q1/Q2/Q6 vào PostgreSQL | ~3 phút |
| `reconcile` | Đối soát số dòng `raw = clean + bad + dedup` | ~1 phút |

Các tùy chọn:

```bash
./run_pipeline.sh --list            # liệt kê tên các bước
./run_pipeline.sh --from clean      # chạy tiếp từ một bước (sau khi sửa lỗi)
./run_pipeline.sh --only ingest     # chạy riêng một bước
```

Nhấn Ctrl+C sẽ dừng cả Spark driver trong container, không để lại job treo trên cụm.

### Chạy từng phần thủ công (khi cần debug)

```powershell
docker compose up -d --build                         # chỉ dựng cụm
python .\ingestion\download_kaggle_dataset.py        # chỉ tải dữ liệu (--force để tải lại)
python .\ingestion\ingest.py                         # chỉ nạp lên MinIO
```

## 6. Kiểm tra kết quả

| Dịch vụ | URL | Cần thấy |
|---|---|---|
| Spark Master UI | http://localhost:8080 | 2 worker *ALIVE*, các application ở mục *Completed* |
| Spark Job UI | http://localhost:4040 | Chỉ có khi một job đang chạy (DAG, Stage, Task) |
| MinIO Console | http://localhost:9001 | Bucket `raw-data` có `raw/books/books_data.csv`, `raw/reviews/Books_rating.csv`; bucket `processed-data` có `processed/books_clean/`, `processed/reviews_clean/` (chia theo `review_year`) và `bad_records/` |
| Metabase | http://localhost:3000 | Dashboard (xem mục 7) |

Đăng nhập MinIO bằng `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` trong `.env`.

Log và minh chứng:

| File | Nội dung |
|---|---|
| `logs/pipeline_*.log` | Log toàn bộ mỗi lần chạy pipeline, thời gian từng bước |
| `ingestion/logs/ingestion.jsonl` | Mỗi lần nạp: dung lượng, số dòng, SHA-256, thời gian, trạng thái `UPLOADED` / `SKIPPED` / `FAILED` |
| `ingestion/logs/reconciliation.json` | Kết quả đối soát số dòng giữa các tầng |
| `spark/event-logs/` | Spark event log (bằng chứng thực thi) |

Kiểm tra bảng trong PostgreSQL:

```powershell
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt"'
```

## 7. Kết nối Metabase lần đầu

1. Mở http://localhost:3000 và tạo tài khoản admin.
2. Thêm database PostgreSQL:
   - Host: `postgres` (tên service trong Docker network, **không** dùng `localhost`)
   - Port: `5432`
   - Database / Username / Password: `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` trong `.env`

Cấu hình Metabase được lưu trong volume `metabase_data`, không mất khi tạo lại container.

## 8. Những lần chạy sau

Mở Docker Desktop rồi chạy lại `./run_pipeline.sh`. Image đã có nên không build lại, CSV đã có nên bỏ qua bước tải, dữ liệu trên MinIO không đổi nên `ingest` báo `SKIPPED`.

Dừng và khởi động lại cụm mà vẫn giữ dữ liệu:

```powershell
docker compose stop
docker compose start
```

## 9. Thiết kế ingestion

`ingestion/ingest.py`:

- Nạp `data/raw/books_data.csv` → `raw-data/raw/books/books_data.csv` và `data/raw/Books_rating.csv` → `raw-data/raw/reviews/Books_rating.csv`; tự tạo bucket nếu chưa có.
- **Chạy lại không tạo bản trùng:** luôn ghi đè cùng object key; nếu object đã có cùng SHA-256 và dung lượng thì bỏ qua (`SKIPPED`).
- **Upload multipart** (phần 64 MB): nếu bị ngắt giữa chừng, object chưa hoàn tất không xuất hiện trong bucket; chạy lại sẽ nạp lại từ đầu.
- **Kiểm tra toàn vẹn:** so dung lượng sau upload, đọc lại object từ MinIO để tính lại SHA-256; số dòng đếm bằng module `csv` (đúng cả khi ô review chứa ký tự xuống dòng). Checksum và số dòng được lưu vào metadata của object.
- Trả exit code 1 nếu có file lỗi.

Tùy chọn: `--force` (nạp lại dù không đổi), `--skip-verify` (bỏ bước đọc lại để kiểm tra).

## 10. Xử lý lỗi thường gặp

### `401 UNAUTHORIZED` từ `quay.io`

Image MinIO được build tại máy, không pull từ registry. Không chạy `docker compose pull`; dùng `docker compose up -d --build`. Nếu cần cập nhật các image khác: `docker compose pull --ignore-buildable`.

### Build MinIO bị kill hoặc hết bộ nhớ

Đóng ứng dụng không cần thiết, tăng RAM cho Docker Desktop rồi build lại: `docker compose build minio`.

### Lỡ xóa image

Dữ liệu nằm trong volume nên không mất. Build lại: `docker compose build`, sau đó `docker compose up -d`.

### `$'\r': command not found` khi chạy `run_pipeline.sh`

File bị chuyển sang CRLF. Repository có `.gitattributes` giữ `*.sh` ở LF; nếu vẫn lỗi:

```bash
git rm --cached run_pipeline.sh && git checkout -- run_pipeline.sh
```

### Spark job treo với "Initial job has not accepted any resources"

Job xin nhiều bộ nhớ hơn worker có. Kiểm tra `SPARK_EXECUTOR_MEMORY` (mặc định `1536m`) nhỏ hơn `SPARK_WORKER_MEMORY` (mặc định `2G`) và job không đặt cứng `spark.executor.memory` trong code.

### Worker không xuất hiện trên Spark Master UI

```powershell
docker compose logs --tail 100 spark-master
docker compose logs --tail 100 spark-worker-1
```

### Không kết nối được MinIO

```powershell
docker compose ps minio
docker compose logs --tail 100 minio
```

Script chạy trên máy host dùng `http://localhost:9000`; Spark trong container dùng `http://minio:9000`.

### `ModuleNotFoundError: No module named 'boto3'`

Cài thư viện vào đúng môi trường Python đang dùng: `python -m pip install -r ingestion/requirements.txt`.

### Lỗi cổng đã được sử dụng

Đổi cổng host trong `docker-compose.yml` và cập nhật URL tương ứng.

### Sửa `postgres/init.sql` nhưng bảng không thay đổi

PostgreSQL chỉ chạy `init.sql` khi volume còn trống. Chạy SQL thủ công qua `psql`, hoặc xóa volume (mất dữ liệu Postgres): `docker compose down` rồi `docker volume rm <project>_postgres_data`.

## 11. Dừng hoặc xóa môi trường

```powershell
docker compose down      # dừng và xóa container, giữ dữ liệu
docker compose down -v   # XÓA cả dữ liệu MinIO, PostgreSQL, Metabase
```

**Cảnh báo:** chỉ dùng `down -v` khi chắc chắn muốn xóa dữ liệu.

## 12. Checklist Infrastructure & Ingestion

- [x] Build MinIO từ source thành công.
- [x] Khởi chạy các service bằng Docker Compose (ghim phiên bản, giới hạn bộ nhớ).
- [x] Spark Master và 2 Worker hoạt động.
- [x] MinIO và PostgreSQL healthy.
- [x] Nạp đủ 2 file CSV (`books_data.csv`, `Books_rating.csv`) vào MinIO.
- [x] Ghi nhận dung lượng, thời gian, số dòng và checksum (`ingestion.jsonl`).
- [x] Xác minh toàn vẹn dữ liệu (SHA-256 đọc lại từ MinIO, số dòng).
- [x] Chạy lại ingestion không tạo bản trùng (`SKIPPED`).
- [x] Spark đọc dữ liệu thô từ MinIO qua S3A.
- [x] `run_pipeline.sh` chạy trọn pipeline bằng một lệnh, dừng khi lỗi, có bước đối soát.
- [ ] Lưu ảnh chụp màn hình (MinIO Console, Spark UI, log) làm minh chứng.
