# SETUP — BDT301 Big Data Assignment

Tài liệu hướng dẫn cài đặt, khởi chạy và kiểm tra môi trường Big Data. Xem [`README.md`](README.md) để biết tổng quan dự án.

## 1. Yêu cầu môi trường

Cài đặt:
- Windows 10/11 hoặc hệ điều hành hỗ trợ Docker.
- Docker Desktop hoặc Docker Engine kèm Docker Compose v2.
- Git.
- Python hoặc Anaconda để chạy ingestion.
- Visual Studio Code (khuyến nghị).
- Kết nối Internet tới GitHub và Docker Hub.

Nên dành tối thiểu khoảng 4 GB RAM cho lần build MinIO đầu tiên; toàn bộ stack có thể cần nhiều RAM hơn. Cần đủ dung lượng ổ đĩa cho image, dữ liệu CSV, volume và dữ liệu xử lý trung gian.

Kiểm tra bằng PowerShell:

```powershell
docker --version
docker compose version
docker info
git --version
python --version
```

Nếu `docker info` báo lỗi kết nối, hãy mở Docker Desktop và chờ Docker Engine khởi động.

## 2. Lấy source code

### Chưa có project

```powershell
git clone <repository-url>
cd <ten-thu-muc-repository>
```

Thay `<repository-url>` bằng URL GitHub của nhóm.

### Đã có project

```powershell
cd D:\Assignment
git status
git pull
```

Thay `D:\Assignment` bằng đường dẫn project thực tế. Nếu đang làm trên branch riêng, hãy kiểm tra branch và lưu các thay đổi chưa commit trước khi pull.

## 3. Build MinIO tại máy

Repository sử dụng phiên bản MinIO source `RELEASE.2025-09-06T17-38-46Z`. Cần có Dockerfile tại `minio/Dockerfile`.

Service MinIO trong `docker-compose.yml` phải có cấu hình build local, tương tự:

```yaml
minio:
  image: quay.io/minio/minio:RELEASE.2025-09-06T17-38-46Z
  build: ./minio
  pull_policy: build
```

Đây là phần cấu hình minh họa. Giữ nguyên `command`, `ports`, `environment`, `volumes`, `networks` và `healthcheck` của service đang dùng. Dockerfile phải thực sự build được đúng phiên bản đã chọn.

**Không chạy `docker compose pull` thông thường** nếu lệnh đó cố tải image MinIO từ registry.

## 4. Tạo file `.env`

Tạo file `.env` ở thư mục gốc, cùng cấp với `docker-compose.yml`:

Repository có `.env.example`, có thể sao chép thành `.env` rồi điền giá trị thực tế.

## 5. Chuẩn bị thư mục

Chạy tại thư mục gốc trong PowerShell:

```powershell
New-Item -ItemType Directory -Force -Path `
  "data/raw", `
  "ingestion/logs", `
  "spark/jobs", `
  "spark/event-logs", `
  "postgres", `
  "reports"
```

Không ghi đè Dockerfile, script hoặc file cấu hình đã có trong repository nếu chưa kiểm tra nội dung.

## 6. Build và khởi chạy Docker Compose

Kiểm tra cấu hình:

```powershell
docker compose config
```

Build MinIO và khởi chạy các service:

```powershell
docker compose up -d --build
```

Lần build MinIO đầu tiên có thể mất 5–10 phút hoặc lâu hơn tùy mạng, CPU và ổ đĩa.

Kiểm tra trạng thái:

```powershell
docker compose ps
```

Xem log tổng thể:

```powershell
docker compose logs --tail 100
```

Xem log từng service nếu cần:

```powershell
docker compose logs --tail 100 minio
docker compose logs --tail 100 spark-master
docker compose logs --tail 100 spark-worker-1
docker compose logs --tail 100 spark-worker-2
docker compose logs --tail 100 postgres
docker compose logs --tail 100 metabase
```

Tên service phải khớp với `docker-compose.yml` của repository.

### Cập nhật các image khác

Để cập nhật các image có thể pull mà bỏ qua service build tại máy:

```powershell
docker compose pull --ignore-buildable
docker compose up -d --build
```

Tùy chọn `--ignore-buildable` yêu cầu phiên bản Docker Compose hỗ trợ. Nếu không được hỗ trợ, hãy kiểm tra `docker compose pull --help`.

## 7. Truy cập giao diện

Các URL áp dụng khi `docker-compose.yml` ánh xạ đúng các cổng sau:

| Dịch vụ | URL |
|---|---|
| MinIO Console | http://localhost:9001 |
| MinIO API | http://localhost:9000 |
| Spark Master UI | http://localhost:8080 |
| Spark Worker 1 UI | http://localhost:8081 |
| Spark Worker 2 UI | http://localhost:8082 |
| Metabase | http://localhost:3000 |

Đăng nhập MinIO bằng `MINIO_ROOT_USER` và `MINIO_ROOT_PASSWORD` trong `.env`. Trên Spark Master UI, xác nhận các Worker đã đăng ký thành công.

## 8. Cấu hình Python cho Ingestion

### Tạo hoặc kích hoạt môi trường

Nếu dùng Anaconda và chưa có môi trường riêng:

```powershell
conda create -n bdt301-ingestion python=3.12 -y
conda activate bdt301-ingestion
```

Nếu đã có môi trường Python phù hợp, có thể sử dụng môi trường đó.

### Cài thư viện

Đảm bảo `ingestion/requirements.txt` có:

```text
boto3
python-dotenv
kagglehub[pandas-datasets]
```

Cài đặt và kiểm tra:

```powershell
python -m pip install -r ingestion/requirements.txt
python --version
python -c "import boto3, dotenv, kagglehub; print('Dependencies OK')"
```

Nếu hiện `Dependencies OK`, các thư viện đã sẵn sàng.

## 9. Tải và nạp dữ liệu CSV

Dataset Amazon Books Reviews được tải bằng KaggleHub. Không dùng
`load_dataset(..., file_path="")`: tham số file rỗng không xác định được file cần
đọc và có thể gây lỗi khi dataset có nhiều file.

Trước lần tải đầu tiên, cấu hình Kaggle credentials theo
[Kaggle API documentation](https://www.kaggle.com/docs/api). Sau đó chạy tại thư
mục gốc:

```powershell
python .\ingestion\download_kaggle_dataset.py
```

Script tải dataset `mohamedbakhet/amazon-books-reviews` vào KaggleHub cache,
kiểm tra đủ hai file nguồn rồi sao chép về:

```text
data/raw/books_data.csv
data/raw/Books_rating.csv
```

Muốn tải lại bản mới từ Kaggle:

```powershell
python .\ingestion\download_kaggle_dataset.py --force
```

Nếu chỉ có sẵn file CSV từ nguồn khác, có thể đặt chúng trực tiếp vào
`data/raw/` với đúng hai tên trên.

Kiểm tra:

```text
data/raw/
```

```powershell
Get-ChildItem .\data\raw\*.csv
```

Tổng dung lượng dataset có thể rất lớn; hãy xác nhận dung lượng trống trước khi
tải và chạy Spark.

Đảm bảo MinIO đang hoạt động:

```powershell
docker compose ps minio
```

Chạy ingestion:

```powershell
python .\ingestion\ingest.py
```

Theo thiết kế dự kiến, script sẽ:
1. Tìm các file CSV trong `data/raw/`.
2. Tạo bucket `raw-data` nếu chưa có.
3. Tính SHA-256 cho từng file nguồn.
4. Upload file vào MinIO.
5. Ghi dung lượng, thời gian thực thi và trạng thái.
6. Kiểm tra kích thước và metadata sau upload.

Object dự kiến có đường dẫn `raw-data/raw/books/books_data.csv` và
`raw-data/raw/reviews/Books_rating.csv`.

### Kiểm tra kết quả

Mở MinIO Console tại http://localhost:9001 và xác nhận bucket `raw-data` cùng các file dưới prefix `raw/` đã xuất hiện.

Log dự kiến nằm ở:

```text
ingestion/logs/ingestion.jsonl
```

Xem log bằng PowerShell:

```powershell
Get-Content .\ingestion\logs\ingestion.jsonl
```

Nếu script hỗ trợ bỏ qua file không thay đổi, hãy chạy lại để kiểm tra hành vi đó.

**Lưu ý về checksum:** Nếu script chỉ kiểm tra SHA-256 trong metadata và dung lượng object, điều này chưa xác minh toàn bộ nội dung object sau upload. Muốn kiểm tra tính toàn vẹn đầy đủ, cần đọc lại object từ MinIO và tính SHA-256 để so sánh với file nguồn.

## 10. Quy trình làm việc hằng ngày

Khi bắt đầu:

```powershell
cd D:\Assignment
git pull
docker compose up -d
docker compose ps
```

Khi cần chạy ingestion:

```powershell
conda activate bdt301-ingestion
python .\ingestion\ingest.py
```

Dừng container nhưng giữ dữ liệu:

```powershell
docker compose stop
```

Khởi động lại container đã dừng:

```powershell
docker compose start
```

## 11. Xử lý lỗi thường gặp

### `401 UNAUTHORIZED` từ `quay.io`

Kiểm tra service MinIO có cấu hình `build` và Dockerfile có tồn tại đúng đường dẫn. Thử:

```powershell
docker compose up -d --build
```

Tránh `docker compose pull` thông thường nếu nó cố tải image MinIO từ registry.

### Build MinIO bị kill hoặc hết bộ nhớ

- Kiểm tra mức sử dụng RAM và ổ đĩa.
- Đóng ứng dụng không cần thiết.
- Điều chỉnh tài nguyên Docker Desktop.
- Thử build lại sau khi giải phóng tài nguyên.

Mức 4 GB chỉ là tham khảo tối thiểu, không đảm bảo mọi cấu hình build đều chạy được.

### Build lỗi khi tải source

Kiểm tra kết nối Internet, GitHub, proxy hoặc firewall; xem log để xác định bước thất bại.

### `ModuleNotFoundError: No module named 'boto3'`

Cài thư viện vào đúng môi trường Python đang chạy:

```powershell
python -m pip install boto3 python-dotenv
```

### Không kết nối được MinIO

```powershell
docker compose ps minio
docker compose logs --tail 100 minio
```

Khi chạy script Python trực tiếp trên Windows, endpoint thường là `http://localhost:9000`. Từ container cùng Docker network, endpoint thường là `http://minio:9000`.

### Không tìm thấy file CSV

```powershell
Get-ChildItem .\data\raw\*.csv
```

Đảm bảo file nằm đúng thư mục và có đuôi `.csv`.

### Worker không xuất hiện trên Spark Master UI

```powershell
docker compose logs --tail 100 spark-master
docker compose logs --tail 100 spark-worker-1
docker compose logs --tail 100 spark-worker-2
```

Kiểm tra hostname, địa chỉ Master, tài nguyên Worker và ánh xạ cổng trong Compose.

### Lỗi cổng đã được sử dụng

Kiểm tra cổng host trong `docker-compose.yml`. Nếu cần, chọn cổng host khác và cập nhật URL truy cập tương ứng.

### Spark không đọc được dữ liệu từ MinIO

Các service cùng Docker network chưa có nghĩa Spark đã được cấu hình đọc `s3a://`. Cần cấu hình Hadoop S3A connector và thông tin endpoint/credentials tương ứng.

## 12. Dừng hoặc xóa môi trường

Dừng container và giữ volume:

```powershell
docker compose down
```

Dừng container và xóa volume:

```powershell
docker compose down -v
```

**Cảnh báo:** `docker compose down -v` có thể xóa dữ liệu MinIO và PostgreSQL trong named volumes. Chỉ chạy khi chắc chắn muốn xóa dữ liệu hoặc đã sao lưu.

## 13. Checklist Infrastructure & Ingestion


- [x] Build MinIO từ source thành công.
- [x] Khởi chạy các service bằng Docker Compose.
- [x] Kiểm tra Spark Master và các Worker.
- [x] Xác nhận MinIO và PostgreSQL hoạt động.
- [x] Nạp đủ 2 file CSV (`books_data.csv`, `Books_rating.csv`) vào MinIO.
- [ ] Ghi nhận dung lượng, thời gian và checksum.
- [ ] Xác minh tính toàn vẹn dữ liệu.
- [ ] Lưu log và ảnh chụp màn hình làm minh chứng.
- [ ] Thống nhất bucket, prefix, endpoint và cách Spark đọc dữ liệu qua S3A với thành viên ETL.

Các kết quả như build thành công, số lượng Worker, thời gian ingestion và checksum cần được xác nhận bằng lần chạy thực tế.
