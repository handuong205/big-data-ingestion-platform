# Phân công công việc nhóm 4 người – Dự án pipeline dữ liệu sách và review

## 0. Ngữ cảnh dự án

- **Dữ liệu:** 2 file CSV: bảng sách (books) và bảng review (reviews), có vẻ lấy từ Kaggle (Amazon Books Reviews).
- **Kiến trúc:** MinIO (raw/processed) → Spark (PySpark) → PostgreSQL (serving) → Metabase (dashboard). Toàn bộ chạy bằng Docker.
- **Luồng pipeline:** ingestion → làm sạch (Job 1) → tổng hợp (Job 2a, Job 2b) → ghi serving → dashboard.
- **Một lệnh chạy:** `run_pipeline.sh`.
- **Ký hiệu:**
  - `[Đề]` = yêu cầu bắt buộc của đề bài.
  - `→ TVx` = sản phẩm này giao cho TVx dùng, tức TVx đang chờ.
  - `← TVx` = cần nhận từ TVx trước khi làm.

### Các câu hỏi phân tích (Q)

| Câu | Nội dung | Cần bảng sách? | Người phụ trách |
|---|---|---|---|
| Q1 | Điểm trung bình và số review theo thể loại × năm | Có | TV3 |
| Q2 | Top tác giả và nhà xuất bản (nhóm có ≥ N review) | Có | TV3 |
| Q3 | Giá và điểm (có phương án dự phòng nếu thiếu giá) | Không | TV2 |
| Q4 | Review hữu ích nhất (window function) | Không | TV2 |
| Q5 | Sách có nhiều review điểm thấp | Không | TV2 |
| Q6 | Phân nhóm người đánh giá theo User_id (nâng cao, chỉ làm nếu User_id thiếu ít) | Có | TV3 |

### Tóm tắt vai trò

| TV | Vai trò | Nhiệm vụ chính |
|---|---|---|
| TV1 | Hạ tầng và nạp dữ liệu | Docker, script nạp dữ liệu raw, `run_pipeline.sh`, sơ đồ kiến trúc |
| TV2 | Làm sạch và chất lượng dữ liệu | Khảo sát dữ liệu, Job 1 (cleaning), Job 2b, bằng chứng Spark, dq_report |
| TV3 | Phân tích và lưu kết quả | Thiết kế bảng Postgres, JDBC, Job 2a (join + Q1, Q2, Q6), đo độ trễ |
| TV4 | BI, kiểm thử và tài liệu | Problem, dashboard Metabase, queries.sql, README, ghép báo cáo và slide, demo |

---

## 1. TV1 – Data Infrastructure & Ingestion Lead

**Vai trò:** Thiết lập môi trường hệ thống, đường ống thu nạp dữ liệu (Raw Layer) và gói cả pipeline thành một lệnh chạy.

### A. Làm trước (người khác đang chờ)

- [ ] Tạo repo, `.gitignore` (không commit dữ liệu), file `.env` chung (đường dẫn, tên bucket, mật khẩu, tên bảng) → TV2, TV3, TV4
- [ ] `docker-compose.yml` bản tối thiểu: MinIO, Spark Master + 1 Worker, PostgreSQL, ghim phiên bản image → TV2, TV3, TV4
- [ ] Đặt giới hạn bộ nhớ từng dịch vụ → cả nhóm
- [ ] Script nạp dữ liệu (Python/Bash) `[Đề]` → TV2
  - Nạp 2 file CSV vào `raw/books/` và `raw/reviews/`.
  - Chạy lại không tạo bản trùng.
  - Có checksum, đếm dòng, log thời gian nạp và dung lượng.
- [ ] Nếu TV2 báo review có xuống dòng trong ô: tách file theo ranh giới bản ghi CSV thành các phần vài trăm MB để Spark đọc song song → TV2
  - ← TV2 (kết quả kiểm tra xuống dòng trong ô)

### B. Bắt buộc còn lại

- [ ] Docker-compose đầy đủ: thêm Metabase, lưu dữ liệu Metabase trong volume.
- [ ] Tạo bucket `processed/`, cấp quyền cho Spark.
- [ ] `run_pipeline.sh` `[Đề]`
  - Chạy đúng thứ tự: ingestion → làm sạch → tổng hợp → ghi serving.
  - Dừng khi lỗi, log từng bước.
- [ ] Gắn bước đối soát số dòng (raw = clean + bị loại) vào script.
  - ← TV2 (hàm đối soát số dòng)
- [ ] Sơ đồ kiến trúc cuối `[Đề]`: mỗi khối trong sơ đồ đều có thật trong project.

### C. Báo cáo và bảo vệ

- [ ] Trade-off: MinIO vs HDFS, batch vs streaming `[Đề]`.
- [ ] Nêu 1 vấn đề độ tin cậy: ingestion bị ngắt giữa chừng rồi chạy lại, kèm giải pháp `[Đề]`.
- [ ] Viết phần báo cáo: kiến trúc và lưu trữ.
- [ ] Chuẩn bị trả lời:
  - Vì sao tách raw và processed?
  - Chạy lại ingestion thì điều gì xảy ra?
  - Vì sao chọn MinIO?

---

## 2. TV2 – Spark Core ETL, Data Quality & Review Analytics Lead

**Vai trò:** Khảo sát và làm sạch dữ liệu thô bằng Spark, tối ưu lưu trữ, và thực hiện các phân tích chỉ cần bảng review.

### A. Làm trước (người khác đang chờ)

- [ ] Khảo sát dữ liệu ngay đầu dự án → TV3, TV4 (ảnh hưởng Q3 và Q6)
  - Phân phối review/score.
  - Ý nghĩa của `ratingsCount`.
  - Tỷ lệ thiếu của `Price`, `User_id`, `categories`, `publishedDate`.
  - Title trùng.
  - Tỷ lệ khớp Title giữa hai bảng.
- [ ] Kiểm tra review có ký tự xuống dòng trong ô không → TV1
- [ ] Khai báo schema tường minh cho cả hai bảng → TV3
- [ ] Xuất **file mẫu Parquet ~10 nghìn dòng**, đúng tên cột đã chốt → TV3, TV4
- [ ] Chốt luật Q3 → TV3, TV4
  - Đủ dữ liệu giá thì dùng giá.
  - Thiếu quá nhiều thì dùng phương án dự phòng (độ dài review và mức hữu ích).

### B. Bắt buộc còn lại

- [ ] **PySpark Job 1 (Data Cleaning & QA)** trên dữ liệu đầy đủ, cho cả hai bảng `[Đề]`: làm sạch thật sự, không chỉ đổi định dạng
  - Schema tường minh: đọc toàn bộ cột dạng chuỗi trước, ép kiểu sau.
  - Cấu hình quote/escape (thêm `multiLine` nếu cần) vì dữ liệu có dấu ngoặc kép trong ô.
  - Tách `review/helpfulness` dạng "x/y" thành `helpful_votes` và `total_votes`.
  - Đổi `review/time` (số giây Unix) sang timestamp, trích `year`, `month`, `day_of_week`. Dữ liệu chỉ chính xác đến ngày nên không có giờ.
  - Ép kiểu `price` và `score`, loại điểm ngoài khoảng hợp lệ, loại bản ghi trùng, tính `review_length`.
  - **Bảng sách:**
    - Tách `categories` và `authors` thành giá trị chính.
    - Chuẩn hoá `publishedDate` về năm.
    - Tạo `title_norm` (viết thường, bỏ khoảng trắng thừa) để join.
    - Loại trùng.
  - Lưu dòng bị loại vào `bad_records/` kèm lý do.
  - Ghi Parquet, partition theo `review_year`.
- [ ] **PySpark Job 2b** (chỉ dùng bảng review) `[Đề: ≥ 2 aggregation]`
  - Q3: giá và điểm.
  - Q4: review hữu ích nhất (window function).
  - Q5: sách nhiều review điểm thấp.
  - Ghi kết quả vào PostgreSQL.
  - ← TV3 (DDL và JDBC)
- [ ] Bằng chứng thực thi Spark `[Đề]` (ảnh chụp không đứng riêng, phải kèm giải thích)
  - Event log.
  - Application ID.
  - Ảnh Spark UI: DAG, Job/Stage/Task.
- [ ] Viết hàm đối soát số dòng giữa các tầng → TV1 gắn vào `run_pipeline.sh`.

### C. Báo cáo và bảo vệ

- [ ] Mô tả dataset: nguồn, dung lượng, schema, lỗi chất lượng `[Đề]`.
- [ ] Data Quality Report (bảng `dq_report`).
- [ ] Đo thời gian chạy mẫu so với đầy đủ.
- [ ] Trade-off: Parquet vs CSV. Giải thích tối ưu Spark: chiến lược partition, quản lý bộ nhớ.
- [ ] Nêu 1 vấn đề độ tin cậy: dữ liệu bẩn (dấu ngoặc kép, thiếu, trùng), kèm giải pháp `[Đề]`.
- [ ] Viết phần báo cáo: chất lượng dữ liệu, Parquet và partition.
- [ ] Chuẩn bị trả lời:
  - Vì sao khai báo schema thay vì `inferSchema`?
  - Vì sao partition theo năm?
  - Dòng nào bị loại và vì sao?

---

## 3. TV3 – Spark Analytics & Serving Layer Lead

**Vai trò:** Nối hai bảng, thực hiện các phép tính nghiệp vụ cần thông tin sách trên Spark và đồng bộ kết quả sang Serving Database.

### A. Làm trước (người khác đang chờ)

- [ ] **DDL Postgres** theo tên bảng đã chốt, chạy được trên Postgres của TV1 → TV4, TV2
  - Bảng: `agg_category_year`, `agg_author`, `agg_publisher`, `agg_price_band`, `top_helpful_reviews`, `book_summary`, `agg_reviewer_segment`, `dq_report`.
  - ← TV1 (Postgres chạy được)
- [ ] Cấu hình driver JDBC cho Spark, ghi thử một bảng nhỏ → TV2
- [ ] Chốt định nghĩa Q1, Q2 (ngưỡng số review tối thiểu N) và Q6 → TV4

### B. Bắt buộc còn lại

- [ ] **PySpark Job 2a** `[Đề: ≥ 2 aggregation]`
  - Phát triển trên file mẫu của TV2, rồi chạy dữ liệu đầy đủ khi Job 1 xong. ← TV2
  - Join `reviews_clean` với `books_clean` theo `title_norm` (thử broadcast join vì bảng sách nhỏ), báo cáo tỷ lệ khớp.
  - Q1: điểm trung bình, số review theo thể loại × năm.
  - Q2: top tác giả và nhà xuất bản (nhóm có ≥ N review).
  - Q6 (nâng cao): phân nhóm người đánh giá theo `User_id`, chỉ làm nếu User_id thiếu ít.
- [ ] Ghi sang Postgres bằng JDBC, chạy lại ghi đè được → TV4
- [ ] Đối chiếu số liệu Postgres với kết quả Spark.

### C. Báo cáo và bảo vệ

- [ ] Tạo chỉ mục, đo độ trễ truy vấn.
- [ ] Kiểm tra lệch dữ liệu (một số sách hoặc người dùng có rất nhiều review).
- [ ] Trade-off: broadcast join vs shuffle join, PostgreSQL vs ClickHouse.
- [ ] Nêu 1 vấn đề độ tin cậy: tỷ lệ khớp join thấp, xử lý bằng cách chỉ tính trên phần khớp và ghi rõ giới hạn `[Đề]`.
- [ ] Viết phần báo cáo:
  - Giải thích các phép biến đổi PySpark (join, groupBy, agg, window).
  - Thiết kế bảng ở Serving Layer và độ trễ truy vấn.
- [ ] Chuẩn bị trả lời:
  - Vì sao join theo tên sách?
  - Tỷ lệ khớp là bao nhiêu?
  - Vì sao chỉ đưa bảng tổng hợp vào Postgres?

---

## 4. TV4 – BI Analytics, QA & Reproducibility Lead

**Vai trò:** Đặt vấn đề, trực quan hóa kết quả, kiểm thử tích hợp toàn bộ pipeline và đảm bảo tính tái lập.

### A. Làm trước (người khác đang chờ)

- [ ] Viết phần **Problem** `[Đề]` → cả nhóm
  - Domain, người dùng hệ thống.
  - Các câu hỏi cần trả lời, đầu ra mong đợi.
- [ ] Tạo bảng công việc và mẫu Contribution record `[Đề]` → cả nhóm
- [ ] Dựng khung báo cáo 5-8 trang `[Đề]` → cả nhóm điền phần mình
- [ ] Thêm mục "Nguồn và thư viện bên ngoài" kèm hướng dẫn tải dữ liệu từ Kaggle `[Đề]`

### B. Bắt buộc còn lại

- [ ] Dựng Metabase nối Postgres, dùng bảng mẫu theo DDL. ← TV3 (DDL), ← TV1 (Metabase trong docker-compose)
- [ ] Viết `queries.sql` cho từng câu hỏi (TV2 và TV3 kiểm tra số liệu). Đây là nguồn kết quả chính vì tái lập được không cần giao diện.
- [ ] Bản nháp README giữa dự án: yêu cầu cài đặt, thứ tự chạy, kết quả mong đợi `[Đề]`.
- [ ] Thử clone repo ở máy khác sớm để phát hiện lỗi → TV1
- [ ] Dashboard cuối trên dữ liệu thật (thay bảng mẫu). ← TV3 (dữ liệu ghi vào Postgres), ← TV2 (kết quả Job 2b)
  - Điểm trung bình và số review theo thể loại, theo năm.
  - Top tác giả, nhà xuất bản.
  - Giá và điểm, danh sách review hữu ích nhất.
  - Heatmap tháng × thứ trong tuần (thay cho giờ × thứ vì dữ liệu chỉ chính xác theo ngày).
- [ ] Insight và khuyến nghị cho từng câu hỏi: con số, ý nghĩa, hạn chế (dữ liệu cũ, tên sách là khoá join yếu) `[Đề: ảnh không giải thích không tính]`.
- [ ] Kiểm thử tích hợp: chạy `run_pipeline.sh` từ máy sạch theo đúng README, sửa tài liệu theo lỗi gặp `[Đề: tái lập]`.
- [ ] Hoàn thiện README: yêu cầu cài đặt, cấu hình, thứ tự lệnh, kết quả kỳ vọng.

### C. Báo cáo và bảo vệ

- [ ] Nêu 1 vấn đề độ tin cậy: môi trường khác máy và dashboard cấu hình tay. Giải pháp là `queries.sql` và ghim phiên bản `[Đề]`.
- [ ] Chủ trì ghép báo cáo (5-8 trang) từ phần của cả 4 người, làm slide.
- [ ] Kịch bản demo: pipeline chạy và ít nhất 1 kết quả tái lập được `[Đề]`. Dẫn phần demo trực tiếp và trả lời phần insight.
- [ ] Tổ chức buổi tập Q&A chéo.
- [ ] Chuẩn bị trả lời:
  - Kết quả nào trả lời câu hỏi nào?
  - Hạn chế của kết luận là gì?
  - Làm sao người khác chạy lại được?

---

## 5. Bảng phụ thuộc giữa các thành viên

| Sản phẩm | Người làm | Người cần dùng | Ghi chú |
|---|---|---|---|
| Repo, `.env`, docker-compose tối thiểu, giới hạn bộ nhớ | TV1 | TV2, TV3, TV4 | Làm đầu tiên |
| Script nạp raw (checksum, không trùng) | TV1 | TV2 | |
| Tách file CSV theo bản ghi | TV1 | TV2 | Chỉ khi TV2 báo có xuống dòng trong ô |
| Kết quả kiểm tra xuống dòng trong ô | TV2 | TV1 | |
| Kết quả khảo sát dữ liệu | TV2 | TV3, TV4 | Ảnh hưởng Q3, Q6 |
| Schema tường minh | TV2 | TV3 | |
| File mẫu Parquet ~10k dòng (tên cột đã chốt) | TV2 | TV3, TV4 | Làm sớm để làm song song |
| Luật Q3 | TV2 | TV3, TV4 | |
| DDL Postgres | TV3 | TV4, TV2 | Chốt sớm |
| JDBC ghi thử một bảng nhỏ | TV3 | TV2 | |
| Định nghĩa Q1, Q2 (ngưỡng N), Q6 | TV3 | TV4 | |
| Job 1 chạy xong trên dữ liệu đầy đủ | TV2 | TV3 | Điểm nghẽn lớn nhất của TV3 |
| Dữ liệu thật trong Postgres | TV3 (Job 2a), TV2 (Job 2b) | TV4 | Dashboard cuối |
| Hàm đối soát số dòng | TV2 | TV1 | Gắn vào `run_pipeline.sh` |
| Phản hồi lỗi khi clone repo ở máy khác | TV4 | TV1 | |
| Phần báo cáo của từng người | TV1, TV2, TV3 | TV4 | TV4 ghép báo cáo và slide |

## 6. Trình tự thời gian gợi ý

1. **Giai đoạn đầu:** TV1 dựng repo, `.env`, docker-compose. TV2 khảo sát dữ liệu và xuất file mẫu Parquet. TV3 chốt DDL, thử JDBC, chốt định nghĩa câu hỏi. TV4 viết Problem, khung báo cáo, bảng công việc.
2. **Giữa dự án:** TV1 hoàn thiện ingestion, bucket processed, Metabase. TV2 chạy Job 1. TV3 viết Job 2a trên file mẫu. TV4 dựng dashboard trên bảng mẫu, viết `queries.sql`, nháp README, thử clone repo.
3. **Cuối dự án:** TV2 chạy Job 2b, thu bằng chứng Spark. TV3 chạy Job 2a trên dữ liệu đầy đủ, đối chiếu, đo độ trễ. TV1 hoàn tất `run_pipeline.sh` và sơ đồ kiến trúc. TV4 làm dashboard cuối, insight, kiểm thử từ máy sạch, ghép báo cáo, slide, demo, tập Q&A chéo.

## 7. Phần báo cáo mỗi người phải viết

| TV | Phần báo cáo | Trade-off | Vấn đề độ tin cậy |
|---|---|---|---|
| TV1 | Kiến trúc và lưu trữ | MinIO vs HDFS, batch vs streaming | Ingestion bị ngắt rồi chạy lại |
| TV2 | Mô tả dataset, chất lượng dữ liệu, Parquet và partition | Parquet vs CSV | Dữ liệu bẩn (ngoặc kép, thiếu, trùng) |
| TV3 | Phép biến đổi Spark, thiết kế bảng serving, độ trễ truy vấn | Broadcast vs shuffle join, PostgreSQL vs ClickHouse | Tỷ lệ khớp join thấp |
| TV4 | Problem, insight, README, ghép báo cáo và slide | Không có yêu cầu riêng | Khác máy, dashboard cấu hình tay |
