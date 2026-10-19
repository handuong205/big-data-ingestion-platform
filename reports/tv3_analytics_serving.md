# TV3 — Spark Analytics & Serving Layer

> **Người thực hiện:** Thành viên 3 — Spark Analytics & Serving Layer Lead
> **Phạm vi:** Job 2a (`spark/jobs/join_analytics_job.py`), DDL Serving Layer
> (`postgres/init.sql`), đo tỷ lệ khớp join và độ trễ truy vấn.

---

## 1. Job 2a làm gì

1. Đọc `reviews_clean` và `books_clean` do Job 1 (TV2) ghi ra Parquet.
2. Join theo khoá chuẩn hoá `title_norm`. Bảng sách là bảng chiều nhỏ nên dùng
   **broadcast join**; bảng sách được khử trùng theo `title_norm` trước khi join
   để tránh fan-out.
3. Thực thi:
   - **Q1** — `agg_category_year`: điểm trung bình, số review, số sách theo
     thể loại × năm. Cột `main_category` được `explode` vì Job 1 chỉ bóc cặp
     ngoặc `[]`, chưa tách từng thể loại trong chuỗi `"Fiction,History"`.
   - **Q2** — `agg_author`, `agg_publisher`: top tác giả / nhà xuất bản có
     `total_reviews >= N`. Ngưỡng N lấy từ `MIN_REVIEWS_Q2` (mặc định **30**),
     ghi rõ trong `.env.example`.
   - **Q6** — `agg_reviewer_segment`: phân nhóm người đánh giá theo `User_id`
     (1 review, 2–5, 6–20, >20). Điểm trung bình và độ hữu ích được tính có
     trọng số theo số review, không phải trung bình cộng của các trung bình.
4. Ghi kết quả vào PostgreSQL bằng JDBC, `mode=overwrite` + `truncate=true`
   để chạy lại không nhân đôi dòng và **không làm mất DDL/index**.
5. Ghi tỷ lệ khớp join vào `dq_report` và `data/reports/join_match.json`.

## 2. Thiết kế Serving Layer

| Bảng | Nguồn | Trả lời |
|---|---|---|
| `agg_category_year` | Job 2a | Q1 |
| `agg_author` | Job 2a | Q2 |
| `agg_publisher` | Job 2a | Q2 |
| `agg_reviewer_segment` | Job 2a | Q6 |
| `agg_price_band` | Job 2b (TV2) | Q3 |
| `agg_review_length_band` | Job 2b (TV2) | Q3 |
| `top_helpful_reviews` | Job 2b (TV2) | Q4 |
| `book_summary` | Job 2b (TV2) | Q5 |
| `dq_report` | Job 2a + Job 1 | Kiểm soát chất lượng |

Chỉ các bảng tổng hợp được đưa vào PostgreSQL. Dữ liệu thô ~3 GB và bảng review
sạch ~2–3 triệu dòng vẫn nằm trong Parquet, vì:

- Metabase chỉ cần kết quả tổng hợp; đưa raw vào Postgres làm tăng dung lượng
  và thời gian backup nhưng không thêm giá trị phân tích.
- Parquet nén cột tốt hơn, Spark đọc song song và prune theo partition.
- Serving Layer giữ schema ổn định cho dashboard, không phụ thuộc cấu trúc file.

Index được tạo trong `postgres/init.sql` cho các cột lọc/sắp xếp chính
(`main_category`, `review_year`, `total_reviews`, `avg_score`).

## 3. Số liệu đo được

Đã chạy `./run_pipeline.sh` trên dữ liệu đầy đủ ngày 19/10/2026. Kết quả:

| Chỉ số | Giá trị |
|---|---|
| Tổng review sạch | 2.430.902 |
| Review khớp sách | 2.430.902 |
| Tỷ lệ khớp review (title_norm) | **100,00 %** |
| Sách distinct | 207.065 |
| Sách có review khớp | 207.065 |
| Tỷ lệ khớp sách | **100,00 %** |
| Khớp theo Title nguyên bản (distinct) | 206.442 / 210.066 = **98,27 %** |
| Thời gian Job 2a | 123,8 giây |
| Thời gian Q1 query (có index) | 0,041 ms |
| Thời gian Q1 query (không index) | 13,938 ms |
| Thời gian Q2 query (có index) | 0,045 ms |

Số dòng các bảng sau khi chạy:

| Bảng | Số dòng |
|---|---|
| `agg_category_year` | 52.508 |
| `agg_author` | 12.215 |
| `agg_publisher` | 3.325 |
| `agg_reviewer_segment` | 4 |
| `book_summary` | 20.476 |
| `top_helpful_reviews` | 200 |

Đối soát Job 1: `SUCCESS`, cả hai bảng đều `PASS`.

- Books: 212.404 = 207.065 clean + 1 bad + 5.338 dedup.
- Reviews: 3.000.000 = 2.430.902 clean + 208 bad + 568.890 dedup.

Thời gian toàn pipeline: 1.475 giây (~24,6 phút), trong đó `clean` 777 giây,
`ingest` 206 giây, `review_analytics` 146 giây, `join_analytics` 128 giây.

### 3.1. Kiểm thử trên sample 10k dòng

Chạy Job 2a và Job 2b trong container Spark với `data/sample` (10.000 review,
1.000 sách). Đây chỉ là kiểm thử chức năng, **không phải số liệu báo cáo cuối**:

| Chỉ số | Giá trị |
|---|---|
| Review khớp sách | 503 / 10.000 (5,03%) |
| Sách có review khớp | 166 / 1.000 (16,60%) |
| Thời gian Job 2a | 21,6 giây (lần 1), 30,9 giây (lần 2) |
| Số dòng sau 2 lần chạy | không đổi (idempotent) |
| Query Q1 có index | 0,049 ms |
| Query Q1 không index | 0,139 ms |

Ghi chú: sample không có `User_id` (toàn null) nên `agg_reviewer_segment`
rỗng trong lần kiểm thử; Job 2a vẫn ghi bảng thành công. Trên dữ liệu thật,
`User_id` có đầy đủ nên Q6 sẽ có dữ liệu. Index vẫn còn nguyên sau khi chạy lại
(10 index trong `pg_indexes`).

Nguồn: `data/reports/join_match.json`, log `logs/pipeline_*.log`, output
`EXPLAIN ANALYZE` trong PostgreSQL.

## 4. Trade-off

**Broadcast join vs shuffle join**

- Bảng sách sau khi làm sạch nhỏ hơn nhiều so với bảng review, nên broadcast
  tránh shuffle toàn bộ ~2–3 triệu dòng review.
- Đổi lại, driver/executor phải giữ bản sao bảng sách trong bộ nhớ. Với dữ liệu
  hiện tại (khoảng vài chục MB sau Parquet) đây là lựa chọn hợp lý; nếu bảng
  chiều lớn dần, cần chuyển sang sort-merge join.

**PostgreSQL vs ClickHouse**

- PostgreSQL phù hợp vì dữ liệu serving chỉ là bảng tổng hợp nhỏ, cần transaction,
  tích hợp sẵn với Metabase và đã có trong hạ tầng Docker.
- ClickHouse nhanh hơn cho quét cột trên tỷ dòng, nhưng thêm một dịch vụ, thêm
  chi phí vận hành và không cần thiết ở quy mô kết quả hiện tại.

## 5. Vấn đề độ tin cậy: join theo tên sách

- Rủi ro lường trước là **tỷ lệ khớp join thấp**, vì khoá join là tên sách đã
  chuẩn hoá chứ không phải ID. Hai file đến từ hai nguồn khác nhau (Amazon
  reviews và Google Books metadata).
- Thực đo trên dataset này:
  - Khớp theo `Title` nguyên bản (distinct): **98,27%**.
  - Sau khi chuẩn hoá `title_norm`: **100%** review và **100%** sách đều khớp.
- Lý do 100%: `books_data.csv` của bộ Kaggle này là metadata được thu thập cho
  đúng tập sách có review, không phải một catalogue độc lập rộng hơn. Vì vậy
  **không được khái quát** tỷ lệ 100% cho bộ dữ liệu khác. Báo cáo ghi rõ đây là
  đặc điểm của dataset, không phải join hoàn hảo nói chung.
- Rủi ro thật còn lại:
  - Chuẩn hoá + khử trùng theo `title_norm` đã gộp **5.338 dòng sách** (các ấn
    bản/tiêu đề gần giống nhau). Các ấn bản khác nhau có thể bị tính chung một
    sách.
  - Review trùng theo `(user_id, title_norm, timestamp)` bị loại: **568.890 dòng**.
  - Không có ISBN/ASIN chung giữa hai bảng, nên không thể xác minh từng cặp
    sách–review là cùng một ấn bản.
- Cách xử lý:
  - Ghi cả hai tỷ lệ (exact 98,27%, normalized 100%) và số dòng bị dedup vào báo
    cáo, `dq_report` và dashboard.
  - Không suy diễn cho phần không khớp; nếu chạy bộ dữ liệu khác, tỷ lệ này phải
    được đo lại.
  - Job 2a khử trùng `books_clean` lần nữa trước khi join và cảnh báo nếu
    `matched_reviews > reviews_total` để tránh fan-out.

## 6. Câu hỏi bảo vệ

**Vì sao join theo tên sách?**
Vì hai file nguồn không có khoá chung đáng tin cậy: `Id` bên review là ASIN,
không khớp với dữ liệu Google Books. Tên sách sau chuẩn hoá là khoá tự nhiên
duy nhất dùng được cho cả hai bảng.

**Tỷ lệ khớp là bao nhiêu?**
Xem mục 3 và `data/reports/join_match.json`; số liệu được Job 2a đo lại mỗi lần chạy,
không dùng số ước lượng.

**Vì sao chỉ đưa bảng tổng hợp vào PostgreSQL?**
Metabase chỉ cần kết quả tổng hợp; giữ raw trong Parquet để tránh phình dung
lượng, giảm thời gian backup và tận dụng partition pruning của Spark.
