# BÁO CÁO TỔNG HỢP ĐỒ ÁN MÔN HỌC BDT301 (BIG DATA TECHNOLOGY)
# HỆ THỐNG XỬ LÝ VÀ PHÂN TÍCH DỮ LIỆU ĐÁNH GIÁ SÁCH AMAZON (AMAZON BOOKS REVIEWS PIPELINE)

**Đơn vị:** Bộ môn Kỹ thuật Dữ liệu & Trí tuệ Nhân tạo  
**Học phần:** BDT301 — Big Data Technology | **Học kỳ:** Fall 2026  
**Nhóm thực hiện:** Nhóm Dự án 4 Thành viên  
**Repository GitHub:** [https://github.com/handuong205/big-data-ingestion-platform.git](https://github.com/handuong205/big-data-ingestion-platform.git)  
**Tập dữ liệu:** Kaggle Amazon Books Reviews (> 1 GB, 3.212.404 bản ghi CSV thô)  

---

## BẢNG PHÂN CÔNG VAI TRÒ VÀ TRÁCH NHIỆM THÀNH VIÊN

| STT | Họ và tên | Mã sinh viên | Vai trò đảm nhiệm trong nhóm | Trọng tâm đóng góp |
| :---: | :--- | :---: | :--- | :--- |
| **1** | Thành viên 1 | TV1 | **Data Infrastructure & Ingestion Lead** | Dựng cụm Docker (MinIO, Spark, Postgres, Metabase), viết script nạp dữ liệu có checksum, đóng gói pipeline `run_pipeline.sh`. |
| **2** | Thành viên 2 (Tiến) | TV2 | **Spark Core ETL, Data Quality & Review Analytics** | Khảo sát EDA, viết PySpark Job 1 làm sạch & phân vùng Parquet, Job 2b (Q3, Q4, Q5), báo cáo `dq_report.md`. |
| **3** | Hán Thành Dương | TV3 | **Analytics & Serving Layer Lead (Nhóm trưởng)** | Thiết kế DDL Serving PostgreSQL, viết PySpark Job 2a (Join + Q1, Q2, Q6), tối ưu Broadcast Join, đo độ trễ truy vấn. |
| **4** | Thành viên 4 | TV4 | **BI Analytics, QA & Documentation Lead** | Đặt vấn đề nghiệp vụ, viết bộ truy vấn độc lập `queries.sql`, kiểm thử máy sạch (Reproducibility), phân tích chuyên sâu Insights & chủ trì báo cáo. |

---

## 1. PROBLEM DEFINITION & DATASET OVERVIEW (ĐẶT VẤN ĐỀ & MỤC TIÊU)

### 1.1. Bối cảnh miền và Vấn đề thực tiễn
Thị trường bán lẻ sách trực tuyến của Amazon sở hữu hàng triệu đầu sách cùng hàng trăm triệu đánh giá từ độc giả toàn cầu. Đây là nguồn tài nguyên tri thức khổng lồ nhưng cũng đặt ra 3 thách thức lớn cho các hệ thống thông tin truyền thống:
1. **Quá tải thông tin (Information Overload):** Một đầu sách kinh điển có thể tích lũy hàng ngàn đánh giá, độc giả không thể tự đọc hết để chắt lọc thông tin nếu thiếu cơ chế xếp hạng bài viết hữu ích (`helpful_votes`).
2. **Sách kém chất lượng và tranh cãi nội dung:** Xuất hiện nhiều tựa sách biên tập cẩu thả, dịch thuật lỗi hoặc nội dung gây tranh cãi, đòi hỏi hệ thống phải phát hiện sớm thông qua các tín hiệu điểm số tiêu cực tập trung.
3. **Chất lượng dữ liệu đánh giá và người dùng ẩn danh:** Khảo sát tập dữ liệu thực tế cho thấy có tới **16,99%** đánh giá khuyết định danh (`User_id = NULL`), gây nhiễu cho các mô hình gợi ý nếu không được phân tách khoa học.

### 1.2. Chân dung Người dùng Hệ thống (Stakeholders)
- **Độc giả (Readers):** Cần công cụ tìm kiếm sách chất lượng cao, tiếp cận các bài đánh giá sâu sắc nhất, và cảnh báo tránh các đầu sách bị phàn nàn nhiều.
- **Tác giả & Nhà xuất bản (Publishers & Authors):** Cần nắm bắt xu hướng thị hiếu theo thể loại, chu kỳ đánh giá của độc giả, và mức độ chấp nhận giá của thị trường.
- **Đội ngũ Vận hành Sàn (Platform Ops / Data Team):** Cần báo cáo đối soát dữ liệu tin cậy, phân khúc hành vi khách hàng, và nhận diện các tác giả uy tín để hợp tác chiến lược.

### 1.3. Mục tiêu phân tích (Core Analytical Objectives)
Hệ thống giải quyết 6 câu hỏi nghiệp vụ trọng tâm kết hợp phân tích ma trận thời gian:
- **Q1:** Điểm trung bình và số lượng review theo Thể loại & theo Năm.
- **Q2:** Top Tác giả và Nhà xuất bản uy tín (có lọc ngưỡng review tối thiểu để tránh bias).
- **Q3:** Tương quan giữa Mức giá sách & Độ dài bài review với Điểm đánh giá.
- **Q4:** Top các bài Review hữu ích nhất mỗi năm (Ứng dụng Window Functions).
- **Q5:** Danh sách các đầu sách có lượng review lớn nhưng điểm thấp (Cảnh báo chất lượng).
- **Q6:** Phân khúc người đánh giá theo tần suất đóng góp (Reviewer Segmentation).
- **Heatmap:** Phân tích mật độ gửi đánh giá theo Tháng × Thứ trong tuần.

---

## 2. SYSTEM ARCHITECTURE & STORAGE DESIGN (KIẾN TRÚC & LƯU TRỮ)

### 2.1. Kiến trúc Pipeline End-to-End
Pipeline được container hóa 100% qua Docker Compose và đóng gói trong script thực thi tự động `run_pipeline.sh`:

```
┌─────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐      ┌─────────────────┐
│   Data Source   │ ───> │       Raw Layer         │ ───> │    Processing Layer     │ ───> │      Serving Layer      │ ───> │  Presentation   │
│ Kaggle CSV 3GB  │      │ MinIO Object Storage    │      │ Apache Spark (PySpark)  │      │ PostgreSQL 16 DB        │      │ Metabase / SQL  │
│ Books + Reviews │      │ raw/books, raw/reviews  │      │ Job 1, Job 2a, Job 2b   │      │ 8 Tables + Indexes      │      │ queries.sql     │
└─────────────────┘      └─────────────────────────┘      └─────────────────────────┘      └─────────────────────────┘      └─────────────────┘
```

1. **Raw Storage (MinIO):** Nhận 2 file CSV gốc từ Kaggle (`books_data.csv` 173 MiB và `Books_rating.csv` 2,66 GiB). Cơ chế tải có kiểm tra toàn vẹn mã băm MD5/Checksum và đếm số dòng.
2. **Processing (Apache Spark):** Cụm Spark (Master + Worker) đọc dữ liệu từ MinIO qua giao thức S3A, thực hiện ETL phân tán, ghi dữ liệu sạch ra Parquet phân vùng theo năm `review_year`.
3. **Serving (PostgreSQL):** Lưu trữ duy nhất các bảng tổng hợp nghiệp vụ, phục vụ truy vấn BI tốc độ cao. Cơ chế ghi JDBC sử dụng `mode=overwrite` kết hợp `truncate=true` để bảo toàn schema DDL và hệ thống index.
4. **BI & Presentation (Metabase & `queries.sql`):** Kết nối trực tiếp vào PostgreSQL để cung cấp dashboard trực quan và bộ truy vấn CLI độc lập.

### 2.2. Phân tích Trade-off Kiến trúc
- **MinIO vs HDFS:** Chọn MinIO vì tương thích hoàn toàn chuẩn API S3, kiến trúc Cloud-Native nhẹ, không phụ thuộc NameNode cồng kềnh, cấu hình đơn giản trên Docker Compose nhưng vẫn đảm bảo tính phân tán và lưu trữ bất biến (Immutable Storage).
- **PostgreSQL vs ClickHouse:** Do toàn bộ dữ liệu serving là các bảng tổng hợp (tổng cộng chỉ vài chục ngàn dòng), PostgreSQL đáp ứng hoàn hảo yêu cầu tính nhất quán ACID, hỗ trợ quan hệ, tương thích rộng rãi với Metabase và đạt độ trễ truy vấn sub-millisecond (< 0.05 ms) khi có index; việc dùng ClickHouse là dư thừa về mặt vận hành ở quy mô Serving này.

---

## 3. DATA QUALITY ASSURANCE & CLEANING (CHẤT LƯỢNG DỮ LIỆU)

### 3.1. Đặc thù và Lỗi chất lượng của Dữ liệu thô
Khảo sát ban đầu trên 3.212.404 dòng dữ liệu CSV thô ghi nhận các vấn đề chất lượng nghiêm trọng:
- **Tỷ lệ khuyết giá (`Price`):** Khuyết tới **83,96%** trong bảng review, khiến việc phân tích giá sách phải kết hợp thêm phân tích dải độ dài bài viết (`review_length`).
- **Khuyết định danh người dùng (`User_id`):** Khuyết **16,99%** trong tập review sạch (412.905 dòng), đòi hỏi phải tách riêng nhóm khách vãng lai/vô danh để không làm lệch số liệu phân khúc.
- **Ký tự đặc biệt và dấu ngoặc kép trong ô:** Đánh giá sách chứa nhiều dấu nháy kép, dấu xuống dòng nội đoạn (`\n`). Spark phải cấu hình `quote` / `escape` và xử lý đọc chuỗi tường minh.
- **Tiêu đề sách và biến thể:** Cùng một tác phẩm có nhiều cách viết (viết hoa/thường, có/không dấu câu, ấn bản).

### 3.2. Cơ chế Bảo toàn Dữ liệu & Đối soát Dòng (Row Count Reconciliation)
Quy trình làm sạch của Job 1 tuân thủ phương trình bảo toàn tuyệt đối:
$$\text{Raw Records} = \text{Clean Records} + \text{Bad Records} + \text{Deduplicated Records}$$

Bảng kết quả đối soát chính thức (Trích xuất từ `ingestion/logs/reconciliation.json` và `reports/dq_report.md`):

| Tập dữ liệu | Tổng dòng Raw | Dòng sạch (Clean Parquet) | Dòng lỗi (Bad Records) | Dòng trùng lặp (Dedup) | Tỷ lệ làm sạch | Trạng thái |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Bảng Sách (`books_data.csv`)** | 212.404 | **207.065** | 1 *(Title rỗng)* | 5.338 *(Trùng `title_norm`)* | 97,49% | **PASS** |
| **Bảng Đánh giá (`Books_rating.csv`)** | 3.000.000 | **2.430.902** | 208 *(Lỗi title/score)* | 568.890 *(Trùng user+sách+time)* | 81,03% | **PASS** |
| **Tổng cộng toàn hệ thống** | **3.212.404** | **2.637.967** | **209** | **574.228** | — | **PASS** |

Dữ liệu lỗi được lưu trữ cách ly tại `data/processed/bad_records/` kèm lý do phục vụ hậu kiểm.

---

## 4. SPARK PROCESSING & SERVING OPTIMIZATION (TỐI ƯU HÓA SPARK)

### 4.1. Kỹ thuật Tối ưu hóa Spark
1. **Explicit Schema vs InferSchema:** Khai báo tường minh cấu trúc `StructType` trong `spark/jobs/schemas.py`, loại bỏ hoàn toàn một lượt quét toàn bộ 3 GB CSV để đoán kiểu, ngăn ngừa lỗi tràn kiểu dữ liệu và tăng tốc độ đọc ban đầu.
2. **Lưu trữ Cột Parquet + Snappy:** Dữ liệu sau khi làm sạch chuyển từ CSV (3,04 GB) sang Parquet (1,6 GB), tiết kiệm **47% dung lượng đĩa**. Parquet hỗ trợ cơ chế nén cột (Column Pruning) và đẩy vị từ lọc (Predicate Pushdown), giúp Job 2a/2b chỉ đọc các cột cần thiết mà không phải nạp toàn bộ khối văn bản review dài hàng ngàn ký tự.
3. **Chiến lược Phân vùng theo Năm (`partitionBy("review_year")`):** 2,43 triệu dòng review sạch được ghi ra các thư mục con phân vùng theo năm đánh giá. Khi truy vấn các năm cụ thể (như đỉnh 2012), Spark kích hoạt tính năng **Partition Pruning**, bỏ qua hoàn toàn các phân vùng năm khác.
4. **Broadcast Hash Join trên Khóa chuẩn hóa `title_norm`:** Bảng sách sạch (207.065 dòng, ~20 MB) sau khi khử trùng theo `title_norm` được nạp vào bộ nhớ và phát sóng (broadcast) tới các executor. Kỹ thuật này tránh hoàn toàn bước Shuffle trên 2,43 triệu dòng review, rút ngắn thời gian Job 2a xuống chỉ còn **123,8 giây**.

### 4.2. Hiệu năng Truy vấn tại Serving Layer (PostgreSQL)
Nhờ hệ thống chỉ mục (B-Tree Indexes) được thiết lập tại `postgres/init.sql`, tốc độ truy vấn tại Serving Layer đạt hiệu năng vượt trội:
- Truy vấn Q1 có Index: **0,041 ms** (so với 13,938 ms khi không có Index — nhanh hơn **340 lần**).
- Truy vấn Q2 có Index: **0,045 ms**.

---

## 5. DETAILED ANALYTICAL INSIGHTS & RECOMMENDATIONS (PHÂN TÍCH CHUYÊN SÂU TV4)

Toàn bộ 6 câu hỏi nghiệp vụ và biểu đồ nhiệt được phân tích theo khung chuẩn học thuật 4 bước:

### Q1: Thể loại & Xu hướng theo Năm
- **Fact:** *Fiction* dẫn đầu tuyệt đối với **622.022 reviews** (chiếm 25,5%), điểm TB 4,09. Tiếp theo là *Juvenile Fiction* (141.804 reviews, điểm 4,43), *Biography & Autobiography* (91.375 reviews, điểm 4,24). Năm 2012 là đỉnh lịch sử của ngành với 61.984 review cho Fiction. Ngành *Computers* có điểm trung bình thấp nhất (3,96).
- **Insight:** Tồn tại nghịch lý giữa quy mô và sự thỏa dụng. Thể loại đại chúng (Fiction) có độ phân tán kỳ vọng cao nên điểm trung bình thấp hơn thể loại chuyên biệt. Sách công nghệ có điểm thấp do vòng đời tri thức ngắn, mã nguồn dễ lỗi thời theo thời gian.
- **Recommendation:** NXB nên áp dụng mô hình danh mục "Core & Star" (dùng Fiction làm dòng tiền và mở rộng Juvenile Fiction để tăng uy tín); sàn E-commerce cần hiển thị điểm chuẩn hóa theo thể loại (Category-Normalized Score).
- **Limitation:** Một số tựa sách chứa nhiều thể loại lồng nhau (`['Fiction', 'History']`) được explode thành nhiều dòng, dẫn đến việc đếm lặp ở cấp độ thể loại.

### Q2: Top Tác giả & Nhà xuất bản uy tín
- **Fact:** Với điều kiện lọc $\ge 300$ reviews và $\ge 3$ đầu sách: J.K. Rowling đạt **4,68/5.0** (7.215 reviews); J.R.R. Tolkien đạt **4,62/5.0** (14.619 reviews); George Orwell đạt **4,55/5.0**. Top 3 NXB lớn nhất thị trường: *Penguin* (100.633 reviews, 4,11), *Simon & Schuster* (97.438 reviews, 4,18), *HarperCollins* (61.798 reviews, 4,19).
- **Insight:** "Hiệu ứng Lindy" thể hiện rõ rệt: Các tác phẩm văn học kinh điển và vũ trụ giả tưởng huyền thoại duy trì sự ngưỡng mộ bền bỉ qua nhiều thế hệ. Các NXB quy mô vừa chuyên biệt (*Houghton Mifflin Harcourt*, điểm 4,40) có chất lượng in ấn và mức độ hài lòng vượt trội so với các tập đoàn xuất bản đại trà.
- **Recommendation:** NXB cần ưu tiên ngân sách tái bản các ấn bản sưu tầm giới hạn (Special Editions) cho nhóm tác giả kinh điển. Sàn cần áp dụng thuật toán Entity Resolution để gộp các tên NXB viết sai chính tả (`Harper Collins` và `HarperCollins`).
- **Limitation:** Tên tác giả bị phân mảnh do gộp chuỗi (`J. R. R. Tolkien` và `John Ronald Reuel Tolkien` bị tính là 2 thực thể riêng).

### Q3: Tương quan Giá sách & Độ dài Review với Điểm đánh giá
- **Fact:** 
  - Giá sách: Nhóm < $10 đạt điểm TB 4,14 (92.942 review); nhóm $10–$20 đạt điểm cao nhất **4,29** (225.728 review); nhóm $20–$50 đạt 4,20 (103.821 review); nhóm > $50 đạt 4,17 (29.261 review, helpful votes đạt 5,65).
  - Độ dài review: Nhóm ngắn (<100 ký tự) có 61.079 review (điểm 4,38; 1,34 vote); nhóm rất dài (>600 ký tự) áp đảo với **1.063.829 review** (điểm 4,11; **8,01 helpful votes**).
- **Insight:** Điểm đánh giá **không tăng tuyến tính theo giá**. Mức giá $10–$20 là "điểm ngọt" (Sweet Spot) tối ưu giữa chi phí và mức độ hài lòng (4,29★). Sách đắt tiền (> $50) có điểm giảm nhẹ (4,17★) do độc giả trả nhiều tiền nên có kỳ vọng khắt khe hơn. Về độ dài, có tới hơn 1 triệu review dài trên 600 ký tự nhận trung bình 8,01 vote hữu ích, chứng minh độc giả Amazon đầu tư công sức viết review rất sâu sắc và có sức thuyết phục cao.
- **Recommendation:** Sàn cần ưu tiên hiển thị các bài review trên 300 ký tự ở đầu trang sản phẩm. NXB nên cân nhắc dải giá $10–$20 cho các tựa sách đại chúng để tối ưu hóa sự hài lòng của độc giả.
- **Limitation:** Cột giá khuyết 83,96% trong dữ liệu thô nên kết quả chỉ phản ánh nhóm sách có giá niêm yết chính thống.

### Q4: Top Review hữu ích nhất mỗi năm (Window Function)
- **Fact:** Trích xuất 200 bài review Top từ Spark Window Function. Bài viết hữu ích nhất mọi thời đại đạt kỷ lục **10.855 helpful votes**; các bài dẫn đầu từng năm gồm: năm 2011 cuốn *Unfinished Business* (1.592 vote), năm 2012 cuốn *Pow Wow Chow* (645 vote), năm 2013 cuốn *Gaudy Night* (30 vote).
- **Insight:** UGC (Nội dung do người dùng tự tạo) có sức mạnh thuyết phục vượt xa quảng cáo thương mại. Các bài viết sâu sắc đóng vai trò là "mỏ neo chuyển đổi" giữ chân người mua trên nền tảng.
- **Recommendation:** Khởi động chương trình huy hiệu độc giả uy tín (Top Contributor Badges) và tặng voucher mua sách cho người dùng có bài viết đạt $\ge 50$ helpful votes.
- **Limitation:** Yếu tố lợi thế thời gian (First-mover Advantage): Các bài viết đăng sớm từ nhiều năm trước tích lũy được nhiều lượt vote hơn dù chất lượng có thể tương đương bài viết mới.

### Q5: Danh sách sách có nguy cơ chất lượng kém
- **Fact:** Phát hiện hàng chục đầu sách có $\ge 100$ reviews nhưng điểm số trượt xuống dưới **2,80/5.0**. Nhiều cuốn có phân cực điểm số cực đoan (`min = 1.0`, `max = 5.0`).
- **Insight:** Cần phân biệt rõ giữa sách "rác" (lỗi in ấn, thiếu trang, dịch máy) và sách "tranh cãi tư tưởng" (phân cực điểm số giữa các trường phái).
- **Recommendation:** Áp dụng thuật toán "Cách ly tự động" (Algorithmic Quarantine): Tạm dừng đề xuất sách có điểm TB dưới 2,80 và gửi thông báo yêu cầu NXB thẩm định lại chất lượng.
- **Limitation:** Dataset thiếu dữ liệu về tỷ lệ trả hàng (return rates) để xác thực xem điểm thấp có dẫn trực tiếp đến hoàn tiền hay không.

### Q6: Phân khúc Người đánh giá (Reviewer Segmentation)
- **Fact:** Trên tổng số **2.430.902 review sạch**:
  - Nhóm khuyết danh (`Missing User_id`): **412.905 reviews (16,99%)**.
  - Nhóm có định danh (`Identified User_id`): **2.017.997 reviews (83,01%)**, gồm:
    * *1 review (One-time):* 746.245 users | 746.245 reviews (30,70%) | Điểm TB: 4,23 | Helpful: 4,37
    * *2–5 reviews (Casual):* 221.360 users | 585.060 reviews (24.07%) | Điểm TB: 4,18 | Helpful: 4,20
    * *6–20 reviews (Active):* 35.220 users | 321.956 reviews (13,24%) | Điểm TB: 4,21 | Helpful: 5,13
    * *> 20 reviews (Super):* **6.136 users** | **364.736 reviews (15,00%)** | Điểm TB: **4,26** | Helpful: **7,25**
- **Insight:** Quy luật Pareto cực đoan: Chỉ **0,6% số người dùng** (> 20 review) đã tạo ra tới **15% tổng lượng review sạch** của toàn sàn. Đây là lực lượng nòng cốt gánh vác uy tín cộng đồng với số vote hữu ích vượt trội (7,25 vote/bài). Nhóm 16,99% thiếu ID là "vùng xám" dữ liệu cần được cách ly khi huấn luyện mô hình gợi ý.
- **Recommendation:** Xây dựng câu lạc bộ độc quyền *Amazon Elite Vine* tặng sách thử nghiệm trước ngày phát hành cho 6.136 Super Reviewers. Bắt buộc loại bỏ nhóm thiếu `User_id` trong thuật toán Collaborative Filtering.
- **Limitation:** Dữ liệu thiếu cờ "Verified Purchase" (Mua hàng xác thực) để phân định tài khoản nhận sách tài trợ review.

### Heatmap Hoạt động: Tháng × Thứ trong tuần
- **Fact:** Hoạt động gửi review tập trung cao độ vào **Thứ Hai đến Thứ Tư** (chiếm 48% tuần) và giảm 30% vào Thứ Bảy, Chủ Nhật. Về chu kỳ năm, **Tháng 12 và Tháng 1** là hai tháng bùng nổ mạnh nhất. Điểm nóng toàn diện là các ngày đầu tuần của Tháng 1.
- **Insight:** Hiệu ứng sau mùa lễ hội (đọc sách quà tặng Giáng Sinh và thực hiện mục tiêu năm mới) kết hợp với thói quen viết review tại máy tính văn phòng trong giờ hành chính đầu tuần.
- **Recommendation:** NXB nên phát hành sách mới vào tháng 11–12 để đón làn sóng mua sắm và tập trung ngân sách quảng cáo review vào các ngày đầu tuần trong tháng 1.
- **Limitation:** Dữ liệu Amazon chỉ lưu trữ chính xác đến cấp độ Ngày (Day level), trường giờ bị gộp về `00:00:00 UTC`, do đó ma trận thời gian phải phân tích theo Tháng × Thứ thay vì Giờ × Thứ.

---

## 6. REPRODUCIBILITY & QA VERIFICATION (TÍNH TÁI LẬP & KIỂM THỬ)

### 6.1. Kiểm thử từ Máy sạch theo Hướng dẫn `README.md`
Toàn bộ pipeline đã được kiểm thử chạy lại từ đầu trên môi trường sạch với quy trình 3 bước:
```bash
# 1. Khởi động hạ tầng Docker
docker compose up -d

# 2. Chạy toàn bộ Data Pipeline tự động (Ingest -> Clean -> Serving)
./run_pipeline.sh

# 3. Kiểm tra tính toàn vẹn độc lập bằng SQL CLI (Không cần Metabase)
docker compose exec -T postgres psql -U postgres -d amazon_books -f queries.sql
```

### 6.2. Kiểm định Tính Toàn vẹn Độc lập của `queries.sql`
File `queries.sql` được xây dựng hoàn toàn độc lập, cho phép người chấm thi tái lập 100% kết quả trên PostgreSQL CLI hoặc DBeaver mà không cần thiết lập cấu hình giao diện Metabase. File tích hợp sẵn truy vấn Audit Checksum tự động:

```sql
SELECT 
    CASE 
        WHEN (SELECT SUM(total_reviews) FROM agg_reviewer_segment) = 2017997
         AND (2430902 - (SELECT SUM(total_reviews) FROM agg_reviewer_segment)) = 412905
        THEN 'PASSED: DATA MATCHES 100% PRODUCTION BENCHMARK (2,017,997 / 412,905)'
        ELSE 'FAILED: AUDIT MISMATCH'
    END AS audit_status;
```
Kết quả kiểm thử trên Serving Database trả về: **`PASSED`** với thời gian phản hồi **0,004 giây**.

---

## 7. SYSTEM RELIABILITY & FAILURE HANDLING (ĐỘ TIN CẬY HỆ THỐNG)

| Vấn đề rủi ro (Failure Concern) | Mức độ | Cơ chế xử lý hiện tại của Pipeline | Đề xuất cải tiến môi trường Production |
| :--- | :---: | :--- | :--- |
| **Ingestion bị ngắt quãng giữa chừng (Network Failure)** | Cao | Script kiểm tra mã băm MD5 và số dòng; nếu file hỏng sẽ xóa và tải lại từ đầu (Idempotent Download). | Áp dụng Chunked Upload có khả năng Resume vị trí byte bị ngắt. |
| **Dữ liệu CSV chứa ký tự nháy kép và xuống dòng lộn xộn** | Cao | Job 1 khai báo Explicit Schema, kích hoạt chế độ xử lý đa dòng `multiLine=True` và bộ lọc regex. | Phối hợp với nguồn upstream chuyển sang truyền tải qua Parquet/Avro trực tiếp. |
| **Tỷ lệ lệch dữ liệu (Data Skewness) khi Join bảng** | Trung bình | Khử trùng sách theo `title_norm` và dùng Broadcast Join, loại bỏ hiện tượng fan-out và giảm thời gian join còn 124 giây. | Sử dụng chuẩn mã số quốc tế ISBN-13 hoặc ASIN thay cho khoá join tự nhiên bằng tên sách. |
| **Môi trường máy chấm khác nhau hoặc Metabase lỗi** | Trung bình | Tạo file `queries.sql` độc lập và `init.sql` nạp schema tự động trên Docker volume mới. | Đóng gói toàn bộ cơ sở dữ liệu Serving vào một container image độc lập đã nạp sẵn dữ liệu. |

---

## 8. KẾT LUẬN & CONTRIBUTION RECORD

### 8.1. Kết luận Đồ án
Dự án đã xây dựng thành công một **Big Data Pipeline hoàn chỉnh, khép kín và tự động hóa 100%** từ tầng nạp thô MinIO, xử lý phân tán PySpark trên cụm Docker, đồng bộ sang PostgreSQL Serving và trực quan hóa trên Metabase. Các câu hỏi nghiệp vụ đặt ra đều được giải quyết thấu đáo dựa trên bằng chứng dữ liệu thực nghiệm, cung cấp cái nhìn sâu sắc và có giá trị thương mại cao cho ngành xuất bản và thương mại điện tử sách.

### 8.2. Bảng Đánh giá Đóng góp Thành viên (Contribution Matrix)

| Thành viên | Hạng mục công việc đã hoàn thành | Minh chứng kỹ thuật trong Repository | Tỷ lệ đóng góp |
| :--- | :--- | :--- | :---: |
| **TV1** | Dựng Docker Compose, script Ingestion, đóng gói `run_pipeline.sh`, sơ đồ kiến trúc hệ thống. | `docker-compose.yml`, `run_pipeline.sh`, `ingestion/` | **25%** |
| **TV2** | Khảo sát EDA, PySpark Job 1 (Làm sạch & Phân vùng Parquet), Job 2b (Review Analytics), báo cáo DQ. | `spark/jobs/clean_data_job.py`, `spark/jobs/review_analytics_job.py`, `reports/dq_report.md` | **25%** |
| **TV3** | Thiết kế DDL Serving PostgreSQL, PySpark Job 2a (Join Analytics Q1, Q2, Q6), tối ưu Broadcast Join, đo độ trễ. | `postgres/init.sql`, `spark/jobs/join_analytics_job.py`, `reports/tv3_analytics_serving.md` | **25%** |
| **TV4** | Đặt vấn đề, xây dựng `queries.sql` độc lập, kiểm thử máy sạch (Reproducibility), phân tích chuyên sâu Insights & chủ trì báo cáo tổng hợp. | `queries.sql`, `docs_tv4_insight_and_problem.md`, `FINAL_REPORT.md`, `README.md` | **25%** |

---
*Báo cáo được hoàn thiện và kiểm thử bởi Thành viên 4 (TV4) — BI Analytics, QA & Documentation Lead.*  
*Dự án BDT301 — Big Data Engineering Pipeline.*
