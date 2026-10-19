-- =====================================================================================
-- BDT301 - BIG DATA TECHNOLOGY GROUP PROJECT
-- DATASET: AMAZON BOOKS REVIEWS (> 1 GB, 2.43M+ RECORDS)
-- SERVING LAYER ANALYTICAL QUERIES (POSTGRESQL & METABASE COMPATIBLE)
--
-- Lead Author: TV4 (BI Analytics, QA & Reproducibility Lead)
-- Serving Schema & PySpark Job 2a: TV3 (Analytics & Serving Lead - Han Duong)
-- Data Cleaning & PySpark Job 2b: TV2 (ETL & DQ Lead - Tien)
-- Infrastructure & Ingestion: TV1 (Docker & Pipeline Lead)
--
-- Schema Source: postgres/init.sql (dev branch)
-- Target Serving Database: PostgreSQL 16+
-- Verified Dataset Metrics (from reports/tv3_analytics_serving.md):
--   * Total Cleaned Reviews : 2,430,902 rows (100.00%)
--   * Reviews with User_id  : 2,017,997 rows ( 83.01%)
--   * Missing User_id       :   412,905 rows ( 16.99%)
--   * Distinct Books Matched:   207,065 titles
--   * Distinct Users (Q6)   : 1,008,961 users
--   * Join Match Rate (norm):   100.00% (broadcast join on title_norm)
-- =====================================================================================

-- =====================================================================================
-- TABLE OF CONTENTS
-- -------------------------------------------------------------------------------------
-- 0. SCHEMA INTEGRITY & DATA QUALITY AUDIT (QA VERIFICATION)
-- 1. QUESTION 1 (Q1): CATEGORY & ANNUAL REVIEW TRENDS
-- 2. QUESTION 2 (Q2): AUTHOR & PUBLISHER PRESTIGE / INFLUENCE (THRESHOLD FILTERED)
-- 3. QUESTION 3 (Q3): PRICE BAND & REVIEW LENGTH CORRELATION WITH RATINGS
-- 4. QUESTION 4 (Q4): TOP HELPFUL REVIEWS PER YEAR (WINDOW FUNCTION OUTPUT)
-- 5. QUESTION 5 (Q5): LOW-RATING BOOKS & CONTENT QUALITY RISK DETECTION
-- 6. QUESTION 6 (Q6): REVIEWER SEGMENTATION & ENGAGEMENT ANALYSIS (VALIDATED NUMBERS)
-- 7. TEMPORAL ACTIVITY: MONTH x DAY-OF-WEEK REVIEW HEATMAP (NO HOURLY BIAS)
-- 8. EXECUTIVE SUMMARY METRICS & REPRODUCIBILITY SCORECARD (METABASE KPI CARDS)
-- =====================================================================================


-- =====================================================================================
-- PHẦN 0: KIỂM TRA TÍNH TOÀN VẸN VÀ BÁO CÁO CHẤT LƯỢNG DỮ LIỆU (DQ VERIFICATION)
-- Mục tiêu: Đối soát chỉ số chất lượng từ bảng dq_report do Job 1 & Job 2a ghi nhận.
-- =====================================================================================

-- [0.1] Xem toàn bộ chỉ số chất lượng dữ liệu được ghi nhận trong Serving Layer
-- Bảng nguồn: dq_report (TV3 Job 2a & TV2 Job 1)
SELECT 
    metric_name,
    metric_value,
    detail,
    measured_at
FROM dq_report
ORDER BY measured_at DESC, metric_name ASC;

-- [0.2] Kiểm tra dung lượng lưu trữ và số dòng thực tế của các bảng Serving Layer
SELECT 
    table_name,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS total_size,
    pg_size_pretty(pg_relation_size(quote_ident(table_name))) AS data_size,
    pg_size_pretty(pg_total_relation_size(quote_ident(table_name)) - pg_relation_size(quote_ident(table_name))) AS index_size
FROM information_schema.tables
WHERE table_schema = 'public' 
  AND table_type = 'BASE TABLE'
  AND table_name IN (
      'agg_category_year',
      'agg_author',
      'agg_publisher',
      'agg_reviewer_segment',
      'agg_price_band',
      'agg_review_length_band',
      'top_helpful_reviews',
      'book_summary',
      'agg_activity_heatmap',
      'dq_report'
  )
ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;


-- =====================================================================================
-- PHẦN 1: CÂU HỎI 1 (Q1) - ĐIỂM TRUNG BÌNH VÀ SỐ LƯỢNG REVIEW THEO THỂ LOẠI & THEO NĂM
-- Mục tiêu nghiệp vụ:
--   - Xác định các thể loại sách thu hút độc giả nhiều nhất trên Amazon Books.
--   - Theo dõi xu hướng tăng trưởng review qua các năm (1997 - 2014) của từng thể loại.
--   - Đo lường mức độ hài lòng của độc giả (avg_score) theo thể loại và năm.
-- Bảng nguồn: agg_category_year (main_category, review_year, total_reviews, total_books, avg_score)
-- Visualization gợi ý trên Metabase:
--   * Multi-series Line Chart: Trục X (review_year), Trục Y (total_reviews), Nhóm (main_category)
--   * Bar Chart: Top 10 thể loại sách có lượng review lớn nhất toàn sàn
-- =====================================================================================

-- [Q1.1] Top 10 thể loại sách có tổng lượng review lớn nhất toàn thời gian
SELECT 
    main_category,
    SUM(total_reviews) AS total_reviews,
    ROUND(SUM(total_reviews * avg_score)::numeric / NULLIF(SUM(total_reviews), 0), 2) AS weighted_avg_score,
    SUM(total_books) AS total_books_catalog,
    MIN(review_year) AS earliest_review_year,
    MAX(review_year) AS latest_review_year
FROM agg_category_year
WHERE main_category IS NOT NULL 
  AND TRIM(main_category) <> '' 
  AND main_category NOT IN ('Unknown', 'None')
GROUP BY main_category
ORDER BY total_reviews DESC
LIMIT 10;

-- [Q1.2] Xu hướng số lượng review và điểm đánh giá qua từng năm của Top 5 thể loại chính
-- Phục vụ trực tiếp cho biểu đồ đường (Multi-series Line Chart) trên Metabase Dashboard
WITH top_5_genres AS (
    SELECT main_category
    FROM agg_category_year
    WHERE main_category IS NOT NULL 
      AND TRIM(main_category) <> '' 
      AND main_category NOT IN ('Unknown', 'None')
    GROUP BY main_category
    ORDER BY SUM(total_reviews) DESC
    LIMIT 5
)
SELECT 
    acy.review_year,
    acy.main_category,
    acy.total_reviews,
    ROUND(acy.avg_score::numeric, 2) AS avg_score,
    acy.total_books
FROM agg_category_year acy
JOIN top_5_genres t5 ON acy.main_category = t5.main_category
WHERE acy.review_year >= 1997 AND acy.review_year <= 2014
ORDER BY acy.main_category, acy.review_year ASC;

-- [Q1.3] Top 15 thể loại có điểm đánh giá trung bình cao nhất (Lọc >= 5.000 reviews để tránh bias)
SELECT 
    main_category,
    SUM(total_reviews) AS total_reviews,
    ROUND(SUM(total_reviews * avg_score)::numeric / NULLIF(SUM(total_reviews), 0), 2) AS weighted_avg_score,
    COUNT(DISTINCT review_year) AS active_years_count
FROM agg_category_year
WHERE main_category IS NOT NULL 
  AND TRIM(main_category) <> '' 
  AND main_category NOT IN ('Unknown', 'None')
GROUP BY main_category
HAVING SUM(total_reviews) >= 5000
ORDER BY weighted_avg_score DESC, total_reviews DESC
LIMIT 15;

-- [Q1.4] Tốc độ tăng trưởng thị trường hàng năm (Annual Market Growth Overview)
SELECT 
    review_year,
    COUNT(DISTINCT main_category) AS active_categories_count,
    SUM(total_books) AS active_books_count,
    SUM(total_reviews) AS annual_reviews_count,
    ROUND(SUM(total_reviews * avg_score)::numeric / NULLIF(SUM(total_reviews), 0), 2) AS annual_avg_score
FROM agg_category_year
WHERE review_year >= 1997 AND review_year <= 2014
GROUP BY review_year
ORDER BY review_year ASC;


-- =====================================================================================
-- PHẦN 2: CÂU HỎI 2 (Q2) - TOP TÁC GIẢ VÀ NHÀ XUẤT BẢN UY TÍN
-- Mục tiêu nghiệp vụ:
--   - Tìm kiếm các tác giả có uy tín văn học cao nhất dựa trên điểm đánh giá từ cộng đồng.
--   - Thiết lập ngưỡng lọc tối thiểu (MIN_REVIEWS_Q2 = 30 hoặc 300) nhằm loại trừ hiện tượng
--     tác giả chỉ có 1 cuốn sách nhận 1 review 5 sao (Sample Bias).
--   - Xếp hạng Nhà xuất bản (Publishers) dựa trên cả quy mô xuất bản và chất lượng tác phẩm.
-- Bảng nguồn: 
--   * agg_author (main_author, total_reviews, total_books, avg_score, avg_helpful_votes)
--   * agg_publisher (publisher, total_reviews, total_books, avg_score)
-- Visualization gợi ý trên Metabase:
--   * Leaderboard Table có thanh tiến trình (Score Progress Bar)
--   * Scatter Plot: Trục X (total_reviews), Trục Y (avg_score), Kích thước bong bóng (total_books)
-- =====================================================================================

-- [Q2.1] Top 20 Tác giả uy tín nhất thị trường (Yêu cầu: >= 300 reviews & >= 3 đầu sách)
SELECT 
    main_author AS author,
    total_books,
    total_reviews,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND(avg_helpful_votes::numeric, 2) AS avg_helpful_votes,
    ROUND((total_reviews::numeric / NULLIF(total_books, 0)), 1) AS avg_reviews_per_book
FROM agg_author
WHERE main_author IS NOT NULL 
  AND TRIM(main_author) <> '' 
  AND main_author NOT IN ('Unknown', 'Anonymous', 'Various', 'None', 'n/a')
  AND total_reviews >= 300
  AND total_books >= 3
ORDER BY avg_score DESC, total_reviews DESC
LIMIT 20;

-- [Q2.2] Top 15 Tác giả có sức ảnh hưởng lớn nhất (Nhiều lượt review nhất toàn sàn)
-- Phản ánh các đại văn hào và tác giả best-seller như Tolkien, Dickens, Rowling, Stephen King
SELECT 
    main_author AS author,
    total_books,
    total_reviews,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND(avg_helpful_votes::numeric, 2) AS avg_helpful_votes
FROM agg_author
WHERE main_author IS NOT NULL 
  AND TRIM(main_author) <> '' 
  AND main_author NOT IN ('Unknown', 'Anonymous', 'Various')
ORDER BY total_reviews DESC
LIMIT 15;

-- [Q2.3] Top 15 Nhà xuất bản (Publishers) uy tín nhất (Yêu cầu: >= 1.000 reviews & >= 10 đầu sách)
SELECT 
    publisher,
    total_books,
    total_reviews,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND((total_reviews::numeric / NULLIF(total_books, 0)), 1) AS avg_reviews_per_title
FROM agg_publisher
WHERE publisher IS NOT NULL 
  AND TRIM(publisher) <> '' 
  AND publisher NOT IN ('Unknown', 'None', 'n/a')
  AND total_reviews >= 1000
  AND total_books >= 10
ORDER BY avg_score DESC, total_reviews DESC
LIMIT 15;

-- [Q2.4] Phân tích phân nhóm quy mô Nhà xuất bản (Major vs Mid-size vs Small vs Indie)
SELECT 
    CASE 
        WHEN total_books >= 100 THEN '1. Major Publisher (>= 100 titles)'
        WHEN total_books BETWEEN 20 AND 99 THEN '2. Mid-size Publisher (20-99 titles)'
        WHEN total_books BETWEEN 5 AND 19 THEN '3. Small Publisher (5-19 titles)'
        ELSE '4. Boutique / Indie (< 5 titles)'
    END AS publisher_scale,
    COUNT(*) AS publisher_count,
    SUM(total_books) AS total_titles_published,
    SUM(total_reviews) AS total_reviews_generated,
    ROUND(AVG(avg_score)::numeric, 2) AS avg_rating,
    ROUND(SUM(total_reviews)::numeric / NULLIF(SUM(total_books), 0), 1) AS reviews_per_title
FROM agg_publisher
WHERE publisher IS NOT NULL AND TRIM(publisher) <> ''
GROUP BY 1
ORDER BY 1;


-- =====================================================================================
-- PHẦN 3: CÂU HỎI 3 (Q3) - TƯƠNG QUAN MỨC GIÁ & ĐỘ DÀI REVIEW VỚI ĐIỂM SỐ ĐÁNH GIÁ
-- Mục tiêu nghiệp vụ:
--   - Kiểm tra giả thuyết: Sách giá cao độc giả có đánh giá khắt khe hơn sách giá rẻ không?
--   - Đo lường mối liên hệ giữa độ dài bài review (review_length) với điểm số và độ hữu ích.
--   - Đối soát đặc thù dữ liệu: Dữ liệu giá bị khuyết ~85%+ trong raw dataset, do đó TV2
--     đã chia thành 2 phân tích song song: agg_price_band và agg_review_length_band.
-- Bảng nguồn:
--   * agg_price_band (price_band, total_reviews, avg_score, avg_helpful_votes, avg_actual_price)
--   * agg_review_length_band (length_band, total_reviews, avg_score, avg_helpful_votes, avg_length)
-- Visualization gợi ý trên Metabase:
--   * Grouped Bar Chart: Trục X (price_band / length_band), Trục Y1 (total_reviews), Trục Y2 (avg_score)
-- =====================================================================================

-- [Q3.1] Bảng tổng hợp tương quan phân khúc giá sách (Price Bands)
SELECT 
    price_band,
    total_reviews,
    ROUND((total_reviews::numeric / SUM(total_reviews) OVER ()) * 100, 2) AS review_share_pct,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND(avg_helpful_votes::numeric, 2) AS avg_helpful_votes,
    ROUND(avg_actual_price::numeric, 2) AS avg_actual_price
FROM agg_price_band
ORDER BY price_band ASC;

-- [Q3.2] Bảng tổng hợp tương quan theo dải độ dài bài viết (Review Length Bands)
-- Phát hiện xu hướng: Bài viết dài (>600 ký tự) nhận được số lượt vote hữu ích cao hơn gấp nhiều lần
SELECT 
    length_band,
    total_reviews,
    ROUND((total_reviews::numeric / SUM(total_reviews) OVER ()) * 100, 2) AS review_share_pct,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND(avg_helpful_votes::numeric, 2) AS avg_helpful_votes,
    ROUND(avg_length::numeric, 0) AS avg_char_length
FROM agg_review_length_band
ORDER BY length_band ASC;

-- [Q3.3] So sánh tỷ lệ tương quan giữa Độ dài review và Lượt bình chọn hữu ích
SELECT 
    length_band,
    total_reviews,
    avg_score,
    avg_helpful_votes,
    ROUND((avg_helpful_votes / (SELECT avg_helpful_votes FROM agg_review_length_band WHERE length_band LIKE '1.%'))::numeric, 2) AS helpfulness_multiplier_vs_short
FROM agg_review_length_band
ORDER BY length_band ASC;


-- =====================================================================================
-- PHẦN 4: CÂU HỎI 4 (Q4) - TOP CÁC REVIEW HỮU ÍCH NHẤT MỖI NĂM (WINDOW FUNCTION)
-- Mục tiêu nghiệp vụ:
--   - Khai thác tính năng Helpful Votes để tìm ra những bài bình luận có giá trị nhất mỗi năm.
--   - Được tạo bởi TV2 trong Job 2b bằng Window Function:
--     Window.partitionBy("review_year").orderBy(desc("helpful_votes"), desc("score"))
-- Bảng nguồn: top_helpful_reviews (review_year, rank, title, user_id, profile_name, score, helpful_votes, total_votes, helpfulness_ratio, review_summary)
-- Visualization gợi ý trên Metabase:
--   * Detail Table Widget có bộ lọc Dropdown theo review_year
-- =====================================================================================

-- [Q4.1] Top 3 review hữu ích nhất mỗi năm (Được trích xuất từ kết quả Window Function)
SELECT 
    review_year,
    rank AS year_rank,
    title,
    profile_name,
    score,
    helpful_votes,
    total_votes,
    ROUND((helpfulness_ratio * 100)::numeric, 1) AS helpful_ratio_pct,
    review_summary
FROM top_helpful_reviews
WHERE rank <= 3
ORDER BY review_year DESC, rank ASC;

-- [Q4.2] Top 20 bài review có lượt bình chọn hữu ích cao nhất toàn thời gian trên hệ thống
SELECT 
    title,
    review_year,
    profile_name,
    score,
    helpful_votes,
    total_votes,
    ROUND((helpfulness_ratio * 100)::numeric, 1) AS helpful_ratio_pct,
    review_summary
FROM top_helpful_reviews
ORDER BY helpful_votes DESC, total_votes DESC
LIMIT 20;

-- [Q4.3] Các bài review đạt tỷ lệ đồng thuận tuyệt đối (Helpful Ratio >= 90% với total_votes >= 50)
SELECT 
    title,
    review_year,
    profile_name,
    score,
    helpful_votes,
    total_votes,
    ROUND((helpfulness_ratio * 100)::numeric, 1) AS helpful_pct,
    review_summary
FROM top_helpful_reviews
WHERE total_votes >= 50 AND helpfulness_ratio >= 0.90
ORDER BY helpful_votes DESC
LIMIT 15;


-- =====================================================================================
-- PHẦN 5: CÂU HỎI 5 (Q5) - DANH SÁCH SÁCH CÓ NHIỀU REVIEW NHƯNG ĐIỂM THẤP (CẢNH BÁO CHẤT LƯỢNG)
-- Mục tiêu nghiệp vụ:
--   - Nhận diện các tựa sách thu hút lượng thảo luận lớn nhưng gây thất vọng / tranh cãi nhất.
--   - Lọc các đầu sách có total_reviews >= 30 hoặc >= 100 và sắp xếp theo avg_score tăng dần.
-- Bảng nguồn: book_summary (title, title_norm, total_reviews, avg_score, avg_helpful_votes, min_score, max_score)
-- Visualization gợi ý trên Metabase:
--   * Alert Table Widget: Bảng cảnh báo chất lượng sách kèm conditional formatting đỏ
-- =====================================================================================

-- [Q5.1] Top 20 cuốn sách có nhiều review nhưng điểm số trung bình thấp nhất (Gây tranh cãi / Thất vọng nhất)
SELECT 
    title,
    total_reviews,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND(avg_helpful_votes::numeric, 1) AS avg_helpful_votes,
    min_score,
    max_score
FROM book_summary
WHERE total_reviews >= 30
ORDER BY avg_score ASC, total_reviews DESC
LIMIT 20;

-- [Q5.2] Danh sách các tựa sách nổi tiếng (> 100 reviews) nhưng nhận đánh giá kém (avg_score <= 3.20)
SELECT 
    title,
    total_reviews,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND(avg_helpful_votes::numeric, 1) AS avg_helpful_votes,
    min_score,
    max_score
FROM book_summary
WHERE total_reviews >= 100 AND avg_score <= 3.20
ORDER BY avg_score ASC, total_reviews DESC
LIMIT 20;

-- [Q5.3] Những đầu sách có độ phân cực đánh giá lớn nhất (Biên độ điểm min = 1, max = 5 và review lớn)
SELECT 
    title,
    total_reviews,
    ROUND(avg_score::numeric, 2) AS avg_score,
    (max_score - min_score) AS score_spread,
    ROUND(avg_helpful_votes::numeric, 1) AS avg_helpful_votes
FROM book_summary
WHERE total_reviews >= 100 AND min_score = 1.0 AND max_score = 5.0
ORDER BY total_reviews DESC
LIMIT 15;


-- =====================================================================================
-- PHẦN 6: CÂU HỎI 6 (Q6) - PHÂN KHÚC NGƯỜI ĐÁNH GIÁ (REVIEWER SEGMENTATION)
-- Mục tiêu nghiệp vụ:
--   - Phân khúc tập người dùng theo tần suất đóng góp đánh giá (Loyalty / Activity Tiers).
--   - Đo lường sự khác biệt về hành vi đánh giá giữa các tầng người dùng.
--
-- SỐ LIỆU ĐỐI SOÁT CHÍNH XÁC XÁC NHẬN BỞI TV3 & TV2 (reports/tv3_analytics_serving.md):
--   * Tổng review sạch               : 2.430.902 dòng (100.00%)
--   * Nhóm CÓ User_id (4 phân khúc)  : 2.017.997 dòng ( 83.01%)
--   * Nhóm KHUYẾT User_id            :   412.905 dòng ( 16.99%)
--
-- Bảng nguồn: agg_reviewer_segment (review_count_band, total_users, total_reviews, avg_score, avg_helpful_votes)
-- Visualization gợi ý trên Metabase:
--   * Donut Chart: Tỷ lệ phân bố số lượng review giữa các tầng người dùng
--   * Grouped Bar Chart: So sánh Điểm TB và Lượt vote hữu ích TB giữa các tầng
-- =====================================================================================

-- [Q6.1] Bảng tổng hợp 4 phân khúc người dùng có User_id (Từ bảng agg_reviewer_segment)
SELECT 
    review_count_band,
    total_users,
    total_reviews,
    ROUND((total_reviews::numeric / SUM(total_reviews) OVER ()) * 100, 2) AS pct_of_identified_reviews,
    ROUND((total_reviews::numeric / 2430902) * 100, 2) AS pct_of_total_clean_reviews,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND(avg_helpful_votes::numeric, 2) AS avg_helpful_votes,
    ROUND((total_reviews::numeric / NULLIF(total_users, 0)), 1) AS avg_reviews_per_user
FROM agg_reviewer_segment
ORDER BY review_count_band ASC;

-- [Q6.2] Bảng phân khúc toàn diện 5 nhóm (Bao gồm cả nhóm 412.905 reviews thiếu User_id ~ 16.99%)
SELECT 
    review_count_band AS segment_name,
    total_users,
    total_reviews,
    ROUND((total_reviews::numeric / 2430902) * 100, 2) AS review_share_pct,
    ROUND(avg_score::numeric, 2) AS avg_score,
    ROUND(avg_helpful_votes::numeric, 2) AS avg_helpful_votes
FROM agg_reviewer_segment
UNION ALL
SELECT 
    '5. Missing / Anonymous User_id' AS segment_name,
    NULL AS total_users,
    2430902 - (SELECT SUM(total_reviews) FROM agg_reviewer_segment) AS total_reviews,
    ROUND(((2430902 - (SELECT SUM(total_reviews) FROM agg_reviewer_segment))::numeric / 2430902) * 100, 2) AS review_share_pct,
    NULL AS avg_score,
    NULL AS avg_helpful_votes
ORDER BY segment_name ASC;

-- [Q6.3] Truy vấn kiểm tra tính toàn vẹn (Checksum & Integrity Verification Query)
-- Đảm bảo tổng số review của 4 nhóm có ID + nhóm thiếu ID phải khớp chính xác 2.430.902 dòng
SELECT 
    (SELECT SUM(total_reviews) FROM agg_reviewer_segment) AS identified_reviews_count,
    2430902 - (SELECT SUM(total_reviews) FROM agg_reviewer_segment) AS missing_userid_reviews_count,
    2430902 AS grand_total_clean_reviews,
    ROUND(((SELECT SUM(total_reviews) FROM agg_reviewer_segment)::numeric / 2430902) * 100, 2) AS identified_pct,
    ROUND(((2430902 - (SELECT SUM(total_reviews) FROM agg_reviewer_segment))::numeric / 2430902) * 100, 2) AS missing_pct,
    CASE 
        WHEN (SELECT SUM(total_reviews) FROM agg_reviewer_segment) = 2017997
         AND (2430902 - (SELECT SUM(total_reviews) FROM agg_reviewer_segment)) = 412905
        THEN 'PASSED: DATA MATCHES 100% PRODUCTION BENCHMARK (2,017,997 / 412,905)'
        ELSE 'FAILED: AUDIT MISMATCH'
    END AS audit_status;

-- [Q6.4] Phân tích mức độ tương tác và độ tín nhiệm cộng đồng theo tầng người dùng
-- Nhận xét: Top reviewer (>20 review) nhận trung bình 7.25 helpful votes/review so với 4.37 của nhóm 1-time
SELECT 
    review_count_band,
    total_users,
    total_reviews,
    avg_score,
    avg_helpful_votes,
    ROUND((avg_helpful_votes / (SELECT avg_helpful_votes FROM agg_reviewer_segment WHERE review_count_band LIKE '1.%'))::numeric, 2) AS helpful_votes_multiplier_vs_onetime
FROM agg_reviewer_segment
ORDER BY review_count_band ASC;


-- =====================================================================================
-- PHẦN 7: HEATMAP HOẠT ĐỘNG - THỐNG KÊ REVIEW THEO THÁNG x THỨ TRONG TUẦN
-- Mục tiêu nghiệp vụ:
--   - Nhận diện chu kỳ đánh giá của độc giả trong năm và theo các ngày trong tuần.
--   - Lưu ý kỹ thuật: Dữ liệu Amazon Books thô chỉ lưu ngày, không có trường giờ hợp lệ,
--     do đó phân tích theo Ma trận Tháng (1-12) x Thứ trong tuần (Thứ Hai - Chủ Nhật).
-- Bảng nguồn: agg_activity_heatmap (review_month, month_name, day_of_week, day_name, total_reviews, avg_score)
-- Visualization gợi ý trên Metabase:
--   * Pivot Table / Heatmap Matrix: Trục Y (Tháng), Trục X (Thứ), Màu ô (Số lượng review)
-- =====================================================================================

-- [Q7.1] Truy vấn danh sách phẳng phục vụ trực tiếp biểu đồ Heatmap trên Metabase
SELECT 
    review_month,
    month_name,
    day_of_week,
    day_name,
    total_reviews,
    ROUND(avg_score::numeric, 2) AS avg_score
FROM agg_activity_heatmap
ORDER BY review_month ASC, day_of_week ASC;

-- [Q7.2] Bảng Ma trận Xoay (Pivoted Cross-Tabulation Matrix) bằng SQL thuần
-- Hàng: Tháng (Tháng 1 -> Tháng 12)
-- Cột: Thứ trong tuần (Thứ Hai -> Chủ Nhật; quy chuẩn Spark: 1=CN, 2=T2, ..., 7=T7)
SELECT 
    review_month,
    month_name,
    SUM(CASE WHEN day_of_week = 2 THEN total_reviews ELSE 0 END) AS monday_reviews,
    SUM(CASE WHEN day_of_week = 3 THEN total_reviews ELSE 0 END) AS tuesday_reviews,
    SUM(CASE WHEN day_of_week = 4 THEN total_reviews ELSE 0 END) AS wednesday_reviews,
    SUM(CASE WHEN day_of_week = 5 THEN total_reviews ELSE 0 END) AS thursday_reviews,
    SUM(CASE WHEN day_of_week = 6 THEN total_reviews ELSE 0 END) AS friday_reviews,
    SUM(CASE WHEN day_of_week = 7 THEN total_reviews ELSE 0 END) AS saturday_reviews,
    SUM(CASE WHEN day_of_week = 1 THEN total_reviews ELSE 0 END) AS sunday_reviews,
    SUM(total_reviews) AS total_month_reviews,
    ROUND(SUM(total_reviews * avg_score) / NULLIF(SUM(total_reviews), 0), 2) AS month_avg_score
FROM agg_activity_heatmap
GROUP BY review_month, month_name
ORDER BY review_month ASC;

-- [Q7.3] Tìm các "Điểm nóng hoạt động" (Top 5 Peak Activity Windows trong năm)
SELECT 
    month_name,
    day_name,
    total_reviews,
    ROUND((total_reviews::numeric / SUM(total_reviews) OVER ()) * 100, 2) AS global_activity_pct,
    ROUND(avg_score::numeric, 2) AS avg_score
FROM agg_activity_heatmap
ORDER BY total_reviews DESC
LIMIT 5;


-- =====================================================================================
-- PHẦN 8: BẢNG CHỈ SỐ TỔNG HỢP VÀ THẺ KPI METABASE DASHBOARD (EXECUTIVE KPI CARDS)
-- Mục tiêu: Tạo các chỉ số KPI Card hiển thị trên hàng đầu của Metabase Dashboard.
-- =====================================================================================

-- [KPI.1] Thẻ chỉ số tổng quan điều hành (Executive Summary Cards)
SELECT 
    2430902 AS total_clean_reviews_catalog,
    (SELECT SUM(total_reviews) FROM agg_reviewer_segment) AS identified_user_reviews,
    (SELECT COUNT(DISTINCT main_author) FROM agg_author WHERE main_author IS NOT NULL AND main_author NOT IN ('Unknown', 'Anonymous')) AS unique_authors_catalog,
    (SELECT COUNT(DISTINCT publisher) FROM agg_publisher WHERE publisher IS NOT NULL AND publisher <> 'Unknown') AS unique_publishers_catalog,
    (SELECT COUNT(DISTINCT main_category) FROM agg_category_year WHERE main_category IS NOT NULL AND main_category <> 'Unknown') AS catalog_genres_count,
    (SELECT ROUND(SUM(total_reviews * avg_score)::numeric / SUM(total_reviews), 2) FROM agg_reviewer_segment) AS platform_overall_avg_rating;

-- [KPI.2] Thống kê tổng số lượng review sách có giá niêm yết (Priced Books Coverage)
SELECT 
    SUM(total_reviews) AS total_priced_reviews,
    ROUND(AVG(avg_score)::numeric, 2) AS priced_books_avg_score,
    ROUND(AVG(avg_actual_price)::numeric, 2) AS platform_average_book_price
FROM agg_price_band;

-- =====================================================================================
-- KẾT THÚC FILE QUERIES.SQL
-- TV4 QA Note: File truy vấn này hoàn toàn độc lập, tương thích 100% chuẩn PostgreSQL 16
-- và sẵn sàng kết nối trực tiếp vào Metabase Native Query Editor.
-- =====================================================================================
