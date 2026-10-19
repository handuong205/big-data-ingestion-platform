-- =====================================================================
-- BDT301 - Serving Layer schema (PostgreSQL)
--
-- Owned by: TV3 (Spark Analytics & Serving Layer Lead)
-- Executed automatically by the postgres image on an EMPTY data volume
-- (mounted at /docker-entrypoint-initdb.d).
--
-- If the postgres_data volume already exists, apply manually:
--   docker compose exec -T postgres \
--     psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < postgres/init.sql
--
-- Tables written by Job 2a (TV3):
--   agg_category_year, agg_author, agg_publisher,
--   agg_reviewer_segment, dq_report
-- Tables written by Job 2b (TV2):
--   agg_price_band, agg_review_length_band,
--   top_helpful_reviews, book_summary
--
-- Column names/types MUST match the Spark DataFrames exactly: the JDBC
-- writer uses mode=overwrite + truncate=true, so rows are replaced but
-- this DDL (and these indexes) are preserved.
-- =====================================================================

-- ---------------------------------------------------------------- Job 2a (TV3)

CREATE TABLE IF NOT EXISTS agg_category_year (
    main_category   TEXT,
    review_year     INTEGER,
    total_reviews   BIGINT,
    total_books     BIGINT,
    avg_score       DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS agg_author (
    main_author       TEXT,
    total_reviews     BIGINT,
    total_books       BIGINT,
    avg_score         DOUBLE PRECISION,
    avg_helpful_votes DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS agg_publisher (
    publisher       TEXT,
    total_reviews   BIGINT,
    total_books     BIGINT,
    avg_score       DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS agg_reviewer_segment (
    review_count_band  TEXT,
    total_users        BIGINT,
    total_reviews      BIGINT,
    avg_score          DOUBLE PRECISION,
    avg_helpful_votes  DOUBLE PRECISION
);

-- Generic data-quality ledger. Job 2a appends join-match metrics; Job 1
-- (TV2) can add row-count metrics later without changing the table.
CREATE TABLE IF NOT EXISTS dq_report (
    metric_name   TEXT,
    metric_value  DOUBLE PRECISION,
    detail        TEXT,
    measured_at   TIMESTAMPTZ DEFAULT now()
);

-- ---------------------------------------------------------------- Job 2b (TV2)

CREATE TABLE IF NOT EXISTS agg_price_band (
    price_band        TEXT,
    total_reviews     BIGINT,
    avg_score         DOUBLE PRECISION,
    avg_helpful_votes DOUBLE PRECISION,
    avg_actual_price  DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS agg_review_length_band (
    length_band       TEXT,
    total_reviews     BIGINT,
    avg_score         DOUBLE PRECISION,
    avg_helpful_votes DOUBLE PRECISION,
    avg_length        DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS top_helpful_reviews (
    review_year       INTEGER,
    rank              INTEGER,
    title             TEXT,
    user_id           TEXT,
    profile_name      TEXT,
    score             DOUBLE PRECISION,
    helpful_votes     BIGINT,
    total_votes       BIGINT,
    helpfulness_ratio DOUBLE PRECISION,
    review_summary    TEXT
);

CREATE TABLE IF NOT EXISTS book_summary (
    title             TEXT,
    title_norm        TEXT,
    total_reviews     BIGINT,
    avg_score         DOUBLE PRECISION,
    avg_helpful_votes DOUBLE PRECISION,
    min_score         DOUBLE PRECISION,
    max_score         DOUBLE PRECISION
);

-- ---------------------------------------------------------------- Indexes
-- Keep these small: every serving table is an aggregate, not raw data.

CREATE INDEX IF NOT EXISTS idx_category_year
    ON agg_category_year (main_category, review_year);
CREATE INDEX IF NOT EXISTS idx_category_year_score
    ON agg_category_year (avg_score DESC);

CREATE INDEX IF NOT EXISTS idx_author_reviews
    ON agg_author (total_reviews DESC);
CREATE INDEX IF NOT EXISTS idx_author_score
    ON agg_author (avg_score DESC);

CREATE INDEX IF NOT EXISTS idx_publisher_reviews
    ON agg_publisher (total_reviews DESC);

CREATE INDEX IF NOT EXISTS idx_reviewer_segment
    ON agg_reviewer_segment (review_count_band);

CREATE INDEX IF NOT EXISTS idx_price_band
    ON agg_price_band (avg_score);
CREATE INDEX IF NOT EXISTS idx_review_length_band
    ON agg_review_length_band (avg_score);

CREATE INDEX IF NOT EXISTS idx_top_helpful
    ON top_helpful_reviews (review_year, rank);
CREATE INDEX IF NOT EXISTS idx_book_summary_score
    ON book_summary (avg_score, total_reviews DESC);

-- ---------------------------------------------------------------- Heatmap (TV4 / BI)

CREATE TABLE IF NOT EXISTS agg_activity_heatmap (
    review_month      INTEGER,
    month_name        TEXT,
    day_of_week       INTEGER, -- 1: Sunday, 2: Monday, ..., 7: Saturday (Spark convention)
    day_name          TEXT,
    total_reviews     BIGINT,
    avg_score         DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_activity_heatmap
    ON agg_activity_heatmap (review_month, day_of_week);
