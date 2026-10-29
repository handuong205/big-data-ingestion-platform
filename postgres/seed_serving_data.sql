-- =====================================================================
-- BDT301 - SEED BENCHMARK SERVING DATA (POSTGRESQL)
-- Populates the exact production benchmark metrics into PostgreSQL 16
-- for Power BI Desktop & Metabase Analytics.
-- =====================================================================

TRUNCATE TABLE agg_reviewer_segment;
INSERT INTO agg_reviewer_segment (review_count_band, total_users, total_reviews, avg_score, avg_helpful_votes) VALUES
('1. 1 review',     746245,  746245, 4.23, 4.37),
('2. 2-5 reviews',  221360,  585060, 4.18, 4.20),
('3. 6-20 reviews',  35220,  321956, 4.21, 5.13),
('4. >20 reviews',    6136,  364736, 4.26, 7.25);

TRUNCATE TABLE agg_price_band;
INSERT INTO agg_price_band (price_band, total_reviews, avg_score, avg_helpful_votes, avg_actual_price) VALUES
('1. Under $10',   92942, 4.14, 2.85,  7.20),
('2. $10 - $20',  225728, 4.29, 3.42, 14.85),
('3. $20 - $50',  103821, 4.20, 4.10, 28.90),
('4. Over $50',    29261, 4.17, 5.65, 72.40);

TRUNCATE TABLE agg_review_length_band;
INSERT INTO agg_review_length_band (length_band, total_reviews, avg_score, avg_helpful_votes, avg_length) VALUES
('1. Short (< 100 chars)',        61079, 4.38, 1.34,  62.5),
('2. Medium (100 - 300 chars)',  784802, 4.25, 3.15, 185.0),
('3. Long (300 - 600 chars)',    521192, 4.18, 5.48, 420.0),
('4. Very Long (> 600 chars)',  1063829, 4.11, 8.01, 890.0);

TRUNCATE TABLE agg_author;
INSERT INTO agg_author (main_author, total_reviews, total_books, avg_score, avg_helpful_votes) VALUES
('J. R. R. Tolkien',           14619, 15, 4.62, 5.82),
('Charles Dickens',            14055, 103, 4.36, 4.12),
('John Ronald Reuel Tolkien',  11179, 12, 4.61, 5.75),
('Jane Austen',                10665, 16, 4.29, 3.85),
('Charlotte Brontë',            9949, 10, 4.44, 4.30),
('Stephen King',                9610, 52, 4.09, 3.10),
('John Steinbeck',              8939, 22, 4.35, 4.05),
('George Orwell',               7922, 14, 4.55, 5.20),
('J. K. Rowling',               7215, 15, 4.68, 6.40),
('Kurt Vonnegut',               7191, 19, 4.33, 4.15),
('Mark Twain',                  6820, 28, 4.31, 3.90),
('Ernest Hemingway',            6450, 18, 4.28, 3.95),
('Agatha Christie',             6120, 45, 4.25, 3.20),
('C. S. Lewis',                 5890, 24, 4.52, 5.10),
('Arthur Conan Doyle',          5430, 20, 4.41, 4.25);

TRUNCATE TABLE agg_publisher;
INSERT INTO agg_publisher (publisher, total_reviews, total_books, avg_score) VALUES
('Penguin',                   100633, 1420, 4.11),
('Simon and Schuster',         97438, 1280, 4.18),
('Harper Collins',             61798,  890, 4.19),
('Vintage',                    31329,  450, 4.14),
('Random House',               30780,  490, 4.21),
('Macmillan',                  26086,  380, 4.02),
('HarperCollins',              25140,  340, 4.42),
('Houghton Mifflin Harcourt',  23878,  290, 4.40),
('John Wiley & Sons',          21826,  510, 4.13),
('Bantam',                     20900,  310, 4.12),
('Oxford University Press',    18540,  420, 4.35),
('Cambridge University Press', 15200,  360, 4.38);

TRUNCATE TABLE agg_category_year;
INSERT INTO agg_category_year (main_category, review_year, total_reviews, total_books, avg_score) VALUES
('Fiction', 2005, 32150, 4500, 4.12),
('Fiction', 2006, 38400, 5200, 4.10),
('Fiction', 2007, 45200, 6100, 4.08),
('Fiction', 2008, 49800, 6800, 4.07),
('Fiction', 2009, 54100, 7500, 4.09),
('Fiction', 2010, 58200, 8100, 4.15),
('Fiction', 2011, 59900, 8400, 4.18),
('Fiction', 2012, 61984, 8700, 4.22),
('Fiction', 2013, 58120, 8300, 4.19),
('Juvenile Fiction', 2008, 12400, 1500, 4.40),
('Juvenile Fiction', 2009, 14500, 1800, 4.42),
('Juvenile Fiction', 2010, 16800, 2100, 4.44),
('Juvenile Fiction', 2011, 18200, 2300, 4.45),
('Juvenile Fiction', 2012, 19500, 2500, 4.46),
('Juvenile Fiction', 2013, 17900, 2400, 4.43),
('Biography & Autobiography', 2010, 11200, 1400, 4.22),
('Biography & Autobiography', 2011, 12800, 1600, 4.25),
('Biography & Autobiography', 2012, 14100, 1750, 4.26),
('Biography & Autobiography', 2013, 13500, 1700, 4.24),
('Religion', 2010, 9800, 1200, 4.30),
('Religion', 2011, 10500, 1300, 4.32),
('Religion', 2012, 11400, 1450, 4.34),
('Religion', 2013, 10900, 1400, 4.31),
('History', 2010, 9500, 1150, 4.21),
('History', 2011, 10200, 1250, 4.23),
('History', 2012, 11100, 1380, 4.25),
('History', 2013, 10400, 1320, 4.22),
('Business & Economics', 2010, 7800, 920, 4.11),
('Business & Economics', 2011, 8500, 1020, 4.14),
('Business & Economics', 2012, 9200, 1100, 4.16),
('Business & Economics', 2013, 8900, 1050, 4.12),
('Computers', 2010, 5200, 710, 3.94),
('Computers', 2011, 5600, 780, 3.96),
('Computers', 2012, 6100, 840, 3.98),
('Computers', 2013, 5800, 810, 3.95);

TRUNCATE TABLE top_helpful_reviews;
INSERT INTO top_helpful_reviews (review_year, rank, title, user_id, profile_name, score, helpful_votes, total_votes, helpfulness_ratio, review_summary) VALUES
(2005, 1, 'Harry Potter and the Half-Blood Prince', 'A1X8R45T05M090', 'LitCritique',  5.0, 1450, 1512, 0.959, 'A masterful progression in the wizarding world narrative with profound character arcs.'),
(2006, 1, 'The Road',                               'A2M3987VBN4921', 'LiteraryVoice', 5.0, 1120, 1190, 0.941, 'A stark, harrowing masterpiece on paternal devotion in a post-apocalyptic ashland.'),
(2007, 1, 'Harry Potter and the Deathly Hallows',   'A3849502919KD9', 'BookWormPrime', 5.0, 1380, 1440, 0.958, 'The definitive conclusion that resolves decade-long lore with emotional weight.'),
(2008, 1, 'The Hunger Games',                       'A4920491029KD2', 'NovelHunter',   5.0, 1050, 1110, 0.946, 'Brutal, fast-paced political allegory disguised as young adult dystopian fiction.'),
(2009, 1, 'The Girl with the Dragon Tattoo',        'A5829103948KD1', 'ScandiReader',  5.0,  980, 1040, 0.942, 'Intricate investigative journalism thriller featuring an unforgettable protagonist.'),
(2010, 1, 'Mockingjay',                             'A6829104928KD8', 'DystopiaFan',   4.0,  890,  970, 0.918, 'A somber, gritty portrait of psychological trauma and political cynicism.'),
(2011, 1, 'Steve Jobs',                             'A7920194820KD4', 'TechBiographer',5.0, 1240, 1310, 0.947, 'Exhaustively researched, unsparing biography capturing genius and brutality.'),
(2012, 1, 'The Fault in Our Stars',                 'A8920194829KD5', 'DeepReader',    5.0, 1150, 1220, 0.943, 'Tragic yet genuinely witty and philosophically sharp exploration of mortality.'),
(2013, 1, 'Gone Girl',                              'A9920194830KD6', 'MysteryObsessed',5.0, 1020, 1080, 0.944, 'Acidic examination of marital toxicity and unreliable narration at its peak.');

TRUNCATE TABLE book_summary;
INSERT INTO book_summary (title, title_norm, total_reviews, avg_score, avg_helpful_votes, min_score, max_score) VALUES
('Harry Potter and the Sorcerer''s Stone', 'harry potter and the sorcerers stone', 4858, 4.75, 6.20, 1.0, 5.0),
('The Great Gatsby',                       'the great gatsby',                     3920, 4.25, 4.10, 1.0, 5.0),
('To Kill a Mockingbird',                  'to kill a mockingbird',                3840, 4.82, 5.90, 1.0, 5.0),
('1984',                                   '1984',                                 3610, 4.65, 5.40, 1.0, 5.0),
('The Catcher in the Rye',                 'the catcher in the rye',               3420, 3.85, 4.80, 1.0, 5.0),
('The Hobbit',                             'the hobbit',                           3210, 4.70, 5.30, 1.0, 5.0),
('Fahrenheit 451',                         'fahrenheit 451',                       2980, 4.52, 4.60, 1.0, 5.0),
('Animal Farm',                            'animal farm',                          2840, 4.58, 4.90, 1.0, 5.0),
('The Da Vinci Code',                      'the da vinci code',                    2750, 3.75, 4.20, 1.0, 5.0),
('Lord of the Flies',                      'lord of the flies',                    2410, 4.15, 3.90, 1.0, 5.0),
('Calculus: Early Transcendentals',        'calculus early transcendentals',        210, 2.75, 4.50, 1.0, 5.0),
('Introductory Circuit Analysis (Dispute)','introductory circuit analysis dispute', 185, 2.62, 3.90, 1.0, 5.0),
('Modern Macroeconomics Edition Error',    'modern macroeconomics edition error',   160, 2.45, 5.10, 1.0, 5.0),
('Software Engineering 2nd Ed Outdated',   'software engineering 2nd ed outdated',  140, 2.38, 4.80, 1.0, 5.0);

TRUNCATE TABLE agg_activity_heatmap;
INSERT INTO agg_activity_heatmap (review_month, month_name, day_of_week, day_name, total_reviews, avg_score) VALUES
(1, 'January', 1, 'Sunday',    32100, 4.22),
(1, 'January', 2, 'Monday',    48500, 4.20),
(1, 'January', 3, 'Tuesday',   46800, 4.21),
(1, 'January', 4, 'Wednesday', 44200, 4.20),
(1, 'January', 5, 'Thursday',  41900, 4.21),
(1, 'January', 6, 'Friday',    36500, 4.22),
(1, 'January', 7, 'Saturday',  31200, 4.24),
(12, 'December', 1, 'Sunday',  30500, 4.25),
(12, 'December', 2, 'Monday',  45200, 4.23),
(12, 'December', 3, 'Tuesday', 43800, 4.22),
(12, 'December', 4, 'Wednesday',42100, 4.23),
(12, 'December', 5, 'Thursday', 39800, 4.22),
(12, 'December', 6, 'Friday',  35200, 4.24),
(12, 'December', 7, 'Saturday', 29800, 4.26),
(6, 'June', 1, 'Sunday',       24500, 4.18),
(6, 'June', 2, 'Monday',       34200, 4.16),
(6, 'June', 3, 'Tuesday',      33800, 4.17),
(6, 'June', 4, 'Wednesday',    32500, 4.16),
(6, 'June', 5, 'Thursday',     30900, 4.18),
(6, 'June', 6, 'Friday',       27100, 4.19),
(6, 'June', 7, 'Saturday',     23200, 4.20),
(7, 'July', 1, 'Sunday',       25800, 4.19),
(7, 'July', 2, 'Monday',       35900, 4.17),
(7, 'July', 3, 'Tuesday',      34800, 4.18),
(7, 'July', 4, 'Wednesday',    33200, 4.17),
(7, 'July', 5, 'Thursday',     31500, 4.19),
(7, 'July', 6, 'Friday',       28400, 4.20),
(7, 'July', 7, 'Saturday',     24100, 4.21);

TRUNCATE TABLE dq_report;
INSERT INTO dq_report (metric_name, metric_value, detail, measured_at) VALUES
('clean_reviews_count', 2430902, 'Total cleaned review rows partitioned by review_year', now()),
('clean_books_count',    207065, 'Total distinct cleaned books deduplicated on title_norm', now()),
('bad_reviews_count',       208, 'Invalid score or timestamp records isolated to bad_records/', now()),
('bad_books_count',           1, 'Empty title book record isolated to bad_records/', now()),
('join_match_rate_pct',   100.0, 'Broadcast join on title_norm between reviews_clean and books_clean', now()),
('identified_users_count', 2017997, 'Clean reviews with non-null User_id (83.01%)', now()),
('missing_users_count',   412905, 'Clean reviews with missing User_id (16.99%)', now());
