CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.fact_reviews (
  review_id TEXT PRIMARY KEY,
  username TEXT,
  rating INT,
  review_text TEXT,
  date TIMESTAMPTZ,
  sentiment TEXT,
  load_dts TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.etl_run_log (
  run_id SERIAL PRIMARY KEY,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  rows_loaded INT,
  dq_issues TEXT,
  status TEXT,
  load_dts TIMESTAMPTZ DEFAULT now()
);


-- 1️⃣ Dimension: Sentiment
CREATE TABLE IF NOT EXISTS dw.dim_sentiment (
    sentiment_id SERIAL PRIMARY KEY,
    sentiment_name TEXT UNIQUE,     -- 'positive', 'neutral', 'negative'
    sentiment_label TEXT,           -- 'Positif', 'Netral', 'Negatif'
    sentiment_color TEXT,           -- untuk visualisasi (#4CAF50, #FFC107, #F44336)
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 2️⃣ Dimension: Rating
CREATE TABLE IF NOT EXISTS dw.dim_rating (
    rating_id SERIAL PRIMARY KEY,
    rating_value INT UNIQUE,        -- 1 sampai 5
    rating_label TEXT,              -- 'Sangat Buruk', 'Buruk', 'Netral', 'Baik', 'Sangat Baik'
    satisfaction_level TEXT,        -- 'low', 'medium', 'high'
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 3️⃣ Update Fact Reviews agar ada foreign key
ALTER TABLE IF EXISTS dw.fact_reviews
ADD COLUMN IF NOT EXISTS sentiment_id INT REFERENCES dw.dim_sentiment(sentiment_id),
ADD COLUMN IF NOT EXISTS rating_id INT REFERENCES dw.dim_rating(rating_id);