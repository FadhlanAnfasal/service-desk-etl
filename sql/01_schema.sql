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


CREATE TABLE IF NOT EXISTS dw.dim_sentiment (
    sentiment_id SERIAL PRIMARY KEY,
    sentiment_name TEXT UNIQUE,     
    sentiment_label TEXT,           
    sentiment_color TEXT,           
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_rating (
    rating_id SERIAL PRIMARY KEY,
    rating_value INT UNIQUE,        
    rating_label TEXT,              
    satisfaction_level TEXT,        
    created_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE IF EXISTS dw.fact_reviews
ADD COLUMN IF NOT EXISTS sentiment_id INT REFERENCES dw.dim_sentiment(sentiment_id),
ADD COLUMN IF NOT EXISTS rating_id INT REFERENCES dw.dim_rating(rating_id);

ALTER TABLE dw.fact_reviews
ADD COLUMN day_name TEXT,
ADD COLUMN day_number INT;

UPDATE dw.fact_reviews
SET 
    day_name = TRIM(TO_CHAR(date, 'Day')),
    day_number = EXTRACT(ISODOW FROM date);

INSERT INTO dw.dim_rating (rating_value, rating_label, satisfaction_level)
VALUES
    (1, 'Terrible', 'Sampah'),
    (2, 'Poor', 'Kureng'),
    (3, 'Average', 'Medioker'),
    (4, 'Good', 'Cakep'),
    (5, 'Excellent', 'Top Markotop')
ON CONFLICT (rating_value) DO NOTHING;

ALTER TABLE dw.fact_reviews
ADD COLUMN rating_label TEXT;

UPDATE dw.fact_reviews fr
SET rating_label = dr.rating_label
FROM dw.dim_rating dr
WHERE fr.rating = dr.rating_value;

INSERT INTO dw.dim_sentiment (sentiment_name, sentiment_label, sentiment_color)
VALUES
    ( 1, 'Negative', 'red'),
    ( 2, 'Neutral', 'yellow'),
    ( 3, 'Positive', 'green');

SELECT
    f.*,
    d.sentiment_color
FROM dw.fact_reviews f
LEFT JOIN dw.dim_sentiment d
    ON f.sentiment = d.sentiment_label;

UPDATE fact_reviews f
SET sentiment_color = d.sentiment_color
FROM dim_sentiment d
WHERE f.sentiment = d.sentiment_label;