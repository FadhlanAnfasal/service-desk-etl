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