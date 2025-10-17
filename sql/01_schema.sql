CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_user (
  user_id        BIGINT PRIMARY KEY,
  user_name      TEXT,
  user_email     TEXT,
  created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_category (
  category_id    BIGINT PRIMARY KEY,
  category_name  TEXT NOT NULL,
  created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_sla (
  sla_id         BIGINT PRIMARY KEY,
  sla_name       TEXT NOT NULL,
  target_hours   NUMERIC(10,2),
  created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.fact_ticket (
  ticket_id      BIGINT PRIMARY KEY,
  user_id        BIGINT REFERENCES dw.dim_user(user_id),
  category_id    BIGINT REFERENCES dw.dim_category(category_id),
  sla_id         BIGINT REFERENCES dw.dim_sla(sla_id),
  status         TEXT,
  priority       TEXT,
  created_time   TIMESTAMPTZ,
  resolved_time  TIMESTAMPTZ,
  subject        TEXT,
  description    TEXT,
  response_time_min   INT,
  resolution_time_min INT,
  is_sla_breached     BOOLEAN,
  load_dts       TIMESTAMPTZ DEFAULT now()
);
