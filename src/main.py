from .extract import fetch_source_data
from .transform import transform_to_dw_struct
from .dq import dq_checks, check_threshold
from .load import upsert_frame
from .logger import get_logger
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import traceback

log = get_logger(__name__)
load_dotenv()


def run(tag=None):
    start_time = datetime.now()
    log.info(f"===== Starting ETL Run ({tag}) =====")
    dq_issues_str = ""
    rows_loaded = 0
    status = "SUCCESS"

    DB_USER = os.getenv("DB_USER", "etl_user")
    DB_PASS = os.getenv("DB_PASS", "etl_pass")
    DB_NAME = os.getenv("DB_NAME", "servicedesk_dw")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    try:
        raw = fetch_source_data()
        log.info(f"Fetched {len(raw)} raw records")

        tables = transform_to_dw_struct(raw)
        dq_issues = dq_checks(tables)
        dq_issues_str = ", ".join(dq_issues) if dq_issues else "None"
        log.info(f"DQ Checks: {dq_issues_str}")

        expected_min, expected_max = 10, 2000
        row_count = len(tables["fact_ticket"])
        if not (expected_min <= row_count <= expected_max):
            log.warning(f"Row count anomaly: {row_count} rows (expected {expected_min}-{expected_max})")

        upsert_frame(tables["dim_user"], "dim_user", ["user_id"])
        upsert_frame(tables["dim_category"], "dim_category", ["category_id"])
        upsert_frame(tables["dim_sla"], "dim_sla", ["sla_id"])
        upsert_frame(tables["fact_ticket"], "fact_ticket", ["ticket_id"])
        rows_loaded = row_count

    except Exception as e:
        status = "FAILED"
        dq_issues_str = traceback.format_exc()
        log.exception("ETL Failed")

    finally:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO dw.etl_run_log (started_at, finished_at, rows_loaded, dq_issues, status)
                VALUES (:start, now(), :rows, :issues, :status)
            """), {
                "start": start_time,
                "rows": rows_loaded,
                "issues": dq_issues_str,
                "status": status
            })
        log.info(f"ETL run complete with status={status}")

if __name__ == "__main__":
    tag = "[AUTO-CRON]" if os.getenv("CRON_RUN") else "MANUAL"
    run(tag)