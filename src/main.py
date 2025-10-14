from .extract import fetch_source_data
from .transform import transform_to_dw_struct
from .dq import dq_checks
from .load import upsert_frame
from .logger import get_logger
log = get_logger(__name__)

def run():
    raw = fetch_source_data()
    tables = transform_to_dw_struct(raw)
    issues = dq_checks(tables)
    if issues:
        log.warning(f"Data Quality issues: {issues}")
    # Load order: dims then fact
    upsert_frame(tables["dim_user"], "dim_user", ["user_id"])
    upsert_frame(tables["dim_category"], "dim_category", ["category_id"])
    upsert_frame(tables["dim_sla"], "dim_sla", ["sla_id"])
    upsert_frame(tables["fact_ticket"], "fact_ticket", ["ticket_id"])

if __name__ == "__main__":
    run()
