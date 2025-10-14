import pandas as pd
from .logger import get_logger
log = get_logger(__name__)

def transform_to_dw_struct(raw: list[dict]) -> dict[str, pd.DataFrame]:
    df = pd.DataFrame(raw)

    # For demo (jsonplaceholder posts) -> map to ticket-like
    df = df.rename(columns={
        "id": "ticket_id",
        "userId": "user_id",
        "title": "subject",
        "body": "description"
    })
    df["status"] = "closed"
    df["priority"] = "normal"
    df["created_time"] = pd.Timestamp.utcnow().normalize()
    df["resolved_time"] = df["created_time"] + pd.to_timedelta(120, unit="m")
    df["response_time_min"] = 15
    df["resolution_time_min"] = 120
    df["is_sla_breached"] = df["resolution_time_min"] > 90

    # Dimensions
    dim_user = (df[["user_id"]]
                .drop_duplicates()
                .assign(user_name=lambda x: "user_" + x["user_id"].astype(str),
                        user_email=lambda x: x["user_id"].astype(str) + "@example.com"))

    dim_category = pd.DataFrame({
        "category_id": [1],
        "category_name": ["General"]
    })
    dim_sla = pd.DataFrame({
        "sla_id": [1],
        "sla_name": ["Default SLA"],
        "target_hours": [1.5]
    })

    fact_ticket = df[[
        "ticket_id","user_id"]].copy()
    fact_ticket["category_id"] = 1
    fact_ticket["sla_id"] = 1
    fact_ticket = fact_ticket.join(df[["status","priority","created_time",
                                       "resolved_time","subject","description",
                                       "response_time_min","resolution_time_min",
                                       "is_sla_breached"]])

    log.info(f"dim_user={len(dim_user)}, dim_category={len(dim_category)}, dim_sla={len(dim_sla)}, fact_ticket={len(fact_ticket)}")
    return {
        "dim_user": dim_user,
        "dim_category": dim_category,
        "dim_sla": dim_sla,
        "fact_ticket": fact_ticket
    }
