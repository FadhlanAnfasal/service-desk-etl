import pandas as pd

def dq_checks(tables: dict[str, pd.DataFrame]) -> list[str]:
    issues = []
    f = tables["fact_ticket"]
    if f["ticket_id"].isna().any():
        issues.append("Null ticket_id in fact_ticket")
    if f["ticket_id"].duplicated().any():
        issues.append("Duplicate ticket_id in fact_ticket")
    if not {"user_id"}.issubset(tables["dim_user"].columns):
        issues.append("dim_user missing user_id")
    return issues
