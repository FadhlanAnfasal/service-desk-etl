import pandas as pd

def dq_checks(tables: dict[str, pd.DataFrame]) -> list[str]:
    issues = []
    f = tables.get("fact_ticket", pd.DataFrame())

    if not f.empty:
        if f["ticket_id"].isna().any():
            issues.append("Null ticket_id in fact_ticket")
        if f["ticket_id"].duplicated().any():
            issues.append("Duplicate ticket_id in fact_ticket")

        if "priority" in f.columns:
            valid_priorities = ["low", "medium", "high"]
            invalid_priorities = f[~f["priority"].isin(valid_priorities)]
            if not invalid_priorities.empty:
                issues.append("Invalid priority values found in fact_ticket")

        if "status" in f.columns:
            valid_statuses = ["open", "in_progress", "resolved", "closed"]
            invalid_statuses = f[~f["status"].isin(valid_statuses)]
            if not invalid_statuses.empty:
                issues.append("Invalid status values found in fact_ticket")

        if "resolution_time_min" in f.columns:
            if (f["resolution_time_min"] < 0).any():
                issues.append("Negative resolution time found in fact_ticket")

        if "is_sla_breached" in f.columns:
            if f["is_sla_breached"].isna().any():
                issues.append("NULL SLA breach flag detected")
            elif f["is_sla_breached"].dtype != bool:
                issues.append("Invalid SLA breach flag datatype (should be boolean)")

    if "dim_user" not in tables or "user_id" not in tables["dim_user"].columns:
        issues.append("dim_user missing user_id")

    if "dim_category" not in tables or "category_id" not in tables["dim_category"].columns:
        issues.append("dim_category missing category_id")

    if "dim_sla" not in tables or "sla_id" not in tables["dim_sla"].columns:
        issues.append("dim_sla missing sla_id")

def check_threshold(table, prev_count, threshold_ratio=0.5):
    """
    Memeriksa apakah jumlah row saat ini turun drastis dibanding run sebelumnya.
    threshold_ratio=0.5 artinya drop lebih dari 50% dianggap anomali.
    """
    current_count = len(table)
    if prev_count > 0 and current_count < prev_count * threshold_ratio:
        return f"Row count drop detected: {prev_count} → {current_count} ({100 - (current_count/prev_count)*100:.1f}% drop)"
    return None