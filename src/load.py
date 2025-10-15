from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd
from .config import DB_URL
from .logger import get_logger
log = get_logger(__name__)

def get_engine() -> Engine:
    return create_engine(DB_URL, future=True)

def upsert_frame(df: pd.DataFrame, table: str, key_cols: list[str]):
    engine = get_engine()
    with engine.begin() as conn:
        # temp table load then merge (simplified approach)
        tmp = f"dw.tmp_{table}"
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp}"))
        df.head(0).to_sql(tmp.split('.')[-1], conn, schema="dw", if_exists="replace", index=False)
        df.to_sql(tmp.split('.')[-1], conn, schema="dw", if_exists="append", index=False)

        all_cols = list(df.columns)
        set_cols = [c for c in all_cols if c not in key_cols]
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in set_cols]) or ''
        keys = ", ".join(key_cols)

        merge_sql = (
            f"INSERT INTO dw.{table} (" + ", ".join(all_cols) + ")\n" +
            f"SELECT " + ", ".join(all_cols) + f" FROM {tmp}\n" +
            f"ON CONFLICT (" + keys + ") DO UPDATE SET " + set_clause + ";"
        )
        if set_clause.strip() == '':
            merge_sql = (
                f"INSERT INTO dw.{table} (" + ", ".join(all_cols) + ")\n" +
                f"SELECT " + ", ".join(all_cols) + f" FROM {tmp}\n" +
                "ON CONFLICT DO NOTHING;"
            )
        conn.execute(text(merge_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp}"))
        log.info(f"Upserted {len(df)} rows into dw.{table}")
