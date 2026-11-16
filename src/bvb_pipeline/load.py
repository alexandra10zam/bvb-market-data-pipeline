"""PostgreSQL loading. Every write is an idempotent upsert, so re-running a day never duplicates data."""

from __future__ import annotations

import json
from importlib import resources

import pandas as pd
from sqlalchemy import Engine, create_engine, text


def engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, future=True)


def ensure_schema(eng: Engine) -> None:
    ddl = resources.files("bvb_pipeline").joinpath("sql/001_schema.sql").read_text()
    with eng.begin() as conn:
        conn.exec_driver_sql(ddl)


def upsert(eng: Engine, df: pd.DataFrame, table: str, keys: list[str]) -> int:
    """Bulk upsert via a temp staging table + INSERT ... ON CONFLICT DO UPDATE."""
    if df.empty:
        return 0
    cols = list(df.columns)
    staging = f"stg_{table.replace('.', '_')}"
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in keys)
    with eng.begin() as conn:
        conn.exec_driver_sql(f"CREATE TEMP TABLE {staging} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
        df.to_sql(staging, conn, if_exists="append", index=False, method="multi", chunksize=1000)
        conn.exec_driver_sql(
            f"INSERT INTO {table} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {staging} "
            f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}"
        )
    return len(df)


def save_rejected(eng: Engine, run_id: int, rejected: pd.DataFrame) -> None:
    if rejected.empty:
        return
    rows = [
        {
            "run_id": run_id,
            "ticker": r["ticker"],
            "trade_date": r["trade_date"],
            "reason": r["reason"],
            "payload": json.dumps({k: (None if pd.isna(v) else str(v)) for k, v in r.items()}),
        }
        for r in rejected.to_dict("records")
    ]
    with eng.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO core.rejected_rows (run_id, ticker, trade_date, reason, payload) "
                "VALUES (:run_id, :ticker, :trade_date, :reason, CAST(:payload AS jsonb))"
            ),
            rows,
        )


def read_prices(eng: Engine, tickers: list[str], since) -> pd.DataFrame:
    query = text(
        "SELECT ticker, trade_date, adj_close::float AS adj_close FROM core.prices_daily "
        "WHERE ticker = ANY(:tickers) AND trade_date >= :since ORDER BY ticker, trade_date"
    )
    with eng.connect() as conn:
        return pd.read_sql(query, conn, params={"tickers": tickers, "since": since})
