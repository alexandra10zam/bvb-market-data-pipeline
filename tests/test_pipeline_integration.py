"""End-to-end against a real PostgreSQL in Docker."""

from datetime import date

import pandas as pd
import pytest
from sqlalchemy import text

from bvb_pipeline import extract, load, pipeline
from bvb_pipeline.config import Settings

pytestmark = pytest.mark.integration
TICKERS = ["TLV.RO", "H2O.RO", "SNP.RO"]


@pytest.fixture(scope="module")
def db():
    try:
        from testcontainers.community.postgres import PostgresContainer
    except ImportError:  # testcontainers < 4.10
        from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine", driver="psycopg") as pg:
        yield pg.get_connection_url()


def scalar(eng, sql):
    with eng.connect() as conn:
        return conn.execute(text(sql)).scalar_one()


def test_full_run_is_idempotent(db, window):
    start, end = window
    settings = Settings(database_url=db)

    first = pipeline.run(settings, TICKERS, start, end, source="synthetic")
    eng = load.engine(db)
    counts = [scalar(eng, f"SELECT count(*) FROM {t}")
              for t in ("raw.prices_daily", "core.prices_daily", "marts.daily_indicators")]

    pipeline.run(settings, TICKERS, start, end, source="synthetic")  # same window again
    counts_again = [scalar(eng, f"SELECT count(*) FROM {t}")
                    for t in ("raw.prices_daily", "core.prices_daily", "marts.daily_indicators")]

    assert first["rejected"] == 0
    assert counts == counts_again == [first["extracted"]] * 3
    assert scalar(eng, "SELECT count(*) FROM marts.latest_snapshot") == len(TICKERS)
    assert scalar(eng, "SELECT count(*) FROM marts.monthly_returns") > 0
    assert scalar(eng, "SELECT count(*) FROM meta.pipeline_runs WHERE status = 'success'") == 2


def test_quality_gate_fails_run_and_keeps_audit_trail(db, monkeypatch):
    def broken(tickers, start, end):
        df = extract.synthetic(tickers, start, end)
        df.loc[: len(df) // 2, "close"] = -1.0  # half the rows are garbage
        return df

    monkeypatch.setitem(extract.SOURCES, "synthetic", broken)
    settings = Settings(database_url=db)
    eng = load.engine(db)
    load.ensure_schema(eng)

    with pytest.raises(pipeline.DataQualityError):
        pipeline.extract_and_validate(eng, settings, ["EL.RO"], date(2024, 1, 1), date(2024, 1, 31),
                                      source="synthetic")

    with eng.connect() as conn:
        run = conn.execute(text("SELECT status, error FROM meta.pipeline_runs ORDER BY run_id DESC LIMIT 1")).one()
        rejected = pd.read_sql("SELECT reason FROM core.rejected_rows WHERE ticker = 'EL.RO'", conn)
        loaded = conn.execute(text("SELECT count(*) FROM core.prices_daily WHERE ticker = 'EL.RO'")).scalar_one()

    assert run.status == "failed"
    assert "rejected" in run.error
    assert set(rejected["reason"]) == {"non_positive_price"}
    assert loaded == 0  # bad batch never reaches the core layer
