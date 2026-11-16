"""Pipeline steps. Each step is idempotent for a given window, so Airflow retries and backfills are safe."""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import Engine, text

from . import extract, load, quality, transform
from .config import Settings

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


def _start_run(eng: Engine, source: str, start: date, end: date) -> int:
    with eng.begin() as conn:
        return conn.execute(
            text("INSERT INTO meta.pipeline_runs (source, window_start, window_end) "
                 "VALUES (:s, :a, :b) RETURNING run_id"),
            {"s": source, "a": start, "b": end},
        ).scalar_one()


def _finish_run(eng: Engine, run_id: int, status: str, **stats) -> None:
    with eng.begin() as conn:
        conn.execute(
            text("UPDATE meta.pipeline_runs SET status = :status, finished_at = now(), "
                 "rows_extracted = :extracted, rows_rejected = :rejected, rows_loaded = :loaded, "
                 "error = :error WHERE run_id = :run_id"),
            {"run_id": run_id, "status": status, "extracted": stats.get("extracted"),
             "rejected": stats.get("rejected"), "loaded": stats.get("loaded"), "error": stats.get("error")},
        )


def extract_and_validate(eng: Engine, settings: Settings, tickers: list[str], start: date, end: date,
                         source: str = "yahoo") -> dict:
    """Extract -> raw layer, quality gate -> core layer."""
    run_id = _start_run(eng, source, start, end)
    stats: dict = {}
    try:
        df = extract.SOURCES[source](tickers, start, end)
        stats["extracted"] = len(df)
        log.info("Extracted %d rows for %d tickers", len(df), len(tickers))

        load.upsert(eng, df.assign(source=source), "raw.prices_daily", ["ticker", "trade_date"])

        report = quality.validate(df)
        stats["rejected"] = len(report.rejected)
        load.save_rejected(eng, run_id, report.rejected)
        log.info("Quality report: %s", report.summary())
        if report.reject_ratio > settings.max_reject_ratio:
            raise DataQualityError(
                f"{report.reject_ratio:.1%} of rows rejected (limit {settings.max_reject_ratio:.0%}): "
                f"{report.summary()}"
            )

        stats["loaded"] = load.upsert(eng, report.clean, "core.prices_daily", ["ticker", "trade_date"])
        _finish_run(eng, run_id, "success", **stats)
        return {"run_id": run_id, **stats, **report.summary()}
    except Exception as exc:
        _finish_run(eng, run_id, "failed", error=str(exc)[:2000], **stats)
        raise


def build_indicators(eng: Engine, settings: Settings, tickers: list[str], start: date) -> int:
    """Recompute marts for dates >= start, reading enough history for the rolling windows."""
    history = load.read_prices(eng, tickers, start - timedelta(days=settings.indicator_lookback_days))
    if history.empty:
        return 0
    indicators = transform.compute_indicators(history)
    indicators = indicators[pd.to_datetime(indicators["trade_date"]) >= pd.Timestamp(start)]
    written = load.upsert(eng, indicators, "marts.daily_indicators", ["ticker", "trade_date"])
    log.info("Wrote %d indicator rows", written)
    return written


def run(settings: Settings, tickers: list[str], start: date, end: date, source: str = "yahoo") -> dict:
    eng = load.engine(settings.database_url)
    load.ensure_schema(eng)
    result = extract_and_validate(eng, settings, tickers, start, end, source)
    result["indicator_rows"] = build_indicators(eng, settings, tickers, start)
    return result
