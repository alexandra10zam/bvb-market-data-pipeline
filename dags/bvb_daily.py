"""Daily BVB prices pipeline.

Runs after the Bucharest Stock Exchange closes (18:00 EET). Each task is idempotent for its
data interval, so retries and `airflow dags backfill` are safe.

The pipeline lives in its own virtualenv (/opt/bvb) inside the image: it uses SQLAlchemy 2,
which would conflict with Airflow's own pinned dependencies.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

BVB = "/opt/bvb/bin/bvb-pipeline"
# A run fires at the END of its data interval, so the trading day it covers is data_interval_end
# (Airflow's `ds` is the interval START, i.e. the previous day). A 5-day lookback re-fetches late corrections.
TRADING_DAY = "{{ data_interval_end | ds }}"
WINDOW = f"--start {{{{ macros.ds_add(data_interval_end | ds, -5) }}}} --end {TRADING_DAY}"

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="bvb_daily_prices",
    description="Extract BVB prices, quality-check them and rebuild indicator marts",
    schedule="30 19 * * 1-5",  # 19:30 Bucharest time, Monday-Friday
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Bucharest"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bvb", "elt", "finance"],
    doc_md=__doc__,
) as dag:
    extract_validate = BashOperator(
        task_id="extract_and_validate",
        bash_command=f"{BVB} --step extract {WINDOW}",
    )

    build_indicators = BashOperator(
        task_id="build_indicators",
        bash_command=f"{BVB} --step indicators {WINDOW}",
    )

    # Fails the run (and alerts) if the marts are stale: every ticker must have a row in the last 7 days
    freshness_check = SQLCheckOperator(
        task_id="marts_freshness_check",
        conn_id="bvb_warehouse",
        sql="""
            SELECT count(*) = count(*) FILTER (WHERE trade_date >= DATE '{{ data_interval_end | ds }}' - 7)
            FROM marts.latest_snapshot
        """,
    )

    extract_validate >> build_indicators >> freshness_check
