"""Hourly dbt build: silver (lake) → DuckDB warehouse → performance marts.

    dbt deps ─► source freshness (warn only) ─► dbt build (models + tests)

`dbt build` runs models and their tests in DAG order, so a failing test
stops everything downstream of it: bad data never reaches the marts.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from transit.alerts import airflow_failure_callback

# Project/profiles dirs come from DBT_PROJECT_DIR / DBT_PROFILES_DIR (docker-compose.yml).
DBT = "$DBT_BIN --no-use-colors"


@dag(
    dag_id="transit_dbt_build",
    description="Build and test the DuckDB warehouse with dbt",
    schedule="15 * * * *",  # hourly, offset from the top of the hour
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kuala_Lumpur"),
    catchup=False,
    max_active_runs=1,  # DuckDB allows one writer at a time
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": airflow_failure_callback,  # e.g. a failing dbt test
    },
    tags=["dbt", "warehouse", "gold"],
)
def transit_dbt_build():
    deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT} deps")

    # Freshness problems should alert, not block the build (|| true):
    # a stale feed still leaves yesterday's marts worth rebuilding.
    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness || true",
    )

    build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build")

    deps >> freshness >> build


transit_dbt_build()
