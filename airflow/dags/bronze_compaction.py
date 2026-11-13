"""Hourly: merge each closed bronze hour partition into a single Parquet file.

See ingestion/transit/compaction.py for why and how (idempotent, verified,
duplicate-safe).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from transit.alerts import airflow_failure_callback


@dag(
    dag_id="bronze_compaction",
    description="Compact small bronze Parquet files into one file per hour partition",
    schedule="40 * * * *",  # after the hour has closed + sink grace period
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kuala_Lumpur"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": airflow_failure_callback,
    },
    tags=["bronze", "maintenance"],
)
def bronze_compaction():
    @task
    def compact() -> dict:
        from transit.compaction import compact_recent

        results = compact_recent(days=2)
        summary = {
            "partitions": len(results),
            "files_removed": sum(r["files_in"] for r in results),
            "rows": sum(r["rows"] for r in results),
        }
        print(summary)
        return summary

    compact()


bronze_compaction()
