"""Daily GTFS static (schedule) ingestion.

    download zip ──► bronze/gtfs_static/feed=*/dt=*/gtfs.zip
           │
           ▼
    validate + convert ──► silver/gtfs_static/<table>/feed=*/dt=*/data.parquet

One mapped task per feed, so a broken feed fails on its own without blocking
the others. Every task writes to a path keyed by the logical date, so re-runs
and backfills are idempotent:

    airflow dags backfill gtfs_static_ingest -s 2026-11-01 -e 2026-11-07
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from transit.alerts import airflow_failure_callback

log = logging.getLogger(__name__)

STATIC_FEEDS = os.getenv("STATIC_FEEDS", "rapid-bus-kl,rapid-bus-mrtfeeder,rapid-rail-kl,ktmb")
FEED_NAMES = [f.strip() for f in STATIC_FEEDS.split(",") if f.strip()]


@dag(
    dag_id="gtfs_static_ingest",
    description="Download GTFS schedules from data.gov.my into the lake (bronze → silver)",
    schedule="0 3 * * *",  # 03:00 Malaysia time, before the morning peak
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kuala_Lumpur"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "on_failure_callback": airflow_failure_callback,  # fires after the last retry
    },
    tags=["gtfs", "static", "bronze", "silver"],
)
def gtfs_static_ingest():
    @task
    def ensure_lake() -> None:
        from transit import lake

        lake.ensure_bucket()

    @task
    def download_to_bronze(feed_name: str, ds: str | None = None) -> dict:
        from transit.feeds import FEEDS
        from transit.gtfs_static import ingest_to_bronze

        info = ingest_to_bronze(FEEDS[feed_name], ds)
        log.info("Bronze: %s", info)
        return info

    @task
    def bronze_to_silver(info: dict) -> dict:
        from transit.gtfs_static import bronze_to_silver

        counts = bronze_to_silver(info["feed"], info["ds"])
        log.info("Silver row counts for %s: %s", info["feed"], counts)
        return {"feed": info["feed"], "row_counts": counts}

    @task
    def summarize(results: list[dict]) -> None:
        for r in results:
            log.info(
                "%-22s stops=%s routes=%s trips=%s stop_times=%s",
                r["feed"],
                *(r["row_counts"].get(t) for t in ("stops", "routes", "trips", "stop_times")),
            )

    bronze = download_to_bronze.expand(feed_name=FEED_NAMES)
    silver = bronze_to_silver.expand(info=bronze)
    ensure_lake() >> bronze
    summarize(silver)


gtfs_static_ingest()
