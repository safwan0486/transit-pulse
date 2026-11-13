"""End-to-end pipeline health checks (run every 15 min by Airflow).

Each check answers "is data still flowing through this stage?":

    producer → Kafka → bronze-sink → bronze/   (bronze freshness, sink lag)
                    → Spark       → silver/    (silver freshness, quarantine rate)
    Airflow daily GTFS            → bronze/gtfs_static/  (timetable freshness)

The evaluate_* functions are pure (easy to unit test); gather_* talk to the
lake and Kafka.
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq

from transit import lake

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    value: float | None
    threshold: float
    unit: str
    detail: str = ""

    def line(self) -> str:
        mark = "OK  " if self.ok else "FAIL"
        value = "n/a" if self.value is None else f"{self.value:,.1f}"
        limit = f"(limit {self.threshold:,.0f})"
        return f"{mark} {self.name}: {value} {self.unit} {limit} {self.detail}".rstrip()


def evaluate_max(name: str, value: float | None, limit: float, unit: str, detail: str = ""):
    """value must exist and be <= limit (None = no data at all = failure)."""
    return CheckResult(name, value is not None and value <= limit, value, limit, unit, detail)


def summarize(results: list[CheckResult]) -> tuple[bool, str]:
    failed = [r for r in results if not r.ok]
    return not failed, "\n".join(r.line() for r in results)


# --------------------------------------------------------------- gatherers
def _list(prefix: str) -> list[dict]:
    objs: list[dict] = []
    pages = lake.s3_client().get_paginator("list_objects_v2")
    for page in pages.paginate(Bucket=lake.bucket(), Prefix=prefix):
        objs += page.get("Contents", [])
    return objs


def _recent_prefixes(base: str, feeds: list[str], part: str, now: datetime) -> list[str]:
    """Today's and yesterday's partitions (UTC and MYT dates both covered)."""
    days = {(now + timedelta(hours=h)).date() for h in (-24, 0, 8)}
    return [f"{base}/feed={f}/{part}={d.isoformat()}/" for f in feeds for d in sorted(days)]


def newest_age_minutes(
    prefixes: list[str], now: datetime, suffix: str = ".parquet"
) -> float | None:
    newest = None
    for p in prefixes:
        for o in _list(p):
            if o["Key"].endswith(suffix) and (newest is None or o["LastModified"] > newest):
                newest = o["LastModified"]
    return None if newest is None else (now - newest).total_seconds() / 60


def rows_written_since(prefixes: list[str], since: datetime) -> int:
    total = 0
    for p in prefixes:
        for o in _list(p):
            if o["Key"].endswith(".parquet") and o["LastModified"] >= since:
                footer = pq.ParquetFile(io.BytesIO(lake.get_bytes(o["Key"])))
                total += footer.metadata.num_rows
    return total


def kafka_consumer_lag(group: str, topic: str) -> float | None:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
            "group.id": group,  # only reads the group's committed offsets; never joins it
            "enable.auto.commit": False,
        }
    )
    try:
        meta = consumer.list_topics(topic, timeout=10)
        partitions = [TopicPartition(topic, p) for p in meta.topics[topic].partitions]
        lag = 0
        for tp in consumer.committed(partitions, timeout=10):
            _, high = consumer.get_watermark_offsets(tp, timeout=10)
            lag += high - (tp.offset if tp.offset >= 0 else 0)
        return float(lag)
    except Exception:
        log.exception("Could not read consumer lag")
        return None
    finally:
        consumer.close()


def run_all(now: datetime | None = None) -> list[CheckResult]:
    now = now or datetime.now(UTC)
    rt_feeds = [f.strip() for f in os.getenv("REALTIME_FEEDS", "rapid-bus-kl").split(",")]
    topic = os.getenv("KAFKA_TOPIC_VEHICLE_POSITIONS", "gtfs.vehicle_positions")
    sink_flush_min = int(os.getenv("SINK_FLUSH_SECONDS", "300")) / 60

    bronze = _recent_prefixes("bronze/vehicle_positions", rt_feeds, "dt", now)
    silver = _recent_prefixes("silver/vehicle_positions", rt_feeds, "event_date", now)
    quarantine = [
        f"quarantine/vehicle_positions/processed_date={d}/"
        for d in sorted({(now - timedelta(hours=1)).date(), now.date()})
    ]
    hour_ago = now - timedelta(hours=1)

    silver_rows = rows_written_since(silver, hour_ago)
    quarantined = rows_written_since(quarantine, hour_ago)
    total = silver_rows + quarantined
    pct_quarantined = 100.0 * quarantined / total if total else None

    static_feeds = [f.strip() for f in os.getenv("STATIC_FEEDS", "rapid-bus-kl").split(",")]
    gtfs_age_min = newest_age_minutes(
        [f"bronze/gtfs_static/feed={f}/" for f in static_feeds], now, suffix=".zip"
    )

    return [
        evaluate_max("bronze freshness", newest_age_minutes(bronze, now),
                     2 * sink_flush_min + 5, "min since last file"),
        evaluate_max("silver freshness", newest_age_minutes(silver, now),
                     10, "min since last file"),
        evaluate_max("bronze-sink lag", kafka_consumer_lag("bronze-sink", topic), 20_000, "msgs"),
        evaluate_max("quarantine rate (1h)", pct_quarantined, 5, "%",
                     f"({quarantined:,} of {total:,} rows)"),
        evaluate_max("timetable freshness",
                     None if gtfs_age_min is None else gtfs_age_min / 60,
                     26, "h since last GTFS download"),
    ]
