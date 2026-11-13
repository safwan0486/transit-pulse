"""Kafka → data lake (bronze) sink.

Consumes vehicle-position events and writes them as Parquet micro-batches:

    s3://<bucket>/bronze/vehicle_positions/feed=<feed>/dt=YYYY-MM-DD/hour=HH/part-<uuid>.parquet

Offsets are committed only *after* a batch is safely written, giving
at-least-once delivery. Duplicates are removed in the silver layer (phase 2,
Spark Structured Streaming) using event_id.

Run locally:   python -m transit.bronze_sink
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
import uuid
from collections import defaultdict
from datetime import datetime

import pyarrow as pa

from transit import lake

log = logging.getLogger("transit.bronze_sink")

BRONZE_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("feed", pa.string()),
        ("entity_id", pa.string()),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("license_plate", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int32()),
        ("start_date", pa.string()),
        ("start_time", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("bearing", pa.float64()),
        ("speed_mps", pa.float64()),
        ("stop_id", pa.string()),
        ("current_stop_sequence", pa.int64()),
        ("current_status", pa.string()),
        ("event_ts", pa.string()),
        ("feed_ts", pa.string()),
        ("ingested_at", pa.string()),
        # Kafka metadata — handy for debugging and replay
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
    ]
)


def partition_key(record: dict) -> tuple[str, str, str]:
    """Bronze is partitioned by *ingestion* time: append-only and easy to reprocess."""
    ts = datetime.fromisoformat(record["ingested_at"])
    return record["feed"], ts.strftime("%Y-%m-%d"), ts.strftime("%H")


def object_key(feed: str, dt: str, hour: str) -> str:
    return (
        f"bronze/vehicle_positions/feed={feed}/dt={dt}/hour={hour}/"
        f"part-{uuid.uuid4().hex}.parquet"
    )


def records_to_table(records: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(records, schema=BRONZE_SCHEMA)


def flush(buffer: list[dict]) -> int:
    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for r in buffer:
        groups[partition_key(r)].append(r)
    for (feed, dt, hour), rows in groups.items():
        lake.put_parquet(object_key(feed, dt, hour), records_to_table(rows))
    return len(buffer)


def run() -> None:
    from confluent_kafka import Consumer, KafkaError

    topic = os.getenv("KAFKA_TOPIC_VEHICLE_POSITIONS", "gtfs.vehicle_positions")
    batch_size = int(os.getenv("SINK_BATCH_SIZE", "5000"))
    flush_seconds = int(os.getenv("SINK_FLUSH_SECONDS", "300"))

    lake.ensure_bucket()
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            "group.id": "bronze-sink",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    buffer: list[dict] = []
    last_flush = time.monotonic()
    log.info("Consuming %s (flush every %d records or %ds)", topic, batch_size, flush_seconds)

    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka error: %s", msg.error())
                else:
                    try:
                        record = json.loads(msg.value())
                    except json.JSONDecodeError:
                        log.warning("Skipping malformed message at offset %s", msg.offset())
                    else:
                        record["kafka_partition"] = msg.partition()
                        record["kafka_offset"] = msg.offset()
                        buffer.append(record)

            due = time.monotonic() - last_flush >= flush_seconds
            if buffer and (len(buffer) >= batch_size or due):
                n = flush(buffer)
                consumer.commit(asynchronous=False)
                log.info("Flushed %d records to bronze", n)
                buffer.clear()
                last_flush = time.monotonic()
            elif due:
                last_flush = time.monotonic()
    finally:
        if buffer:
            flush(buffer)
            consumer.commit(asynchronous=False)
            log.info("Flushed %d records on shutdown", len(buffer))
        consumer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run()


if __name__ == "__main__":
    main()
