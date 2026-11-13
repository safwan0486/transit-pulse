"""GTFS-Realtime → Kafka producer.

Polls each configured vehicle-position feed every POLL_INTERVAL_SECONDS and
publishes one JSON message per vehicle to KAFKA_TOPIC_VEHICLE_POSITIONS,
keyed by "<feed>:<vehicle_id>" so each vehicle's events stay ordered in one
partition.

Run locally:   python -m transit.producer
Dry run:       python -m transit.producer --once --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from datetime import UTC, datetime

from transit.feeds import Feed, feeds_from_env
from transit.gtfs_rt import parse_vehicle_positions
from transit.http import make_session

log = logging.getLogger("transit.producer")


class LastSeen:
    """Drops positions we already published (API repeats them until a vehicle reports again).

    This is only an optimisation to save bandwidth — it is lost on restart,
    so downstream jobs still deduplicate on event_id.
    """

    def __init__(self) -> None:
        self._seen: dict[str, str] = {}

    def is_new(self, record: dict) -> bool:
        key = f"{record['feed']}:{record['vehicle_id']}"
        if self._seen.get(key) == record["event_id"]:
            return False
        self._seen[key] = record["event_id"]
        return True


def build_kafka_producer():
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            "client.id": "gtfs-rt-producer",
            "enable.idempotence": True,  # no duplicates caused by producer retries
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 100,
        }
    )


def _delivery_report(err, msg) -> None:
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)


def poll_feed(session, feed: Feed) -> list[dict]:
    resp = session.get(feed.realtime_url, timeout=20)
    resp.raise_for_status()
    return parse_vehicle_positions(resp.content, feed.name, datetime.now(UTC))


def run(once: bool = False, dry_run: bool = False) -> None:
    feeds = [f for f in feeds_from_env("REALTIME_FEEDS", "rapid-bus-kl") if f.has_realtime]
    topic = os.getenv("KAFKA_TOPIC_VEHICLE_POSITIONS", "gtfs.vehicle_positions")
    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))

    session = make_session()
    producer = None if dry_run else build_kafka_producer()
    last_seen = LastSeen()

    stopping = False

    def _stop(*_):
        nonlocal stopping
        log.info("Shutdown requested, finishing current poll…")
        stopping = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info("Polling %s every %ss → topic %s", [f.name for f in feeds], interval, topic)
    while not stopping:
        started = time.monotonic()
        for feed in feeds:
            try:
                records = poll_feed(session, feed)
            except Exception:  # keep running when one feed is down
                log.exception("Failed to poll %s", feed.name)
                continue

            new = [r for r in records if last_seen.is_new(r)]
            if dry_run:
                for r in new[:3]:
                    print(json.dumps(r, indent=2))
            else:
                for r in new:
                    producer.produce(
                        topic,
                        key=f"{r['feed']}:{r['vehicle_id']}",
                        value=json.dumps(r).encode(),
                        on_delivery=_delivery_report,
                    )
                producer.poll(0)
            log.info("%s: %d vehicles, %d new events", feed.name, len(records), len(new))

        if producer is not None:
            producer.flush(10)
        if once:
            break
        # Sleep in small steps so Ctrl+C / docker stop is responsive.
        while not stopping and time.monotonic() - started < interval:
            time.sleep(0.5)

    if producer is not None:
        producer.flush(30)
    log.info("Producer stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="poll each feed once and exit")
    parser.add_argument("--dry-run", action="store_true", help="print records instead of sending")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(once=args.once, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
