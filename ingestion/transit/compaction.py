"""Small-file compaction for bronze vehicle positions.

The sink flushes every few minutes, so each hour partition collects ~12 small
Parquet files per feed (thousands per month). Many small files make every
later read slow (one request per file) and cost more on cloud storage.

This merges each *closed* hour partition into one file:

    bronze/vehicle_positions/feed=X/dt=D/hour=H/part-<uuid>.parquet  (x12)
        → bronze/vehicle_positions/feed=X/dt=D/hour=H/part-compacted-<hash>.parquet

Safe to re-run at any point (idempotent):
  * the output name is a hash of the exact input files, and
  * inputs are deleted only after the output is written AND its row count
    has been verified. If a run dies between write and delete, the next run
    sees the output already exists and just finishes the delete.
Late files that land after compaction are merged on the next run.
"""

from __future__ import annotations

import hashlib
import io
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from transit import lake

log = logging.getLogger(__name__)

PREFIX = "bronze/vehicle_positions/"
_PARTITION = re.compile(
    r"^(?P<part>.*/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hour=(?P<hour>\d{2})/)[^/]+\.parquet$"
)


@dataclass(frozen=True)
class Plan:
    partition: str
    inputs: tuple[str, ...]

    @property
    def output(self) -> str:
        digest = hashlib.sha256("\n".join(sorted(self.inputs)).encode()).hexdigest()[:16]
        return f"{self.partition}part-compacted-{digest}.parquet"


def plan_compaction(keys: list[str], now: datetime, grace: timedelta = timedelta(minutes=20)):
    """Pick hour partitions that are closed (hour ended > grace ago) and have >1 file."""
    by_partition: dict[str, list[str]] = defaultdict(list)
    for key in keys:
        m = _PARTITION.match(key)
        if not m:
            continue
        hour_end = datetime.fromisoformat(f"{m['dt']}T{m['hour']}:00:00+00:00") + timedelta(hours=1)
        if now >= hour_end + grace:
            by_partition[m["part"]].append(key)
    return [Plan(p, tuple(sorted(ks))) for p, ks in sorted(by_partition.items()) if len(ks) > 1]


def dedupe_kafka_messages(table: pa.Table) -> pa.Table:
    """Keep one row per Kafka message (partition, offset).

    Bronze is at-least-once, and a compacted file can be merged again together
    with inputs it already contains (crash before cleanup + a late file). The
    Kafka coordinates identify a message exactly, so duplicates of any origin
    collapse to one row.
    """
    seen: set[tuple[int, int]] = set()
    keep = []
    parts = table.column("kafka_partition").to_pylist()
    offsets = table.column("kafka_offset").to_pylist()
    for i, coord in enumerate(zip(parts, offsets, strict=True)):
        if coord not in seen:
            seen.add(coord)
            keep.append(i)
    return table if len(keep) == table.num_rows else table.take(keep)


def _exists(key: str) -> bool:
    try:
        lake.s3_client().head_object(Bucket=lake.bucket(), Key=key)
        return True
    except Exception:  # noqa: BLE001 - botocore raises ClientError(404)
        return False


def execute(plan: Plan) -> dict:
    output = plan.output
    inputs = [k for k in plan.inputs if k != output]

    if not _exists(output):
        tables = [pq.read_table(io.BytesIO(lake.get_bytes(k))) for k in inputs]
        merged = dedupe_kafka_messages(pa.concat_tables(tables, promote_options="default"))
        lake.put_parquet(output, merged)
        written = pq.ParquetFile(io.BytesIO(lake.get_bytes(output))).metadata.num_rows
        if written != merged.num_rows:
            raise RuntimeError(f"{output}: wrote {written} rows, expected {merged.num_rows}")
        rows = merged.num_rows
    else:
        log.info("%s already exists (earlier run stopped before cleanup)", output)
        rows = pq.ParquetFile(io.BytesIO(lake.get_bytes(output))).metadata.num_rows

    client = lake.s3_client()
    for i in range(0, len(inputs), 1000):
        client.delete_objects(
            Bucket=lake.bucket(),
            Delete={"Objects": [{"Key": k} for k in inputs[i : i + 1000]], "Quiet": True},
        )
    return {"partition": plan.partition, "files_in": len(inputs), "rows": rows}


def compact_recent(days: int = 2, now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(UTC)
    wanted = {f"/dt={(now - timedelta(days=d)).date().isoformat()}/" for d in range(days)}
    keys: list[str] = []
    pages = lake.s3_client().get_paginator("list_objects_v2")
    for page in pages.paginate(Bucket=lake.bucket(), Prefix=PREFIX):
        keys += [o["Key"] for o in page.get("Contents", []) if any(w in o["Key"] for w in wanted)]
    results = [execute(p) for p in plan_compaction(keys, now)]
    for r in results:
        log.info("Compacted %(files_in)d files (%(rows)d rows) in %(partition)s", r)
    return results
