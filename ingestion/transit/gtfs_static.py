"""Download GTFS static zips and convert each .txt table to Parquet.

Pure functions here; the Airflow DAG (airflow/dags/gtfs_static_ingest.py)
wires them together and adds scheduling, retries and backfills.
"""

from __future__ import annotations

import csv
import hashlib
import io
import zipfile

import pyarrow as pa

from transit import lake
from transit.feeds import Feed
from transit.http import make_session

# Tables every usable GTFS feed must contain (https://gtfs.org/schedule/reference/)
REQUIRED_TABLES = {"stops", "routes", "trips", "stop_times"}


class GtfsValidationError(ValueError):
    pass


def bronze_key(feed: str, ds: str) -> str:
    return f"bronze/gtfs_static/feed={feed}/dt={ds}/gtfs.zip"


def silver_key(table: str, feed: str, ds: str) -> str:
    # One file per (table, feed, day): re-running a day overwrites it → idempotent.
    return f"silver/gtfs_static/{table}/feed={feed}/dt={ds}/data.parquet"


def download(feed: Feed) -> bytes:
    resp = make_session().get(feed.static_url, timeout=120)
    resp.raise_for_status()
    if not zipfile.is_zipfile(io.BytesIO(resp.content)):
        raise GtfsValidationError(f"{feed.name}: response is not a zip file")
    return resp.content


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def zip_to_tables(zip_bytes: bytes, feed: str, ds: str) -> dict[str, pa.Table]:
    """Read every GTFS .txt file in the zip into an Arrow table.

    All columns are kept as strings on purpose: silver stays faithful to the
    source, and typing/casting happens in dbt where it is tested and documented.
    """
    tables: dict[str, pa.Table] = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.endswith(".txt") or name.startswith("__MACOSX"):
                continue
            table_name = name.rsplit("/", 1)[-1].removesuffix(".txt")
            text = zf.read(name).decode("utf-8-sig")  # strip BOM if present
            reader = csv.DictReader(io.StringIO(text))
            columns = [c.strip() for c in (reader.fieldnames or [])]
            rows = [
                {k.strip(): (v.strip() if v else None) for k, v in r.items() if k} for r in reader
            ]

            schema = pa.schema([(c, pa.string()) for c in columns])
            table = pa.Table.from_pylist(rows, schema=schema)
            n = table.num_rows
            table = table.append_column("feed", pa.array([feed] * n, pa.string()))
            table = table.append_column("snapshot_date", pa.array([ds] * n, pa.string()))
            tables[table_name] = table

    missing = REQUIRED_TABLES - tables.keys()
    if missing:
        raise GtfsValidationError(f"{feed}: missing required GTFS tables {sorted(missing)}")
    empty = [t for t in REQUIRED_TABLES if tables[t].num_rows == 0]
    if empty:
        raise GtfsValidationError(f"{feed}: required GTFS tables are empty: {sorted(empty)}")
    return tables


def ingest_to_bronze(feed: Feed, ds: str) -> dict:
    data = download(feed)
    key = bronze_key(feed.name, ds)
    lake.put_bytes(key, data, content_type="application/zip")
    return {"feed": feed.name, "ds": ds, "key": key, "bytes": len(data), "sha256": sha256(data)}


def bronze_to_silver(feed: str, ds: str) -> dict[str, int]:
    tables = zip_to_tables(lake.get_bytes(bronze_key(feed, ds)), feed, ds)
    for name, table in tables.items():
        lake.put_parquet(silver_key(name, feed, ds), table)
    return {name: t.num_rows for name, t in tables.items()}
