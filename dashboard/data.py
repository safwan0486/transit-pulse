"""Data access for the dashboard: DuckDB over Parquet (gold marts + live silver).

Every function takes a connection and a root path, so tests can point them at
local Parquet files instead of the S3 lake.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta

import duckdb
import pandas as pd

LOCAL_OFFSET_HOURS = 8  # Malaysia, no daylight saving


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _day(service_date) -> str:
    """Accept a date, datetime or Timestamp; return 'YYYY-MM-DD' for SQL filters."""
    return pd.Timestamp(service_date).strftime("%Y-%m-%d")


def connect() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with S3 access to the lake (settings from the environment)."""
    con = duckdb.connect()
    endpoint = os.getenv("S3_ENDPOINT_HOST")
    if endpoint:
        con.sql("LOAD httpfs")
        settings = {
            "s3_endpoint": endpoint,
            "s3_url_style": "path",
            "s3_use_ssl": "false",
            "s3_region": os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1"),
            "s3_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        }
        for key, value in settings.items():
            con.sql(f"SET {key} = {_quote(value)}")
    return con


def lake_root() -> str:
    return os.getenv("LAKE_ROOT", f"s3://{os.getenv('LAKE_BUCKET', 'transit-lake')}")


def service_dates(con, root: str) -> list[date]:
    rows = con.sql(
        f"""
        select distinct cast(service_date as date) as d from read_parquet([
            '{root}/gold/mart_route_on_time_daily.parquet',
            '{root}/gold/mart_route_headway_daily.parquet'
        ], union_by_name = true)
        order by d desc
        """
    ).fetchall()
    return [r[0] for r in rows]


def on_time_routes(con, root: str, service_date, min_arrivals: int = 20) -> pd.DataFrame:
    return con.sql(
        f"""
        select * from read_parquet('{root}/gold/mart_route_on_time_daily.parquet')
        where cast(service_date as date) = '{_day(service_date)}'
          and observed_arrivals >= {int(min_arrivals)}
        order by pct_on_time, route_name
        """
    ).df()


def network_on_time(con, root: str, service_date) -> dict:
    """Arrival-weighted on-time % and median delay for the day (timetable service)."""
    row = con.sql(
        f"""
        select
            sum(observed_arrivals)                                         as arrivals,
            sum(pct_on_time * observed_arrivals) / sum(observed_arrivals)  as pct_on_time,
            sum(median_delay_min * observed_arrivals) / sum(observed_arrivals) as delay_min
        from read_parquet('{root}/gold/mart_network_delay_hourly.parquet')
        where cast(service_date as date) = '{_day(service_date)}'
        """
    ).fetchone()
    return {"arrivals": row[0] or 0, "pct_on_time": row[1], "delay_min": row[2]}


def delay_by_hour(con, root: str, days: int = 7) -> pd.DataFrame:
    return con.sql(
        f"""
        select service_date, hour_local, observed_arrivals, routes,
               median_delay_min, p90_delay_min, pct_on_time
        from read_parquet('{root}/gold/mart_network_delay_hourly.parquet')
        where service_date >= (
            select max(service_date) - {days - 1}
            from read_parquet('{root}/gold/mart_network_delay_hourly.parquet')
        )
        order by service_date, hour_local
        """
    ).df()


def headway_routes(con, root: str, service_date, min_headways: int = 5) -> pd.DataFrame:
    return con.sql(
        f"""
        select * from read_parquet('{root}/gold/mart_route_headway_daily.parquet')
        where cast(service_date as date) = '{_day(service_date)}'
          and observed_headways >= {int(min_headways)}
        order by pct_evenly_spaced, route_name
        """
    ).df()


def network_headway(con, root: str, service_date) -> dict:
    row = con.sql(
        f"""
        select
            sum(observed_headways)                                              as headways,
            sum(excess_wait_min * observed_headways) / sum(observed_headways)   as ewt_min,
            sum(pct_bunched * observed_headways) / sum(observed_headways)       as pct_bunched,
            sum(pct_evenly_spaced * observed_headways) / sum(observed_headways) as pct_even
        from read_parquet('{root}/gold/mart_route_headway_daily.parquet')
        where cast(service_date as date) = '{_day(service_date)}'
        """
    ).fetchone()
    return {"headways": row[0] or 0, "ewt_min": row[1], "pct_bunched": row[2], "pct_even": row[3]}


def feed_health(con, root: str) -> pd.DataFrame:
    """Each feed's latest complete hour, with a status the dashboard can show."""
    df = con.sql(
        f"""
        with h as (select * from read_parquet('{root}/gold/mart_feed_health_hourly.parquet')),
        latest as (
            select feed, max(hour_local) as hour_local from h
            where hour_local < date_trunc(
                'hour', now()::timestamp + interval {LOCAL_OFFSET_HOURS} hour)
            group by feed
        )
        select h.* from h join latest using (feed, hour_local) order by feed
        """
    ).df()
    df["status"] = df.apply(_feed_status, axis=1)
    return df


def _feed_status(row) -> str:
    if row["vehicles"] >= 3 and row["pct_frozen"] > 50:
        return "frozen"
    if row["pct_trip_matched"] < 90:
        return "degraded"
    return "healthy"


def recent_silver_files(root: str, minutes: int, now_utc: datetime) -> list[str]:
    """Silver files written in the last `minutes` (+2 min slack), via one S3 listing.

    Spark writes a small file about every minute, so a day partition holds
    hundreds of files; opening all of them to find the last 10 minutes took
    ~8 s. Listing is one cheap API call that returns each file's write time.
    """
    import boto3

    bucket = root.removeprefix("s3://").split("/", 1)[0]
    local_day = (now_utc + timedelta(hours=LOCAL_OFFSET_HOURS)).strftime("%Y-%m-%d")
    cutoff = (now_utc - timedelta(minutes=minutes + 2)).replace(tzinfo=UTC)
    s3 = boto3.client("s3", endpoint_url=f"http://{os.environ['S3_ENDPOINT_HOST']}")
    pages = s3.get_paginator("list_objects_v2")
    # List only today's folder of each feed, so listing cost doesn't grow with history.
    feeds = [
        p["Prefix"]
        for page in pages.paginate(Bucket=bucket, Prefix="silver/vehicle_positions/", Delimiter="/")
        for p in page.get("CommonPrefixes", [])
        if p["Prefix"].rstrip("/").split("/")[-1].startswith("feed=")
    ]
    keys = []
    for feed_prefix in feeds:
        for page in pages.paginate(Bucket=bucket, Prefix=f"{feed_prefix}event_date={local_day}/"):
            keys += [
                f"s3://{bucket}/{o['Key']}"
                for o in page.get("Contents", [])
                if o["Key"].endswith(".parquet") and o["LastModified"] >= cutoff
            ]
    return keys


def live_vehicles(
    con, root: str, minutes: int = 10, now_utc=None, files: list[str] | None = None
) -> pd.DataFrame:
    """Latest position per vehicle seen in the last `minutes` (straight from silver).

    `files` narrows the read to recently written files (see recent_silver_files);
    without it, all of today's silver files are scanned (fine for tests/local).
    """
    now_utc = now_utc or datetime.now(UTC).replace(tzinfo=None)
    local_day = (now_utc + timedelta(hours=LOCAL_OFFSET_HOURS)).strftime("%Y-%m-%d")
    since = (now_utc - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    if files is None:
        files = [f"{root}/silver/vehicle_positions/*/event_date={local_day}/*.parquet"]
    if not files:
        return pd.DataFrame(columns=["feed", "vehicle_id", "route_id", "latitude", "longitude"])
    file_list = "[" + ", ".join(_quote(f) for f in files) + "]"
    return con.sql(
        f"""
        with recent as (
            select * from read_parquet({file_list}, hive_partitioning = true)
            where event_ts >= '{since}'::timestamp
        )
        select feed, vehicle_id, route_id, latitude, longitude, speed_kmh, event_ts
        from recent
        qualify row_number() over (partition by feed, vehicle_id order by event_ts desc) = 1
        """
    ).df()
