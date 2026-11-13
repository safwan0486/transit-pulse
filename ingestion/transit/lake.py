"""Thin helpers around the S3-compatible data lake (SeaweedFS locally, S3/GCS in the cloud).

Layout (medallion architecture):
    bronze/  raw data exactly as received (zip files, raw events)
    silver/  cleaned, typed, deduplicated Parquet
    gold/    business-level aggregates (built by dbt in phase 3)
"""

from __future__ import annotations

import io
import logging
import os
from functools import lru_cache

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


def bucket() -> str:
    return os.getenv("LAKE_BUCKET", "transit-lake")


@lru_cache(maxsize=1)
def s3_client():
    # S3_ENDPOINT_URL points at the local S3-compatible store; unset it to use real AWS S3.
    return boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL") or None)


def ensure_bucket() -> None:
    client = s3_client()
    try:
        client.head_bucket(Bucket=bucket())
    except ClientError:
        log.info("Creating bucket %s", bucket())
        client.create_bucket(Bucket=bucket())


def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    s3_client().put_object(Bucket=bucket(), Key=key, Body=data, ContentType=content_type)
    uri = f"s3://{bucket()}/{key}"
    log.info("Wrote %s (%d bytes)", uri, len(data))
    return uri


def get_bytes(key: str) -> bytes:
    return s3_client().get_object(Bucket=bucket(), Key=key)["Body"].read()


def table_to_parquet_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def put_parquet(key: str, table: pa.Table) -> str:
    return put_bytes(key, table_to_parquet_bytes(table))
