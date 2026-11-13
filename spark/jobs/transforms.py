"""Pure DataFrame transformations for the vehicle-positions silver layer.

Kept free of I/O so the same functions run in the streaming job and in
batch-mode unit tests.

    Kafka rows ─► parse_kafka ─► standardize ─► add_quality_flags ─┬─► valid ─► dedupe ─► silver
                                                                   └─► invalid ─────────► quarantine
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOCAL_TZ = "Asia/Kuala_Lumpur"

# JSON contract published by ingestion/transit/producer.py
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("feed", T.StringType()),
        T.StructField("entity_id", T.StringType()),
        T.StructField("vehicle_id", T.StringType()),
        T.StructField("vehicle_label", T.StringType()),
        T.StructField("license_plate", T.StringType()),
        T.StructField("trip_id", T.StringType()),
        T.StructField("route_id", T.StringType()),
        T.StructField("direction_id", T.IntegerType()),
        T.StructField("start_date", T.StringType()),
        T.StructField("start_time", T.StringType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("bearing", T.DoubleType()),
        T.StructField("speed_mps", T.DoubleType()),
        T.StructField("stop_id", T.StringType()),
        T.StructField("current_stop_sequence", T.LongType()),
        T.StructField("current_status", T.StringType()),
        T.StructField("event_ts", T.StringType()),
        T.StructField("feed_ts", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
    ]
)

# GTFS-RT says `speed` is metres/second, but these feeds actually send km/h.
# Verified by comparing reported speed against speed implied by consecutive
# GPS fixes (median ratio ≈3.3–3.45, expected 3.6 for km/h, 1.0 for m/s).
# Kuantan/Penang come from the same Prasarana system and are assumed to match.
SPEED_KMH_FEEDS = frozenset(
    {"rapid-bus-kl", "rapid-bus-mrtfeeder", "rapid-bus-kuantan", "rapid-bus-penang"}
)
# KTMB sends exactly 90.0 for every train on every poll: a placeholder, not a
# measurement. Speed is set to null (raw value kept in speed_reported_raw).
SPEED_UNAVAILABLE_FEEDS = frozenset({"ktmb"})
# Any other feed follows the GTFS-RT spec (metres/second).

# Generous bounding box around Peninsular Malaysia + Sabah/Sarawak (incl. Singapore).
LAT_RANGE = (0.5, 7.5)
LON_RANGE = (99.0, 120.0)
MAX_SPEED_KMH = 160.0  # faster than any bus or KTM train → GPS glitch
MAX_CLOCK_SKEW = "5 minutes"  # events claiming to be from the future

SILVER_COLUMNS = [
    "event_id",
    "feed",
    "vehicle_id",
    "license_plate",
    "trip_id",
    "route_id",
    "direction_id",
    "start_date",
    "start_time",
    "latitude",
    "longitude",
    "bearing",
    "speed_kmh",
    "speed_mps",
    "speed_reported_raw",
    "stop_id",
    "current_stop_sequence",
    "current_status",
    "event_ts",
    "feed_ts",
    "ingested_at",
    "processed_at",
    "event_date",
    "kafka_partition",
    "kafka_offset",
]


def parse_kafka(raw: DataFrame) -> DataFrame:
    """Kafka source rows → typed event columns. Keeps raw_value for the quarantine."""
    value = F.col("value").cast("string")
    return raw.select(
        value.alias("raw_value"),
        F.from_json(value, EVENT_SCHEMA).alias("e"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).select("e.*", "raw_value", "kafka_partition", "kafka_offset")


def standardize(df: DataFrame) -> DataFrame:
    is_kmh = F.col("feed").isin(*SPEED_KMH_FEEDS)
    unavailable = F.col("feed").isin(*SPEED_UNAVAILABLE_FEEDS)
    raw_speed = F.col("speed_mps")
    null = F.lit(None).cast("double")
    event_ts = F.to_timestamp("event_ts")
    return (
        df.withColumn("speed_reported_raw", raw_speed)
        .withColumn(
            "speed_kmh",
            F.when(unavailable, null).when(is_kmh, raw_speed).otherwise(raw_speed * 3.6),
        )
        .withColumn(
            "speed_mps",
            F.when(unavailable, null).when(is_kmh, raw_speed / 3.6).otherwise(raw_speed),
        )
        .withColumn("event_ts", event_ts)
        .withColumn("feed_ts", F.to_timestamp("feed_ts"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("processed_at", F.current_timestamp())
        # Partition by the *local* service day: that's how riders and planners think.
        .withColumn(
            "event_date", F.date_format(F.from_utc_timestamp(event_ts, LOCAL_TZ), "yyyy-MM-dd")
        )
    )


def _between(col: str, bounds: tuple[float, float]) -> Column:
    return F.col(col).between(*bounds)


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Add `dq_reason`: null for good rows, otherwise the first rule that failed."""
    reason = (
        F.when(F.col("event_id").isNull(), "unparseable_or_missing_event_id")
        .when(F.col("vehicle_id").isNull(), "missing_vehicle_id")
        .when(F.col("event_ts").isNull(), "missing_event_ts")
        .when(F.col("latitude").isNull() | F.col("longitude").isNull(), "missing_position")
        .when(
            ~(_between("latitude", LAT_RANGE) & _between("longitude", LON_RANGE)),
            "position_outside_malaysia",
        )
        .when(F.col("speed_kmh") < 0, "negative_speed")
        .when(F.col("speed_kmh") > MAX_SPEED_KMH, "implausible_speed")
        .when(
            F.col("event_ts") > F.col("ingested_at") + F.expr(f"INTERVAL {MAX_CLOCK_SKEW}"),
            "event_in_future",
        )
    )
    return df.withColumn("dq_reason", reason)


def dedupe(df: DataFrame, watermark: str = "15 minutes") -> DataFrame:
    """Drop repeated events (same event_id) that arrive within the watermark.

    The watermark bounds the state Spark keeps; events arriving later than
    that are dropped as too late, which is acceptable for analytics on
    30-second positions. Batch DataFrames (backfills, tests) use a plain
    dropDuplicates, which has the same result without a watermark.
    """
    if df.isStreaming:
        return df.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(["event_id"])
    return df.dropDuplicates(["event_id"])


def to_silver(events: DataFrame, watermark: str = "15 minutes") -> DataFrame:
    valid = events.filter(F.col("dq_reason").isNull())
    return dedupe(valid, watermark).select(*SILVER_COLUMNS)


def to_quarantine(events: DataFrame) -> DataFrame:
    return events.filter(F.col("dq_reason").isNotNull()).select(
        "dq_reason",
        "event_id",
        "feed",
        "vehicle_id",
        "latitude",
        "longitude",
        "speed_kmh",
        "event_ts",
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "processed_at",
        F.date_format("processed_at", "yyyy-MM-dd").alias("processed_date"),
    )
