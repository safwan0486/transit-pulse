"""Unit tests for the silver transforms. Run inside the Spark image:

    docker compose run --rm --no-deps spark-streaming python3 -m pytest -v tests
"""

import json

import pytest
from jobs.transforms import (
    SILVER_COLUMNS,
    add_quality_flags,
    parse_kafka,
    standardize,
    to_quarantine,
    to_silver,
)
from pyspark.sql import types as T

KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("value", T.BinaryType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
    ]
)


def event(**overrides):
    base = {
        "event_id": "rapid-bus-kl:WVC3917:1794536681",
        "feed": "rapid-bus-kl",
        "vehicle_id": "WVC3917",
        "license_plate": "WVC3917",
        "trip_id": "weekday_U8220_U822002_0",
        "route_id": "U8220",
        "latitude": 3.1373,
        "longitude": 101.674,
        "bearing": 90.0,
        "speed_mps": 36.0,  # field name from the spec, value is really km/h
        "event_ts": "2026-11-13T02:24:41+00:00",
        "feed_ts": "2026-11-13T02:25:03+00:00",
        "ingested_at": "2026-11-13T02:25:09.690015+00:00",
    }
    return {**base, **overrides}


@pytest.fixture
def run(spark):
    """Push raw Kafka values through the pipeline; return (silver, quarantine) rows."""

    def _run(*values):
        rows = [
            (v if isinstance(v, bytes) else json.dumps(v).encode(), 0, i)
            for i, v in enumerate(values)
        ]
        kafka = spark.createDataFrame(rows, KAFKA_SCHEMA)
        events = add_quality_flags(standardize(parse_kafka(kafka)))
        silver = [r.asDict() for r in to_silver(events).collect()]
        quarantine = [r.asDict() for r in to_quarantine(events).collect()]
        return silver, quarantine

    return _run


def test_valid_event_reaches_silver_with_all_columns(run):
    [row], quarantine = run(event())
    assert quarantine == []
    assert list(row) == SILVER_COLUMNS
    assert row["vehicle_id"] == "WVC3917"
    assert row["kafka_offset"] == 0


def test_bus_speed_is_treated_as_kmh(run):
    [row], _ = run(event(speed_mps=36.0))
    assert row["speed_kmh"] == pytest.approx(36.0)
    assert row["speed_mps"] == pytest.approx(10.0)
    assert row["speed_reported_raw"] == pytest.approx(36.0)


def test_other_feeds_follow_the_spec_mps(run):
    [row], _ = run(event(feed="some-new-feed", event_id="x:T1:1", speed_mps=10.0))
    assert row["speed_mps"] == pytest.approx(10.0)
    assert row["speed_kmh"] == pytest.approx(36.0)


def test_ktmb_placeholder_speed_becomes_null_not_quarantined(run):
    # KTMB always sends 90.0; read as m/s that would be 324 km/h → wrongly "implausible"
    [row], quarantine = run(event(feed="ktmb", event_id="ktmb:9224:1", speed_mps=90.0))
    assert quarantine == []
    assert row["speed_kmh"] is None
    assert row["speed_mps"] is None
    assert row["speed_reported_raw"] == pytest.approx(90.0)


def test_event_date_uses_malaysia_local_day(run):
    # 17:30 UTC on the 13th is 01:30 on the 14th in Kuala Lumpur
    late_evening = event(
        event_ts="2026-11-13T17:30:00+00:00", ingested_at="2026-11-13T17:30:05+00:00"
    )
    [row], _ = run(late_evening)
    assert row["event_date"] == "2026-11-14"


def test_duplicates_are_removed(run):
    silver, _ = run(event(), event(), event(event_id="rapid-bus-kl:WVC3917:1794536711"))
    assert sorted(r["event_id"] for r in silver) == [
        "rapid-bus-kl:WVC3917:1794536681",
        "rapid-bus-kl:WVC3917:1794536711",
    ]


@pytest.mark.parametrize(
    ("bad", "reason"),
    [
        (b"{not json", "unparseable_or_missing_event_id"),
        (event(vehicle_id=None), "missing_vehicle_id"),
        (event(event_ts=None), "missing_event_ts"),
        (event(latitude=None), "missing_position"),
        (event(latitude=0.0, longitude=0.0), "position_outside_malaysia"),  # "null island"
        (event(speed_mps=-5.0), "negative_speed"),
        (event(speed_mps=250.0), "implausible_speed"),
        (event(event_ts="2026-11-13T03:00:00+00:00"), "event_in_future"),
    ],
)
def test_bad_rows_go_to_quarantine_with_reason(run, bad, reason):
    silver, [q] = run(bad)
    assert silver == []
    assert q["dq_reason"] == reason
    assert q["raw_value"]  # original payload kept for debugging / replay


def test_streaming_pipeline_dedupes_across_micro_batches(spark, tmp_path):
    """Run the real streaming path (watermark + state), not just batch mode.

    The same event arrives in two separate micro-batches — exactly what
    happens when the producer restarts and re-sends a position.
    """
    src = tmp_path / "src"
    out = tmp_path / "out"

    def write_batch(name, *events):
        rows = [(json.dumps(e).encode(), 0, i) for i, e in enumerate(events)]
        spark.createDataFrame(rows, KAFKA_SCHEMA).write.parquet(str(src / name))

    write_batch("b1", event())
    stream = spark.readStream.schema(KAFKA_SCHEMA).parquet(str(src / "*"))
    silver = to_silver(add_quality_flags(standardize(parse_kafka(stream))))
    assert silver.isStreaming

    def run_once():
        (
            silver.writeStream.format("parquet")
            .option("path", str(out / "data"))
            .option("checkpointLocation", str(out / "checkpoint"))
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )

    run_once()
    write_batch("b2", event(), event(event_id="rapid-bus-kl:WVC3917:1794536711"))
    run_once()  # resumes from the checkpoint, remembers event 1 in dedupe state

    ids = sorted(r.event_id for r in spark.read.parquet(str(out / "data")).collect())
    assert ids == ["rapid-bus-kl:WVC3917:1794536681", "rapid-bus-kl:WVC3917:1794536711"]


def test_one_bad_row_does_not_block_good_rows(run):
    silver, quarantine = run(event(), b"garbage", event(latitude=0.0, longitude=0.0))
    assert len(silver) == 1
    assert sorted(q["dq_reason"] for q in quarantine) == [
        "position_outside_malaysia",
        "unparseable_or_missing_event_id",
    ]
