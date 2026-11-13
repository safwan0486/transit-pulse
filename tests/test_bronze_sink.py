import pyarrow as pa

from transit.bronze_sink import object_key, partition_key, records_to_table


def _record(**overrides):
    base = {
        "event_id": "rapid-bus-kl:A:1",
        "feed": "rapid-bus-kl",
        "vehicle_id": "A",
        "latitude": 3.1,
        "longitude": 101.6,
        "direction_id": 1,
        "ingested_at": "2026-11-13T09:15:00+00:00",
        "kafka_partition": 0,
        "kafka_offset": 42,
    }
    return {**base, **overrides}


def test_partition_by_feed_and_ingestion_hour():
    assert partition_key(_record()) == ("rapid-bus-kl", "2026-11-13", "09")


def test_object_key_layout():
    key = object_key("ktmb", "2026-11-13", "09")
    assert key.startswith("bronze/vehicle_positions/feed=ktmb/dt=2026-11-13/hour=09/part-")
    assert key.endswith(".parquet")


def test_records_match_schema_and_missing_fields_are_null():
    table = records_to_table([_record()])
    row = table.to_pylist()[0]
    assert row["kafka_offset"] == 42
    assert row["route_id"] is None  # not in the record → null, not an error
    assert table.schema.field("direction_id").type == pa.int32()
