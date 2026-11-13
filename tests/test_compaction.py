from datetime import UTC, datetime

import pyarrow as pa

from transit.compaction import Plan, dedupe_kafka_messages, plan_compaction

P = "bronze/vehicle_positions/feed=rapid-bus-kl/dt=2026-11-13"
NOW = datetime(2026, 11, 13, 10, 30, tzinfo=UTC)


def keys(hour: str, n: int, prefix: str = P) -> list[str]:
    return [f"{prefix}/hour={hour}/part-{i}.parquet" for i in range(n)]


def test_only_closed_hours_with_several_files_are_compacted():
    plans = plan_compaction(
        keys("08", 12)      # closed long ago → compact
        + keys("09", 12)    # ended 10:00, grace 20 min → 10:20 < 10:30 → compact
        + keys("10", 6)     # still being written → skip
        + keys("07", 1),    # already one file → skip
        NOW,
    )
    assert [p.partition for p in plans] == [f"{P}/hour=08/", f"{P}/hour=09/"]
    assert len(plans[0].inputs) == 12


def test_hour_inside_grace_period_is_left_alone():
    # hour 09 ended at 10:00; at 10:10 the sink may still flush a late file
    assert plan_compaction(keys("09", 5), datetime(2026, 11, 13, 10, 10, tzinfo=UTC)) == []


def test_output_name_is_deterministic_for_the_same_inputs():
    a = Plan(f"{P}/hour=08/", tuple(keys("08", 3)))
    b = Plan(f"{P}/hour=08/", tuple(reversed(keys("08", 3))))
    assert a.output == Plan(b.partition, tuple(sorted(b.inputs))).output
    assert a.output.startswith(f"{P}/hour=08/part-compacted-")


def test_late_file_after_compaction_triggers_a_new_merge():
    compacted = Plan(f"{P}/hour=08/", tuple(keys("08", 3))).output
    [plan] = plan_compaction([compacted, f"{P}/hour=08/part-late.parquet"], NOW)
    assert compacted in plan.inputs
    assert plan.output != compacted


def test_ignores_files_that_are_not_bronze_partitions():
    assert plan_compaction(["bronze/vehicle_positions/_tmp/x.json", "random.parquet"], NOW) == []


def test_dedupe_keeps_one_row_per_kafka_message():
    t = pa.table({
        "kafka_partition": [0, 0, 0, 1],
        "kafka_offset": [10, 11, 10, 10],   # (0,10) twice; (1,10) is a different message
        "event_id": ["a", "b", "a", "c"],
    })
    out = dedupe_kafka_messages(t)
    assert out.column("event_id").to_pylist() == ["a", "b", "c"]
