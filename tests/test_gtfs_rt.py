from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2

from transit.gtfs_rt import parse_vehicle_positions
from transit.producer import LastSeen

INGESTED = datetime(2026, 11, 13, 1, 0, tzinfo=UTC)


def _feed_message(*vehicles: dict) -> bytes:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = 1794536400
    for i, v in enumerate(vehicles):
        e = msg.entity.add()
        e.id = str(i)
        vp = e.vehicle
        if "vehicle_id" in v:
            vp.vehicle.id = v["vehicle_id"]
        if "route_id" in v:
            vp.trip.route_id = v["route_id"]
        if "lat" in v:
            vp.position.latitude = v["lat"]
            vp.position.longitude = v["lon"]
        if "ts" in v:
            vp.timestamp = v["ts"]
    # A non-vehicle entity (alert) must be ignored
    alert = msg.entity.add()
    alert.id = "alert-1"
    alert.alert.header_text.translation.add(text="Service disruption")
    return msg.SerializeToString()


def test_parses_vehicle_fields():
    payload = _feed_message(
        {"vehicle_id": "WB1234", "route_id": "U81", "lat": 3.139, "lon": 101.6869, "ts": 1794536390}
    )
    [r] = parse_vehicle_positions(payload, "rapid-bus-kl", INGESTED)

    assert r["vehicle_id"] == "WB1234"
    assert r["route_id"] == "U81"
    assert abs(r["latitude"] - 3.139) < 1e-4
    assert r["event_id"] == "rapid-bus-kl:WB1234:1794536390"
    assert r["event_ts"].startswith("2026-")
    assert r["ingested_at"] == INGESTED.isoformat()


def test_missing_optional_fields_become_none():
    payload = _feed_message({"vehicle_id": "X1"})
    [r] = parse_vehicle_positions(payload, "ktmb", INGESTED)

    assert r["latitude"] is None
    assert r["route_id"] is None
    assert r["current_status"] is None
    # Falls back to the feed header timestamp when the vehicle has none
    assert r["event_id"] == "ktmb:X1:1794536400"


def test_vehicle_id_falls_back_to_entity_id():
    [r] = parse_vehicle_positions(_feed_message({"route_id": "T100"}), "rapid-bus-kl", INGESTED)
    assert r["vehicle_id"] == "0"


def test_last_seen_drops_repeated_positions():
    payload = _feed_message({"vehicle_id": "A", "ts": 100})
    seen = LastSeen()
    first = parse_vehicle_positions(payload, "f", INGESTED)
    second = parse_vehicle_positions(payload, "f", INGESTED)

    assert [seen.is_new(r) for r in first] == [True]
    assert [seen.is_new(r) for r in second] == [False]
