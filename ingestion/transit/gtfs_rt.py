"""Parse GTFS-Realtime VehiclePosition protobuf payloads into flat, JSON-friendly records."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from google.transit import gtfs_realtime_pb2

VehiclePosition = gtfs_realtime_pb2.VehiclePosition


def _iso(epoch: int | None) -> str | None:
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC).isoformat()


def _opt(msg: Any, field: str) -> Any:
    """Return an optional proto2 field, or None when it was not sent."""
    return getattr(msg, field) if msg.HasField(field) else None


def parse_vehicle_positions(
    payload: bytes, feed: str, ingested_at: datetime | None = None
) -> list[dict[str, Any]]:
    """Turn one GTFS-RT FeedMessage into a list of vehicle-position records.

    `event_id` (feed + vehicle + timestamp) is a natural key used downstream
    to deduplicate: the API returns the same position on consecutive polls
    until the vehicle reports again.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(payload)

    ingested_at = ingested_at or datetime.now(UTC)
    feed_ts = message.header.timestamp if message.header.HasField("timestamp") else None

    records: list[dict[str, Any]] = []
    for entity in message.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        trip, pos, desc = v.trip, v.position, v.vehicle

        vehicle_id = desc.id or desc.label or entity.id
        event_ts = _opt(v, "timestamp") or feed_ts
        status = _opt(v, "current_status")

        records.append(
            {
                "event_id": f"{feed}:{vehicle_id}:{event_ts}",
                "feed": feed,
                "entity_id": entity.id,
                "vehicle_id": vehicle_id,
                "vehicle_label": desc.label or None,
                "license_plate": desc.license_plate or None,
                "trip_id": trip.trip_id or None,
                "route_id": trip.route_id or None,
                "direction_id": _opt(trip, "direction_id"),
                "start_date": trip.start_date or None,
                "start_time": trip.start_time or None,
                "latitude": _opt(pos, "latitude") if v.HasField("position") else None,
                "longitude": _opt(pos, "longitude") if v.HasField("position") else None,
                "bearing": _opt(pos, "bearing") if v.HasField("position") else None,
                "speed_mps": _opt(pos, "speed") if v.HasField("position") else None,
                "stop_id": v.stop_id or None,
                "current_stop_sequence": _opt(v, "current_stop_sequence"),
                "current_status": (
                    VehiclePosition.VehicleStopStatus.Name(status) if status is not None else None
                ),
                "event_ts": _iso(event_ts),
                "feed_ts": _iso(feed_ts),
                "ingested_at": ingested_at.isoformat(),
            }
        )
    return records
