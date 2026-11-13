import io
import zipfile

import pytest

from transit.gtfs_static import GtfsValidationError, silver_key, zip_to_tables


def _zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


VALID = {
    # BOM + stray spaces in the header: both happen in real feeds
    "stops.txt": "﻿stop_id, stop_name,stop_lat,stop_lon\n1,KL Sentral,3.134,101.686\n",
    "routes.txt": "route_id,route_short_name\nU81,U81\n",
    "trips.txt": "route_id,service_id,trip_id\nU81,WD,t1\n",
    "stop_times.txt": (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "t1,06:00:00,06:00:00,1,1\n"
    ),
}


def test_reads_all_tables_as_strings_with_metadata():
    tables = zip_to_tables(_zip(VALID), "rapid-bus-kl", "2026-11-13")

    stops = tables["stops"].to_pylist()
    assert stops == [
        {
            "stop_id": "1",
            "stop_name": "KL Sentral",
            "stop_lat": "3.134",
            "stop_lon": "101.686",
            "feed": "rapid-bus-kl",
            "snapshot_date": "2026-11-13",
        }
    ]
    assert set(tables) == {"stops", "routes", "trips", "stop_times"}


def test_missing_required_table_fails():
    files = {k: v for k, v in VALID.items() if k != "stop_times.txt"}
    with pytest.raises(GtfsValidationError, match="stop_times"):
        zip_to_tables(_zip(files), "f", "2026-11-13")


def test_empty_required_table_fails():
    files = {**VALID, "routes.txt": "route_id,route_short_name\n"}
    with pytest.raises(GtfsValidationError, match="empty"):
        zip_to_tables(_zip(files), "f", "2026-11-13")


def test_silver_key_is_deterministic_for_idempotent_reruns():
    assert silver_key("stops", "ktmb", "2026-11-13") == (
        "silver/gtfs_static/stops/feed=ktmb/dt=2026-11-13/data.parquet"
    )
