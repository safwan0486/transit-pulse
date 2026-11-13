"""Dashboard queries against small local Parquet files (no lake needed)."""

from datetime import date, datetime

import duckdb
import pandas as pd
import pytest

import data


@pytest.fixture
def lake(tmp_path):
    gold = tmp_path / "gold"
    gold.mkdir()
    d1, d2 = date(2026, 11, 12), date(2026, 11, 13)

    pd.DataFrame(
        {
            "feed": ["rapid-bus-mrtfeeder"] * 3,
            "service_date": [d2, d2, d1],
            "route_id": ["r1", "r2", "r1"],
            "route_name": ["T803", "T117", "T803"],
            "observed_arrivals": [100, 10, 50],
            "observed_trips": [5, 1, 3],
            "median_delay_min": [15.8, 1.0, 2.0],
            "p90_delay_min": [19.2, 3.0, 4.0],
            "pct_on_time": [5.0, 90.0, 60.0],
            "pct_early": [0.0, 5.0, 10.0],
            "pct_late": [95.0, 5.0, 30.0],
        }
    ).to_parquet(gold / "mart_route_on_time_daily.parquet")

    pd.DataFrame(
        {
            "feed": ["rapid-bus-mrtfeeder"] * 2,
            "service_date": [d2, d2],
            "hour_local": [8, 9],
            "observed_arrivals": [300, 100],
            "routes": [20, 10],
            "median_delay_min": [2.0, 6.0],
            "p90_delay_min": [5.0, 12.0],
            "pct_on_time": [80.0, 40.0],
        }
    ).to_parquet(gold / "mart_network_delay_hourly.parquet")

    pd.DataFrame(
        {
            "feed": ["rapid-bus-kl"] * 2,
            "service_date": [d2, d2],
            "route_id": ["300", "303"],
            "route_name": ["300", "303"],
            "observed_headways": [30, 3],
            "median_headway_ratio": [1.1, 0.4],
            "pct_bunched": [20.0, 60.0],
            "pct_gaps": [10.0, 0.0],
            "pct_evenly_spaced": [70.0, 40.0],
            "avg_wait_min": [12.0, 4.0],
            "scheduled_wait_min": [10.0, 10.0],
            "excess_wait_min": [2.0, -6.0],
        }
    ).to_parquet(gold / "mart_route_headway_daily.parquet")

    pd.DataFrame(
        {
            "feed": ["ktmb", "rapid-bus-kl", "rapid-bus-kl"],
            "hour_local": pd.to_datetime(
                ["2026-11-13 14:00", "2026-11-13 14:00", "2026-11-13 13:00"]
            ),
            "events": [115, 6187, 1914],
            "vehicles": [5, 145, 138],
            "frozen_vehicles": [5, 1, 0],
            "pct_frozen": [100.0, 0.7, 0.0],
            "pct_trip_matched": [100.0, 97.3, 99.1],
        }
    ).to_parquet(gold / "mart_feed_health_hourly.parquet")
    return str(tmp_path).replace("\\", "/")


@pytest.fixture
def con():
    return duckdb.connect()


def test_service_dates_newest_first(con, lake):
    assert data.service_dates(con, lake) == [date(2026, 11, 13), date(2026, 11, 12)]


def test_route_ranking_filters_small_samples_and_sorts_worst_first(con, lake):
    df = data.on_time_routes(con, lake, date(2026, 11, 13), min_arrivals=20)
    assert list(df["route_name"]) == ["T803"]  # T117 has only 10 arrivals


def test_network_on_time_is_weighted_by_arrivals(con, lake):
    result = data.network_on_time(con, lake, date(2026, 11, 13))
    assert result["arrivals"] == 400
    assert result["pct_on_time"] == pytest.approx((80 * 300 + 40 * 100) / 400)  # 70, not 60
    assert result["delay_min"] == pytest.approx((2 * 300 + 6 * 100) / 400)


def test_headway_ranking_least_regular_first(con, lake):
    df = data.headway_routes(con, lake, date(2026, 11, 13), min_headways=1)
    assert list(df["route_name"]) == ["303", "300"]


def test_network_headway_weights_by_observed_headways(con, lake):
    result = data.network_headway(con, lake, date(2026, 11, 13))
    assert result["headways"] == 33
    assert result["pct_even"] == pytest.approx((70 * 30 + 40 * 3) / 33)


def test_feed_health_uses_latest_hour_and_flags_frozen(con, lake):
    df = data.feed_health(con, lake).set_index("feed")
    assert df.loc["ktmb", "status"] == "frozen"
    assert df.loc["rapid-bus-kl", "status"] == "healthy"
    assert df.loc["rapid-bus-kl", "events"] == 6187  # 14:00, not 13:00


def test_live_vehicles_keeps_latest_ping_per_vehicle(con, tmp_path):
    part = tmp_path / "silver" / "vehicle_positions" / "feed=rapid-bus-kl" / "event_date=2026-11-13"
    part.mkdir(parents=True)
    pd.DataFrame(
        {
            "vehicle_id": ["A", "A", "B"],
            "route_id": ["U81", "U81", "300"],
            "latitude": [3.10, 3.11, 3.20],
            "longitude": [101.60, 101.61, 101.70],
            "speed_kmh": [20.0, 25.0, 0.0],
            "event_ts": pd.to_datetime(
                ["2026-11-13 07:20", "2026-11-13 07:25", "2026-11-13 06:00"]
            ),
        }
    ).to_parquet(part / "part-0.parquet")

    root = str(tmp_path).replace("\\", "/")
    df = data.live_vehicles(con, root, minutes=10, now_utc=datetime(2026, 11, 13, 7, 28))
    assert list(df["vehicle_id"]) == ["A"]  # B is older than 10 minutes
    assert df.iloc[0]["latitude"] == pytest.approx(3.11)
    assert df.iloc[0]["feed"] == "rapid-bus-kl"
