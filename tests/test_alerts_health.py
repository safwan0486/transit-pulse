from datetime import UTC, datetime
from types import SimpleNamespace

import requests

from transit import alerts
from transit.health import _recent_prefixes, evaluate_max, summarize


class _Resp:
    def __init__(self, status=204):
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"HTTP {self.status}")


def test_payload_works_for_discord_and_slack():
    p = alerts.build_payload("Pipeline unhealthy", "silver freshness: 42 min", "critical")
    assert p["content"] == p["text"]
    assert p["text"].startswith("🔴 **[transit-pulse] Pipeline unhealthy**")
    assert len(alerts.build_payload("t", "x" * 5000)["content"]) <= 1900  # Discord limit


def test_without_webhook_alert_is_only_logged(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(requests, "post", lambda *a, **k: (_ for _ in ()).throw(AssertionError))
    assert alerts.send_alert("t", "b") is False


def test_alert_posted_to_webhook(monkeypatch):
    sent = {}
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.test/abc")
    def fake_post(url, json, timeout):
        sent.update(url=url, json=json)
        return _Resp()

    monkeypatch.setattr(requests, "post", fake_post)
    assert alerts.send_alert("Pipeline recovered", "all OK", "resolved") is True
    assert sent["url"] == "https://hooks.example.test/abc"
    assert sent["json"]["text"].startswith("🟢")


def test_broken_webhook_never_raises(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.test/abc")
    monkeypatch.setattr(requests, "post", lambda *a, **k: _Resp(500))
    assert alerts.send_alert("t", "b") is False


def test_airflow_callback_names_the_failed_task(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        alerts, "send_alert", lambda title, body, sev: captured.update(t=title, b=body)
    )
    ti = SimpleNamespace(
        dag_id="transit_dbt_build", task_id="dbt_build", try_number=2, log_url="http://x"
    )
    context = {"task_instance": ti, "run_id": "r1", "exception": "test failed"}
    alerts.airflow_failure_callback(context)
    assert captured["t"] == "transit_dbt_build failed"
    assert "transit_dbt_build.dbt_build" in captured["b"]
    assert "test failed" in captured["b"]


def test_missing_data_counts_as_failure():
    assert not evaluate_max("silver freshness", None, 10, "min").ok
    assert not evaluate_max("silver freshness", 11, 10, "min").ok
    assert evaluate_max("silver freshness", 10, 10, "min").ok


def test_summary_lists_every_check():
    healthy, report = summarize(
        [evaluate_max("a", 1, 10, "min"), evaluate_max("b", 50, 10, "min")]
    )
    assert not healthy
    assert report.splitlines()[0].startswith("OK   a")
    assert report.splitlines()[1].startswith("FAIL b")


def test_recent_prefixes_cover_utc_and_malaysia_day_boundary():
    # 23:30 UTC on the 13th is already the 14th in Kuala Lumpur
    now = datetime(2026, 11, 13, 23, 30, tzinfo=UTC)
    prefixes = _recent_prefixes("silver/vehicle_positions", ["ktmb"], "event_date", now)
    assert "silver/vehicle_positions/feed=ktmb/event_date=2026-11-14/" in prefixes
    assert "silver/vehicle_positions/feed=ktmb/event_date=2026-11-13/" in prefixes
