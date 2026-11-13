"""Send alerts to a chat webhook (Discord or Slack).

Set ALERT_WEBHOOK_URL in .env to a Discord or Slack "incoming webhook" URL.
When it is unset, alerts are only logged, so local runs never fail because
alerting isn't configured.
"""

from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)

ICONS = {"critical": "🔴", "warning": "🟠", "resolved": "🟢", "info": "🔵"}


def build_payload(title: str, body: str, severity: str = "critical") -> dict:
    text = f"{ICONS.get(severity, '')} **[transit-pulse] {title}**\n{body}".strip()
    # Discord reads "content", Slack reads "text"; each ignores the other key.
    return {"content": text[:1900], "text": text}


def send_alert(title: str, body: str, severity: str = "critical") -> bool:
    """Returns True if delivered to a webhook, False if only logged."""
    payload = build_payload(title, body, severity)
    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    log_fn = log.error if severity == "critical" else log.warning
    log_fn("ALERT %s: %s | %s", severity.upper(), title, body.replace("\n", " | "))
    if not url:
        return False
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException:
        # Never let a broken webhook hide the original failure.
        log.exception("Could not deliver alert to webhook")
        return False


def airflow_failure_callback(context: dict) -> None:
    """Use as `on_failure_callback` in a DAG's default_args."""
    ti = context.get("task_instance")
    dag_id = getattr(ti, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    body = "\n".join(
        [
            f"Task `{dag_id}.{task_id}` failed (try {getattr(ti, 'try_number', '?')}).",
            f"Run: {context.get('run_id', '?')}",
            f"Error: {str(context.get('exception', 'unknown'))[:500]}",
            f"Logs: {getattr(ti, 'log_url', '')}",
        ]
    )
    send_alert(f"{dag_id} failed", body, "critical")
