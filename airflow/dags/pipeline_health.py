"""Every 15 minutes: is data still flowing through every stage of the pipeline?

Alerts only on state CHANGES (healthy → failing, failing → recovered), so a
long outage produces one alert, not one every 15 minutes. The last state is
kept in the Airflow Variable `pipeline_health_failing`.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable

STATE_VAR = "pipeline_health_failing"


@dag(
    dag_id="pipeline_health",
    description="End-to-end freshness, lag and quarantine-rate checks with alerting",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kuala_Lumpur"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"owner": "data-eng", "retries": 0},
    tags=["monitoring", "alerting"],
)
def pipeline_health():
    @task
    def run_checks() -> None:
        from transit.alerts import send_alert
        from transit.health import run_all, summarize

        healthy, report = summarize(run_all())
        print(report)

        was_failing = Variable.get(STATE_VAR, default_var="false") == "true"
        if not healthy and not was_failing:
            send_alert("Pipeline unhealthy", report, "critical")
        elif healthy and was_failing:
            send_alert("Pipeline recovered", report, "resolved")
        Variable.set(STATE_VAR, "false" if healthy else "true")

        if not healthy:
            # Red in the Airflow UI too, but the callback-based alert is skipped:
            # this DAG already alerted above (and only on the state change).
            raise AirflowException("Health checks failed:\n" + report)

    run_checks()


pipeline_health()
