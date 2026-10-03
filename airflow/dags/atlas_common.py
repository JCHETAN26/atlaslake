"""Shared defaults for AtlasLake DAGs (imported by the DAG files; defines no DAG itself)."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import timedelta

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import ShortCircuitOperator
    from airflow.sdk import DAG, Param
except ImportError:  # Airflow 2 fallback
    from airflow import DAG  # type: ignore[no-redef]
    from airflow.models.param import Param  # type: ignore[no-redef]
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef]
    from airflow.operators.python import ShortCircuitOperator  # type: ignore[no-redef]

log = logging.getLogger(__name__)
ALERT_WEBHOOK = os.environ.get("ATLAS_ALERT_WEBHOOK", "http://host.docker.internal:9111/alerts")


def notify_failure(context: dict) -> None:
    """on_failure_callback: log loudly and POST an alert (never raises)."""
    ti = context.get("task_instance")
    payload = {
        "source": "airflow",
        "severity": "critical",
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
        "logical_date": str(context.get("logical_date")),
        "exception": str(context.get("exception"))[:500],
    }
    log.error("ATLASLAKE TASK FAILED: %s", payload)
    try:
        req = urllib.request.Request(ALERT_WEBHOOK, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=5)  # noqa: S310 (fixed internal URL)
    except Exception as exc:  # alerting must not mask the original failure
        log.warning("alert webhook unreachable: %s", exc)


DEFAULT_ARGS = {
    "owner": "atlaslake",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_failure,
    "depends_on_past": False,
}


def atlas(task_id: str, command: str, timeout_min: int = 60, **kw) -> BashOperator:
    """BashOperator running the AtlasLake CLI (`atlas` wrapper installed in the image)."""
    return BashOperator(task_id=task_id, bash_command=f"set -euo pipefail; {command}",
                        execution_timeout=timedelta(minutes=timeout_min), **kw)


def snowflake_configured() -> bool:
    configured = bool(os.environ.get("ATLAS_SNOWFLAKE_ACCOUNT"))
    if not configured:
        log.info("ATLAS_SNOWFLAKE_ACCOUNT not set: skipping Snowflake publish")
    return configured


__all__ = ["DAG", "DEFAULT_ARGS", "BashOperator", "Param", "ShortCircuitOperator", "atlas",
           "notify_failure", "snowflake_configured"]
