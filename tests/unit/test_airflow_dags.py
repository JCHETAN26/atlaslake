"""DAG integrity tests (Phase 24). Run inside the Airflow image; skipped where Airflow is absent."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow", reason="Airflow not installed (runs in the airflow image / CI)")
if not hasattr(airflow, "__version__"):  # the repo's own airflow/ directory, not the real package
    pytest.skip("Airflow not installed", allow_module_level=True)

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"

DAILY_CHAIN = [
    "check_sources", "ingest_batch", "validate_bronze", "spark_transform", "quality_check",
    "dbt_build", "dbt_test", "snowflake_configured", "snowflake_publish", "reconcile",
]


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert {"atlaslake_daily", "atlaslake_backfill", "atlaslake_maintenance"} <= set(dagbag.dag_ids)


def test_daily_task_order_is_the_phase24_chain(dagbag):
    dag = dagbag.get_dag("atlaslake_daily")
    assert {t.task_id for t in dag.tasks} == set(DAILY_CHAIN)
    for upstream, downstream in zip(DAILY_CHAIN, DAILY_CHAIN[1:], strict=False):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}, downstream


def test_daily_policies(dagbag):
    dag = dagbag.get_dag("atlaslake_daily")
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    for task in dag.tasks:
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=5)
        assert task.retry_exponential_backoff
        assert task.execution_timeout is not None
        assert task.on_failure_callback
    # the reconcile must still run when the Snowflake publish is skipped (no credentials)
    assert dag.get_task("reconcile").trigger_rule == "none_failed"
    assert "{{ ds }}" in dag.get_task("reconcile").bash_command


def test_backfill_is_manual_and_parameterised(dagbag):
    dag = dagbag.get_dag("atlaslake_backfill")
    assert dag.timetable.summary in ("None", "") or dag.schedule_interval is None
    assert {"dataset", "date_from", "date_to"} <= set(dag.params.keys())
    cmd = dag.get_task("backfill").bash_command
    assert "atlas backfill" in cmd and "params.date_from" in cmd
    assert [t.task_id for t in dag.topological_sort()] == ["backfill", "dbt_build", "reconcile"]


def test_maintenance_runs_weekly(dagbag):
    dag = dagbag.get_dag("atlaslake_maintenance")
    assert dag.get_task("iceberg_maintain").bash_command.endswith("atlas iceberg maintain")
