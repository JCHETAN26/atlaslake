"""AtlasLake historical backfill (Phase 25) — manually triggered.

Trigger with params, e.g. {"dataset": "orders", "date_from": "2026-01-01", "date_to": "2026-01-31"}.

Catch-up semantics: this DAG has no schedule and catchup=False. Backfills are expressed as an
explicit event-date range rather than N scheduled runs, because the Silver processor is idempotent
and range-based: re-running the same range converges to the same state (no double counting).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from atlas_common import DAG, DEFAULT_ARGS, Param, atlas

with DAG(
    dag_id="atlaslake_backfill",
    description="Re-process an event-date range Bronze → Silver → Gold, then reconcile",
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    default_args={**DEFAULT_ARGS, "retries": 1},
    params={
        "dataset": Param("orders", type="string",
                         enum=["orders", "payments", "inventory", "customers", "products"]),
        "date_from": Param("2026-01-01", type="string", format="date"),
        "date_to": Param("2026-01-31", type="string", format="date"),
    },
    tags=["atlaslake", "backfill"],
) as dag:
    backfill = atlas(
        "backfill",
        "atlas backfill --dataset {{ params.dataset }} --from {{ params.date_from }} --to {{ params.date_to }} --no-gold",
        timeout_min=600,
    )
    dbt_build = atlas("dbt_build", "atlas gold build", timeout_min=120)
    reconcile = atlas("reconcile", "atlas reconcile", timeout_min=60)
    backfill >> dbt_build >> reconcile
