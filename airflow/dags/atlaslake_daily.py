"""Airflow DAG — AtlasLake daily batch / control-plane DAG (Phase 24).

check_sources → ingest_batch → validate_bronze → spark_transform → quality_check → dbt_build
→ dbt_test → snowflake_configured → snowflake_publish → reconcile

Real-time Silver is maintained by the always-on streaming job; this DAG is the *batch* control plane:
it drains Kafka with the same code (availableNow), re-derives the day from Bronze idempotently,
rebuilds Gold incrementally, publishes, and proves source → Gold reconciliation for `{{ ds }}`.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # shared helpers live beside the DAGs

from atlas_common import (
    DAG,
    DEFAULT_ARGS,
    Param,
    ShortCircuitOperator,
    atlas,
    snowflake_configured,
)

DATASET_FLAG = "{{ '' if params.dataset == 'all' else '--dataset ' ~ params.dataset }}"

with DAG(
    dag_id="atlaslake_daily",
    description="Daily AtlasLake batch pipeline: ingest → Silver → quality → Gold → publish → reconcile",
    doc_md=__doc__,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,  # history is rebuilt with the explicit backfill DAG, never by silent catch-up
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=DEFAULT_ARGS,
    params={
        "dataset": Param(
            "all", type="string", enum=["all", "orders", "payments", "inventory", "customers", "products"]
        ),
        "scale": Param("small", type="string", enum=["small", "medium", "large"]),
    },
    tags=["atlaslake", "batch"],
) as dag:
    check_sources = atlas(
        "check_sources",
        'curl -fsS "${ATLAS_SCHEMA_REGISTRY_URL:-http://schema-registry:8081}/subjects" > /dev/null && atlas status',
        timeout_min=10,
    )
    ingest_batch = atlas(
        "ingest_batch",
        "atlas stream --available-now --metrics-port 0 && atlas silver ingest-stores",
        timeout_min=120,
    )
    validate_bronze = atlas("validate_bronze", "atlas quality", timeout_min=30)
    spark_transform = atlas(
        "spark_transform",
        f"atlas silver rebuild {DATASET_FLAG} --from {{{{ ds }}}} --to {{{{ ds }}}}",
        timeout_min=120,
    )
    quality_check = atlas("quality_check", "atlas quality", timeout_min=30)
    dbt_build = atlas("dbt_build", "atlas gold build", timeout_min=60)
    dbt_test = atlas("dbt_test", "atlas gold build --select test_type:singular", timeout_min=30)
    sf_configured = ShortCircuitOperator(
        task_id="snowflake_configured",
        python_callable=snowflake_configured,
        ignore_downstream_trigger_rules=False,  # only skip the publish, never the reconcile
    )
    snowflake_publish = atlas("snowflake_publish", "atlas publish --target snowflake", timeout_min=60)
    reconcile = atlas(
        "reconcile", "atlas reconcile --date {{ ds }}", timeout_min=30, trigger_rule="none_failed"
    )

    (
        check_sources
        >> ingest_batch
        >> validate_bronze
        >> spark_transform
        >> quality_check
        >> dbt_build
        >> dbt_test
        >> sf_configured
        >> snowflake_publish
        >> reconcile
    )
