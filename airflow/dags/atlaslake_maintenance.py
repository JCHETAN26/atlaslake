"""Weekly Iceberg maintenance (Phase 28): compaction, snapshot expiration, orphan-file and metadata
cleanup, per the retention policy in docs/ICEBERG_MAINTENANCE.md."""

from __future__ import annotations

from datetime import datetime, timedelta

from atlas_common import DAG, DEFAULT_ARGS, atlas

with DAG(
    dag_id="atlaslake_maintenance",
    description="Weekly Iceberg table maintenance",
    doc_md=__doc__,
    schedule="0 4 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args=DEFAULT_ARGS,
    tags=["atlaslake", "maintenance"],
) as dag:
    atlas("iceberg_maintain", "atlas iceberg maintain", timeout_min=300)
