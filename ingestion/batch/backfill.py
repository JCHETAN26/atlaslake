"""Historical backfills (Phase 25): re-drive a Bronze event-date range through Silver (and Gold).

Semantics: a backfill *re-asserts* history. Because every Silver write is idempotent and commutative,
running a range that is already present changes nothing, and running a range that is missing (or was
rejected under older validation rules) fills it in — never double counting. Gold is refreshed through
the normal incremental dbt run (Silver changes carry a new processing timestamp).
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

from contracts import events as ev


def run_backfill(datasets: list[str], date_from: date, date_to: date, rebuild_gold: bool = True,
                 spark=None) -> dict[str, Any]:
    from lakehouse.fingerprint import silver_fingerprint
    from monitoring.metrics import push_job_metrics
    from spark.batch.bronze_to_silver import run as silver_run
    from spark.common.session import get_spark

    unknown = [d for d in datasets if d not in ev.DATASETS]
    if unknown:
        raise ValueError(f"unknown dataset(s) {unknown}; expected {sorted(ev.DATASETS)}")
    if date_to < date_from:
        raise ValueError("--to must be on or after --from")
    spark = spark or get_spark("atlas-backfill")
    t0 = time.time()
    silver = silver_run(spark, datasets, date_from, date_to, by="event",
                        query=f"backfill:{','.join(sorted(datasets))}:{date_from}:{date_to}")
    silver_s = time.time() - t0
    gold: dict[str, Any] | None = None
    if rebuild_gold:
        from lineage.dbt_runner import dbt_build

        g = dbt_build()
        gold = {"success": g["success"], "elapsed_s": g["elapsed_s"]}
    elapsed = time.time() - t0
    out = {
        "datasets": datasets, "from": str(date_from), "to": str(date_to),
        "records_read": silver["received"], "accepted_new": silver["accepted_new"],
        "already_present": silver["duplicates_known"] + silver["duplicates_in_batch"],
        "rejected": silver["rejected"], "silver_runtime_s": round(silver_s, 3),
        "throughput_records_per_s": round(silver["received"] / silver_s, 1) if silver_s else None,
        "gold": gold, "total_runtime_s": round(elapsed, 3),
        "silver_fingerprint": silver_fingerprint(),
    }
    push_job_metrics("backfill", {"runtime_seconds": elapsed, "records": silver["received"],
                                  "accepted_new": silver["accepted_new"]},
                     labels={"dataset": ",".join(datasets)}, success=not gold or gold["success"])
    return out
