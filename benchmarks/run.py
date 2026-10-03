"""`make benchmark` / `atlas benchmark`: run the reproducible suites in a dependency-safe order.

Order matters: experiments that share the working-scale lake run first, destructive ones (that reset
the pipeline) run after, and the last step rebuilds a clean, reconciled demo state.
"""

from __future__ import annotations

import logging
import time
import traceback
from typing import Any

from benchmarks import env as E
from benchmarks import suites as S

log = logging.getLogger(__name__)

ORDER = [
    "baseline_pipeline",
    "spark_performance",
    "query_performance",
    "full_vs_incremental",
    "backfill_benchmark",
    "iceberg_partitioning",
    "duplicate_test",
    "late_event_test",
    "schema_evolution",
    "iceberg_compaction",
    "streaming_throughput",
    "consumer_recovery",
    "final_state",
]


def _alive(spark) -> bool:
    try:
        spark.sql("SELECT 1").collect()
        return True
    except Exception:
        return False


def ensure_spark(spark):
    """Recreate the session if the driver JVM died (e.g. killed under memory pressure)."""
    if spark is not None and _alive(spark):
        return spark
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    from spark.common.session import get_spark

    log.warning("Spark driver is gone; starting a fresh session")
    try:
        if spark is not None:
            spark.stop()
    except Exception:
        pass
    SparkContext._active_spark_context = None
    SparkContext._gateway = None
    SparkContext._jvm = None
    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None
    return get_spark("atlas-benchmark")


def run_benchmarks(suite: str = "all", scale: str = "small") -> dict[str, Any]:
    from spark.common.session import get_spark

    selected = ORDER if suite == "all" else [s.strip() for s in suite.split(",")]
    working = E.SCALES[E.WORKING_SCALE[scale]]
    spark = get_spark("atlas-benchmark")
    results: dict[str, Any] = {}
    t_all = time.time()
    for name in ORDER:
        if name not in selected:
            continue
        t0 = time.time()
        log.info("=== %s ===", name)
        spark = ensure_spark(spark)
        try:
            if name == "baseline_pipeline":
                S.baseline_pipeline(E.LADDERS[scale])
            elif name == "spark_performance":
                S.spark_performance(spark, working)
                E.build_gold()
            elif name == "query_performance":
                S.query_performance(working)
            elif name == "full_vs_incremental":
                S.full_vs_incremental(spark)
            elif name == "backfill_benchmark":
                S.backfill_benchmark(spark)
            elif name == "iceberg_partitioning":
                S.iceberg_partitioning(spark)
            elif name == "duplicate_test":
                S.duplicate_test(spark, working)
            elif name == "late_event_test":
                S.late_event_test(spark)
            elif name == "schema_evolution":
                S.schema_evolution(spark)
            elif name == "iceberg_compaction":
                S.iceberg_compaction(spark, E.SCALES["100k"])
            elif name == "streaming_throughput":
                E.prepare_source(E.SCALES["100k"])
                E.reset_pipeline(spark)
                S.streaming_throughput(spark, rates=[100, 300, 700, 1400])
            elif name == "consumer_recovery":
                S.consumer_recovery(spark)
            elif name == "final_state":
                E.full_pipeline(spark, working)
                S.reconciliation()
                S.data_quality()
            results[name] = {"ok": True, "elapsed_s": round(time.time() - t0, 1)}
        except Exception as exc:
            log.error("suite %s failed: %s", name, exc)
            results[name] = {
                "ok": False,
                "elapsed_s": round(time.time() - t0, 1),
                "error": f"{type(exc).__name__}: {exc}",
                "trace": traceback.format_exc()[-2000:],
            }
    results["_total_elapsed_s"] = round(time.time() - t_all, 1)
    from benchmarks.common import write_result

    write_result("benchmark_run", {"scale": scale, "suite": suite, "results": results})
    return results
