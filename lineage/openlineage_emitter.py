"""OpenLineage emission for every AtlasLake job (Phase 29) → Marquez.

The graph spans the whole platform:

    postgres commerce.outbox ─► relay ─► kafka commerce.* ─► silver_ingest ─► iceberg bronze.*/silver.*/ops.dlq
    postgres commerce.stores ─► stores_ingest ─► iceberg silver.stores
    iceberg silver.* ─► dbt model jobs (one per model, from manifest.json) ─► duckdb gold.*
    duckdb gold.* ─► publish ─► snowflake / postgres serving / iceberg gold.*

Emission is best-effort: lineage must never fail a pipeline.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from contracts import events as ev
from core.config import settings

log = logging.getLogger(__name__)
NAMESPACE = "atlaslake"
PRODUCER = "https://github.com/JCHETAN26/atlaslake"

DS_KAFKA = "kafka://kafka:29092"
DS_ICEBERG = "iceberg://atlas"
DS_POSTGRES = "postgres://postgres:5432"
DS_GOLD = "duckdb://gold"
DS_SNOWFLAKE = "snowflake://atlaslake"

BRONZE = [f"bronze.{d}" for d in ev.DATASETS]
SILVER = [
    "silver.events",
    "silver.orders",
    "silver.order_items",
    "silver.payments",
    "silver.customers",
    "silver.customer_changes",
    "silver.products",
    "silver.product_changes",
    "silver.inventory",
    "silver.inventory_movements",
]

# Static (non-dbt) jobs: name -> (inputs, outputs) as (namespace, dataset) pairs
STATIC_JOBS: dict[str, tuple[list[tuple[str, str]], list[tuple[str, str]]]] = {
    "relay": ([(DS_POSTGRES, "commerce.commerce.outbox")], [(DS_KAFKA, t) for t in ev.TOPICS]),
    "silver_ingest": (
        [(DS_KAFKA, t) for t in ev.TOPICS],
        [(DS_ICEBERG, t) for t in [*BRONZE, *SILVER, "ops.dlq", "ops.stream_batches"]],
    ),
    "realtime_sales": ([(DS_KAFKA, ev.TOPIC_ORDERS)], [(DS_ICEBERG, "silver.realtime_sales_1m")]),
    "silver_rebuild": ([(DS_ICEBERG, t) for t in BRONZE], [(DS_ICEBERG, t) for t in SILVER]),
    "stores_ingest": ([(DS_POSTGRES, "commerce.commerce.stores")], [(DS_ICEBERG, "silver.stores")]),
    "dlq_replay": (
        [(DS_ICEBERG, "ops.dlq"), (DS_POSTGRES, "commerce.commerce.outbox")],
        [(DS_KAFKA, t) for t in ev.TOPICS],
    ),
}

GOLD_PUBLISHED = [
    "dim_customer",
    "dim_product",
    "dim_store",
    "dim_date",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_inventory",
    "mart_daily_sales",
    "mart_order_funnel",
    "mart_payment_metrics",
    "mart_inventory_health",
]


_AVAILABLE: bool | None = None


def _available() -> bool:
    """One fast TCP probe per process: never pay HTTP retry back-off when Marquez is not running."""
    global _AVAILABLE
    if _AVAILABLE is None:
        import socket
        from urllib.parse import urlparse

        u = urlparse(settings().openlineage_url)
        try:
            with socket.create_connection((u.hostname or "localhost", u.port or 80), timeout=0.5):
                _AVAILABLE = True
        except OSError:
            _AVAILABLE = False
    return _AVAILABLE


def _client():
    from openlineage.client import OpenLineageClient
    from openlineage.client.transport.http import HttpConfig, HttpTransport

    return OpenLineageClient(transport=HttpTransport(HttpConfig(url=settings().openlineage_url, timeout=3)))


def _datasets(pairs: list[tuple[str, str]], output: bool) -> list[Any]:
    from openlineage.client.event_v2 import InputDataset, OutputDataset

    cls = OutputDataset if output else InputDataset
    return [cls(namespace=ns, name=name) for ns, name in pairs]


def emit(
    job: str,
    inputs: list[tuple[str, str]],
    outputs: list[tuple[str, str]],
    success: bool = True,
    started: datetime | None = None,
    run_id: str | None = None,
    facets: dict[str, Any] | None = None,
) -> bool:
    """Emit START + COMPLETE/FAIL for one job run."""
    if not _available():
        return False
    try:
        from openlineage.client.event_v2 import Job, Run, RunEvent, RunState

        client = _client()
        rid = run_id or str(uuid.uuid4())
        t0 = (started or datetime.now(UTC)).isoformat()
        job_obj = Job(namespace=NAMESPACE, name=job)
        run = Run(runId=rid)
        ins, outs = _datasets(inputs, False), _datasets(outputs, True)
        client.emit(
            RunEvent(
                eventType=RunState.START,
                eventTime=t0,
                run=run,
                job=job_obj,
                inputs=ins,
                outputs=outs,
                producer=PRODUCER,
            )
        )
        client.emit(
            RunEvent(
                eventType=RunState.COMPLETE if success else RunState.FAIL,
                eventTime=datetime.now(UTC).isoformat(),
                run=run,
                job=job_obj,
                inputs=ins,
                outputs=outs,
                producer=PRODUCER,
            )
        )
        return True
    except Exception as exc:
        log.debug("openlineage emit failed for %s: %s", job, exc)
        return False


def emit_static(job: str, success: bool = True) -> bool:
    ins, outs = STATIC_JOBS[job]
    return emit(job, ins, outs, success)


def dbt_model_edges(
    manifest: dict[str, Any],
) -> dict[str, tuple[list[tuple[str, str]], list[tuple[str, str]]]]:
    """model name -> (inputs, outputs), resolving dbt sources to the Iceberg tables they read."""
    nodes = manifest["nodes"]
    out = {}
    for node in nodes.values():
        if node["resource_type"] != "model":
            continue
        inputs = []
        for dep in node["depends_on"]["nodes"]:
            if dep.startswith("source."):
                src = manifest["sources"][dep]
                inputs.append((DS_ICEBERG, f"{src['source_name']}.{src['name']}"))
            elif dep.startswith("model."):
                d = nodes[dep]
                inputs.append((DS_GOLD, f"{d['schema']}.{d['name']}"))
        out[node["name"]] = (inputs, [(DS_GOLD, f"{node['schema']}.{node['name']}")])
    return out


def emit_dbt_run(result: dict[str, Any]) -> int:
    from lineage.dbt_runner import manifest

    edges = dbt_model_edges(manifest())
    status = {n["node"]: n["status"] for n in result.get("nodes", [])}
    sent = 0
    for model, (ins, outs) in edges.items():
        if model in status:
            sent += emit(f"dbt.{model}", ins, outs, success=status[model] == "success")
    return sent


def emit_publish(targets: list[str], success: bool = True) -> bool:
    ins = [(DS_GOLD, f"gold.{t}") for t in GOLD_PUBLISHED]
    ns = {"snowflake": DS_SNOWFLAKE, "postgres": DS_POSTGRES, "iceberg": DS_ICEBERG}
    outs = []
    for target in targets:
        prefix = {"snowflake": "ATLASLAKE.GOLD.", "postgres": "analytics.gold.", "iceberg": "gold."}[target]
        outs += [(ns[target], f"{prefix}{t}") for t in GOLD_PUBLISHED]
    return emit("publish", ins, outs, success)


def emit_full_graph() -> int:
    """Emit the whole platform graph once (used by `make demo` and the lineage test)."""
    sent = sum(emit_static(j) for j in STATIC_JOBS)
    from lineage.dbt_runner import manifest

    for model, (ins, outs) in dbt_model_edges(manifest()).items():
        sent += emit(f"dbt.{model}", ins, outs)
    sent += emit_publish(["postgres", "iceberg", "snowflake"])
    return sent
