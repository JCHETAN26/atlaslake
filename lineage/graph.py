"""Upstream lineage lookup for `atlas lineage --table X` (Marquez first, local graph fallback)."""

from __future__ import annotations

from collections import deque
from typing import Any

import httpx

from core.config import settings
from lineage import openlineage_emitter as ol


def _local_edges() -> dict[tuple[str, str], list[tuple[str, tuple[str, str]]]]:
    """dataset -> [(job, upstream dataset)] from the static job map + dbt manifest."""
    edges: dict[tuple[str, str], list[tuple[str, tuple[str, str]]]] = {}
    jobs = dict(ol.STATIC_JOBS)
    try:
        from lineage.dbt_runner import manifest

        jobs.update({f"dbt.{m}": v for m, v in ol.dbt_model_edges(manifest()).items()})
    except Exception:
        pass
    jobs["publish"] = ([(ol.DS_GOLD, f"gold.{t}") for t in ol.GOLD_PUBLISHED],
                       [(ol.DS_SNOWFLAKE, f"ATLASLAKE.GOLD.{t}") for t in ol.GOLD_PUBLISHED])
    for job, (ins, outs) in jobs.items():
        for o in outs:
            edges.setdefault(o, []).extend((job, i) for i in ins)
    return edges


def _resolve(table: str, edges) -> tuple[str, str] | None:
    for key in edges:
        if key[1] == table or key[1].endswith("." + table):
            return key
    return None


def upstream(table: str, depth: int = 10) -> dict[str, Any]:
    marquez = _marquez_upstream(table, depth)
    if marquez is not None:
        return marquez
    edges = _local_edges()
    start = _resolve(table, edges)
    if start is None:
        return {"table": table, "source": "local", "error": "unknown table"}
    seen, out, q = {start}, [], deque([(start, 0)])
    while q:
        node, d = q.popleft()
        if d >= depth:
            continue
        for job, up in edges.get(node, []):
            out.append({"dataset": f"{node[0]}/{node[1]}", "via_job": job, "upstream": f"{up[0]}/{up[1]}", "depth": d + 1})
            if up not in seen:
                seen.add(up)
                q.append((up, d + 1))
    roots = sorted({e["upstream"] for e in out} - {e["dataset"] for e in out})
    return {"table": f"{start[0]}/{start[1]}", "source": "local-graph", "edges": out, "root_sources": roots}


def _marquez_upstream(table: str, depth: int) -> dict[str, Any] | None:
    base = settings().openlineage_url.rstrip("/")
    try:
        found = httpx.get(f"{base}/api/v1/search", params={"q": table, "filter": "DATASET", "limit": 10}, timeout=3)
        hits = [r for r in found.json().get("results", []) if r["name"].split(".")[-1] == table.split(".")[-1]]
        if not hits:
            return None
        hits.sort(key=lambda r: (r['namespace'] != ol.DS_GOLD, r['namespace'] != ol.DS_ICEBERG))
        hit = hits[0]
        node_id = f"dataset:{hit['namespace']}:{hit['name']}"
        lin = httpx.get(f"{base}/api/v1/lineage", params={"nodeId": node_id, "depth": depth}, timeout=5).json()
    except Exception:
        return None
    graph = {n["id"]: n for n in lin.get("graph", [])}
    edges = []
    for nid, n in graph.items():
        for e in n.get("inEdges", []):
            edges.append({"node": nid, "from": e["origin"]})
    datasets = sorted(n for n in graph if n.startswith("dataset:"))
    jobs = sorted(n for n in graph if n.startswith("job:"))
    return {"table": node_id, "source": "marquez", "datasets": datasets, "jobs": jobs, "edges": len(edges),
            "ui": f"http://localhost:3001/lineage/dataset/{hit['namespace']}/{hit['name']}"}
