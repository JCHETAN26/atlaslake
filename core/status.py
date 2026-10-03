"""`atlas status`: one-screen view of every layer (best-effort: a down component shows as such)."""

from __future__ import annotations

from typing import Any

from contracts import events as ev


def _safe(fn) -> Any:
    try:
        return fn()
    except Exception as exc:
        return f"unavailable ({type(exc).__name__})"


def collect_status() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    def source() -> dict[str, Any]:
        from ingestion.cdc.outbox_relay import outbox_backlog
        from simulator.postgres_sink import connect

        with connect() as c:
            r = {t: c.execute(f"SELECT count(*) FROM commerce.{t}").fetchone()[0]
                 for t in ("orders", "customers", "payments", "outbox")}
        r["relay_backlog"] = outbox_backlog()
        return r

    def kafka() -> dict[str, Any]:
        from producers.topics import end_offsets

        return {t: sum(end_offsets(t).values()) for t in ev.TOPICS}

    def lake() -> dict[str, Any]:
        import duckdb

        from lakehouse.duckdb_plugin import configure

        con = duckdb.connect()
        configure(con)
        r = {f"silver.{t}": con.execute(f"SELECT count(*) FROM silver.{t}").fetchone()[0]
             for t in ("events", "orders", "payments", "inventory")}
        r["dlq_pending"] = con.execute("SELECT count(*) FROM ops.dlq WHERE status = 'PENDING'").fetchone()[0]
        r["last_stream_batch"] = str(con.execute("SELECT max(processed_at) FROM ops.stream_batches").fetchone()[0])
        r["silver_freshness"] = str(con.execute("SELECT now()::timestamp - max(silver_updated_at) FROM silver.orders").fetchone()[0])
        return r

    def gold() -> dict[str, Any]:
        import duckdb

        from core.config import settings

        with duckdb.connect(str(settings().gold_duckdb), read_only=True) as con:
            return {"fact_orders": con.execute("SELECT count(*) FROM gold.gold.fact_orders").fetchone()[0],
                    "last_dbt_load": str(con.execute("SELECT max(dbt_loaded_at) FROM gold.gold.fact_orders").fetchone()[0])}

    def evaluation() -> dict[str, Any]:
        import json

        from core.config import settings

        r = {}
        for name, key in (("reconciliation", "passed"), ("data_quality", "failed_blocking")):
            p = settings().evaluation_dir / f"{name}.json"
            if p.exists():
                d = json.loads(p.read_text())
                r[f"{name}.{key}"] = d["summary"][key]
                r[f"{name}.at"] = d["context"]["generated_at"]
        return r

    for name, fn in (("source", source), ("kafka", kafka), ("lake", lake), ("gold", gold), ("checks", evaluation)):
        res = _safe(fn)
        out[name] = res if isinstance(res, dict) else {"status": res}
    return out
