"""The fixed analytical query set, run identically against every serving engine (Phase 42/44)."""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from typing import Any

from benchmarks.common import percentiles

QUERIES: dict[str, str] = {
    "revenue_by_day_range": """
        SELECT order_date, currency, sum(gross_revenue_minor) AS revenue
        FROM {s}.mart_daily_sales WHERE order_date BETWEEN '{d1}' AND '{d2}'
        GROUP BY 1, 2 ORDER BY 1, 2""",
    "top_products_range": """
        SELECT product_id, sum(line_amount_minor) AS revenue
        FROM {s}.fact_order_items WHERE order_date BETWEEN '{d1}' AND '{d2}'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 20""",
    "customer_ltv_top100": """
        SELECT customer_id, count(*) AS orders, sum(amount_minor) AS revenue
        FROM {s}.fact_orders WHERE is_revenue GROUP BY 1 ORDER BY 3 DESC, 1 LIMIT 100""",
    "single_day_orders": """
        SELECT count(*) AS orders, sum(amount_minor) AS amount FROM {s}.fact_orders WHERE order_date = '{d1}'""",
    "funnel_range": """
        SELECT order_date, orders_created, orders_paid, orders_shipped, orders_delivered, orders_cancelled
        FROM {s}.mart_order_funnel WHERE order_date BETWEEN '{d1}' AND '{d2}' ORDER BY 1""",
    "store_channel_mix": """
        SELECT store_id, sales_channel, count(*) AS orders FROM {s}.fact_orders GROUP BY 1, 2 ORDER BY 1, 2""",
}


def date_params(first_day: date) -> dict[str, str]:
    return {"d1": str(first_day + timedelta(days=3)), "d2": str(first_day + timedelta(days=9))}


def _time(fn, repeats: int) -> tuple[float, list[float], Any]:
    t0 = time.perf_counter()
    result = fn()
    cold = time.perf_counter() - t0
    warm = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        warm.append(time.perf_counter() - t0)
    return cold, warm, result


def run_postgres(schema: str = "baseline", repeats: int = 15, first_day: date = date(2026, 1, 1)) -> dict[str, Any]:
    from core.config import settings
    from simulator.postgres_sink import connect

    params = {"s": schema, **date_params(first_day)}
    out = {}
    with connect(settings().pg.analytics_db) as conn:
        for name, sql in QUERIES.items():
            q = sql.format(**params)
            cold, warm, rows = _time(lambda q=q: conn.execute(q).fetchall(), repeats)
            plan = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {q}").fetchone()[0][0]
            top = plan["Plan"]
            blocks = top.get("Shared Hit Blocks", 0) + top.get("Shared Read Blocks", 0)
            out[name] = {"cold_s": round(cold, 4), "warm": percentiles(warm), "rows_returned": len(rows),
                         "bytes_touched": blocks * 8192, "result_digest": _digest(rows)}
    return out


def run_duckdb(con, schema: str = "gold_db.gold", repeats: int = 15, first_day: date = date(2026, 1, 1)
               ) -> dict[str, Any]:
    params = {"s": schema, **date_params(first_day)}
    out = {}
    for name, sql in QUERIES.items():
        q = sql.format(**params)
        cold, warm, rows = _time(lambda q=q: con.execute(q).fetchall(), repeats)
        out[name] = {"cold_s": round(cold, 4), "warm": percentiles(warm), "rows_returned": len(rows),
                     "rows_scanned": _duckdb_rows_scanned(con, q), "result_digest": _digest(rows)}
    return out


def _duckdb_rows_scanned(con, q: str) -> int | None:
    try:
        plan = con.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {q}").fetchall()[0][1]
        doc = json.loads(plan)

        def walk(node) -> int:
            own = int(node.get("operator_rows_scanned", 0) or 0)
            return own + sum(walk(c) for c in node.get("children", []))

        return walk(doc) if isinstance(doc, dict) else sum(walk(n) for n in doc)
    except Exception:
        return None


def _digest(rows: list) -> str:
    import hashlib
    from decimal import Decimal

    def n(x):
        return round(float(x), 2) if isinstance(x, (int, float, Decimal)) and not isinstance(x, bool) else str(x)

    norm = sorted(json.dumps([n(x) for x in r]) for r in rows)
    return hashlib.sha1("\n".join(norm).encode()).hexdigest()[:12]
