"""Data-level Prometheus exporter + alert webhook receiver (Phase 30).

    python -m monitoring.exporter --port 9110 --interval 30     # data metrics on :9110/metrics
    python -m monitoring.exporter --webhook-port 9111           # Alertmanager webhook → data/alerts.jsonl

The exporter turns *data* facts (DLQ size, duplicates, freshness, reconciliation and quality status)
into metrics so the same alerting path that watches infrastructure also watches correctness. Every
source is collected independently: one failing source never stops the others.
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

from prometheus_client import CollectorRegistry, Gauge, start_http_server

from core.config import settings

log = logging.getLogger("atlas.exporter")

SILVER_FRESHNESS_COLUMNS: dict[str, str] = {
    "orders": "silver_updated_at",
    "payments": "silver_updated_at",
    "customers": "silver_updated_at",
    "products": "silver_updated_at",
    "inventory": "silver_updated_at",
    "order_items": "silver_loaded_at",
    "customer_changes": "silver_loaded_at",
    "product_changes": "silver_loaded_at",
    "inventory_movements": "silver_loaded_at",
    "events": "silver_loaded_at",
}
DUPLICATE_KEYS: dict[str, str] = {
    "silver.orders": "order_id",
    "silver.payments": "payment_id",
    "silver.events": "event_id",
}


# ------------------------------------------------------------------------------ pure helpers
def duplicate_count_sql(table: str, key: str) -> str:
    """Logical duplicates = rows beyond one per business key."""
    return f"SELECT count(*) - count(DISTINCT {key}) FROM {table}"


def freshness_sql(table: str, column: str) -> str:
    return f"SELECT max({column}) FROM silver.{table}"


def seconds_since(ts: datetime | None, now: datetime | None = None) -> float | None:
    if ts is None:
        return None
    if ts.tzinfo is None:  # lake timestamps are UTC, DuckDB hands them back naive
        ts = ts.replace(tzinfo=UTC)
    now = now or datetime.now(UTC)
    return max(0.0, (now - ts).total_seconds())


def read_summary(path: Path) -> dict[str, Any] | None:
    """Return the ``summary`` object of an evaluation report, or None if absent/unreadable."""
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    summary = data.get("summary") if isinstance(data, dict) else None
    return summary if isinstance(summary, dict) else None


def reconciliation_values(summary: dict[str, Any] | None) -> tuple[float, float] | None:
    """(passed 0/1, unexplained count) from a reconciliation summary."""
    if summary is None:
        return None
    passed = 1.0 if summary.get("passed") else 0.0
    unexplained = summary.get("unexplained", 0)
    if isinstance(unexplained, dict):
        unexplained = sum(abs(float(v)) for v in unexplained.values() if isinstance(v, int | float))
    return passed, float(unexplained or 0)


def quality_failed(summary: dict[str, Any] | None) -> float | None:
    if summary is None:
        return None
    for key in ("failed", "failed_blocking"):
        if isinstance(summary.get(key), int | float):
            return float(summary[key])
    return None


# ------------------------------------------------------------------------------ collector
class DataExporter:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.dlq_pending = Gauge("atlas_data_dlq_pending", "DLQ records awaiting correction", ["topic"], registry=r)
        self.dlq_total = Gauge("atlas_data_dlq_total", "DLQ records by status", ["topic", "status"], registry=r)
        self.silver_rows = Gauge("atlas_data_silver_rows", "Rows per Silver table", ["table"], registry=r)
        self.silver_fresh = Gauge("atlas_data_silver_freshness_seconds",
                                  "now - last processing-time change per Silver table", ["table"], registry=r)
        self.duplicates = Gauge("atlas_data_logical_duplicates", "Business keys present more than once",
                                ["table"], registry=r)
        # Label-less gauges are created lazily: an unset gauge would export 0, and e.g.
        # reconciliation_passed=0 before any report exists would fire a false alert.
        self._lazy: dict[str, Gauge] = {}
        self.errors = Gauge("atlas_exporter_source_up", "1 if the source was collected successfully",
                            ["source"], registry=r)
        self.last = Gauge("atlas_exporter_last_collect_timestamp", "Unix time of the last collection", registry=r)

    _LAZY_HELP = {
        "atlas_data_gold_freshness_seconds": "now - last dbt load of fact_orders",
        "atlas_data_reconciliation_passed": "1 if the last reconciliation passed",
        "atlas_data_reconciliation_unexplained": "Unexplained records in the last reconciliation",
        "atlas_data_quality_failed": "Failing data-quality checks in the last run",
        "atlas_relay_backlog": "Outbox rows not yet relayed to Kafka",
    }

    def set_lazy(self, name: str, value: float) -> None:
        if name not in self._lazy:
            self._lazy[name] = Gauge(name, self._LAZY_HELP[name], registry=self.registry)
        self._lazy[name].set(value)

    def _safe(self, name: str, fn: Callable[[], None]) -> None:
        try:
            fn()
            self.errors.labels(name).set(1)
        except Exception as exc:  # one broken source must not stop the loop
            log.warning("source %s failed: %s", name, exc)
            self.errors.labels(name).set(0)

    def collect_lake(self) -> None:
        import duckdb

        from lakehouse.duckdb_plugin import configure

        con = duckdb.connect()
        try:
            configure(con)

            def dlq() -> None:
                rows = con.execute("SELECT topic, status, count(*) FROM ops.dlq GROUP BY 1, 2").fetchall()
                self.dlq_pending.clear()
                self.dlq_total.clear()
                for topic, status, n in rows:
                    self.dlq_total.labels(topic, status).set(n)
                    if status == "PENDING":
                        self.dlq_pending.labels(topic).set(n)

            def silver() -> None:
                for table, col in SILVER_FRESHNESS_COLUMNS.items():
                    try:
                        n, ts = con.execute(f"SELECT count(*), max({col}) FROM silver.{table}").fetchone()  # type: ignore[misc]
                    except duckdb.Error:
                        continue
                    self.silver_rows.labels(f"silver.{table}").set(n)
                    age = seconds_since(ts)
                    if age is not None:
                        self.silver_fresh.labels(f"silver.{table}").set(age)

            def dups() -> None:
                for table, key in DUPLICATE_KEYS.items():
                    self.duplicates.labels(table).set(con.execute(duplicate_count_sql(table, key)).fetchone()[0])

            self._safe("dlq", dlq)
            self._safe("silver", silver)
            self._safe("duplicates", dups)
        finally:
            con.close()

    def collect_gold(self) -> None:
        from lakehouse.duckdb_plugin import connect_gold

        con = connect_gold(read_only=True, with_lake=False)
        try:
            catalog = con.execute("SELECT current_database()").fetchone()[0]  # file gold.duckdb ⇒ catalog "gold"
            ts = con.execute(f'SELECT max(dbt_loaded_at) FROM "{catalog}".gold.fact_orders').fetchone()[0]
            age = seconds_since(ts)
            if age is not None:
                self.set_lazy("atlas_data_gold_freshness_seconds", age)
        finally:
            con.close()

    def collect_reports(self) -> None:
        ev = settings().evaluation_dir
        rec = reconciliation_values(read_summary(ev / "reconciliation.json"))
        if rec is not None:
            self.set_lazy("atlas_data_reconciliation_passed", rec[0])
            self.set_lazy("atlas_data_reconciliation_unexplained", rec[1])
        q = quality_failed(read_summary(ev / "data_quality.json"))
        if q is not None:
            self.set_lazy("atlas_data_quality_failed", q)

    def collect_backlog(self) -> None:
        from ingestion.cdc.outbox_relay import outbox_backlog

        self.set_lazy("atlas_relay_backlog", outbox_backlog())

    def collect(self) -> None:
        self._safe("lake", self.collect_lake)
        self._safe("gold", self.collect_gold)
        self._safe("reports", self.collect_reports)
        self._safe("relay_backlog", self.collect_backlog)
        self.last.set(time.time())


# ------------------------------------------------------------------------------ webhook
def make_webhook_handler(out_path: Path) -> type[BaseHTTPRequestHandler]:
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                payload = json.loads(body or b"{}")
            except ValueError:
                payload = {"raw": body.decode(errors="replace")}
            record = {"received_at": datetime.now(UTC).isoformat(), "path": self.path, "payload": payload}
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with lock, out_path.open("a") as fh:
                fh.write(json.dumps(record) + "\n")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args: Any) -> None:
            pass

    return Handler


def serve_webhook(port: int, out_path: Path | None = None) -> HTTPServer:
    path = out_path or settings().data_dir / "alerts.jsonl"
    server = HTTPServer(("0.0.0.0", port), make_webhook_handler(path))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("alert webhook on :%s → %s", port, path)
    return server


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=None, help="serve data metrics on this port")
    ap.add_argument("--interval", type=float, default=30.0)
    ap.add_argument("--webhook-port", type=int, default=None, help="serve the Alertmanager webhook")
    ap.add_argument("--once", action="store_true", help="collect once, print, exit")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.webhook_port:
        serve_webhook(args.webhook_port)
    exporter = DataExporter()
    if args.once:
        from prometheus_client import generate_latest

        exporter.collect()
        print(generate_latest(exporter.registry).decode())
        return
    if args.port is None and args.webhook_port is None:
        args.port = 9110
    if args.port:
        start_http_server(args.port, registry=exporter.registry)
        log.info("data metrics on :%s/metrics every %ss", args.port, args.interval)
        while True:
            exporter.collect()
            time.sleep(args.interval)
    else:
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    main()
