import json
import urllib.request
from datetime import UTC, datetime, timedelta

from prometheus_client import CollectorRegistry

from monitoring import exporter as ex


def test_duplicate_count_sql():
    assert ex.duplicate_count_sql("silver.orders", "order_id") == (
        "SELECT count(*) - count(DISTINCT order_id) FROM silver.orders")


def test_seconds_since_handles_naive_utc():
    now = datetime(2026, 1, 1, 12, tzinfo=UTC)
    assert ex.seconds_since(datetime(2026, 1, 1, 11, 59), now) == 60
    assert ex.seconds_since(None, now) is None
    assert ex.seconds_since(now + timedelta(seconds=5), now) == 0.0


def test_read_summary_and_reconciliation(tmp_path):
    p = tmp_path / "reconciliation.json"
    assert ex.read_summary(p) is None
    p.write_text("not json")
    assert ex.read_summary(p) is None
    p.write_text(json.dumps({"summary": {"passed": False, "unexplained": 7}}))
    assert ex.reconciliation_values(ex.read_summary(p)) == (0.0, 7.0)
    p.write_text(json.dumps({"summary": {"passed": True, "unexplained": {"orders": 0, "payments": -2}}}))
    assert ex.reconciliation_values(ex.read_summary(p)) == (1.0, 2.0)
    assert ex.reconciliation_values(None) is None


def test_quality_failed():
    assert ex.quality_failed({"failed": 3}) == 3.0
    assert ex.quality_failed({"failed_blocking": 1}) == 1.0
    assert ex.quality_failed({}) is None
    assert ex.quality_failed(None) is None


def test_failing_source_is_isolated():
    e = ex.DataExporter(CollectorRegistry())

    def boom() -> None:
        raise RuntimeError("down")

    e._safe("broken", boom)
    e._safe("fine", lambda: None)
    assert e.registry.get_sample_value("atlas_exporter_source_up", {"source": "broken"}) == 0
    assert e.registry.get_sample_value("atlas_exporter_source_up", {"source": "fine"}) == 1


def test_webhook_appends_jsonl(tmp_path):
    out = tmp_path / "alerts.jsonl"
    server = ex.serve_webhook(0, out)
    try:
        port = server.server_address[1]
        body = json.dumps({"alerts": [{"labels": {"alertname": "DuplicateLeak"}}]}).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{port}/alerts", data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
        assert urllib.request.urlopen(req, timeout=5).status == 200
    finally:
        server.shutdown()
    rec = json.loads(out.read_text().splitlines()[0])
    assert rec["payload"]["alerts"][0]["labels"]["alertname"] == "DuplicateLeak"


def test_report_gauges_absent_until_reports_exist(tmp_path, monkeypatch):
    e = ex.DataExporter(CollectorRegistry())
    monkeypatch.setattr(ex, "settings", lambda: type("S", (), {"evaluation_dir": tmp_path})())
    e.collect_reports()
    assert e.registry.get_sample_value("atlas_data_reconciliation_passed") is None
    (tmp_path / "reconciliation.json").write_text(json.dumps({"summary": {"passed": True, "unexplained": 0}}))
    (tmp_path / "data_quality.json").write_text(json.dumps({"summary": {"failed": 2}}))
    e.collect_reports()
    assert e.registry.get_sample_value("atlas_data_reconciliation_passed") == 1
    assert e.registry.get_sample_value("atlas_data_quality_failed") == 2
