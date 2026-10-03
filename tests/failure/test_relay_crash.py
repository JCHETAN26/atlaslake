"""Relay crash between Kafka ack and checkpoint → at-least-once re-send → no logical duplicates."""

from __future__ import annotations

import pytest

from tests.integration.helpers import drain, table, totals


def test_relay_crash_after_flush_resends_but_nothing_duplicates(clean_platform):
    from ingestion.cdc.outbox_relay import get_checkpoint, run_relay
    from reconciliation.reconcile import run_reconciliation
    from simulator.generator import CommerceSimulator, SimulationConfig
    from simulator.postgres_sink import apply_stream, connect, load_stores

    spark = clean_platform
    sim = CommerceSimulator(SimulationConfig(orders=300, seed=21, days=2))
    with connect() as conn:
        load_stores(conn, sim.world.stores)
        apply_stream(conn, sim.stream())
        source_events = conn.execute("SELECT count(*) FROM commerce.outbox").fetchone()[0]

    with pytest.raises(SystemExit):
        run_relay(batch_size=1_000, crash_after_flush_once=True, metrics_port=None)
    with connect() as conn:
        assert get_checkpoint(conn, "outbox-relay") == 0  # crashed before checkpointing
    res = run_relay(batch_size=1_000, metrics_port=None)  # restart: re-sends the first batch
    assert res.events == source_events

    t = totals(drain(spark))
    assert t["received"] == source_events + 1_000  # physical duplicates arrived...
    assert t["duplicates_in_batch"] + t["duplicates_known"] == 1_000  # ...and were all detected
    assert t["accepted_new"] == source_events
    assert len(table(spark, "silver.events")) == source_events

    rec = run_reconciliation(write=False)
    assert rec["summary"]["unexplained"] == 0
    assert rec["summary"]["logical_duplicates"] == 0
