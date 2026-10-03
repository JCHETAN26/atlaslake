"""Regression for the chaos-test finding: corrupt bytes + a lost delivery-log row = unexplained event.

The relay now stamps every delivery with an ``atlas-event-id`` Kafka header, which survives payload
corruption. Even with the delivery log wiped, every corrupt record must be attributed, replayed from
the source and reconciled.
"""

from __future__ import annotations

from tests.integration.helpers import drain, table, totals


def test_corrupt_records_are_attributed_without_the_delivery_log(clean_platform):
    from ingestion.cdc.outbox_relay import run_relay
    from ingestion.streaming.dlq_replay import replay
    from producers.chaos import ChaosConfig
    from reconciliation.reconcile import run_reconciliation
    from simulator.generator import CommerceSimulator, SimulationConfig
    from simulator.postgres_sink import apply_stream, connect, load_stores

    spark = clean_platform
    sim = CommerceSimulator(SimulationConfig(orders=200, seed=39, days=2))
    with connect() as conn:
        load_stores(conn, sim.world.stores)
        apply_stream(conn, sim.stream())

    run_relay(batch_size=2_000, metrics_port=None, chaos=ChaosConfig(corrupt_probability=0.05, seed=39))
    with connect() as conn:  # the failure mode: delivery-log rows never persisted
        conn.execute("TRUNCATE ops.delivery_log")
        conn.commit()

    t = totals(drain(spark))
    assert t["rejected"] > 0
    pending = table(spark, "ops.dlq", "status = 'PENDING'")
    assert pending and all(r["event_id"] for r in pending)  # attributed via the header

    res = replay(recover_from_source=True, spark=spark)
    assert res["replayed"] == len(pending) and res["unresolved"] == 0
    drain(spark)

    rec = run_reconciliation(write=False)
    assert rec["summary"]["unexplained"] == 0, rec["unexplained_examples"]
    assert rec["summary"]["accepted"] == rec["summary"]["source_events"]
