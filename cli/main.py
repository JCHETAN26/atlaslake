"""``atlas`` — the operator CLI for AtlasLake (Phase 43)."""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import UTC, date, datetime
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="AtlasLake operator CLI", no_args_is_help=True, pretty_exceptions_show_locals=False)
db_app = typer.Typer(help="Operational source database")
contracts_app = typer.Typer(help="Schema contracts / registry")
silver_app = typer.Typer(help="Silver layer (Spark)")
gold_app = typer.Typer(help="Gold layer (dbt)")
iceberg_app = typer.Typer(help="Iceberg table maintenance")
app.add_typer(db_app, name="db")
app.add_typer(contracts_app, name="contracts")
app.add_typer(silver_app, name="silver")
app.add_typer(gold_app, name="gold")
app.add_typer(iceberg_app, name="iceberg")
console = Console()

DateOpt = Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"])]


def _setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    for noisy in ("py4j", "httpx", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _print(obj) -> None:
    console.print_json(json.dumps(obj, default=str))


def _d(x: datetime | None) -> date | None:
    return x.date() if x else None


# --------------------------------------------------------------------------- platform
@app.command()
def init(partitions: int = typer.Option(None, help="Kafka partitions per topic")) -> None:
    """Create bucket, source schema, register contracts, create topics and Iceberg tables."""
    _setup_logging()
    from core.platform import init_all
    from spark.common.session import get_spark
    from spark.common.tables import create_all

    out = init_all(partitions)
    out["iceberg_tables"] = create_all(get_spark("atlas-init"))
    _print(out)


@app.command()
def reset(yes: bool = typer.Option(False, "--yes", help="Confirm destroying all local data")) -> None:
    """DESTRUCTIVE: wipe source DB, Kafka topics, lake tables, checkpoints and Gold."""
    if not yes:
        typer.confirm("This deletes ALL local AtlasLake data. Continue?", abort=True)
    _setup_logging()
    from core.platform import reset_everything

    reset_everything()
    console.print("[green]reset complete[/green]")


@app.command()
def status() -> None:
    """One-screen health/volume summary of every layer."""
    from core.status import collect_status

    st = collect_status()
    table = Table(title="AtlasLake status")
    table.add_column("layer")
    table.add_column("metric")
    table.add_column("value", justify="right")
    for layer, metrics in st.items():
        for k, v in metrics.items():
            table.add_row(layer, k, f"{v:,}" if isinstance(v, int) else str(v))
    console.print(table)


# --------------------------------------------------------------------------- source
@db_app.command("init")
def db_init() -> None:
    from simulator.postgres_sink import connect, init_schema

    with connect() as conn:
        init_schema(conn)
    console.print("source schema ready")


@app.command()
def simulate(
    orders: int = typer.Option(10_000, help="Orders to generate"),
    seed: int = 42,
    days: float = 31.0,
    start: Annotated[datetime, typer.Option(formats=["%Y-%m-%d"])] = datetime(2026, 1, 1),
    stores: int = 30,
    products: int = 2_000,
    reset_source: bool = typer.Option(False, help="Truncate the source DB first"),
) -> None:
    """Generate a historical commerce world into Postgres (entity tables + outbox)."""
    _setup_logging()
    from simulator.generator import CommerceSimulator, SimulationConfig
    from simulator.postgres_sink import apply_stream, connect, init_schema, load_stores, record_run
    from simulator.postgres_sink import reset_source as _reset

    cfg = SimulationConfig(seed=seed, orders=orders, days=days, start=start.replace(tzinfo=UTC),
                           stores=stores, products=products)
    run_id = f"sim-{seed}-{orders}-{int(time.time())}"
    t0 = time.time()
    with connect() as conn:
        init_schema(conn)
        if reset_source:
            _reset(conn)
        sim = CommerceSimulator(cfg)
        record_run(conn, run_id, "historical", cfg.to_json())
        load_stores(conn, sim.world.stores)
        totals = apply_stream(conn, sim.stream())
        elapsed = time.time() - t0
        stats = {"events": sum(totals.values()), "by_topic": totals, "elapsed_s": round(elapsed, 2),
                 "events_per_s": round(sum(totals.values()) / elapsed, 1), "reference": sim.ref.source}
        record_run(conn, run_id, "historical", cfg.to_json(), stats, finished=True)
    _print({"run_id": run_id, **stats})


@app.command()
def workload(
    rate: float = typer.Option(50.0, help="Orders per second"),
    duration: float = typer.Option(60.0, help="Seconds to run (0 = forever)"),
    seed: int = 7,
    time_scale: float = typer.Option(1 / 720, help="Lifecycle compression (ship/deliver delays)"),
) -> None:
    """Continuously perform live commerce activity against Postgres (Phase 3/7)."""
    _setup_logging()
    from simulator.workload import run_workload

    _print(run_workload(rate=rate, duration_s=duration or None, seed=seed, time_scale=time_scale))


# --------------------------------------------------------------------------- kafka
@contracts_app.command("register")
def contracts_register() -> None:
    """Register every contract version (v1, v2, ...) under BACKWARD_TRANSITIVE."""
    from contracts.registry import SchemaRegistry, register_all

    reg = SchemaRegistry()
    reg.set_compatibility(None, "BACKWARD_TRANSITIVE")
    _print(register_all(reg))


@contracts_app.command("check")
def contracts_check(schema_file: str, topic: str = "commerce.orders") -> None:
    """Ask the registry whether a candidate schema is compatible (exit 1 if not)."""
    from pathlib import Path

    from contracts.registry import SchemaRegistry
    from contracts.schemas import subject

    res = SchemaRegistry().check_compatibility(subject(topic), Path(schema_file).read_text())
    _print({"compatible": res.compatible, "messages": res.messages})
    raise typer.Exit(0 if res.compatible else 1)


@app.command()
def relay(
    follow: bool = typer.Option(False, help="Keep tailing the outbox"),
    batch_size: int = 20_000,
    orders_schema_version: int = typer.Option(2, help="OrderEvent version to produce (1 or 2)"),
    duplicate_p: float = 0.0, late_p: float = 0.0, late_delay_s: float = 30.0,
    invalid_p: float = 0.0, corrupt_p: float = 0.0, reorder_window: int = 0, chaos_seed: int = 1234,
    idle_exit_s: float = typer.Option(None, help="With --follow: exit after this many idle seconds"),
) -> None:
    """Publish outbox events to Kafka (at-least-once), optionally injecting faults."""
    _setup_logging()
    from ingestion.cdc.outbox_relay import run_relay
    from producers.chaos import ChaosConfig

    chaos = ChaosConfig(duplicate_probability=duplicate_p, late_probability=late_p, late_delay_s=late_delay_s,
                        invalid_probability=invalid_p, corrupt_probability=corrupt_p,
                        reorder_window=reorder_window, seed=chaos_seed)
    res = run_relay(batch_size=batch_size, follow=follow, chaos=chaos,
                    schema_versions={"commerce.orders": orders_schema_version}, idle_exit_s=idle_exit_s)
    _print({"events": res.events, "last_seq": res.last_seq, "elapsed_s": res.elapsed_s,
            "events_per_s": res.events_per_s, "producer": res.producer})


# --------------------------------------------------------------------------- silver
@app.command()
def stream(
    available_now: bool = typer.Option(False, "--available-now", help="Drain Kafka then stop (batch mode)"),
    trigger_s: float = 5.0,
    max_offsets: int = 250_000,
    metrics_port: int = 9108,
    timeout_s: float = typer.Option(None, help="Stop after N seconds"),
) -> None:
    """Run the Kafka → Bronze/DLQ/Silver streaming job."""
    _setup_logging()
    from spark.streaming.silver_stream import run

    p = run(available_now=available_now, trigger_s=trigger_s, max_offsets=max_offsets,
            metrics_port=metrics_port, timeout_s=timeout_s)
    totals: dict[str, int] = {}
    for s in p.history:
        for k in ("received", "rejected", "duplicates_in_batch", "duplicates_known", "accepted_new", "late_events"):
            totals[k] = totals.get(k, 0) + getattr(s, k)
    _print({"batches": len(p.history), **totals})


@silver_app.command("rebuild")
def silver_rebuild(
    dataset: list[str] = typer.Option(None, help="orders/payments/inventory/customers/products"),
    date_from: DateOpt = typer.Option(None, "--from"),
    date_to: DateOpt = typer.Option(None, "--to"),
) -> None:
    """Batch re-process Bronze → Silver (idempotent)."""
    _setup_logging()
    from spark.batch.bronze_to_silver import run
    from spark.common.session import get_spark

    res = run(get_spark("atlas-silver-batch"), dataset or None, _d(date_from), _d(date_to))
    _print(res)


@app.command()
def backfill(
    dataset: Annotated[list[str], typer.Option(help="Dataset(s) to backfill")],
    date_from: Annotated[datetime, typer.Option("--from", formats=["%Y-%m-%d"])],
    date_to: Annotated[datetime, typer.Option("--to", formats=["%Y-%m-%d"])],
    gold: bool = typer.Option(True, help="Also rebuild affected Gold partitions"),
) -> None:
    """Re-process a historical event-date range from Bronze through Silver (and Gold)."""
    _setup_logging()
    from ingestion.batch.backfill import run_backfill

    _print(run_backfill(dataset, date_from.date(), date_to.date(), rebuild_gold=gold))


@app.command("replay-dlq")
def replay_dlq(
    topic: str = typer.Option(None, help="Dataset or topic (orders | commerce.orders)"),
    corrections: str = typer.Option(None, help="JSONL file: {event_id|dlq_id, set:{field:value}} or discard"),
    from_source: bool = typer.Option(True, help="Recover undecodable records from the source outbox"),
    dry_run: bool = False,
) -> None:
    """Correct DLQ records and re-publish them to Kafka; they re-enter via normal validation."""
    _setup_logging()
    from ingestion.streaming.dlq_replay import replay

    _print(replay(topic=topic, corrections_path=corrections, recover_from_source=from_source, dry_run=dry_run))


# --------------------------------------------------------------------------- gold / ops
@gold_app.command("build")
def gold_build(full_refresh: bool = False, select: str = typer.Option(None)) -> None:
    """dbt build (run + test) of the Gold layer."""
    _setup_logging()
    from lineage.dbt_runner import dbt_build

    raise typer.Exit(dbt_build(full_refresh=full_refresh, select=select)["returncode"])


@app.command()
def reconcile(
    run_date: DateOpt = typer.Option(None, "--date", help="Restrict to one event date"),
    write: bool = typer.Option(True, help="Write evaluation/reconciliation.json"),
) -> None:
    """Source → Bronze → Silver → Gold reconciliation (exit 1 on unexplained differences)."""
    _setup_logging()
    from reconciliation.reconcile import run_reconciliation

    rep = run_reconciliation(_d(run_date), write=write)
    _print(rep["summary"])
    raise typer.Exit(0 if rep["summary"]["passed"] else 1)


@app.command()
def quality(write: bool = True) -> None:
    """Run the data-quality suite (technical + business checks)."""
    _setup_logging()
    from quality.runner import run_suite

    rep = run_suite(write=write)
    _print(rep["summary"])
    raise typer.Exit(0 if rep["summary"]["failed_blocking"] == 0 else 1)


@app.command()
def compact(table: str = typer.Option(..., help="e.g. silver.orders"), target_file_mb: int = 128) -> None:
    """Iceberg compaction (rewrite_data_files) for one table."""
    _setup_logging()
    from spark.batch.maintenance import compact_table
    from spark.common.session import get_spark

    _print(compact_table(get_spark("atlas-compact"), table, target_file_mb=target_file_mb))


@iceberg_app.command("maintain")
def iceberg_maintain(dry_run: bool = False) -> None:
    """Compaction + snapshot expiration + orphan cleanup + manifest rewrite, per retention policy."""
    _setup_logging()
    from spark.batch.maintenance import run_maintenance
    from spark.common.session import get_spark

    _print(run_maintenance(get_spark("atlas-maintenance"), dry_run=dry_run))


@app.command()
def publish(target: str = typer.Option("all", help="postgres | iceberg | snowflake | all")) -> None:
    """Publish Gold marts to serving targets."""
    _setup_logging()
    from ingestion.batch.publish import publish as _publish

    _print(_publish(target))


@app.command()
def lineage(table: str = typer.Option(..., help="e.g. mart_daily_sales"), depth: int = 10) -> None:
    """Show upstream lineage of a table (Marquez if available, local graph otherwise)."""
    from lineage.graph import upstream

    _print(upstream(table, depth=depth))


@app.command()
def benchmark(
    suite: str = typer.Option("all", help="Comma-separated suites or 'all'"),
    scale: str = typer.Option("small", help="small | medium | large"),
) -> None:
    """Run reproducible benchmarks into evaluation/*.json."""
    _setup_logging()
    from benchmarks.run import run_benchmarks

    _print(run_benchmarks(suite, scale))


if __name__ == "__main__":
    app()
