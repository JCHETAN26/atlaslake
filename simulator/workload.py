"""Live commerce workload (Phase 3/7): continuous activity against the operational Postgres source.

Every tick (default 1 s) it places Poisson(rate) new orders stamped with wall-clock time, and releases
every previously scheduled lifecycle event (payment, ship, deliver, cancel, inventory) whose time has
come, plus a trickle of customer moves. Each tick is ONE Postgres transaction covering entity tables
and the outbox, so the relay sees exactly what the database committed.

The world (customers/products/stores) is rebuilt from the most recent historical run's config, so live
orders reference entities that exist in the source.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import replace
from datetime import datetime

import numpy as np
import pandas as pd

from contracts import events as ev
from simulator.generator import MICROS, CommerceSimulator, InventoryTracker, SimulationConfig, sort_events
from simulator.postgres_sink import apply_batch, connect, load_stores, record_run

log = logging.getLogger(__name__)


def _base_config(conn) -> SimulationConfig:
    row = conn.execute("SELECT config FROM ops.simulator_runs WHERE mode = 'historical' "
                       "ORDER BY started_at DESC LIMIT 1").fetchone()
    if not row:
        return SimulationConfig(orders=1_000, customers=5_000)
    cfg = dict(row[0])
    cfg["start"] = datetime.fromisoformat(cfg["start"])
    cfg["channel_probs"] = tuple(cfg["channel_probs"])
    return SimulationConfig(**cfg)


def _customer_moves(conn, rng: np.random.Generator, n: int, now_us: int, sim: CommerceSimulator,
                    tick: int) -> pd.DataFrame:
    if n <= 0:
        return pd.DataFrame()
    rows = conn.execute(
        "SELECT customer_id, email, phone, first_name, last_name, city, state, country, segment, "
        "(extract(epoch from created_at) * 1000000)::bigint FROM commerce.customers "
        "ORDER BY random() LIMIT %s", (n,)).fetchall()
    from simulator.generator import COUNTRY_CITIES

    out = []
    for i, r in enumerate(rows):
        options = [c for c in COUNTRY_CITIES.get(r[7], COUNTRY_CITIES["United States"]) if c[0] != r[5]]
        city, state = options[int(rng.integers(len(options)))]
        out.append({"event_type": "CUSTOMER_UPDATED", "event_timestamp": now_us + i, "customer_id": r[0],
                    "email": r[1], "phone": r[2], "first_name": r[3], "last_name": r[4], "city": city,
                    "state": state, "country": r[7], "segment": r[8], "signup_timestamp": int(r[9])})
    df = pd.DataFrame(out)
    df.insert(0, "event_id", sim.ids.many([f"live-move:{c}:{t}:{tick}" for c, t in
                                           zip(df["customer_id"], df["event_timestamp"], strict=True)]))
    return df


def run_workload(rate: float = 50.0, duration_s: float | None = 60.0, seed: int = 7,
                 time_scale: float = 1 / 720, tick_s: float = 1.0, move_rate: float = 0.01) -> dict:
    """Run the live workload. ``rate`` = orders/second (~14 events per order over its lifecycle)."""
    conn = connect()
    base = _base_config(conn)
    cfg = replace(base, seed=seed, lifecycle_time_scale=time_scale, truncate_at_end=False)
    sim = CommerceSimulator(cfg)
    load_stores(conn, sim.world.stores)
    next_index = int(conn.execute(
        "SELECT coalesce(max(substr(order_id, 2)::bigint), -1) + 1 FROM commerce.orders "
        "WHERE order_id ~ '^o[0-9]+$'").fetchone()[0])
    avail = {f"{s}:{p}": int(oh - rs) for s, p, oh, rs in conn.execute(
        "SELECT store_id, product_id, quantity_on_hand, quantity_reserved FROM commerce.inventory").fetchall()}
    tracker = InventoryTracker(sim.ids, available=avail)
    run_id = f"live-{seed}-{int(time.time())}"
    record_run(conn, run_id, "live", {**cfg.to_json(), "rate": rate, "tick_s": tick_s})
    rng = np.random.default_rng([seed, 99, next_index])
    pending: dict[str, list[pd.DataFrame]] = {t: [] for t in ev.TOPICS}
    started = time.time()
    totals: dict[str, int] = {t: 0 for t in ev.TOPICS}
    orders_created = ticks = 0
    commit_latency: list[float] = []
    try:
        while duration_s is None or time.time() - started < duration_s:
            tick_start = time.time()
            now_us = int(tick_start * MICROS)
            n = int(rng.poisson(rate * tick_s))
            if n:
                times = np.sort(now_us + (rng.random(n) * tick_s * MICROS).astype(np.int64))
                for topic, df in sim.generate_orders(next_index, times, stream=9).items():
                    pending[topic].append(df)
                next_index += n
                orders_created += n
            moves = _customer_moves(conn, rng, int(rng.poisson(rate * tick_s * move_rate)), now_us, sim, ticks)
            if not moves.empty:
                pending[ev.TOPIC_CUSTOMERS].append(moves)
            horizon = now_us + int(tick_s * MICROS)
            batch: dict[str, pd.DataFrame] = {}
            for topic in ev.TOPICS:
                if not pending[topic]:
                    batch[topic] = pd.DataFrame()
                    continue
                df = pd.concat(pending[topic], ignore_index=True)
                due = df["event_timestamp"] < horizon
                batch[topic] = df[due]
                pending[topic] = [df[~due]] if (~due).any() else []
            batch[ev.TOPIC_INVENTORY] = tracker.add_restocks(batch[ev.TOPIC_INVENTORY])
            batch = {t: sort_events(d) for t, d in batch.items()}
            t0 = time.time()
            apply_batch(conn, batch)
            commit_latency.append(time.time() - t0)
            for t, d in batch.items():
                totals[t] += len(d)
            ticks += 1
            sleep = tick_s - (time.time() - tick_start)
            if sleep > 0:
                time.sleep(sleep)
            elif ticks % 10 == 0:
                log.warning("workload falling behind: tick took %.2fs (> %.2fs)", time.time() - tick_start, tick_s)
    except KeyboardInterrupt:
        log.info("workload interrupted")
    elapsed = time.time() - started
    events = sum(totals.values())
    stats = {"run_id": run_id, "orders_created": orders_created, "events": events, "by_topic": totals,
             "elapsed_s": round(elapsed, 2), "events_per_s": round(events / elapsed, 1),
             "ticks": ticks, "commit_p95_s": round(float(np.percentile(commit_latency, 95)), 3) if commit_latency else None,
             "pending_future_events": sum(len(x) for v in pending.values() for x in v)}
    record_run(conn, run_id, "live", {**cfg.to_json(), "rate": rate}, stats, finished=True)
    conn.close()
    log.info("workload done: %s", json.dumps(stats))
    return stats
