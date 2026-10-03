"""The Bronze → DLQ → Silver micro-batch processor, shared by streaming and batch paths.

    raw (Kafka or Bronze) ─decode─► decoded ─┬─► Bronze (append, raw bytes kept)        [streaming only]
                                             ├─► ops.dlq (invalid, with reasons)
                                             └─► valid ─dedup in batch─► anti-join ledger ─► NEW events
                                                    NEW ─► silver entity MERGEs (commutative, idempotent)
                                                    NEW ─► silver.events ledger (append, last)

Crash safety: entity MERGEs are idempotent; the only additive write (inventory state) and every raw
append carry a *content-addressed* batch key in their Iceberg snapshot summary, checked before
writing. The ledger is appended last, so a retry after a partial batch recomputes the identical NEW
set. Net effect: effectively-once Silver state on top of at-least-once transport.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from contracts import events as ev
from core.config import settings
from spark.common import transforms as T
from spark.common.decode import decode
from spark.common.tables import fq

log = logging.getLogger(__name__)

ALLOWED_LATENESS_S = 10 * 60  # Phase 12 policy (docs/LATE_DATA_POLICY.md)
BATCH_KEY_PROP = "atlas.batch-key"


@dataclass
class BatchStats:
    received: int = 0
    rejected: int = 0
    duplicates_in_batch: int = 0
    duplicates_known: int = 0
    accepted_new: int = 0
    late_events: int = 0
    by_topic: dict[str, dict[str, Any]] = field(default_factory=dict)
    duration_s: float = 0.0
    skipped_writes: list[str] = field(default_factory=list)
    stage_s: dict[str, float] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


def content_key(df: DataFrame, cols: list[str], scope: str) -> str:
    """Order-independent fingerprint of a row set (count + sum of 64-bit hashes)."""
    row = df.agg(
        F.count("*").alias("n"), F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h")
    ).collect()[0]
    return f"{scope}:{row['n']}:{row['h']}"


def already_committed(spark: SparkSession, table: str, key: str, lookback: int = 50) -> bool:
    """True iff a commit carrying ``key`` is still part of the table's *current* lineage.

    Walk snapshots newest → oldest: finding the key means the write is present; hitting a destructive
    commit made by someone else (a DELETE, or an overwrite without a batch key) first means any older
    marker no longer proves anything — e.g. Silver was wiped and is being rebuilt by a backfill whose
    event set happens to equal an earlier micro-batch. Compaction (``replace``) keeps data and is
    walked through.
    """
    rows = spark.sql(
        f"SELECT summary['{BATCH_KEY_PROP}'] AS k, operation FROM {fq(table)}.snapshots "
        f"ORDER BY committed_at DESC LIMIT {lookback}"
    ).collect()
    for r in rows:
        if r["k"] == key:
            return True
        if r["k"] is None and r["operation"] in ("delete", "overwrite"):
            return False
    return False


def append_once(spark: SparkSession, df: DataFrame, table: str, key: str, stats: BatchStats) -> bool:
    if already_committed(spark, table, key):
        stats.skipped_writes.append(table)
        log.warning("skip %s: batch %s already committed", table, key)
        return False
    df.writeTo(fq(table)).option(f"snapshot-property.{BATCH_KEY_PROP}", key).append()
    return True


def _merge(spark: SparkSession, df: DataFrame, sql: str, view: str, **fmt: str) -> None:
    # foreachBatch hands us DataFrames bound to a cloned session: register + run SQL on *that* one.
    df.createOrReplaceTempView(view)
    df.sparkSession.sql(sql.format(source=view, **fmt))


class _Lap:
    def __init__(self, sink: dict[str, float]) -> None:
        self.sink, self.t = sink, time.perf_counter()

    def __call__(self, name: str) -> None:
        now = time.perf_counter()
        self.sink[name] = round(self.sink.get(name, 0.0) + now - self.t, 3)
        self.t = now


class SilverProcessor:
    def __init__(
        self,
        spark: SparkSession,
        query: str,
        write_bronze: bool = True,
        allowed_lateness_s: int = ALLOWED_LATENESS_S,
        parallel_writes: bool = True,
    ) -> None:
        self.spark = spark
        # Writes to *different* Iceberg tables are independent commits: run them concurrently.
        self.parallel_writes = parallel_writes
        self.query = query
        self.write_bronze = write_bronze
        self.allowed_lateness_s = allowed_lateness_s
        self.registry_url = settings().kafka.schema_registry_url
        self.history: list[BatchStats] = []

    # ------------------------------------------------------------------ entry points
    def process_raw(self, raw: DataFrame, batch_id: int) -> BatchStats:
        decoded = decode(raw, self.registry_url)
        return self.process_decoded(decoded, batch_id)

    def process_decoded(self, decoded: DataFrame, batch_id: int) -> BatchStats:
        t0 = time.time()
        spark = self.spark
        stats = BatchStats()
        decoded = decoded.persist()
        try:
            lap = _Lap(stats.stage_s)
            stats.received = decoded.count()
            lap("decode_validate")
            if stats.received == 0:
                return stats
            topics = [r["topic"] for r in decoded.select("topic").distinct().collect()]
            now = F.current_timestamp()

            if self.write_bronze:

                def bronze_task(topic: str):
                    def run() -> None:
                        rows = decoded.filter(F.col("topic") == topic)
                        bronze = rows.select(
                            "source",
                            "topic",
                            "kafka_partition",
                            "kafka_offset",
                            "kafka_key",
                            "payload",
                            "ingestion_timestamp",
                            "schema_id",
                            "schema_version",
                            "event_id",
                            "event_type",
                            "event_timestamp",
                            F.when(
                                F.col("error_codes").isin(
                                    "CORRUPT_PAYLOAD", "AVRO_DECODE_ERROR", "UNKNOWN_SCHEMA_ID"
                                ),
                                F.col("error_codes"),
                            ).alias("decode_error"),
                            now.alias("bronze_loaded_at"),
                        )
                        key = content_key(
                            rows, ["topic", "kafka_partition", "kafka_offset"], f"bronze:{topic}"
                        )
                        append_once(spark, bronze, f"bronze.{ev.TOPIC_TO_DATASET[topic]}", key, stats)

                    return run

                self._run_all([bronze_task(t) for t in topics])
            lap("bronze_append")
            invalid = decoded.filter(F.col("error_codes").isNotNull())
            valid = decoded.filter(F.col("error_codes").isNull())
            self._write_dlq(invalid)

            lap("dlq_merge")
            ranked = valid.withColumn(
                "_rn",
                F.row_number().over(
                    Window.partitionBy("event_id").orderBy(
                        "ingestion_timestamp", "kafka_partition", "kafka_offset"
                    )
                ),
            )
            deduped = ranked.filter("_rn = 1").drop("_rn")
            bounds = deduped.agg(
                F.min("event_timestamp").alias("lo"), F.max("event_timestamp").alias("hi")
            ).collect()[0]
            ledger = spark.table(fq("silver.events")).select("event_id", "event_timestamp", "topic")
            if bounds["lo"] is not None:
                ledger = ledger.filter(
                    F.col("topic").isin(topics) & F.col("event_timestamp").between(bounds["lo"], bounds["hi"])
                )
            new = deduped.join(ledger.select("event_id"), "event_id", "left_anti").persist()

            new.count()
            lap("dedup_ledger_antijoin")
            per_topic = {
                r["topic"]: r.asDict()
                for r in decoded.groupBy("topic")
                .agg(
                    F.count("*").alias("received"),
                    F.sum(F.col("error_codes").isNotNull().cast("int")).alias("rejected"),
                )
                .collect()
            }
            for r in (
                valid.groupBy("topic")
                .agg((F.count("*") - F.countDistinct("event_id")).alias("dup_in_batch"))
                .collect()
            ):
                per_topic[r["topic"]]["duplicates_in_batch"] = int(r["dup_in_batch"])
            for r in deduped.groupBy("topic").count().collect():
                per_topic[r["topic"]]["deduped"] = int(r["count"])
            late_expr = (
                F.unix_timestamp("ingestion_timestamp") - F.unix_timestamp("event_timestamp")
            ) > self.allowed_lateness_s
            for r in (
                new.groupBy("topic")
                .agg(
                    F.count("*").alias("n"),
                    F.sum(late_expr.cast("int")).alias("late"),
                    F.min("event_timestamp").alias("min_ts"),
                    F.max("event_timestamp").alias("max_ts"),
                    F.max("ingestion_timestamp").alias("max_ing"),
                )
                .collect()
            ):
                per_topic[r["topic"]].update(
                    accepted_new=int(r["n"]),
                    late=int(r["late"] or 0),
                    min_ts=r["min_ts"],
                    max_ts=r["max_ts"],
                    max_ing=r["max_ing"],
                )
            for t, d in per_topic.items():
                d.setdefault("duplicates_in_batch", 0)
                d.setdefault("accepted_new", 0)
                d.setdefault("late", 0)
                d["duplicates_known"] = d.get("deduped", 0) - d["accepted_new"]
                d["rejected"] = int(d.get("rejected") or 0)
                stats.by_topic[t] = {k: v for k, v in d.items() if k not in ("topic",)}
            stats.rejected = sum(d["rejected"] for d in per_topic.values())
            stats.duplicates_in_batch = sum(d["duplicates_in_batch"] for d in per_topic.values())
            stats.duplicates_known = sum(d["duplicates_known"] for d in per_topic.values())
            stats.accepted_new = sum(d["accepted_new"] for d in per_topic.values())
            stats.late_events = sum(d["late"] for d in per_topic.values())

            lap("batch_stats")
            if stats.accepted_new:
                self._apply_entities(new, topics, stats)
                lap("entity_merges")
                ledger_rows = new.select(
                    "event_id",
                    "event_type",
                    "topic",
                    "entity_key",
                    "event_timestamp",
                    "ingestion_timestamp",
                    "schema_version",
                    "kafka_partition",
                    "kafka_offset",
                    "source",
                    "record_json",
                    now.alias("silver_loaded_at"),
                )
                ledger_rows.writeTo(fq("silver.events")).append()
                lap("ledger_append")
            new.unpersist()
            stats.duration_s = round(time.time() - t0, 3)
            self._write_batch_stats(batch_id, stats)
            self.history.append(stats)
            return stats
        finally:
            decoded.unpersist()

    # ------------------------------------------------------------------ writers
    def _write_dlq(self, invalid: DataFrame) -> None:
        dlq = invalid.select(
            F.concat_ws("/", "topic", "kafka_partition", "kafka_offset").alias("dlq_id"),
            "source",
            "topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_key",
            "payload",
            "schema_id",
            "schema_version",
            "event_id",
            "event_type",
            "event_timestamp",
            "error_codes",
            "error_message",
            "ingestion_timestamp",
            F.current_timestamp().alias("dlq_timestamp"),
            F.lit("PENDING").alias("status"),
            F.lit(None).cast("string").alias("resolution_note"),
            F.lit(None).cast("timestamp").alias("resolved_at"),
        )
        if dlq.isEmpty():
            return
        _merge(
            self.spark,
            dlq,
            T.MERGE_INSERT_ONLY,
            "dlq_batch",
            target=fq("ops.dlq"),
            on="t.dlq_id = s.dlq_id AND t.topic = s.topic",
        )

    def _run_all(self, tasks: list) -> None:
        if not self.parallel_writes or len(tasks) <= 1:
            for t in tasks:
                t()
            return
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=len(tasks), thread_name_prefix="silver-write") as pool:
            for f in [pool.submit(t) for t in tasks]:
                f.result()  # re-raise the first failure: the batch fails and is retried as a whole

    def _apply_entities(self, new: DataFrame, topics: list[str], stats: BatchStats) -> None:
        spark = self.spark
        tasks = []

        def orders_task() -> None:
            orders = T.typed(new, ev.TOPIC_ORDERS)
            _merge(spark, T.reduce_orders(orders), T.MERGE_ORDERS, "orders_patch", target=fq("silver.orders"))
            _merge(
                spark,
                T.order_items(orders),
                T.MERGE_INSERT_ONLY,
                "items_patch",
                target=fq("silver.order_items"),
                on="t.order_id = s.order_id AND t.line_number = s.line_number",
            )

        def payments_task() -> None:
            pays = T.typed(new, ev.TOPIC_PAYMENTS)
            _merge(
                spark,
                T.reduce_payments(pays),
                T.MERGE_PAYMENTS,
                "payments_patch",
                target=fq("silver.payments"),
            )

        def customers_task() -> None:
            cust = T.typed(new, ev.TOPIC_CUSTOMERS)
            _merge(
                spark,
                T.customers_state(cust),
                T.MERGE_LWW,
                "customers_patch",
                target=fq("silver.customers"),
                key="customer_id",
            )
            _merge(
                spark,
                T.customer_changes(cust),
                T.MERGE_INSERT_ONLY,
                "customer_changes_patch",
                target=fq("silver.customer_changes"),
                on="t.event_id = s.event_id",
            )

        def products_task() -> None:
            prod = T.typed(new, ev.TOPIC_PRODUCTS)
            _merge(
                spark,
                T.products_state(prod),
                T.MERGE_LWW,
                "products_patch",
                target=fq("silver.products"),
                key="product_id",
            )
            _merge(
                spark,
                T.product_changes(prod),
                T.MERGE_INSERT_ONLY,
                "product_changes_patch",
                target=fq("silver.product_changes"),
                on="t.event_id = s.event_id",
            )

        def inventory_task() -> None:
            mov = T.inventory_movements(T.typed(new, ev.TOPIC_INVENTORY)).persist()
            if not mov.isEmpty():
                key = content_key(mov, ["event_id"], "inventory")
                if already_committed(spark, "silver.inventory", key):
                    stats.skipped_writes.append("silver.inventory")
                else:
                    deltas = T.inventory_deltas(mov)
                    cur = spark.table(fq("silver.inventory"))
                    merged = (
                        cur.alias("c")
                        .join(deltas.alias("d"), "inventory_key", "full_outer")
                        .select(
                            "inventory_key",
                            F.coalesce("c.store_id", "d.store_id").alias("store_id"),
                            F.coalesce("c.product_id", "d.product_id").alias("product_id"),
                            (
                                F.coalesce("c.quantity_on_hand", F.lit(0))
                                + F.coalesce("d.quantity_on_hand", F.lit(0))
                            ).alias("quantity_on_hand"),
                            (
                                F.coalesce("c.quantity_reserved", F.lit(0))
                                + F.coalesce("d.quantity_reserved", F.lit(0))
                            ).alias("quantity_reserved"),
                            (
                                F.coalesce("c.movement_count", F.lit(0))
                                + F.coalesce("d.movement_count", F.lit(0))
                            ).alias("movement_count"),
                            F.greatest("c.updated_at", "d.updated_at").alias("updated_at"),
                            F.when(F.col("d.inventory_key").isNotNull(), F.current_timestamp())
                            .otherwise(F.col("c.silver_updated_at"))
                            .alias("silver_updated_at"),
                        )
                    ).localCheckpoint(eager=True)
                    (
                        merged.writeTo(fq("silver.inventory"))
                        .option(f"snapshot-property.{BATCH_KEY_PROP}", key)
                        .overwrite(F.lit(True))
                    )
                append_once(
                    spark, mov, "silver.inventory_movements", key.replace("inventory", "movements", 1), stats
                )
            mov.unpersist()

        for topic, task in (
            (ev.TOPIC_ORDERS, orders_task),
            (ev.TOPIC_PAYMENTS, payments_task),
            (ev.TOPIC_CUSTOMERS, customers_task),
            (ev.TOPIC_PRODUCTS, products_task),
            (ev.TOPIC_INVENTORY, inventory_task),
        ):
            if topic in topics:
                tasks.append(task)
        self._run_all(tasks)

    def _write_batch_stats(self, batch_id: int, stats: BatchStats) -> None:
        rows = [
            (
                self.query,
                int(batch_id),
                t,
                int(d.get("received", 0)),
                int(d.get("received", 0)) - int(d["rejected"]),
                int(d["rejected"]),
                int(d["duplicates_in_batch"]),
                int(d["duplicates_known"]),
                int(d["accepted_new"]),
                int(d["late"]),
                d.get("min_ts"),
                d.get("max_ts"),
                d.get("max_ing"),
                int(stats.duration_s * 1000),
            )
            for t, d in stats.by_topic.items()
        ]
        if not rows:
            return
        schema = (
            "query string, batch_id bigint, topic string, received bigint, decoded bigint, rejected bigint, "
            "duplicates_in_batch bigint, duplicates_known bigint, accepted_new bigint, late_events bigint, "
            "min_event_timestamp timestamp, max_event_timestamp timestamp, max_ingestion_timestamp timestamp, "
            "duration_ms bigint"
        )
        df = self.spark.createDataFrame(rows, schema).withColumn("processed_at", F.current_timestamp())
        append_once(self.spark, df, "ops.stream_batches", f"stats:{self.query}:{batch_id}", stats)
