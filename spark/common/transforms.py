"""Business transformations shared by batch and streaming (pure DataFrame → DataFrame).

Every reduction here is **order-independent** (commutative + idempotent): applying the same events in
any order, any number of times, in any batch split, yields the same state. That property is what
makes duplicates, late/out-of-order delivery, crash-replays and backfills safe — and it is tested
directly in tests/unit/test_spark_transforms.py.
"""

from __future__ import annotations

import json
from functools import cache

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from contracts import events as ev
from contracts import schemas

PAYMENT_STATUS_RANK = {"STARTED": 0, "FAILED": 1, "COMPLETED": 2}


def _rank(mapping: dict[str, int], col: str) -> Column:
    """Status → rank lookup (built lazily: Column expressions need an active SparkContext)."""
    return F.create_map(*[x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))])[F.col(col)]


def _avro_to_spark(t) -> DataType:
    if isinstance(t, list):  # union ["null", X]
        non_null = [x for x in t if x != "null"]
        return _avro_to_spark(non_null[0])
    if isinstance(t, dict):
        if t.get("type") == "array":
            return ArrayType(_avro_to_spark(t["items"]))
        if t.get("type") == "record":
            return StructType([StructField(f["name"], _avro_to_spark(f["type"])) for f in t["fields"]])
        return _avro_to_spark(t["type"])
    return {"string": StringType(), "long": LongType(), "int": IntegerType(), "boolean": BooleanType()}[t]


@cache
def record_schema(topic: str) -> StructType:
    """Spark schema of the decoded record JSON (latest reader schema)."""
    avsc = json.loads(schemas.schema_text(topic, schemas.latest_version(topic)))
    return StructType([StructField(f["name"], _avro_to_spark(f["type"])) for f in avsc["fields"]])


def micros_to_ts(col: Column) -> Column:
    return F.timestamp_micros(col)


def typed(events: DataFrame, topic: str) -> DataFrame:
    """Accepted events of one topic → typed columns (envelope + payload)."""
    rec = F.from_json("record_json", record_schema(topic))
    return (events.filter(F.col("topic") == topic)
            .select("event_id", "event_type", "event_timestamp", "ingestion_timestamp", "schema_version",
                    "source", rec.alias("r"))
            .select("event_id", "event_type", "event_timestamp", "ingestion_timestamp", "schema_version",
                    "source", *[F.col(f"r.{f}").alias(f) for f in ev.PAYLOAD_FIELDS[topic]]))


def _ts_of(event_type: str) -> Column:
    return F.min(F.when(F.col("event_type") == event_type, F.col("event_timestamp")))


def _first_of(event_type: str, col: str) -> Column:
    return F.first(F.when(F.col("event_type") == event_type, F.col(col)), ignorenulls=True)


def reduce_orders(orders: DataFrame) -> DataFrame:
    """Order events (typed) → one patch row per order_id."""
    o = orders.withColumn("status_rank", _rank(ev.ORDER_STATUS_RANK, "status"))
    latest = F.max_by(F.struct("status", "status_rank", "event_timestamp", "event_id"),
                      F.struct("event_timestamp", "status_rank", "event_id"))
    return (o.groupBy("order_id").agg(
        _first_of("ORDER_CREATED", "customer_id").alias("customer_id"),
        _first_of("ORDER_CREATED", "store_id").alias("store_id"),
        _ts_of("ORDER_CREATED").alias("order_timestamp"),
        latest.alias("latest"),
        _first_of("ORDER_CREATED", "amount_minor").alias("amount_minor"),
        _first_of("ORDER_CREATED", "currency").alias("currency"),
        _first_of("ORDER_CREATED", "sales_channel").alias("sales_channel"),
        _first_of("ORDER_CREATED", "items").alias("items"),
        _ts_of("ORDER_UPDATED").alias("paid_at"),
        _ts_of("ORDER_CANCELLED").alias("cancelled_at"),
        _first_of("ORDER_CANCELLED", "cancel_reason").alias("cancel_reason"),
        _ts_of("ORDER_SHIPPED").alias("shipped_at"),
        _first_of("ORDER_SHIPPED", "carrier").alias("carrier"),
        _ts_of("ORDER_DELIVERED").alias("delivered_at"),
        F.max("event_timestamp").alias("updated_at"),
        F.first("source", ignorenulls=True).alias("source"),
    ).select(
        "order_id", "customer_id", "store_id", "order_timestamp",
        F.col("latest.status").alias("status"), F.col("latest.status_rank").alias("status_rank"),
        F.col("latest.event_timestamp").alias("status_timestamp"),
        "amount_minor", "currency", "sales_channel",
        F.aggregate("items", F.lit(0), lambda acc, i: acc + i["quantity"]).cast("int").alias("item_count"),
        F.size("items").cast("int").alias("line_count"),
        "paid_at", "cancelled_at", "cancel_reason", "shipped_at", "carrier", "delivered_at", "source",
        F.col("latest.event_id").alias("last_event_id"), "updated_at",
        F.current_timestamp().alias("silver_updated_at"),
    ).withColumn("line_count", F.when(F.col("line_count") >= 0, F.col("line_count"))))


def order_items(orders: DataFrame) -> DataFrame:
    created = orders.filter(F.col("event_type") == "ORDER_CREATED")
    return (created.select("order_id", "currency", F.col("event_timestamp").alias("order_timestamp"), "event_id",
                           F.explode("items").alias("i"))
            .select("order_id", F.col("i.line_number").alias("line_number"), F.col("i.product_id").alias("product_id"),
                    F.col("i.quantity").alias("quantity"), F.col("i.unit_price_minor").alias("unit_price_minor"),
                    (F.col("i.quantity") * F.col("i.unit_price_minor")).alias("line_amount_minor"),
                    "currency", "order_timestamp", "event_id", F.current_timestamp().alias("silver_loaded_at"))
            .dropDuplicates(["order_id", "line_number"]))


def reduce_payments(payments: DataFrame) -> DataFrame:
    p = payments.withColumn("status_rank", _rank(PAYMENT_STATUS_RANK, "status"))
    latest = F.max_by(F.struct("status", "status_rank", "event_timestamp", "failure_reason"),
                      F.struct("event_timestamp", "status_rank", "event_id"))
    return p.groupBy("payment_id").agg(
        F.first("order_id", ignorenulls=True).alias("order_id"),
        F.first("attempt", ignorenulls=True).alias("attempt"),
        latest.alias("latest"),
        F.first("method", ignorenulls=True).alias("method"),
        F.first("amount_minor", ignorenulls=True).alias("amount_minor"),
        F.first("currency", ignorenulls=True).alias("currency"),
        _ts_of("PAYMENT_STARTED").alias("started_at"),
        _ts_of("PAYMENT_COMPLETED").alias("completed_at"),
        _ts_of("PAYMENT_FAILED").alias("failed_at"),
        F.max("event_timestamp").alias("updated_at"),
    ).select("payment_id", "order_id", "attempt", F.col("latest.status").alias("status"),
             F.col("latest.status_rank").alias("status_rank"),
             F.col("latest.event_timestamp").alias("status_timestamp"), "method", "amount_minor", "currency",
             F.col("latest.failure_reason").alias("failure_reason"), "started_at", "completed_at", "failed_at",
             "updated_at", F.current_timestamp().alias("silver_updated_at"))


def latest_snapshot(df: DataFrame, key: str, cols: list[str]) -> DataFrame:
    """Last-writer-wins snapshot per key (ties broken by event_id)."""
    latest = F.max_by(F.struct(*cols, "event_timestamp", "event_id"), F.struct("event_timestamp", "event_id"))
    return (df.groupBy(key).agg(latest.alias("l"))
            .select(key, *[F.col(f"l.{c}").alias(c) for c in cols],
                    F.col("l.event_timestamp").alias("updated_at"), F.col("l.event_id").alias("last_event_id"),
                    F.current_timestamp().alias("silver_updated_at")))


CUSTOMER_COLS = ["email", "phone", "first_name", "last_name", "city", "state", "country", "segment",
                 "signup_timestamp"]
PRODUCT_COLS = ["sku", "name", "category", "unit_price_minor", "currency", "active"]


def customers_state(customers: DataFrame) -> DataFrame:
    c = customers.withColumn("signup_timestamp", micros_to_ts(F.col("signup_timestamp")))
    return latest_snapshot(c, "customer_id", CUSTOMER_COLS)


def customer_changes(customers: DataFrame) -> DataFrame:
    return customers.select("event_id", "customer_id", "event_type", *CUSTOMER_COLS[:-1],
                            micros_to_ts(F.col("signup_timestamp")).alias("signup_timestamp"), "event_timestamp",
                            F.current_timestamp().alias("silver_loaded_at"))


def products_state(products: DataFrame) -> DataFrame:
    return latest_snapshot(products, "product_id", PRODUCT_COLS)


def product_changes(products: DataFrame) -> DataFrame:
    return products.select("event_id", "product_id", "event_type", *PRODUCT_COLS, "event_timestamp",
                           F.current_timestamp().alias("silver_loaded_at"))


def inventory_movements(inv: DataFrame) -> DataFrame:
    return inv.select("event_id", "inventory_key", "store_id", "product_id", "reason", "on_hand_delta",
                      "reserved_delta", "order_id", "event_timestamp", F.current_timestamp().alias("silver_loaded_at"))


def inventory_deltas(movements: DataFrame) -> DataFrame:
    return movements.groupBy("inventory_key").agg(
        F.first("store_id").alias("store_id"), F.first("product_id").alias("product_id"),
        F.sum("on_hand_delta").cast("long").alias("quantity_on_hand"),
        F.sum("reserved_delta").cast("long").alias("quantity_reserved"),
        F.count("*").alias("movement_count"),
        F.max("event_timestamp").alias("updated_at"))


# ----------------------------------------------------------------------------- MERGE statements
# Each MERGE is commutative and idempotent with respect to the events it summarises.

MERGE_ORDERS = """
MERGE INTO {target} t USING {source} s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  customer_id      = coalesce(t.customer_id, s.customer_id),
  store_id         = coalesce(t.store_id, s.store_id),
  order_timestamp  = least(t.order_timestamp, s.order_timestamp),
  amount_minor     = coalesce(t.amount_minor, s.amount_minor),
  currency         = coalesce(t.currency, s.currency),
  sales_channel    = coalesce(t.sales_channel, s.sales_channel),
  item_count       = coalesce(t.item_count, s.item_count),
  line_count       = coalesce(t.line_count, s.line_count),
  status           = CASE WHEN t.status_timestamp IS NULL
                            OR s.status_timestamp > t.status_timestamp
                            OR (s.status_timestamp = t.status_timestamp AND s.status_rank > t.status_rank)
                          THEN s.status ELSE t.status END,
  status_rank      = CASE WHEN t.status_timestamp IS NULL
                            OR s.status_timestamp > t.status_timestamp
                            OR (s.status_timestamp = t.status_timestamp AND s.status_rank > t.status_rank)
                          THEN s.status_rank ELSE t.status_rank END,
  last_event_id    = CASE WHEN t.status_timestamp IS NULL
                            OR s.status_timestamp > t.status_timestamp
                            OR (s.status_timestamp = t.status_timestamp AND s.status_rank > t.status_rank)
                          THEN s.last_event_id ELSE t.last_event_id END,
  status_timestamp = greatest(t.status_timestamp, s.status_timestamp),
  paid_at          = least(t.paid_at, s.paid_at),
  cancelled_at     = least(t.cancelled_at, s.cancelled_at),
  cancel_reason    = coalesce(t.cancel_reason, s.cancel_reason),
  shipped_at       = least(t.shipped_at, s.shipped_at),
  carrier          = coalesce(t.carrier, s.carrier),
  delivered_at     = least(t.delivered_at, s.delivered_at),
  source           = coalesce(t.source, s.source),
  updated_at       = greatest(t.updated_at, s.updated_at),
  silver_updated_at = s.silver_updated_at
WHEN NOT MATCHED THEN INSERT *
"""

MERGE_PAYMENTS = """
MERGE INTO {target} t USING {source} s ON t.payment_id = s.payment_id
WHEN MATCHED THEN UPDATE SET
  order_id       = coalesce(t.order_id, s.order_id),
  attempt        = coalesce(t.attempt, s.attempt),
  method         = coalesce(t.method, s.method),
  amount_minor   = coalesce(t.amount_minor, s.amount_minor),
  currency       = coalesce(t.currency, s.currency),
  status         = CASE WHEN s.status_timestamp > t.status_timestamp
                          OR (s.status_timestamp = t.status_timestamp AND s.status_rank > t.status_rank)
                        THEN s.status ELSE t.status END,
  failure_reason = CASE WHEN s.status_timestamp > t.status_timestamp
                          OR (s.status_timestamp = t.status_timestamp AND s.status_rank > t.status_rank)
                        THEN s.failure_reason ELSE t.failure_reason END,
  status_rank    = CASE WHEN s.status_timestamp > t.status_timestamp
                          OR (s.status_timestamp = t.status_timestamp AND s.status_rank > t.status_rank)
                        THEN s.status_rank ELSE t.status_rank END,
  status_timestamp = greatest(t.status_timestamp, s.status_timestamp),
  started_at     = least(t.started_at, s.started_at),
  completed_at   = least(t.completed_at, s.completed_at),
  failed_at      = least(t.failed_at, s.failed_at),
  updated_at     = greatest(t.updated_at, s.updated_at),
  silver_updated_at = s.silver_updated_at
WHEN NOT MATCHED THEN INSERT *
"""

MERGE_LWW = """
MERGE INTO {target} t USING {source} s ON t.{key} = s.{key}
WHEN MATCHED AND (s.updated_at > t.updated_at
                  OR (s.updated_at = t.updated_at AND s.last_event_id > t.last_event_id))
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

MERGE_INSERT_ONLY = """
MERGE INTO {target} t USING {source} s ON {on}
WHEN NOT MATCHED THEN INSERT *
"""
