"""Iceberg table definitions for Bronze, Silver and operational tables.

Layout choices here are the *initial, deliberately unoptimised* ones (Phase 26 measures and evolves
them). Every table is format-version 2.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from core.config import settings

CAT = settings().catalog_name

COMMON_PROPS = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
    "history.expire.max-snapshot-age-ms": str(7 * 24 * 3600 * 1000),
}
# Row-level changes on Silver state tables: copy-on-write keeps every reader (DuckDB, PyIceberg,
# Snowflake) simple; the write amplification it causes is one of the things compaction/partitioning
# experiments measure.
STATE_PROPS = {**COMMON_PROPS, "write.merge.mode": "copy-on-write", "write.update.mode": "copy-on-write",
               "write.delete.mode": "copy-on-write"}

BRONZE_COLUMNS = """
    source               string    COMMENT 'kafka | replay | batch',
    topic                string,
    kafka_partition      int,
    kafka_offset         bigint,
    kafka_key            string,
    payload              binary    COMMENT 'raw Confluent-Avro bytes, exactly as received',
    ingestion_timestamp  timestamp COMMENT 'broker LogAppendTime',
    schema_id            int,
    schema_version       int,
    event_id             string,
    event_type           string,
    event_timestamp      timestamp,
    decode_error         string,
    bronze_loaded_at     timestamp
"""

TABLES: dict[str, tuple[str, str, dict[str, str]]] = {
    # name: (columns, partition spec, properties)
    **{
        f"bronze.{d}": (BRONZE_COLUMNS, "days(ingestion_timestamp)", COMMON_PROPS)
        for d in ("orders", "payments", "inventory", "customers", "products")
    },
    "silver.events": ("""
        event_id             string NOT NULL,
        event_type           string NOT NULL,
        topic                string NOT NULL,
        entity_key           string NOT NULL,
        event_timestamp      timestamp NOT NULL,
        ingestion_timestamp  timestamp,
        schema_version       int,
        kafka_partition      int,
        kafka_offset         bigint,
        source               string,
        record_json          string    COMMENT 'decoded record (reader schema), JSON',
        silver_loaded_at     timestamp
    """, "topic", COMMON_PROPS),
    "silver.orders": ("""
        order_id         string NOT NULL,
        customer_id      string,
        store_id         string,
        order_timestamp  timestamp,
        status           string,
        status_rank      int,
        status_timestamp timestamp,
        amount_minor     bigint,
        currency         string,
        sales_channel    string,
        item_count       int,
        line_count       int,
        paid_at          timestamp,
        cancelled_at     timestamp,
        cancel_reason    string,
        shipped_at       timestamp,
        carrier          string,
        delivered_at     timestamp,
        source           string,
        last_event_id    string,
        updated_at       timestamp,
        silver_updated_at timestamp COMMENT 'processing time of the last change (incremental key)'
    """, "bucket(8, order_id)", STATE_PROPS),
    "silver.order_items": ("""
        order_id          string NOT NULL,
        line_number       int NOT NULL,
        product_id        string,
        quantity          int,
        unit_price_minor  bigint,
        line_amount_minor bigint,
        currency          string,
        order_timestamp   timestamp,
        event_id          string,
        silver_loaded_at timestamp COMMENT 'processing time the row was inserted (incremental key)'
    """, "bucket(8, order_id)", STATE_PROPS),
    "silver.payments": ("""
        payment_id      string NOT NULL,
        order_id        string,
        attempt         int,
        status          string,
        status_rank     int,
        status_timestamp timestamp,
        method          string,
        amount_minor    bigint,
        currency        string,
        failure_reason  string,
        started_at      timestamp,
        completed_at    timestamp,
        failed_at       timestamp,
        updated_at      timestamp,
        silver_updated_at timestamp COMMENT 'processing time of the last change (incremental key)'
    """, "bucket(8, payment_id)", STATE_PROPS),
    "silver.customers": ("""
        customer_id      string NOT NULL,
        email            string COMMENT 'PII: restricted',
        phone            string COMMENT 'PII: restricted',
        first_name       string COMMENT 'PII: confidential',
        last_name        string COMMENT 'PII: confidential',
        city             string,
        state            string,
        country          string,
        segment          string,
        signup_timestamp timestamp,
        updated_at       timestamp,
        last_event_id    string,
        silver_updated_at timestamp COMMENT 'processing time of the last change (incremental key)'
    """, "", STATE_PROPS),
    "silver.customer_changes": ("""
        event_id         string NOT NULL,
        customer_id      string NOT NULL,
        event_type       string,
        email            string COMMENT 'PII: restricted',
        phone            string COMMENT 'PII: restricted',
        first_name       string COMMENT 'PII: confidential',
        last_name        string COMMENT 'PII: confidential',
        city             string,
        state            string,
        country          string,
        segment          string,
        signup_timestamp timestamp,
        event_timestamp  timestamp,
        silver_loaded_at timestamp COMMENT 'processing time the row was inserted (incremental key)'
    """, "", COMMON_PROPS),
    "silver.products": ("""
        product_id        string NOT NULL,
        sku               string,
        name              string,
        category          string,
        unit_price_minor  bigint,
        currency          string,
        active            boolean,
        updated_at        timestamp,
        last_event_id     string,
        silver_updated_at timestamp COMMENT 'processing time of the last change (incremental key)'
    """, "", STATE_PROPS),
    "silver.product_changes": ("""
        event_id          string NOT NULL,
        product_id        string NOT NULL,
        event_type        string,
        sku               string,
        name              string,
        category          string,
        unit_price_minor  bigint,
        currency          string,
        active            boolean,
        event_timestamp   timestamp,
        silver_loaded_at timestamp COMMENT 'processing time the row was inserted (incremental key)'
    """, "", COMMON_PROPS),
    "silver.inventory_movements": ("""
        event_id         string NOT NULL,
        inventory_key    string NOT NULL,
        store_id         string,
        product_id       string,
        reason           string,
        on_hand_delta    int,
        reserved_delta   int,
        order_id         string,
        event_timestamp  timestamp,
        silver_loaded_at timestamp COMMENT 'processing time the row was inserted (incremental key)'
    """, "days(event_timestamp)", COMMON_PROPS),
    "silver.inventory": ("""
        inventory_key      string NOT NULL,
        store_id           string,
        product_id         string,
        quantity_on_hand   bigint,
        quantity_reserved  bigint,
        movement_count     bigint,
        updated_at         timestamp,
        silver_updated_at timestamp COMMENT 'processing time of the last change (incremental key)'
    """, "", STATE_PROPS),
    "silver.stores": ("""
        store_id    string NOT NULL,
        name        string,
        city        string,
        country     string,
        currency    string,
        opened_at   timestamp,
        updated_at  timestamp,
        extracted_at timestamp
    """, "", STATE_PROPS),
    "silver.realtime_sales_1m": ("""
        window_start     timestamp NOT NULL,
        window_end       timestamp NOT NULL,
        currency         string NOT NULL,
        orders           bigint,
        revenue_minor    bigint,
        computed_at      timestamp
    """, "days(window_start)", STATE_PROPS),
    "ops.dlq": ("""
        dlq_id               string NOT NULL COMMENT 'topic/partition/offset (or replay id)',
        source               string,
        topic                string,
        kafka_partition      int,
        kafka_offset         bigint,
        kafka_key            string,
        payload              binary,
        schema_id            int,
        schema_version       int,
        event_id             string,
        event_type           string,
        event_timestamp      timestamp,
        error_codes          string,
        error_message        string,
        ingestion_timestamp  timestamp,
        dlq_timestamp        timestamp,
        status               string COMMENT 'PENDING | REPLAYED | DISCARDED',
        resolution_note      string,
        resolved_at          timestamp
    """, "topic", STATE_PROPS),
    "ops.stream_batches": ("""
        query               string,
        batch_id            bigint,
        topic               string,
        received            bigint,
        decoded             bigint,
        rejected            bigint,
        duplicates_in_batch bigint,
        duplicates_known    bigint,
        accepted_new        bigint,
        late_events         bigint,
        min_event_timestamp timestamp,
        max_event_timestamp timestamp,
        max_ingestion_timestamp timestamp,
        duration_ms         bigint,
        processed_at        timestamp
    """, "", COMMON_PROPS),
}


def fq(name: str) -> str:
    return f"{CAT}.{name}"


def create_all(spark: SparkSession, only: list[str] | None = None) -> list[str]:
    created = []
    for ns in ("bronze", "silver", "ops", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.{ns}")
    for name, (cols, partition, props) in TABLES.items():
        if only and name not in only:
            continue
        part = f"PARTITIONED BY ({partition})" if partition else ""
        tblprops = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fq(name)} ({cols}) USING iceberg {part} TBLPROPERTIES ({tblprops})")
        created.append(name)
    return created


def drop_all(spark: SparkSession, prefixes: tuple[str, ...] = ("bronze.", "silver.", "ops.")) -> None:
    for name in TABLES:
        if name.startswith(prefixes):
            spark.sql(f"DROP TABLE IF EXISTS {fq(name)} PURGE")


def truncate(spark: SparkSession, names: list[str]) -> None:
    for name in names:
        spark.sql(f"DELETE FROM {fq(name)} WHERE true")
