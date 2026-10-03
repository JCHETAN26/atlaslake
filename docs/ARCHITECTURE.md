# AtlasLake architecture

```
                       ┌──────────────────────── SOURCE (PostgreSQL 16) ───────────────────────┐
 UCI Online Retail II ─► seeded simulator ─► commerce.* entity tables  +  commerce.outbox (same tx)│
 (real catalogue +       (historical bulk                                                          │
  distributions)          or live workload)                                                         │
                       └──────────────┬──────────────────────────────────────────┬───────────────┘
                                      │ outbox relay (at-least-once, chaos knobs) │ JDBC (stores)
                                      ▼                                          │
                  Kafka (KRaft) + Confluent Schema Registry (Avro, BACKWARD_TRANSITIVE)
                  commerce.{orders,payments,inventory,customers,products}        │
                                      │                                          │
                     Spark Structured Streaming (foreachBatch, checkpointed)     │
                     decode with WRITER schema → validate → dedup → MERGE        │
                ┌─────────────────────┼──────────────────────────┐               │
                ▼                     ▼                          ▼               ▼
        Iceberg BRONZE          Iceberg ops.dlq           Iceberg SILVER ◄──────┘
        raw bytes + Kafka       payload + reasons         events ledger + entity state
        coordinates             atlas replay-dlq ─► Kafka (customers, products: + change history)
                │                                                │
                └── batch / backfill (same processor) ──────────►│
                                                                 ▼
                           dbt on DuckDB (iceberg_scan views, no copy) ─► GOLD star schema
                           SCD2 dims · facts with documented grain · incremental marts
                                                                 │
                              ┌──────────────────────────────────┼─────────────────────┐
                              ▼                                  ▼                     ▼
                     Postgres analytics.gold             Iceberg gold.*         Snowflake (MERGE)

 Control plane: Airflow DAGs · Great Expectations + dbt tests · event-level reconciliation ·
 OpenLineage → Marquez · Prometheus + Grafana + Alertmanager · Terraform (AWS) · GitHub Actions
```

## Layers and their contracts

| Layer | Answers | Storage | Written by | Key property |
|---|---|---|---|---|
| Source | what the business did | Postgres `commerce.*` | simulator / workload | entity change + outbox row in one transaction |
| Kafka | what was published | 5 topics × 6 partitions | relay, DLQ replay | keyed by `order_id` (orders *and* payments) → per-order ordering |
| Bronze | **what arrived** | Iceberg `bronze.<dataset>`, `days(ingestion_timestamp)` | streaming job | every physical delivery, raw bytes, Kafka coordinates, decode errors. Append-only, exactly-once per offset |
| DLQ | what was rejected and why | Iceberg `ops.dlq` | streaming / batch | original payload + error codes; status `PENDING → REPLAYED / DISCARDED / REPROCESSED` |
| Silver | **what we believe happened** | Iceberg `silver.*` | shared processor | unique events ledger; state from commutative MERGEs; change history for SCD2 |
| Gold | what the business asks | DuckDB file (+ published copies) | dbt | star schema, tested, reconciled to Silver to the cent |

## Why the guarantees hold

| Guarantee | Mechanism | Code |
|---|---|---|
| No silent loss (C1) | every record lands in Bronze or DLQ or Silver; reconciliation classifies every source event id | `spark/common/pipeline.py`, `reconciliation/reconcile.py` |
| No logical duplicates (C2) | in-batch window dedup + anti-join against the `silver.events` ledger; MERGEs keyed by business key | `pipeline.py`, `transforms.py` |
| Idempotent reprocessing (C3) | commutative MERGEs; additive writes guarded by content-addressed batch keys stored in Iceberg snapshot summaries | `pipeline.append_once`, `already_committed` |
| Event-time correctness (S1) | LWW by `(event_timestamp, status_rank)`; Spark windows on event time | `transforms.MERGE_ORDERS`, `realtime_aggregates.py` |
| Late data policy (S2) | Silver applies all valid events; the real-time aggregate uses a 10-min watermark | [LATE_DATA_POLICY.md](LATE_DATA_POLICY.md) |
| Recovery (S3) | Kafka offsets in the Spark checkpoint; replayed batch → same writes | `silver_stream.py` + C3 |
| Schema safety (L1) | registry rejects incompatible versions; consumers decode with writer schema → reader schema | `contracts/`, `decode.py` |
| Gold correctness | dbt tests + singular invariants (revenue = Silver, inventory = Σ movements) | `dbt/tests/` |

## Running it

```bash
make up && make init          # core stack + schemas/topics/tables
make demo                     # simulate → relay → stream → gold → reconcile
docker compose --profile monitoring --profile lineage up -d
make benchmark SCALE=small    # writes evaluation/*.json
python -m benchmarks.chaos    # Phase 45 chaos run
```

See also [ORCHESTRATION.md](ORCHESTRATION.md), [MONITORING.md](MONITORING.md),
[AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md), [SECURITY.md](SECURITY.md) and [RESULTS.md](RESULTS.md),
which is generated from `evaluation/` and never edited by hand.
