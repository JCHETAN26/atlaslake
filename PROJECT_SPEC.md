# AtlasLake — Project Specification

Status: **implemented** — contract created 2026-10-03 (Phase 0) and kept current as phases landed.

AtlasLake is a streaming + batch lakehouse for commerce data. Its single claim is:

> We can reliably process large-scale commerce data through failures, duplicates, late events,
> schema changes and historical backfills while maintaining trustworthy analytical tables.

This document is the contract every later phase is held to. It defines **what must be true**, **how
we will prove it**, and **what we will measure**. It deliberately contains no performance targets:
every number in this project comes out of `make benchmark` (see §6), never out of this file.

---

## 1. Ground rules

1. **Earn every component.** A technology (Kafka, Spark, Iceberg, dbt, Snowflake, Airflow, …) is
   introduced only after the simpler system it replaces has been measured and a concrete problem is
   recorded in the decision log (§8). "Everyone uses it" is not a reason.
2. **Baseline first.** The Phase 4 naive pipeline is a permanent fixture, not a throwaway. It is
   re-run at every scale step so comparisons are like-for-like.
3. **No invented numbers.** Documentation cites `evaluation/*.json` files produced by a command.
   A metric with no generating command does not exist.
4. **Every guarantee has a test.** A guarantee in §3 without a named test or experiment is a wish
   and must be marked as such.
5. **Reproducible from a seed.** Same seed + same config ⇒ same source data, same event stream
   (modulo injected wall-clock arrival jitter, which is itself seeded).
6. **Bronze is the system of record for history.** Gold is never the only copy of anything.

---

## 2. Definitions

These terms are used precisely throughout the project.

| Term | Definition |
|---|---|
| **Event** | One immutable business fact emitted by the source (e.g. `ORDER_CREATED`). Identified by `event_id` (UUID, assigned at the source, stable across redelivery). |
| **Delivery** | One physical arrival of an event at the platform. One event may have many deliveries. |
| **Physical duplicate** | A second+ delivery of the same `event_id`. Expected; must be absorbed. |
| **Logical duplicate** | Two rows in a Silver/Gold table representing the same business entity at the same grain (e.g. two rows for one `order_id` in `silver.orders`, or one order counted twice in `fact_orders`). **Never acceptable.** |
| **Business key** | Natural identifier of an entity: `order_id`, `(order_id, line_number)`, `payment_id`, `customer_id`, `product_id`, `store_id`, `(store_id, product_id)` for inventory. |
| **Event time** | `event_timestamp` — when the fact happened at the source. |
| **Ingestion time** | `ingestion_timestamp` — when the platform first received the delivery. |
| **Lateness** | `ingestion_timestamp − event_timestamp` for a delivery. |
| **Watermark** | Per-stream threshold `max(event_time seen) − allowed_lateness`. Defined per stream in Phase 12. |
| **Accepted** | Passed validation and reached Silver. |
| **Rejected** | Failed validation; written to the DLQ with a reason code. |
| **Filtered** | Intentionally excluded by a documented rule (e.g. test traffic, beyond-watermark for a streaming aggregate). Must carry a reason code and be countable. |
| **Data loss** | A source event that is not in Bronze, **or** an event in Bronze that is not accounted for as accepted, rejected or filtered downstream. |
| **Freshness** | `now − max(event_timestamp)` visible in a given table. |
| **End-to-end latency** | For one event: time it becomes queryable in a layer − `event_timestamp` (or − source commit time for CDC). |

---

## 3. Guarantees

Each guarantee lists how it is verified and the phase that delivers it. "Verified by" names are the
intended test/experiment identifiers; they become real files as phases land.

### 3.1 Correctness

| ID | Guarantee | Verified by | Phase |
|---|---|---|---|
| C1 | **No silent data loss.** Every source event is in Bronze, and every Bronze event is accepted, rejected or filtered — with counts that close exactly. | `reconciliation.json` invariant check; `tests/failure/test_consumer_crash` | 23, 36 |
| C2 | **No unexpected logical duplicates** in Silver or Gold under any amount of redelivery. | uniqueness tests on business keys; `duplicate_test.json` | 11, 37 |
| C3 | **Idempotent reprocessing.** Re-running any pipeline step over the same input range yields byte-for-byte equivalent logical state (same rows, same aggregates). | `backfill_benchmark.json` (run twice, diff) | 25, 41 |
| C4 | **Source → Gold reconciliation.** Per window: `source = accepted + rejected + filtered`; financial totals reconcile to the cent between source and Gold. | `atlas reconcile`; `reconciliation.json` | 23 |

### 3.2 Streaming

| ID | Guarantee | Verified by | Phase |
|---|---|---|---|
| S1 | **Event-time correctness.** Aggregates are assigned to windows by `event_timestamp`, not arrival order. | `late_event_test.json` (shuffled arrival) | 12 |
| S2 | **Explicit late-event policy.** Behaviour for on-time, late-within-watermark, and beyond-watermark events is written down per stream and tested at each boundary. Beyond-watermark events are never dropped silently: they are routed and counted. | `tests/integration/test_late_events`; `late_event_test.json` | 12, 38 |
| S3 | **Recoverable processing.** A killed consumer resumes from its checkpoint with zero loss and zero logical duplicates. | `consumer_recovery.json` | 36 |
| S4 | **DLQ + replay.** Invalid events land in a DLQ with original payload, error, source, schema version and timestamp; corrected events can be replayed and are applied exactly once logically. | `tests/integration/test_dlq_replay`; `atlas replay-dlq` | 13, 39 |

### 3.3 Lakehouse

| ID | Guarantee | Verified by | Phase |
|---|---|---|---|
| L1 | **Schema evolution.** Compatible changes (per the policy in §5) flow end-to-end with old and new records readable; incompatible changes are rejected at the registry before any consumer is affected. | `schema_evolution.json` | 16, 40 |
| L2 | **Partition management.** Partition layout is chosen by benchmark, and pruning is demonstrated by files/bytes scanned. | `iceberg_partitioning.json` | 26 |
| L3 | **Compaction.** Small-file accumulation from streaming is measured and remediated by scheduled compaction. | `iceberg_compaction.json` | 27, 28 |
| L4 | **Time travel.** Any Silver table can be queried as of a snapshot within the retention window; retention is explicit. | `tests/integration/test_time_travel` | 14, 28 |

### 3.4 Analytics

| ID | Guarantee | Verified by | Phase |
|---|---|---|---|
| A1 | **SCD Type 2** on customer (and product where attributes change) with non-overlapping `[valid_from, valid_to)` and exactly one current row per key. | dbt tests + historical-join tests | 17 |
| A2 | **Incremental transformations** produce the same result as full refresh. | `full_vs_incremental.json` (result equality + cost) | 20 |
| A3 | **Star schema** with documented grain for every fact, surrogate keys, conformed dimensions. | dbt docs + relationship tests | 19 |
| A4 | **Tested Gold.** Every Gold model has uniqueness/not-null/relationship tests and at least one business-rule test. | `dbt test`; `data_quality.json` | 18, 22 |

### 3.5 Operations

| ID | Guarantee | Verified by | Phase |
|---|---|---|---|
| O1 | **Backfills** over an arbitrary date range are a single command and idempotent (C3). | `atlas backfill`; `backfill_benchmark.json` | 25, 41 |
| O2 | **Monitoring** of throughput, lag, freshness, failures, DLQ, quality and reconciliation status. | Grafana dashboards | 30 |
| O3 | **Lineage** from Kafka topic to Snowflake table for every Gold model. | Marquez graph; `atlas lineage` | 29 |
| O4 | **Alerts** fire on: reconciliation mismatch, DLQ rate spike, freshness breach, consumer lag growth, failed quality check, failed job. | alert rule tests / chaos test | 30, 45 |

### 3.6 Infrastructure

| ID | Guarantee | Verified by | Phase |
|---|---|---|---|
| I1 | **Reproducible deployment.** Local stack is one `docker compose up`; AWS stack is `terraform apply` from a clean account. | CI + fresh-clone run | 2, 31, 32 |
| I2 | **CI/CD gates.** Nothing deploys if lint, types, unit, Spark, contract, dbt or integration tests fail. | GitHub Actions | 34 |

---

## 4. Domain model

### Entities (source of truth: PostgreSQL, Phase 3)

`customers`, `products`, `stores`, `orders`, `order_items`, `payments`, `inventory`.

### Event types

| Domain | Events | Kafka topic | Partition key |
|---|---|---|---|
| Orders | `ORDER_CREATED`, `ORDER_UPDATED`, `ORDER_CANCELLED`, `ORDER_SHIPPED`, `ORDER_DELIVERED` | `commerce.orders` | `order_id` |
| Payments | `PAYMENT_STARTED`, `PAYMENT_COMPLETED`, `PAYMENT_FAILED` | `commerce.payments` | `order_id` ¹ |
| Inventory | `INVENTORY_RESERVED`, `INVENTORY_UPDATED` | `commerce.inventory` | `store_id:product_id` |
| Customers | `CUSTOMER_CREATED`, `CUSTOMER_UPDATED` (full snapshot) | `commerce.customers` | `customer_id` |
| Products | `PRODUCT_CREATED`, `PRODUCT_UPDATED` (full snapshot) | `commerce.products` | `product_id` |
| Stores | — (batch JDBC extract, no CDC) | — | — |

¹ Keyed by `order_id` (not `payment_id`) so all payment attempts for an order are ordered relative to
each other. Revisit in Phase 6 if skew is measured.

### Order lifecycle (valid transitions)

```
CREATED ──► PAID ──► SHIPPED ──► DELIVERED
   │          │
   └──────────┴──► CANCELLED      (customer request, or payment failed after ≤ 2 attempts;
                                    cancellation allowed until SHIPPED)
```
Payment attempts live on their own entity (`STARTED → COMPLETED | FAILED`, up to 2 attempts).

Transitions outside this graph are a validation error (DLQ) for the source generator's output, and a
quality-check failure if they ever appear in Silver.

### Mandatory event envelope

Every event, every version:

```
event_id            string (UUID)   stable across redelivery
event_type          enum
event_timestamp     timestamp-micros, UTC
ingestion_timestamp timestamp-micros, UTC (set by platform, not producer)
producer            string
schema_version      int
```

Bronze additionally records: `source`, `topic`, `partition`, `offset`, raw payload bytes.

### Money

Amounts are stored as **integer minor units** (`amount_minor BIGINT`) plus ISO-4217 `currency`.
Floating point never touches a financial total. (Prevents C4 from failing on rounding noise.)

---

## 5. Policies (to be finalised in the named phase)

These are stated now so later phases cannot quietly skip them. Values marked **TBD** are decided
from measurements, and the decision is recorded in §8.

| Policy | Initial position | Finalised in |
|---|---|---|
| Delivery semantics | At-least-once transport; effectively-once *logical* state via `event_id` dedup + idempotent MERGE on business key. | 10, 11 |
| Dedup state horizon | Streaming dedup state kept for `allowed_lateness`; anything older is caught by Silver MERGE on `event_id`. Horizon **TBD**. | 11 |
| Allowed lateness / watermark | Per stream, **TBD**. Beyond-watermark events: written to Bronze, excluded from streaming aggregates, routed to `late_events` and reconciled by the next batch run. Never silently dropped. | 12 |
| Conflicting updates | Last-writer-wins by `(event_timestamp, source sequence)`; ties broken deterministically by `event_id`. | 15 |
| Schema compatibility | Registry mode `BACKWARD_TRANSITIVE` per subject. Additive optional fields with defaults allowed; removing/renaming required fields or changing types rejected. | 5, 16 |
| Snapshot retention | **TBD** (must exceed the longest supported backfill/replay window). | 28 |
| DLQ retention | DLQ records retained until replayed or explicitly discarded with a reason. | 13 |
| PII | `email`, `phone`, street address classified *restricted*; masked or excluded in Gold; never in logs. | 33 |

---

## 6. Metrics

All metrics are produced by `make benchmark` into `evaluation/` (Phase 44). Each run records the
git SHA, seed, data scale, hardware and component versions alongside the numbers.

| Metric | Precise definition | Method | Output |
|---|---|---|---|
| Throughput | Events/sec sustained into a layer over a fixed window, excluding warm-up | count / wall time | `streaming_throughput.json`, `baseline_pipeline.json` |
| Freshness | `now − max(event_timestamp)` per table, sampled | periodic query | `baseline_pipeline.json`, dashboards |
| Latency p50 / p95 | Per-event end-to-end latency (§2) distribution | instrumented probe events | `streaming_throughput.json` |
| Consumer lag | Sum of `(log-end offset − committed offset)` per consumer group, per partition | Kafka admin API / exporter | `consumer_recovery.json` |
| Processing runtime | Wall time per pipeline stage and total | stage timers | `baseline_pipeline.json`, `spark_performance.json` |
| Bytes / files scanned | As reported by the engine for a query (Iceberg scan metrics / Snowflake query profile / Postgres `EXPLAIN (ANALYZE, BUFFERS)`) | engine metrics | `query_performance.json`, `iceberg_*.json` |
| Query p50 / p95 | Latency over N repetitions of a fixed query set, cold and warm reported separately | benchmark harness | `query_performance.json` |
| Backfill runtime | Wall time + throughput for a fixed date range, run twice | `atlas backfill` | `backfill_benchmark.json` |
| Recovery time | Time from consumer restart until lag returns to its pre-kill steady state | lag time series | `consumer_recovery.json` |
| Duplicate count | Deliveries received, duplicates detected, logical duplicates in Silver, logical duplicates in Gold | counters + uniqueness queries | `duplicate_test.json` |
| Reconciliation accuracy | Per window: source count/sum vs accepted + rejected + filtered; mismatch count and magnitude | reconciliation job | `reconciliation.json` |

Required report files (Phase 44):
`baseline_pipeline.json`, `streaming_throughput.json`, `spark_performance.json`,
`duplicate_test.json`, `late_event_test.json`, `consumer_recovery.json`, `schema_evolution.json`,
`full_vs_incremental.json`, `iceberg_partitioning.json`, `iceberg_compaction.json`,
`backfill_benchmark.json`, `reconciliation.json`, `query_performance.json`, `data_quality.json`.

---

## 7. Development environment & known constraints

Measured on the primary dev machine, 2026-10-03:

| Resource | Value | Implication |
|---|---|---|
| CPU | 16 logical cores | Local Spark `local[*]` is viable for development. |
| RAM | 19 GB | Full stack (Postgres + Kafka + Schema Registry + Spark + MinIO + Airflow + Marquez + Prometheus/Grafana) will not all fit comfortably at once. Use compose **profiles** and keep JVM heaps explicit. |
| Free disk | ~76 GB on the project drive | Limits the scale ladder. Rough budget must be computed before each step (Bronze + Silver + Kafka retention + Postgres). 100M+ events likely needs aggressive Kafka retention and cleanup. |
| Project filesystem | **exFAT** (external SSD) | No symlinks, no POSIX ownership. Python venv and Docker data volumes should live off this drive (`UV_PROJECT_ENVIRONMENT` outside repo; Docker **named** volumes, not bind mounts, for Postgres/Kafka data). Git needs `core.fileMode=false`. |
| Python | 3.12 (via uv) | Target 3.12 for PySpark compatibility. |
| Java | OpenJDK 11 | Spark 4.x requires Java 17 — run Spark in containers or install JDK 17. Decide in Phase 9. |
| Docker | 29.x, Compose v5 | OK. |

Scale ladder: **100K → 1M → 10M → 50M → 100M+ events**. We stop climbing when a bottleneck is
demonstrated *and* explained, or when hardware limits are hit — whichever comes first. The stopping
point and reason are recorded in §8.

**Where the ladder stopped: 10M events for the full pipeline.** Measured footprint is ~525 bytes/event
in Postgres (source + outbox) and ~300 bytes/event in Kafka, both on the root disk (33 GB free after
images). 50M events would need ~26 GB of Postgres plus ~15 GB of Kafka, which does not fit alongside
the Docker images. The bottlenecks that *were* exposed and explained before that point:

* **Spark micro-batch fixed cost.** Throughput rises steeply with batch size (`spark_performance.json`)
  because every batch pays ~10 Iceberg commits plus MERGE planning; independent table writes were
  parallelised as a result.
* **Copy-on-write MERGE rewrites.** Every micro-batch rewrites the touched buckets of `silver.orders`,
  so file-level pruning by processing time is impossible downstream.
* **The live source is the throughput ceiling for live tests.** The single-process Postgres workload
  generator, not Kafka or Spark, bounds the sustained live rate (`streaming_throughput.json`).

---

## 8. Decision log

Each entry: date · phase · decision · evidence (link to an `evaluation/` file or measurement) ·
alternatives rejected.

| Date | Phase | Decision | Evidence | Alternatives |
|---|---|---|---|---|
| 2026-10-03 | 0 | Money as integer minor units | Avoids float drift breaking C4 | `NUMERIC` everywhere (kept in Postgres source; minor units in events/lake) |
| 2026-10-03 | 0 | Payments keyed by `order_id` | Per-order ordering across payment attempts | `payment_id` |
| 2026-10-03 | 1 | Seed data: UCI Online Retail II (real catalogue, prices, basket/quantity/hour/weekday/country distributions), expanded by a seeded generator | Public, no login, CC BY 4.0; derived reference set committed (`simulator/reference_data`) so generation never depends on the download | Olist (Kaggle login), fully synthetic |
| 2026-10-03 | 3 | Transactional outbox in Postgres + relay instead of Debezium | Entity change and its event commit atomically; relay measured at ~37K events/s single process; no Kafka Connect cluster (RAM budget §7) | Debezium/Kafka Connect |
| 2026-10-03 | 5 | One Avro record per topic (wide, nullable payload) under TopicNameStrategy + `BACKWARD_TRANSITIVE` | Lets the registry enforce compatibility per topic; consumers decode with the *writer* schema by id | One schema per event type (TopicRecordNameStrategy) |
| 2026-10-03 | 9-10 | Spark 3.5.9 on Java 11, `local[*]`, Iceberg 1.10 | JDK 11 is what the host has; Spark 4 needs 17 (containers use 17) | Spark 4.x |
| 2026-10-03 | 10 | Single Kafka → Bronze+DLQ+Silver `foreachBatch` job; Silver dedup = in-batch window + anti-join against the `silver.events` ledger (not Spark watermark dedup) | Spark's stateful dedup *drops* rows behind the watermark — that would be silent loss for very late events (violates C1). Watermark state is used only for the real-time aggregate, where exclusion is the documented policy | `dropDuplicatesWithinWatermark` on the main path |
| 2026-10-03 | 10 | Idempotent writes: commutative MERGEs + content-addressed batch keys in Iceberg snapshot summaries | Crash/replay safe without multi-table transactions; proven by re-running batches/backfills with identical fingerprints | Exactly-once Kafka transactions |
| 2026-10-03 | 14 | Iceberg JDBC catalog in Postgres (Spark, PyIceberg and DuckDB share it) | No extra catalog service; transactional commits | REST catalog, Hive metastore |
| 2026-10-03 | 18 | dbt on DuckDB reading Silver Iceberg in place (custom dbt-duckdb plugin creating `iceberg_scan` views) | No data copy out of the lake; whole Gold build in seconds at 1M events | dbt-spark + Thrift server (RAM) |
| 2026-10-03 | 20 | Incremental models key on *processing time* (`silver_updated_at`) | Event-time keys miss late events that modify old rows — demonstrated in design review; dims rebuilt in full (cheap, always correct with late SCD2 changes) | event-time high-water mark |
| 2026-10-03 | 22 | Great Expectations 1.x (programmatic, ephemeral context) + dbt tests | Resolved cleanly alongside Spark/dbt; suites are code (`quality/suites.py`) | Soda Core |
| 2026-10-03 | 21 | Snowflake as *serving* target loaded from Gold (dims full-replace, facts/marts MERGE by grain key with a watermark) | Exercised against a fake connector in CI; no live account used | Snowflake reading Iceberg directly (documented as alternative) |
| 2026-10-03 | 7 | MinIO via `bitnamilegacy/minio` | Docker Hub `minio/*` images no longer available | SeaweedFS, RustFS |

### Defects found by the experiments (and fixed)

| Found by | Defect | Fix | Regression test |
|---|---|---|---|
| dbt singular test `assert_gold_revenue_reconciles_to_silver` | Order dates depended on the host timezone (DuckDB casts `timestamptz` with the session zone) | `TZ=UTC` platform-wide + `SET TimeZone='UTC'` in every DuckDB session | revenue + reconciliation tests |
| `backfill_benchmark` (Silver wiped → rebuilt from Bronze) | Content-addressed batch markers outlived a table wipe; a backfill chunk equal to an earlier micro-batch was skipped (inventory missing) | markers only count within the current table lineage (stop at unkeyed delete/overwrite) | `tests/unit/test_idempotency_markers.py` |
| `consumer_recovery` showing a zero backlog | Live workload restock ids restarted per run → outbox unique violation → workload died silently | per-run id namespace; live suites now fail loudly if the workload dies | `test_restock_ids_are_unique_across_independent_live_runs` |
| `full_vs_incremental` (incremental slower than full) | Look-back window (clock-skew guard) larger than the run cadence re-read everything | 1-minute look-back; key-set semi-join in `fact_orders` | `results_identical` in the benchmark |
| `full_vs_incremental` at 10M (fact_orders 50 s for 705K rows) | SCD2 point-in-time join written as `ts < valid_to OR valid_to IS NULL` pushed DuckDB off the hash join (112 s) | `as_of()` macro with an open upper bound (0.09 s, identical result) | dbt tests + Gold fingerprints |
| Spark driver vanished mid-benchmark at 10M | JVM (6 GB heap) + DuckDB (6 GB limit) + services exceeded 19 GB → killed | driver 5 GB, DuckDB 3 GB; benchmark runner recreates a dead session | — |
| **chaos test** (1 of 2,751,035 events unexplained) | a delivery corrupted in transit (`UNKNOWN_SCHEMA_ID`) could only be attributed through the relay's delivery log, whose last rows were lost when the relay was stopped → not attributable, not replayable | `atlas-event-id` Kafka header on every delivery (survives payload corruption) + continuous delivery-log writes + graceful SIGTERM | `tests/failure/test_corrupt_attribution.py` (delivery log wiped → still 0 unexplained) |
| repeated `Connection reset` from S3 | (1) Bitnami MinIO wrapper exited every ~70 s; (2) Docker userland proxy (daemon has `iptables=false`) reset connections under many small writes; (3) container `nofile=1024` | run MinIO binary directly, host networking, `nofile=65536`, S3 retry/backoff policy | small-file experiment: 0 resets |

---

## 9. Out of scope (for now)

- Multi-region / cross-region replication.
- Exactly-once *transport* (Kafka transactions end-to-end) — we prove effectively-once *state* instead.
- Real payment processing or real PII. All personal data is synthetic.
- Serving APIs / BI front end beyond Snowflake queries and Grafana.
