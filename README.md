# AtlasLake

**A streaming + batch lakehouse for commerce data that stays correct under failures.**
It handles crashes, duplicate deliveries, late and out-of-order events, malformed records,
schema changes and historical backfills, and it proves its correctness with measurements.

```
Postgres (outbox) → Kafka + Avro/Schema Registry → Spark Structured Streaming → Iceberg Bronze / DLQ / Silver
                  → dbt on DuckDB (SCD2 star schema) → Gold → Postgres · Iceberg · Snowflake
control plane: Airflow · Great Expectations · reconciliation · OpenLineage/Marquez · Prometheus/Grafana · Terraform · GitHub Actions
```

* **Results:** [`docs/RESULTS.md`](docs/RESULTS.md). It is generated from `evaluation/*.json`, and no
  number in this repo is typed by hand.
* **Contract:** [`PROJECT_SPEC.md`](PROJECT_SPEC.md) lists the guarantees, the metric definitions and
  a decision log with the evidence for every technology choice.
* **Design:** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and
  [`docs/LATE_DATA_POLICY.md`](docs/LATE_DATA_POLICY.md).

## What it guarantees, and how that's checked

| Guarantee | How it is enforced | How it is checked |
|---|---|---|
| No silent data loss | Every delivery lands in Bronze (raw bytes) and in either Silver or the DLQ | `atlas reconcile` accounts for every source event id as accepted, rejected, in flight or pending; anything else is *unexplained* and fails the run |
| No logical duplicates | In-batch dedup, plus an anti-join against the `silver.events` ledger, plus MERGEs keyed by business key | duplicate injection experiment (`evaluation/duplicate_test.json`) and dbt uniqueness tests |
| Idempotent reprocessing | Commutative MERGEs. Additive writes carry content-addressed batch keys in their Iceberg snapshots | re-running a backfill leaves the order-independent state fingerprints identical |
| Event-time correctness | Last-writer-wins by `(event_time, status rank)`, plus a watermark policy for real-time windows | out-of-order and late-tier tests |
| Crash recovery | Spark checkpoints combined with idempotent writes | `kill -9` mid-stream (`evaluation/consumer_recovery.json`) |
| Schema safety | Registry runs `BACKWARD_TRANSITIVE`; consumers decode with the *writer's* schema | v1 and v2 producers coexist; a breaking change is rejected (HTTP 409) |
| Trustworthy Gold | 53 dbt tests, 48 Great Expectations checks, and revenue reconciled to the cent | `dbt build`, `atlas quality` |

## Quick start

Requirements: Docker, [uv](https://github.com/astral-sh/uv), Java 11+ (Spark 3.5), and about 8 GB of free RAM.

```bash
make setup                # uv sync (+ pre-commit)
make up                   # postgres, minio, kafka (KRaft), schema-registry, kafka-ui
make init                 # bucket, source schema, Avro contracts, topics, Iceberg tables
make demo                 # simulate → relay → stream → gold → reconcile
```

Then explore:

| UI | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| MinIO console | http://localhost:9001 (atlas / atlas-secret-key) |
| Grafana (`--profile monitoring`) | http://localhost:3000 |
| Marquez lineage (`--profile lineage`) | http://localhost:3001 |
| Airflow (`--profile orchestration`) | http://localhost:8085 |

## The `atlas` CLI

```bash
atlas status                                   # every layer on one screen
atlas simulate --orders 70000                  # seeded historical world → Postgres + outbox
atlas workload --rate 200 --duration 300       # live activity
atlas relay --follow --duplicate-p 0.02 --late-p 0.01 --invalid-p 0.003 --corrupt-p 0.001
atlas stream                                   # Kafka → Bronze / DLQ / Silver (or --available-now)
atlas silver rebuild --from 2026-01-01 --to 2026-01-31
atlas backfill --dataset orders --from 2026-01-01 --to 2026-01-31
atlas replay-dlq --topic orders --corrections fixes.jsonl
atlas gold build [--full-refresh]
atlas reconcile [--date 2026-01-15]
atlas quality
atlas compact --table silver.orders
atlas iceberg maintain
atlas publish --target postgres|iceberg|snowflake|all
atlas lineage --table mart_daily_sales
atlas benchmark --scale small|medium|large
```

## Data

The source behaves like an operational system rather than a static file. The real
[UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) dataset (Chen 2019,
CC BY 4.0; about 1.07M transaction lines) supplies the product catalogue, prices, and the basket-size,
quantity, hour-of-day, weekday and country distributions. A seeded generator expands that into
customers who move cities, products whose prices change, and full order lifecycles: payment retries,
cancellations, shipments, deliveries and inventory reservations with restocks. The same seed always
produces byte-identical events.

## Repository map

```
simulator/      UCI-seeded world + event generator, Postgres sink, live workload
contracts/      Avro schemas (v1, v2, a deliberately breaking v3), registry client, serde, validation rules
producers/      Kafka topics, Avro producer, chaos injection
ingestion/      cdc/ outbox relay · streaming/ DLQ replay · batch/ stores extract, backfill, publish
spark/          common/ decode, transforms, idempotent processor · streaming/ · batch/ (rebuild, maintenance)
lakehouse/      PyIceberg catalog, DuckDB plugin for dbt, state fingerprints
dbt/            staging → intermediate → marts (SCD2 dims, facts, incremental marts) + tests
quality/        Great Expectations suites      reconciliation/  event-level Source→Gold reconciliation
lineage/        OpenLineage emitter, Marquez lookup, dbt runner
monitoring/     Prometheus metrics, data exporter, Prometheus/Alertmanager/Grafana config
airflow/dags/   daily, backfill and maintenance DAGs
benchmarks/     experiment suites, chaos test, report generator     evaluation/  measured JSON
infrastructure/ Terraform (AWS: VPC, S3+KMS, RDS, ECR, ECS Fargate, CloudWatch, IAM)
snowflake/      RBAC, masking policies, tables, benchmark queries
tests/          unit · contracts · integration (live stack) · failure
```

## Honest scope notes

* Snowflake publishing is implemented, and unit-tested against a fake connector. It was **not** run
  against a live account, because no credentials were available.
* AWS infrastructure is complete Terraform that passes `validate`, but it was **not applied**,
  to avoid cloud spend. All results are from the local stack. The host is listed in every JSON file.
* The scale ladder stopped where this machine's disk and RAM said so. See `PROJECT_SPEC.md §7–8`.
