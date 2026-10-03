# AtlasLake runbook

Each section starts from a symptom: an alert or a question an operator asks. The rule for every
action here: **all Silver writes are idempotent**, so re-running a step is always safe. When in doubt,
re-run and reconcile.

## First look

```bash
atlas status          # volumes and freshness per layer, relay backlog, DLQ pending, last checks
atlas reconcile       # exit 1 = some source event is unexplained (or financials differ)
```
Grafana → *AtlasLake / Data* shows duplicates, the DLQ, quality and reconciliation in one place.

## `ConsumerLagGrowing` / `StreamingQueryStalled`

1. Is the stream process alive? `pgrep -af "cli.main stream"`. If not, restart it with `atlas stream`.
   It resumes from its checkpoint (`data/checkpoints/silver_ingest`).
2. Is it alive but slow? Check `atlas_stream_batch_duration_seconds` and the per-stage timers in the
   log (`batch N: ...`). Raising `--max-offsets` amortises the fixed per-batch cost: throughput is
   strongly batch-size dependent (see `evaluation/spark_performance.json`).
3. Never delete a checkpoint to "unstick" a query. The writes are idempotent, so a replay is safe,
   but deleting the checkpoint re-reads from `earliest` and wastes hours.

## `DLQRateSpike`

```bash
atlas status                                   # dlq_pending
# inspect reasons:
python -c "import duckdb; from lakehouse.duckdb_plugin import configure; c=duckdb.connect(); configure(c); \
print(c.sql(\"select topic, error_codes, count(*) from ops.dlq where status='PENDING' group by all order by 3 desc\"))"
```
* **`CORRUPT_PAYLOAD`, `AVRO_DECODE_ERROR` or `UNKNOWN_SCHEMA_ID`** is a producer or transport
  problem. The authoritative copy is in the source outbox: run `atlas replay-dlq --topic X` (the
  default, `--from-source`, recovers it).
* **A semantic rule (`NEGATIVE_AMOUNT`, `INVALID_CURRENCY`, …)** means the data is wrong. Write
  corrections to a JSONL file, one per line, as
  `{"event_id": "...", "set": {"currency": "GBP"}}` or `{"event_id": "...", "action": "discard",
  "reason": "..."}`, then run `atlas replay-dlq --topic X --corrections fixes.jsonl`.
  Corrections may not change `event_id` or `event_timestamp`.
* **A validation rule changed** (records are now acceptable): run
  `atlas silver rebuild --from D1 --to D2`. Matching DLQ rows become `REPROCESSED`.

Replayed records go back through Kafka and the normal validation path, and are deduplicated by
`event_id`. Replaying twice is harmless.

## `ReconciliationMismatch`

`evaluation/reconciliation.json` classifies every source event:

| outcome | meaning | action |
|---|---|---|
| `in_flight` | still in the outbox after the relay checkpoint | relay running? `atlas relay --follow` |
| `pending_processing` | in Kafka, not yet in Bronze | stream lag; see above |
| `rejected` | in the DLQ (pending or discarded) | DLQ section |
| `unexplained` | **none of the above: a real bug** | `unexplained_examples` lists ids. Trace with `delivery_log`, Bronze, `silver.events` |
| `unknown_origin` | in Silver but not in the source | a producer bypassing the outbox (tests do this) |

Financial mismatches exclude orders that have a non-accepted event (they are listed as explained).
Any remaining difference is a bug.

## Backfill / re-processing a date range

```bash
atlas backfill --dataset orders --dataset payments --from 2026-01-01 --to 2026-01-31
```
This re-drives Bronze through Silver and Gold. Running it on an already-processed range changes
nothing; this is proven in `evaluation/backfill_benchmark.json`. Ranges are by **event** date.

## Late data questions ("why doesn't the live dashboard match Gold?")

The real-time aggregate excludes events more than 10 minutes behind the watermark, by design. Gold
includes everything. See [LATE_DATA_POLICY.md](LATE_DATA_POLICY.md). The gap is visible as
`atlas_stream_late_events`.

## Schema change request

1. Edit or add `contracts/avro/<topic>.vN.avsc`.
2. `atlas contracts check contracts/avro/....avsc --topic commerce.orders`. Exit 1 means it is
   incompatible with the `BACKWARD_TRANSITIVE` history.
3. Only additive optional fields with defaults pass. For anything else, make a new field or a new
   topic plus a migration, never an in-place type change.
4. `atlas contracts register`, then roll consumers first (they read with the latest reader schema),
   then producers.

## Small files / slow lake queries

```bash
atlas iceberg maintain            # compaction where needed + snapshot expiry + orphan cleanup
atlas compact --table silver.events
```
The retention policy is in `spark/batch/maintenance.py::RETENTION`. Time travel is guaranteed for
7 days on Silver and 30 days on Bronze.

## Disaster: Silver lost or corrupted

Bronze is the system of record:
```bash
atlas silver rebuild                 # or atlas backfill per range
atlas gold build --full-refresh
atlas reconcile
```
The rebuilt state is fingerprint-identical to the streaming-built state
(`evaluation/backfill_benchmark.json → matches_streaming_state`).
