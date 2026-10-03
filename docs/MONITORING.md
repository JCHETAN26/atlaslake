# Monitoring & alerting (Phase 30)

AtlasLake watches **infrastructure** (Kafka, Spark, jobs) and **correctness** (duplicates, DLQ,
quality, reconciliation) through one Prometheus → Alertmanager → Grafana path.

## Start

```bash
docker compose --profile monitoring up -d          # prometheus, alertmanager, pushgateway, kafka-exporter, grafana
python -m monitoring.exporter --port 9110 --interval 30 --webhook-port 9111   # data metrics + alert webhook
atlas stream ...                                   # streaming job serves :9108/metrics
```

Grafana: http://localhost:3000 (anonymous viewer; admin / `atlas`) → folder **AtlasLake**.
Prometheus: http://localhost:9090 · Alertmanager: http://localhost:9093.

| Port | Process | Metrics |
|---|---|---|
| 9090 | Prometheus | — |
| 9091 | Pushgateway | `atlas_job_*` pushed by batch jobs (`monitoring.metrics.push_job_metrics`) |
| 9093 | Alertmanager | — |
| 9108 | Spark streaming driver (host) | `atlas_stream_*` |
| 9109 | Outbox relay (host) | `atlas_relay_*` |
| 9110 | Data exporter (host) | `atlas_data_*`, `atlas_relay_backlog`, `atlas_exporter_*` |
| 9111 | Alert webhook receiver (host) | appends every Alertmanager notification to `data/alerts.jsonl` |
| 9308 | kafka-exporter | `kafka_topic_*`, `kafka_consumergroup_*` |

Host processes are scraped through `host.docker.internal` (mapped to the host gateway in compose).

## Dashboards

| Dashboard | Shows |
|---|---|
| **AtlasLake · Kafka** | events/sec per topic (offset rate), relay events/sec, Spark consumer lag, consumer-group lag per partition, relay backlog, message totals |
| **AtlasLake · Spark Streaming** | records/sec, micro-batch duration p50/p95, last batch id, failed batches, seconds since last progress, processing lag, watermark lag, late events beyond the watermark |
| **AtlasLake · Pipeline** | last-run success per job, job runtimes, time since last run, Silver freshness per table, Gold freshness, retries, exporter source health |
| **AtlasLake · Data correctness** | reconciliation passed/unexplained, quality failures, logical duplicates *leaked* vs duplicates *detected*, accepted events/sec, DLQ pending/by status, rejections by reason, Silver row counts |

## Data exporter

`monitoring/exporter.py` computes correctness facts every `--interval` seconds:

| Metric | Source |
|---|---|
| `atlas_data_dlq_pending{topic}`, `atlas_data_dlq_total{topic,status}` | `ops.dlq` (Iceberg, via DuckDB) |
| `atlas_data_silver_rows{table}`, `atlas_data_silver_freshness_seconds{table}` | Silver tables, `max(silver_updated_at / silver_loaded_at)` (processing time) |
| `atlas_data_gold_freshness_seconds` | `max(dbt_loaded_at)` of `gold.fact_orders` |
| `atlas_data_logical_duplicates{table}` | `count(*) - count(distinct key)` on silver.orders / payments / events |
| `atlas_data_reconciliation_passed`, `..._unexplained` | `evaluation/reconciliation.json` summary |
| `atlas_data_quality_failed` | `evaluation/data_quality.json` summary |
| `atlas_relay_backlog` | outbox rows after the relay checkpoint |
| `atlas_exporter_source_up{source}` | 1/0 per source — one failing source never stops the others |

Report-derived gauges are only exported once a report exists, so a fresh platform does not raise a
false `ReconciliationMismatch`.

## Alert catalog

Rules: `monitoring/prometheus/alerts.yml` (unit-tested in `alerts_test.yml`).

| Alert | Expression (abridged) | For | Severity | Runbook |
|---|---|---|---|---|
| StreamingQueryStalled | `time() - atlas_stream_last_progress_timestamp > 300` | 2m | critical | Check the Spark driver log; restart `atlas stream` (checkpoint makes the restart safe). |
| StreamingTargetDown | `up{job="atlas-streaming"} == 0` | 5m | warning | Streaming driver not running or not serving :9108. |
| ConsumerLagGrowing | `deriv(atlas_stream_consumer_lag[10m]) > 0 and lag > 10k` | 10m | warning | Ingest > processing: raise `maxOffsetsPerTrigger`/resources, check batch duration. |
| KafkaConsumerGroupLagHigh | `sum(kafka_consumergroup_lag) > 100k` | 10m | warning | Same as above for group-based consumers. |
| StreamingBatchFailures | `increase(atlas_stream_failed_batches_total[10m]) > 0` | 0m | critical | Read the exception; fix and restart (batch is retried from checkpoint). |
| DLQRateSpike | rejected / input rate > 1% | 5m | warning | Inspect `ops.dlq.error_codes`; fix producer or correct + `atlas replay-dlq`. |
| DLQBacklog | `sum(atlas_data_dlq_pending) > 1000` | 15m | warning | Triage pending DLQ records. |
| ReconciliationMismatch | `unexplained > 0 or passed == 0` | 0m | critical | Open `evaluation/reconciliation.json`; locate the layer where counts diverge. |
| FreshnessBreach | Silver > 1h or Gold > 24h stale | 10m | warning | Check streaming job / Airflow DAG runs. |
| QualityCheckFailed | `atlas_data_quality_failed > 0` | 0m | critical | `atlas quality`; see `evaluation/data_quality.json`. |
| DuplicateLeak | `atlas_data_logical_duplicates > 0` | 0m | critical | Guarantee C2 violated — stop publishing Gold, find the offending batch in `ops.stream_batches`. |
| RelayBacklogHigh | `atlas_relay_backlog > 100k` | 10m | warning | Relay down or slow: check `atlas relay --follow`. |
| JobFailed | `atlas_job_success == 0` | 0m | critical | Check the Airflow task log for that job. |

## Alert routing

`monitoring/alertmanager/alertmanager.yml` routes by `severity` (critical: 5s group wait, 1h repeat;
warning: defaults) to a single webhook receiver, `http://host.docker.internal:9111/alerts`, served
by `python -m monitoring.exporter --webhook-port 9111`. Each notification is appended to
`data/alerts.jsonl`, which the chaos test reads to show which alerts fired. Critical alerts
inhibit warnings with the same `alertname`. For real paging, add a Slack or PagerDuty receiver to
the critical route.

## Validation

```bash
docker run --rm -v "$PWD/monitoring/prometheus":/p --entrypoint promtool prom/prometheus:v3.5.0 check config /p/prometheus.yml
docker run --rm -v "$PWD/monitoring/prometheus":/p -w /p --entrypoint promtool prom/prometheus:v3.5.0 test rules alerts_test.yml
docker run --rm -v "$PWD/monitoring/alertmanager":/a --entrypoint amtool prom/alertmanager:v0.28.1 check-config /a/alertmanager.yml
```
