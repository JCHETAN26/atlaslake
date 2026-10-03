# Orchestration (Phase 24)

Airflow runs the **batch / control plane**. The always-on Spark streaming job keeps Silver fresh;
Airflow re-derives, validates, models, publishes and *proves* each day.

```
make images                                               # builds atlaslake/airflow:local
docker compose --profile orchestration up -d airflow      # UI: http://localhost:8085
```

Image builds use `--network host` because the dev host's Docker daemon runs with `iptables: false`
(bridge containers have no outbound internet, so `apt-get`/`pip` would hang inside `docker compose build`).

The image (`docker/airflow/Dockerfile`) keeps Airflow and AtlasLake in **separate Python
environments**: AtlasLake pins pyspark/pandas/dbt versions that conflict with Airflow's constraints.
Tasks call `atlas …`, a wrapper that runs the repo (mounted at `/opt/atlaslake`) on
`/opt/atlas-venv`, with JDK 17 for Spark.

## DAGs

### `atlaslake_daily` — `0 2 * * *`

```
check_sources ─► ingest_batch ─► validate_bronze ─► spark_transform ─► quality_check
   ─► dbt_build ─► dbt_test ─► snowflake_configured ─► snowflake_publish ─► reconcile
```

| task | command | purpose |
|---|---|---|
| check_sources | registry ping + `atlas status` | fail fast if a dependency is down |
| ingest_batch | `atlas stream --available-now` + `atlas silver ingest-stores` | drain Kafka with the *same* streaming code (Trigger.AvailableNow); JDBC-extract stores |
| validate_bronze | `atlas quality` | Bronze/Silver technical checks |
| spark_transform | `atlas silver rebuild --from {{ ds }} --to {{ ds }}` | idempotent re-derivation of the day from Bronze |
| quality_check | `atlas quality` | technical + business checks before modelling |
| dbt_build | `atlas gold build` | incremental Gold models + tests |
| dbt_test | `atlas gold build --select test_type:singular` | business invariants (revenue reconciles, inventory = Σ movements, …) |
| snowflake_configured | ShortCircuit on `ATLAS_SNOWFLAKE_ACCOUNT` | skip publishing cleanly when no credentials |
| snowflake_publish | `atlas publish --target snowflake` | serve Gold marts |
| reconcile | `atlas reconcile --date {{ ds }}` | source → Bronze → Silver → Gold proof; `trigger_rule=none_failed` so it runs even when publish is skipped |

Params: `dataset` (`all` or one dataset; narrows `spark_transform`) and `scale`.

### `atlaslake_backfill` — manual

```
backfill (atlas backfill --dataset D --from F --to T --no-gold) ─► dbt_build ─► reconcile
```

Trigger with `{"dataset": "orders", "date_from": "2026-01-01", "date_to": "2026-01-31"}`.

### `atlaslake_maintenance` — `0 4 * * 0`

`atlas iceberg maintain`: compaction, snapshot expiration, orphan-file and metadata cleanup.

## Policies

| Policy | Setting | Why |
|---|---|---|
| Retries | 2 per task, 5 min base delay, exponential backoff capped at 30 min | absorbs transient broker/registry/S3 blips; every task is idempotent so a retry is always safe |
| Timeouts | `execution_timeout` per task (10–600 min), `dagrun_timeout` per DAG | a hung Spark job must fail loudly, not block the next day |
| Concurrency | `max_active_runs=1` | runs touch the same Iceberg tables; serialising avoids commit contention |
| Alerts | `on_failure_callback` logs at ERROR and POSTs JSON to `ATLAS_ALERT_WEBHOOK` (default `http://host.docker.internal:9111/alerts`) | failures surface in Alertmanager/Grafana as well as the Airflow UI; the callback never raises |
| Catch-up | `catchup=False` everywhere | history is rebuilt with the explicit backfill DAG over an event-date range, not by N silent scheduled runs |
| Backfills | range-based, idempotent | the Silver processor anti-joins the event ledger and its MERGEs are commutative, so re-running a range converges to the same state (no double counting) |
| SLAs | `dagrun_timeout` + freshness alerts in Prometheus | Airflow 3 removed task SLAs; freshness is enforced by monitoring (docs/MONITORING.md) |

## Testing

`tests/unit/test_airflow_dags.py` (DagBag import, exact Phase-24 order, retry/timeout/callback
defaults, backfill params) runs inside the Airflow image. CI does this in the `docker-build` job:

```
docker run --rm -v "$PWD":/opt/atlaslake -e PYTHONPATH=/opt/atlaslake atlaslake/airflow:local \
  python -m pytest /opt/atlaslake/tests/unit/test_airflow_dags.py -q -p no:cacheprovider
```

## Verified run

`atlaslake_daily` was executed end-to-end inside the `atlaslake/airflow` image against the live stack
(`airflow dags test atlaslake_daily 2026-01-15`) — result in `evaluation/airflow_daily_run.json` and
[RESULTS.md](RESULTS.md). On hosts whose Docker daemon gives containers no internet access, the compose
service mounts the host's Spark jar cache (`~/.ivy2` → `ATLAS_SPARK_IVY_DIR`) and DuckDB extension cache
(`~/.duckdb/extensions` → `ATLAS_DUCKDB_EXTENSION_DIR`), and runs as uid 1000 so it can write to the
repo-mounted data directory.
