"""Platform-wide setup / teardown used by the CLI and the integration tests."""

from __future__ import annotations

import shutil
import time

from core.config import settings
from core.storage import ensure_bucket


def init_all(partitions: int | None = None) -> dict:
    from contracts.registry import SchemaRegistry, register_all
    from producers.topics import ensure_topics
    from simulator.postgres_sink import connect, init_schema

    ensure_bucket()
    with connect() as conn:
        init_schema(conn)
    reg = SchemaRegistry()
    reg.set_compatibility(None, "BACKWARD_TRANSITIVE")
    ids = register_all(reg)
    created = ensure_topics(partitions)
    return {"schemas": ids, "topics_created": created}


def reset_source() -> None:
    from simulator.postgres_sink import connect, init_schema
    from simulator.postgres_sink import reset_source as _reset

    with connect() as conn:
        init_schema(conn)
        _reset(conn)


def reset_kafka(partitions: int | None = None) -> None:
    from producers.topics import delete_topics, ensure_topics

    delete_topics()
    for _ in range(30):  # topic deletion is asynchronous
        try:
            ensure_topics(partitions)
            return
        except Exception:
            time.sleep(1)
    ensure_topics(partitions)


def reset_lake(spark=None) -> None:
    from simulator.postgres_sink import connect
    from spark.common.session import get_spark
    from spark.common.tables import create_all, drop_all

    spark = spark or get_spark("atlas-reset")
    drop_all(spark)
    with connect() as conn:  # batch high-water marks describe lake state: reset them with it
        conn.execute("DELETE FROM ops.batch_watermarks")
    shutil.rmtree(settings().checkpoint_dir, ignore_errors=True)
    create_all(spark)


def reset_gold() -> None:
    for suffix in ("", ".wal"):
        p = settings().gold_duckdb.with_name(settings().gold_duckdb.name + suffix)
        p.unlink(missing_ok=True)


def reset_everything(spark=None) -> None:
    reset_source()
    reset_kafka()
    reset_lake(spark)
    reset_gold()
