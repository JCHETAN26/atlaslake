"""Integration fixtures: require the docker-compose stack (skipped automatically when it is down).

WARNING: these tests reset the local platform (source DB, Kafka topics, lake tables).
"""

from __future__ import annotations

import socket

import pytest

from core.config import settings


def _up(hostport: str) -> bool:
    host, port = hostport.rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            return True
    except OSError:
        return False


STACK_UP = all(_up(hp) for hp in (settings().kafka.bootstrap, f"{settings().pg.host}:{settings().pg.port}",
                                  settings().s3.endpoint_host,
                                  settings().kafka.schema_registry_url.split("://")[1]))


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/integration" in str(item.fspath) or "tests/failure" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
            if not STACK_UP:
                item.add_marker(pytest.mark.skip(reason="docker-compose stack not running"))


@pytest.fixture(scope="session")
def spark():
    from spark.common.session import get_spark

    s = get_spark("atlas-tests")
    yield s


@pytest.fixture
def clean_platform(spark):
    from core.platform import init_all, reset_gold, reset_kafka, reset_lake, reset_source

    init_all()
    reset_source()
    reset_kafka()
    reset_lake(spark)
    reset_gold()
    yield spark


@pytest.fixture
def producer():
    from producers.kafka_producer import EventProducer
    from simulator.postgres_sink import connect

    conn = connect()
    p = EventProducer(delivery_sink=conn, run_id="test")
    yield p
    p.flush()
    conn.close()
