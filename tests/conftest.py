# Copyright (c) 2026 Kenneth Stott
# Canary: be5aefb1-047c-45bf-bbd3-3d7280b5f906
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import os
import socket

import asyncpg
import pytest
import pytest_asyncio
import trino


def _server_reachable(url: str) -> bool:
    import urllib.request
    try:
        urllib.request.urlopen(f"{url}/health", timeout=3)
        return True
    except Exception:
        return False


def _tcp_reachable(host: str, port: int) -> bool:
    import socket
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def _pgbouncer_auth_ok(host: str, port: int) -> bool:
    import asyncio

    async def _try():
        try:
            conn = await asyncpg.connect(
                host=host,
                port=port,
                user=os.environ.get("PG_USER", "provisa"),
                password=os.environ.get("PG_PASSWORD", "provisa"),
                database=os.environ.get("PG_DATABASE", "provisa"),
                timeout=5,
            )
            await conn.close()
            return True
        except Exception:
            return False

    return asyncio.run(_try())


def _trino_catalog_exists(catalog: str) -> bool:
    import trino
    try:
        conn = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "localhost"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user="test",
        )
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        cur.fetchone()
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    server_url = os.environ.get("PROVISA_URL", "http://localhost:8000")
    kafka_host = os.environ.get("KAFKA_HOST", "localhost")
    kafka_port = int(os.environ.get("KAFKA_PORT", "9092"))
    debezium_host = os.environ.get("DEBEZIUM_HOST", "localhost")
    debezium_port = int(os.environ.get("DEBEZIUM_PORT", "8083"))
    mongo_host = os.environ.get("MONGODB_HOST", "localhost")
    mongo_port = int(os.environ.get("MONGODB_PORT", "27017"))
    es_host = os.environ.get("ELASTICSEARCH_HOST", "localhost")
    es_port = int(os.environ.get("ELASTICSEARCH_PORT", "9200"))
    neo4j_host = os.environ.get("NEO4J_HOST", "localhost")
    neo4j_port = int(os.environ.get("NEO4J_PORT", "7687"))
    sparql_host = os.environ.get("SPARQL_HOST", "localhost")
    sparql_port = int(os.environ.get("SPARQL_PORT", "3030"))
    pgbouncer_host = os.environ.get("PGBOUNCER_HOST", "localhost")
    pgbouncer_port = int(os.environ.get("PGBOUNCER_PORT", "6432"))

    _checked: dict[str, bool] = {}

    def _check(key: str, fn) -> bool:
        if key not in _checked:
            _checked[key] = fn()
        return _checked[key]

    for item in items:
        if item.get_closest_marker("requires_provisa_server"):
            if not _check(server_url, lambda: _server_reachable(server_url)):
                item.add_marker(pytest.mark.skip(reason=f"Provisa server not reachable at {server_url}"))
        if item.get_closest_marker("requires_kafka"):
            if not _check("kafka", lambda: _tcp_reachable(kafka_host, kafka_port)):
                item.add_marker(pytest.mark.skip(reason=f"Kafka not reachable at {kafka_host}:{kafka_port}"))
        if item.get_closest_marker("requires_debezium"):
            if not _check("debezium", lambda: _tcp_reachable(debezium_host, debezium_port)):
                item.add_marker(pytest.mark.skip(reason=f"Debezium not reachable at {debezium_host}:{debezium_port}"))
        if item.get_closest_marker("requires_mongodb"):
            if not _check("mongodb", lambda: _tcp_reachable(mongo_host, mongo_port)):
                item.add_marker(pytest.mark.skip(reason=f"MongoDB not reachable at {mongo_host}:{mongo_port}"))
        if item.get_closest_marker("requires_elasticsearch"):
            if not _check("elasticsearch", lambda: _tcp_reachable(es_host, es_port)):
                item.add_marker(pytest.mark.skip(reason=f"Elasticsearch not reachable at {es_host}:{es_port}"))
        if item.get_closest_marker("requires_neo4j"):
            if not _check("neo4j", lambda: _tcp_reachable(neo4j_host, neo4j_port)):
                item.add_marker(pytest.mark.skip(reason=f"Neo4j not reachable at {neo4j_host}:{neo4j_port}"))
        if item.get_closest_marker("requires_sparql"):
            if not _check("sparql", lambda: _tcp_reachable(sparql_host, sparql_port)):
                item.add_marker(pytest.mark.skip(reason=f"SPARQL endpoint not reachable at {sparql_host}:{sparql_port}"))
        if item.get_closest_marker("requires_pgbouncer"):
            if not _check("pgbouncer", lambda: _pgbouncer_auth_ok(pgbouncer_host, pgbouncer_port)):
                item.add_marker(pytest.mark.skip(reason=f"PgBouncer auth failed at {pgbouncer_host}:{pgbouncer_port}"))


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session", autouse=True)
def _reserve_flight_port():
    """Allocate a free port for the Arrow Flight server before any test starts.

    Multiple integration tests spin up an in-process FastAPI app via
    ASGITransport. Each app instance tries to bind the Arrow Flight gRPC
    server on FLIGHT_PORT (default 8815). If port 8815 is already in use
    (e.g. by a previous test run's zombie process or the live server) the
    lifespan fails and every request returns 400. Setting a random free port
    here ensures every in-process app gets a usable socket.
    """
    port = _free_port()
    os.environ.setdefault("FLIGHT_PORT", str(port))
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    # Limit pool size per in-process app so concurrent module fixtures
    # don't exhaust PostgreSQL max_connections.
    os.environ.setdefault("PG_POOL_MIN", "1")
    os.environ.setdefault("PG_POOL_MAX", "3")


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    return (
        f"postgresql://{os.environ.get('PG_USER', 'provisa')}"
        f":{os.environ.get('PG_PASSWORD', 'provisa')}"
        f"@{os.environ.get('PG_HOST', 'localhost')}"
        f":{os.environ.get('PG_PORT', '5432')}"
        f"/{os.environ.get('PG_DATABASE', 'provisa')}"
    )


@pytest_asyncio.fixture(scope="session")
async def pg_pool(pg_dsn):
    pool = await asyncpg.create_pool(
        pg_dsn, min_size=1, max_size=5,
        command_timeout=30,
    )
    yield pool
    await pool.close()


@pytest.fixture(scope="session")
def trino_conn():
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test",
        catalog="sales_pg",
        schema="public",
    )
    yield conn
    conn.close()


@pytest.fixture
def sample_config():
    import yaml
    from pathlib import Path

    config_path = Path(__file__).parent / "fixtures" / "sample_config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
