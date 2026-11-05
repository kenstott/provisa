# Copyright (c) 2026 Kenneth Stott
# Canary: cf90da6e-203a-4c08-aa67-1ccb123d3615
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a table on the debezium change signal is kept current by its Debezium stream (REQ-261,
REQ-824, REQ-929).

A pg-engine server reads a PostgreSQL table of the test stack, captured by a Debezium connector
into Kafka. The source declares its CDC transport and the debezium signal; the table is
registered through the admin API. Its first read builds the replica. After that, rows inserted
into and deleted from the source reach the replica through the Debezium stream: reads over
/data/sql see them, and the replica is never built again (a read never rebuilds a table its
change stream keeps current).

Before this, nothing received a debezium signal on a non-Kafka source: the replica stayed as its
first build forever, yet was counted fresh.
"""

# Requirements: REQ-261, REQ-824, REQ-929

from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_pg_engine_landing_never_writes_source_e2e import _SourceAndEngine

pytestmark = [pytest.mark.integration, pytest.mark.requires_debezium]

_NAME = "debezium_landing"
_ROLE = "org_admin"
_SUFFIX = uuid.uuid4().hex[:8]
_TABLE = f"cdc_orders_{_SUFFIX}"
_PREFIX = f"provisa_land_{_SUFFIX}"
_CONNECTOR = f"provisa-land-{_SUFFIX}"
_DEBEZIUM = f"http://{os.environ.get('DEBEZIUM_HOST', 'localhost')}:{os.environ.get('DEBEZIUM_PORT', '8083')}"


def _source_dsn() -> str:
    """The test stack's Postgres, as this process reaches it."""
    return (
        f"postgresql://{os.environ.get('PG_USER', 'provisa')}:"
        f"{os.environ.get('PG_PASSWORD', 'provisa')}@{os.environ.get('PG_HOST', 'localhost')}:"
        f"{os.environ.get('PG_PORT', '5432')}/{os.environ.get('PG_DATABASE', 'provisa')}"
    )


def _source(sql: str) -> None:
    with psycopg.connect(_source_dsn(), autocommit=True) as conn:
        conn.execute(sql)  # pyright: ignore[reportCallIssue, reportArgumentType] -- composed DDL


def _register_connector() -> None:
    """Capture the table: Connect reaches Postgres by its service name on the stack's network."""
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.environ.get("DEBEZIUM_PG_HOST", "postgres"),
        "database.port": os.environ.get("DEBEZIUM_PG_PORT", "5432"),
        "database.user": os.environ.get("PG_USER", "provisa"),
        "database.password": os.environ.get("PG_PASSWORD", "provisa"),
        "database.dbname": os.environ.get("PG_DATABASE", "provisa"),
        "topic.prefix": _PREFIX,
        "table.include.list": f"public.{_TABLE}",
        "plugin.name": "pgoutput",
        "slot.name": f"land_{_SUFFIX}",
        "publication.name": f"land_pub_{_SUFFIX}",
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "always",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
    }
    r = httpx.post(f"{_DEBEZIUM}/connectors", json={"name": _CONNECTOR, "config": config})
    assert r.status_code in (200, 201), r.text
    deadline = time.monotonic() + 120
    while True:
        status = httpx.get(f"{_DEBEZIUM}/connectors/{_CONNECTOR}/status").json()
        tasks = status.get("tasks") or []
        if status.get("connector", {}).get("state") == "RUNNING" and tasks:
            if tasks[0]["state"] == "RUNNING":
                return
        assert time.monotonic() < deadline, f"connector not running: {status}"
        time.sleep(1)


def _drop_connector() -> None:
    httpx.put(f"{_DEBEZIUM}/connectors/{_CONNECTOR}/stop")
    httpx.delete(f"{_DEBEZIUM}/connectors/{_CONNECTOR}")
    deadline = time.monotonic() + 60
    while True:
        try:
            _source(f"SELECT pg_drop_replication_slot('land_{_SUFFIX}')")
            break
        except psycopg.errors.ObjectInUse:
            assert time.monotonic() < deadline, "the connector kept its replication slot"
            time.sleep(1)
        except psycopg.errors.UndefinedObject:
            break
    _source(f"DROP PUBLICATION IF EXISTS land_pub_{_SUFFIX}")


def _config() -> dict:
    return {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [],
        "sources": [],
        "tables": [],
    }


@pytest.fixture
def server():
    from tests.integration.isolated_server import IsolatedServer

    _source(f"CREATE TABLE {_TABLE} (id integer PRIMARY KEY, name text)")
    _source(f"INSERT INTO {_TABLE} VALUES (1, 'first')")
    _register_connector()
    engine = _SourceAndEngine()
    engine.start()
    try:
        with tempfile.TemporaryDirectory() as workdir:
            path = Path(workdir) / "config.yaml"
            path.write_text(yaml.safe_dump(_config()))
            control_plane = engine.url(engine.engine_port, "provisa", "+psycopg")
            srv = IsolatedServer(
                _NAME,
                engine="pg",
                config=str(path),
                control_plane="postgres",
                env={
                    "PLATFORM_DATABASE_URL": control_plane,
                    "TENANT_DATABASE_URL": control_plane,
                    "PROVISA_MATERIALIZE_URL": engine.url(engine.engine_port, "provisa"),
                    "PROVISA_REDIS_EMBEDDED": "1",
                },
            )
            try:
                srv.start()
                yield srv
            finally:
                srv.stop_process()
    finally:
        engine.stop()
        _drop_connector()
        _source(f"DROP TABLE IF EXISTS {_TABLE}")


def _mutate(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _builds(srv) -> list[dict]:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "{ replicaBuilds { builds { tableName state completedAt lastError } } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    return [
        b for b in response.json()["data"]["replicaBuilds"]["builds"] if b["tableName"] == _TABLE
    ]


def _read(srv) -> list[tuple[int, str]]:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": f"SELECT id, name FROM {_TABLE} ORDER BY id", "role": _ROLE},
        timeout=120,
    )
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-6000:]}"
    body = response.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return [(int(r["id"]), r["name"]) for r in rows]


def _until(srv, expected: list[tuple[int, str]], what: str) -> None:
    deadline = time.monotonic() + 90
    while (seen := _read(srv)) != expected:
        assert time.monotonic() < deadline, (
            f"{what}: read {seen}, builds {_builds(srv)}\n{srv.dump_stderr_debug()[-6000:]}"
        )
        time.sleep(1)


def test_changes_reach_the_replica_through_the_debezium_stream(server):
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    _mutate(
        server,
        'createSource(input: {id: "shop_pg", type: "postgresql", '
        f'host: "{os.environ.get("PG_HOST", "localhost")}", port: {os.environ.get("PG_PORT", "5432")}, '
        f'database: "{os.environ.get("PG_DATABASE", "provisa")}", '
        f'username: "{os.environ.get("PG_USER", "provisa")}", '
        f'password: "{os.environ.get("PG_PASSWORD", "provisa")}", changeSignal: "debezium", '
        # Served from its replica: the pg engine otherwise reads a PostgreSQL source in place,
        # through postgres_fdw, and keeps no replica for the stream to keep current (REQ-826).
        "replicate: 0, "
        f'cdc: {{bootstrapServers: "{bootstrap}", topicPrefix: "{_PREFIX}"}}}})',
    )
    registered = _mutate(
        server,
        'registerTable(input: {sourceId: "shop_pg", domainId: "shop", schemaName: "public", '
        f'tableName: "{_TABLE}", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "name", visibleTo: ["org_admin"], dataType: "varchar"}]})',
    )
    _mutate(server, release_field(registered_id(registered["message"])))

    assert _read(server) == [(1, "first")]  # the first read builds the replica
    (built,) = _builds(server)

    _source(f"INSERT INTO {_TABLE} VALUES (2, 'second')")
    _until(server, [(1, "first"), (2, "second")], "an inserted row never reached the replica")
    _source(f"DELETE FROM {_TABLE} WHERE id = 1")
    _until(server, [(2, "second")], "a deleted row stayed in the replica")

    (after,) = _builds(server)
    assert after["completedAt"] == built["completedAt"], "a read rebuilt the replica"
