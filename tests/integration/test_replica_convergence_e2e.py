# Copyright (c) 2026 Kenneth Stott
# Canary: 1778bdec-88ea-497d-bd78-8f6239fe3f62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration: replicas converge to the declared model (REQ-1915, REQ-1919).

A real pg-engine server (and a second one on the same control plane) over a SOURCE Postgres and
an ENGINE Postgres. A source and a table are created through the admin API and set to replicate.
Nothing reads the table and nothing asks for a build:

* the replica is built because the model declares it, by the build runner, and the admin build
  query shows how;
* setting the source to Never retires the replica — it stands for the grace period — and then
  drops it; setting it back builds it again;
* deleting the table drops its replica the same way;
* two servers that both see the declaration build the replica once between them.
"""

# Requirements: REQ-1915, REQ-1919, REQ-826

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.integration.test_pg_engine_landing_never_writes_source_e2e import (
    _ROLE,
    _ROWS,
    _SourceAndEngine,
)

pytestmark = [pytest.mark.integration]

_NAME = "replica_convergence"
_REPLICA = f'"org_{_NAME}_replicas"."src__public__orders"'
# The drop's grace: twice the reload interval plus the longest request timeout.
_RELOAD_S, _TIMEOUT_S = 0.5, 3
_GRACE_S = 2 * _RELOAD_S + _TIMEOUT_S


def _config() -> dict:
    return {
        "federation_engine": "pg",
        "server": {
            "config_reload_interval": _RELOAD_S,
            "limits": {
                "request_timeout": _TIMEOUT_S,
                "request_timeouts": {"flight": _TIMEOUT_S, "pgwire": _TIMEOUT_S},
            },
        },
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [
            {
                "id": _ROLE,
                "capabilities": [
                    "source_registration",
                    "table_registration",
                    "query_development",
                    "access_config",
                    "observability",
                ],
                "domain_access": ["*"],
            }
        ],
        "sources": [],
        "tables": [],
    }


@pytest.fixture
def databases():
    pg = _SourceAndEngine()
    pg.start()
    try:
        yield pg
    finally:
        pg.stop()


def _server(pg: _SourceAndEngine, workdir: str):
    from tests.integration.isolated_server import IsolatedServer

    path = Path(workdir) / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    control_plane = pg.url(pg.engine_port, "provisa", "+psycopg")
    return IsolatedServer(
        _NAME,
        engine="pg",
        config=str(path),
        control_plane="postgres",
        env={
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_MATERIALIZE_URL": pg.url(pg.engine_port, "provisa"),
            "PROVISA_REDIS_EMBEDDED": "1",
        },
    )


def _admin(srv, document: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": document},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return response.json()["data"]


def _mutate(srv, mutation: str) -> None:
    (result,) = _admin(srv, "mutation { " + mutation + " { success message } }").values()
    assert result["success"], result["message"]


def _register(srv, pg: _SourceAndEngine) -> None:
    _mutate(
        srv,
        'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
        f'port: {pg.source_port}, database: "shop", username: "provisa", password: "provisa"}})',
    )
    _mutate(
        srv,
        'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
    )
    # REQ-1907: a replicated table on the ttl change signal needs its refresh clock.
    _mutate(srv, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 3600)')


def _replica_rows(pg: _SourceAndEngine) -> int | None:
    """The replica's row count, or None when the store has no such table."""
    with psycopg.connect(pg.url(pg.engine_port, "provisa"), autocommit=True) as conn:
        if conn.execute("SELECT to_regclass(%s)", (_REPLICA,)).fetchone()[0] is None:
            return None
        return conn.execute(f"SELECT COUNT(*) FROM {_REPLICA}").fetchone()[0]


def _wait(condition, *, seconds: float, what: str) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.25)


def _builds(srv) -> dict:
    return _admin(
        srv,
        "{ replicaBuilds { builds { sourceId schemaName tableName state requestedReason method "
        "loadKind rowsCopied completedAt lastError } convergenceError } }",
    )["replicaBuilds"]


def _full_reads_of_orders(pg: _SourceAndEngine) -> list[str]:
    """Statements the SOURCE executed that read ``orders`` in full."""
    return [
        line.split("db=shop ", 1)[1]
        for line in pg._source_log()
        if "db=shop " in line and "FROM public.orders" in line  # the copy's read, not the catalog's
    ]


def test_a_declared_replica_is_built_retired_and_dropped_with_no_read(databases):
    pg = databases
    with tempfile.TemporaryDirectory() as workdir:
        srv = _server(pg, workdir)
        try:
            srv.start()
            _register(srv, pg)
            assert _replica_rows(pg) in (None, 0)  # declared live: nothing is copied

            # Declared replicated. No statement reads the table, nothing asks for a build.
            _mutate(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')
            _wait(lambda: _replica_rows(pg) == len(_ROWS), seconds=60, what="the replica")
            _wait(
                lambda: _builds(srv)["builds"] and _builds(srv)["builds"][0]["state"] == "idle",
                seconds=30,
                what="the build's completion on the record",
            )
            shown = _builds(srv)
            (build,) = shown["builds"]
            assert (build["sourceId"], build["schemaName"], build["tableName"]) == (
                "src",
                "public",
                "orders",
            )
            assert build["method"] == "engine_statement" and build["loadKind"] == "bulk_stream"
            assert build["rowsCopied"] == len(_ROWS) and build["completedAt"]
            assert build["lastError"] is None and shown["convergenceError"] is None

            # No longer declared: retired first, standing through the grace, then dropped.
            _mutate(srv, 'updateSourceReplicate(sourceId: "src", replicate: -1)')
            changed = time.monotonic()
            _wait(
                lambda: _builds(srv)["builds"] and _builds(srv)["builds"][0]["state"] == "retired",
                seconds=30,
                what="the replica being retired",
            )
            if time.monotonic() - changed < _GRACE_S:
                assert _replica_rows(pg) == len(
                    _ROWS
                )  # it stands while nodes may still route to it
            _wait(lambda: _replica_rows(pg) is None, seconds=60, what="the replica's drop")
            assert time.monotonic() - changed >= _GRACE_S
            _wait(lambda: _builds(srv)["builds"] == [], seconds=30, what="the record's removal")

            # Declared again: built again.
            _mutate(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')
            _wait(lambda: _replica_rows(pg) == len(_ROWS), seconds=60, what="the rebuilt replica")

            # The table is deleted: its replica goes the same way.
            (table,) = [
                t
                for t in _admin(srv, "{ tables { id tableName } }")["tables"]
                if t["tableName"] == "orders"
            ]
            _mutate(srv, f"deleteTable(id: {table['id']})")
            _wait(lambda: _replica_rows(pg) is None, seconds=60, what="the deleted table's drop")
            _wait(lambda: _builds(srv)["builds"] == [], seconds=30, what="the record's removal")
        finally:
            srv.stop_process()


def test_two_servers_that_see_the_declaration_build_the_replica_once(databases):
    pg = databases
    with tempfile.TemporaryDirectory() as dir_a, tempfile.TemporaryDirectory() as dir_b:
        first, second = _server(pg, dir_a), _server(pg, dir_b)
        try:
            first.start()
            second.start()
            _register(first, pg)
            _mutate(first, 'updateSourceReplicate(sourceId: "src", replicate: 0)')
            _wait(lambda: _replica_rows(pg) == len(_ROWS), seconds=60, what="the replica")
            time.sleep(2 * _RELOAD_S + 2)  # the other server has reloaded and converged too
            (build,) = _builds(second)["builds"]  # it sees the same record
            assert build["state"] == "idle"
        finally:
            first.stop_process()
            second.stop_process()
    reads = _full_reads_of_orders(pg)
    assert len(reads) == 1, reads
