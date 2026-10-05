# Copyright (c) 2026 Kenneth Stott
# Canary: 51d92067-dfd7-4fbb-abb9-c34c41cc93c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a table registered through the admin starts as draft, out of service until its
domain's owners release it (REQ-1921), on a real pg-engine server over a real source.

* Freshly registered it is refused, by name and as draft, on GraphQL and SQL alike, and no schema
  offers it.
* Released, it is read; replicated, its replica is built.
* Set as draft again, it is refused again and its replica is removed — not only no longer built.
"""

# Requirements: REQ-1921

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.helpers import registered_id
from tests.integration.test_pg_engine_landing_never_writes_source_e2e import (
    _ROLE,
    _ROWS,
    _SourceAndEngine,
)
from tests.integration.test_replica_convergence_e2e import _config

pytestmark = [pytest.mark.integration]

_NAME = "draft_e2e"
_REPLICA = f'"org_{_NAME}_replicas"."src__public__orders"'
_ID_AMOUNT = [(i, amount) for i, amount, _note in _ROWS]


@pytest.fixture
def server():
    from tests.integration.isolated_server import IsolatedServer

    pg = _SourceAndEngine()
    pg.start()
    with tempfile.TemporaryDirectory() as workdir:
        path = Path(workdir) / "config.yaml"
        path.write_text(yaml.safe_dump(_config()))
        control_plane = pg.url(pg.engine_port, "provisa", "+psycopg")
        srv = IsolatedServer(
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
        try:
            srv.start()
            yield srv, pg
        finally:
            srv.stop_process()
            pg.stop()


def _mutate(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _graphql(srv, query: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def _sql(srv, sql: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql, "role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def _read(srv) -> list[tuple]:
    response = _graphql(srv, "{ orders { id amount } }")
    assert response.status_code == 200, response.text
    return sorted((r["id"], r["amount"]) for r in response.json()["data"]["orders"])


def _refused_as_draft(srv) -> None:
    graphql = _graphql(srv, "{ orders { id amount } }")
    assert graphql.status_code == 409, graphql.text
    assert graphql.json()["code"] == "data.table_is_draft", graphql.text
    assert graphql.json()["params"] == {"table": "orders"}, graphql.text
    sql = _sql(srv, "SELECT id FROM orders")
    assert sql.status_code != 200, sql.text
    assert "'orders' is a draft" in sql.text, sql.text
    fields = _graphql(srv, "{ __schema { queryType { fields { name } } } }")
    assert fields.status_code == 200, fields.text
    offered = {f["name"] for f in fields.json()["data"]["__schema"]["queryType"]["fields"]}
    assert "orders" not in offered


def _replica_rows(pg: _SourceAndEngine) -> int | None:
    with psycopg.connect(pg.url(pg.engine_port, "provisa"), autocommit=True) as conn:
        if conn.execute("SELECT to_regclass(%s)", (_REPLICA,)).fetchone()[0] is None:
            return None
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {_REPLICA}").fetchone()[0]
        except psycopg.errors.UndefinedTable:
            return None  # dropped between the two statements: the drop being waited for


def _wait(condition, *, seconds: float, what: str) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.25)


def test_a_table_registered_through_the_admin_is_out_of_service_until_released(server):
    srv, pg = server
    _mutate(
        srv,
        'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
        f'port: {pg.source_port}, database: "shop", username: "provisa", password: "provisa"}})',
    )
    registered = _mutate(
        srv,
        'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
    )
    table_id = registered_id(registered["message"])
    _refused_as_draft(srv)

    _mutate(srv, f"setTableDraft(tableId: {table_id}, draft: false)")
    assert _read(srv) == _ID_AMOUNT

    _mutate(srv, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 3600)')
    _mutate(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')
    _wait(lambda: _replica_rows(pg) == len(_ROWS), seconds=90, what="the replica")
    assert _read(srv) == _ID_AMOUNT

    # Set as draft again: refused again, and its replica goes (retired, then dropped).
    _mutate(srv, f"setTableDraft(tableId: {table_id}, draft: true)")
    _refused_as_draft(srv)
    _wait(lambda: _replica_rows(pg) is None, seconds=90, what="the replica's removal")
