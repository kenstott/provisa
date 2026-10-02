# Copyright (c) 2026 Kenneth Stott
# Canary: 1f5c8b73-2e9d-4a06-b4c1-7d3a9e6f2c58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-030, amended 2026-09-30): there is no getting around the operator's floor.

A real Provisa server reads tables in the test stack's Postgres: one source ``load_protected``,
the others ``prefer_materialized``. It runs once on the DuckDB engine (SQLite control plane, DuckDB
materialize store) and once on the test stack's Trino (Postgres control plane and store). The
first read lands each. The upstream tables are then RENAMED away, so any read that reaches the source
live fails with "does not exist" -- the source's own proof it was hit. Every later read, over
/data/sql, pgwire and GraphQL, with no hint or a ``route=federated`` hint, must still answer from the
landed copy; a ``route=direct`` hint must be refused with an error naming the operator setting.

On Trino a catalog IS the live attach of a source, so a floored source has none (REQ-1912): the
coordinator is asked for its catalogs and none of the three sources is among them.
"""

# Requirements: REQ-030, REQ-826, REQ-1141, REQ-1912

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pytest.importorskip("duckdb")

_ISOLATED_ORG = "operator_floor_e2e"
_CONFIG = "tests/fixtures/duckdb_operator_floor_config.yaml"
_SCHEMA = "op_floor_e2e"
_ROWS = [(1, "one"), (2, "two"), (3, "three")]


async def _pg():
    import asyncpg

    return await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
        timeout=15,
    )


async def _seed() -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        for table in ("lp_items", "pm_items", "bare_items"):
            await conn.execute(f"CREATE TABLE {_SCHEMA}.{table} (id int PRIMARY KEY, name text)")
            await conn.executemany(f"INSERT INTO {_SCHEMA}.{table} VALUES ($1, $2)", _ROWS)
        # A table of the floored source that no config registers: admin discovery finds it and
        # registers it (REQ-1912). It is never renamed away.
        await conn.execute(
            f"CREATE TABLE {_SCHEMA}.extra_items "
            "(id int PRIMARY KEY, name text, created timestamp DEFAULT now())"
        )
        await conn.executemany(
            f"INSERT INTO {_SCHEMA}.extra_items (id, name) VALUES ($1, $2)", _ROWS
        )
    finally:
        await conn.close()


async def _take_upstream_away() -> None:
    """Rename the upstream tables: from now on any live read of them fails at the source."""
    conn = await _pg()
    try:
        for table in ("lp_items", "pm_items", "bare_items"):
            await conn.execute(f"ALTER TABLE {_SCHEMA}.{table} RENAME TO {table}_gone")
    finally:
        await conn.close()


async def _drop() -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", params=["duckdb", "trino"])
async def floor_server(request):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    await _seed()
    store_dir = tempfile.TemporaryDirectory()
    org = f"{_ISOLATED_ORG}_{request.param}"
    if request.param == "duckdb":
        server = IsolatedServer(
            org,
            engine="duckdb",
            config=_CONFIG,
            control_plane="sqlite",
            enable_pgwire=True,
            materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
        )
    else:
        server = IsolatedServer(org, engine="trino", config=_CONFIG, enable_pgwire=True)
    server.start()
    try:
        async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
            for table in ("lp_items", "pm_items", "bare_items"):
                first = await _sql(client, f"SELECT id, name FROM {table} ORDER BY id")
                assert first.status_code == 200, first.text
                assert _ids(first) == [1, 2, 3], first.text
        await _take_upstream_away()
        yield server
    finally:
        server.stop_process()
        store_dir.cleanup()
        await _drop()
        if request.param == "trino":
            await drop_org_schema(org)


async def _sql(client: httpx.AsyncClient, sql: str) -> httpx.Response:
    return await client.post("/data/sql", json={"sql": sql, "role": "org_admin"})


def _ids(resp: httpx.Response) -> list[int]:
    body = resp.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return sorted(int(r["id"]) for r in rows)


@pytest.mark.parametrize("table", ["lp_items", "pm_items", "bare_items"])
async def test_sql_reads_come_from_the_landed_copy_not_the_source(floor_server, table):
    async with httpx.AsyncClient(base_url=floor_server.base_url, timeout=120.0) as client:
        resp = await _sql(client, f"SELECT id, name FROM {table} ORDER BY id")
    assert resp.status_code == 200, f"a read reached the upstream live: {resp.text}"
    assert _ids(resp) == [1, 2, 3]


@pytest.mark.parametrize("field", ["lpItems", "pmItems", "bareItems"])
async def test_graphql_reads_come_from_the_landed_copy_not_the_source(floor_server, field):
    async with httpx.AsyncClient(base_url=floor_server.base_url, timeout=120.0) as client:
        resp = await client.post("/data/graphql", json={"query": f"{{ {field} {{ id name }} }}"})
    body = resp.json()
    assert resp.status_code == 200 and not body.get("errors"), resp.text
    assert sorted(r["id"] for r in body["data"][field]) == [1, 2, 3]


@pytest.mark.parametrize(
    ("field", "setting"),
    [
        ("lpItems", "load_protected"),
        ("pmItems", "prefer_materialized"),
        ("bareItems", "prefer_materialized"),
    ],
)
async def test_a_direct_route_hint_is_refused_naming_the_operator_setting(
    floor_server, field, setting
):
    query = f"# @provisa route=direct\n{{ {field} {{ id name }} }}"
    async with httpx.AsyncClient(base_url=floor_server.base_url, timeout=120.0) as client:
        resp = await client.post("/data/graphql", json={"query": query})
    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == "query.operator_floor", resp.text
    assert setting in resp.text


async def test_a_federated_route_hint_is_served_from_the_landed_copy(floor_server):
    query = "# @provisa route=federated\n{ lpItems { id name } }"
    async with httpx.AsyncClient(base_url=floor_server.base_url, timeout=120.0) as client:
        resp = await client.post("/data/graphql", json={"query": query})
    body = resp.json()
    assert resp.status_code == 200 and not body.get("errors"), resp.text
    assert sorted(r["id"] for r in body["data"]["lpItems"]) == [1, 2, 3]


async def test_pgwire_reads_come_from_the_landed_copy_not_the_source(floor_server):
    import asyncpg

    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=floor_server.pgwire_port,
        user="org_admin",
        password="provisa",
        database="provisa",
        timeout=30,
    )
    try:
        rows = await conn.fetch("SELECT id, name FROM lp_items ORDER BY id")
    finally:
        await conn.close()
    assert [r["id"] for r in rows] == [1, 2, 3]


@pytest.mark.parametrize("floor_server", ["trino"], indirect=True)
async def test_trino_holds_no_catalog_of_a_floored_source(floor_server):
    """The engine has no live attach of a floored source: nothing a statement could name on the
    coordinator reads the source. (DuckDB's live attach is a view inside the server's own
    process; tests/unit/test_floor_enforced_on_every_engine.py covers its removal.)"""
    import trino

    # The test stack's own coordinator: its port is the one the session allocated (the stack
    # publishes it on this host), never a default that could name another instance's.
    conn = trino.dbapi.connect(host="localhost", port=int(os.environ["TRINO_PORT"]), user="itest")
    try:
        cur = conn.cursor()
        cur.execute("SHOW CATALOGS")
        catalogs = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()
    assert not catalogs & {"floor_protected", "floor_materialized", "floor_bare"}, catalogs
    assert "provisa_admin" in catalogs  # the store the replicas are read from


# -- admin discovery of a floored source (REQ-1912) -------------------------------------------------
#
# A floored source has no live attach: on Trino no catalog is registered for it. Listing its
# schemas, tables and columns, and registering one of its tables, therefore go through the
# source's own driver, never through an engine catalog.


async def _admin(client: httpx.AsyncClient, query: str) -> dict:
    resp = await client.post(
        "/admin/graphql", json={"query": query}, headers={"X-Provisa-Role": "org_admin"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    return body["data"]


async def test_admin_discovery_lists_a_floored_source_through_its_own_driver(floor_server):
    async with httpx.AsyncClient(base_url=floor_server.base_url, timeout=120.0) as client:
        schemas = await _admin(client, '{ availableSchemas(sourceId: "floor-materialized") }')
        tables = await _admin(
            client,
            '{ availableTables(sourceId: "floor-materialized", schemaName: "op_floor_e2e") '
            "{ name } }",
        )
        columns = await _admin(
            client,
            '{ availableColumnsMetadata(sourceId: "floor-materialized", '
            'schemaName: "op_floor_e2e", tableName: "extra_items") '
            "{ name dataType isPrimaryKey } }",
        )
    assert _SCHEMA in schemas["availableSchemas"], schemas
    assert "extra_items" in {t["name"] for t in tables["availableTables"]}, tables
    assert [
        (c["name"], c["dataType"], c["isPrimaryKey"]) for c in columns["availableColumnsMetadata"]
    ] == [("id", "integer", True), ("name", "text", False), ("created", "timestamp", False)]


async def test_a_table_of_a_floored_source_registers_with_its_types_resolved(floor_server):
    """Registration resolves each column's type from the source. On Trino that read used to go
    to the source's engine catalog, which a floored source does not have, and the registration
    was refused with no type resolved."""
    async with httpx.AsyncClient(base_url=floor_server.base_url, timeout=180.0) as client:
        registered = await _admin(
            client,
            """
            mutation {
                registerTable(input: {
                    sourceId: "floor-materialized",
                    domainId: "floor",
                    schemaName: "op_floor_e2e",
                    tableName: "extra_items",
                    columns: [
                        { name: "id", visibleTo: ["org_admin"] },
                        { name: "name", visibleTo: ["org_admin"] },
                        { name: "created", visibleTo: ["org_admin"] }
                    ]
                }) { success message }
            }
            """,
        )
        assert registered["registerTable"]["success"], registered
        listed = await _admin(client, "{ tables { tableName columns { columnName dataType } } }")
    table = next(t for t in listed["tables"] if t["tableName"] == "extra_items")
    assert {c["columnName"]: c["dataType"] for c in table["columns"]} == {
        "id": "integer",
        "name": "text",
        "created": "timestamp",
    }
