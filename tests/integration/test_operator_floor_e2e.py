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

A real Provisa server (DuckDB engine, SQLite control plane, DuckDB materialize store) reads two
tables in the test stack's Postgres: one source ``load_protected``, one ``prefer_materialized``. The
first read lands each. The upstream tables are then RENAMED away, so any read that reaches the source
live fails with "does not exist" -- the source's own proof it was hit. Every later read, over
/data/sql, pgwire and GraphQL, with no hint or a ``route=federated`` hint, must still answer from the
landed copy; a ``route=direct`` hint must be refused with an error naming the operator setting.
"""

# Requirements: REQ-030, REQ-826, REQ-1141

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


@pytest_asyncio.fixture(scope="module")
async def floor_server():
    from tests.integration.isolated_server import IsolatedServer

    await _seed()
    store_dir = tempfile.TemporaryDirectory()
    server = IsolatedServer(
        _ISOLATED_ORG,
        engine="duckdb",
        config=_CONFIG,
        control_plane="sqlite",
        enable_pgwire=True,
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
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
