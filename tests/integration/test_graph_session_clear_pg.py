# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e51d7-8a24-4f06-b1d3-5e7a20c4f918
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph session written and read on the pg engine is cleared at once (REQ-2176).

The pg engine's Arrow reads ran on pooled ADBC connections that, out of autocommit, were given back
idle in their read's transaction, still holding its lock on the session's tables; clearing the
session drops its store schema in the same PostgreSQL, so DELETE /data/cypher/session waited on
them until the request's 60 s deadline answered 504."""

# Requirements: REQ-2176

from __future__ import annotations

import os
import time

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_ORG = "graph_session_clear_pg"
_CONFIG = "tests/fixtures/graph_session_clear_pg_config.yaml"
_SCHEMA = "graph_session_clear_pg"
_ROLE = "org_admin"


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
        await conn.execute(f"CREATE TABLE {_SCHEMA}.orders (id int PRIMARY KEY, region text)")
        await conn.execute(f"INSERT INTO {_SCHEMA}.orders VALUES (1, 'east')")
    finally:
        await conn.close()


async def _drop() -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module")
async def server():
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    await _seed()
    instance = IsolatedServer(_ORG, engine="pg", config=_CONFIG)
    instance.start()
    try:
        yield instance
    finally:
        instance.stop_process()
        await _drop()
        await drop_org_schema(_ORG)


async def _call(server, method: str, path: str, body: dict | None = None) -> dict:
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.request(method, path, json=body, headers={"X-Provisa-Role": _ROLE})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _cypher(server, statement: str) -> dict:
    return await _call(server, "POST", "/data/cypher", {"query": statement, "params": {}})


async def _idle_in_transaction_on_the_session() -> list[tuple]:
    """Engine backends idle in a transaction whose last statement read this org's session tables."""
    conn = await _pg()
    try:
        rows = await conn.fetch(
            "SELECT pid, state, left(query, 120) AS query FROM pg_stat_activity"
            " WHERE state LIKE 'idle in transaction%' AND query LIKE $1",
            f"%org_{_ORG}%_tmp__%",
        )
    finally:
        await conn.close()
    return [tuple(r) for r in rows]


async def test_a_session_written_and_read_on_the_pg_engine_is_cleared_at_once(server):
    await _cypher(server, "CREATE (:scratch {n: 1}), (:scratch {n: 2})")
    answer = await _cypher(server, "MATCH (s:scratch) RETURN s.n AS n")
    assert sorted(r["n"] if isinstance(r, dict) else r[0] for r in answer["rows"]) == [1, 2]
    held = await _call(server, "GET", "/data/cypher/session")
    assert held["labels"] == [{"label": "scratch", "nodes": 2, "tagged_model_nodes": 0}]
    assert await _idle_in_transaction_on_the_session() == []
    started = time.monotonic()
    assert await _call(server, "DELETE", "/data/cypher/session") == {"cleared": True}
    # The request's own deadline is 60 s; a clear waiting on a lock ran into it.
    assert time.monotonic() - started < 30
    assert await _call(server, "GET", "/data/cypher/session") == {
        "labels": [],
        "types": [],
        "empty": True,
    }
