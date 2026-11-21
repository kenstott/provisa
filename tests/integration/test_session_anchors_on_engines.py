# Copyright (c) 2026 Kenneth Stott
# Canary: 7a445b17-e537-4f12-8c7e-89e56ea7c586
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph session's labels in capitals over POST /data/cypher on each engine (REQ-2176): a write
that matches two of them, MATCH (b:Big), (s:Small) WHERE b.k = s.k CREATE (b)-[:R]->(s), reads
their anchor tables (cypher__anchor__Big, cypher__anchor__Small) as the session holds them, and
governance takes them for the session's own -- not for relations that are not registered (V006),
as Trino's run of it once was refused -- and, named in lower case as Trino's PostgreSQL catalog
finds them, the relationships it made are read back."""

# Requirements: REQ-2176

from __future__ import annotations

import os

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_ORG = "session_anchors_e2e"
_CONFIG = "tests/fixtures/session_anchors_config.yaml"
_SCHEMA = "session_anchors_e2e"
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


@pytest_asyncio.fixture(
    scope="module",
    params=["duckdb", "pg", pytest.param("trino", marks=pytest.mark.requires_trino)],
)
async def server(request):
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    await _seed()
    org = f"{_ORG}_{request.param}"
    instance = IsolatedServer(org, engine=request.param, config=_CONFIG)
    instance.start()
    try:
        yield instance
    finally:
        instance.stop_process()
        await _drop()
        await drop_org_schema(org)


async def _call(server, method: str, path: str, body: dict | None = None) -> dict:
    async with httpx.AsyncClient(base_url=server.base_url, timeout=120.0) as client:
        resp = await client.request(method, path, json=body, headers={"X-Provisa-Role": _ROLE})
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _cypher(server, statement: str) -> dict:
    return await _call(server, "POST", "/data/cypher", {"query": statement, "params": {}})


def _values(answer: dict) -> list[tuple]:
    columns = answer["columns"]
    rows = [
        tuple(r[c] for c in columns) if isinstance(r, dict) else tuple(r) for r in answer["rows"]
    ]
    return sorted(rows, key=repr)


async def test_a_write_matching_two_session_labels_in_capitals_is_governed_and_read(server):
    assert await _call(server, "DELETE", "/data/cypher/session") == {"cleared": True}
    await _cypher(
        server,
        "CREATE (:Big {k: 1}), (:Big {k: 2}), (:Big {k: 3}), (:Small {k: 1}), (:Small {k: 2})",
    )
    await _cypher(server, "MATCH (b:Big), (s:Small) WHERE b.k = s.k CREATE (b)-[:R]->(s)")
    answer = await _cypher(server, "MATCH (a:Big)-[:R]->(b:Small) RETURN a.k AS a, b.k AS b")
    assert _values(answer) == [(1, 1), (2, 2)]
    assert await _call(server, "DELETE", "/data/cypher/session") == {"cleared": True}
