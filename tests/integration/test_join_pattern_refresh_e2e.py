# Copyright (c) 2026 Kenneth Stott
# Canary: 2f43eb08-b38b-4159-ba69-63e4ef8860cc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A join-pattern materialized view refreshes on a real engine (REQ-135, REQ-1912).

A join-pattern view names its tables by registered name only (``orders``, ``customers``). The
refresh resolves each to where the bound engine reads it: its catalog-physical name from the
registry, then the address seam. Here one input is read live and the other is floored onto its
replica; a real Provisa server refreshes the view on DuckDB and on the test stack's Trino.

The floored input's upstream table is then renamed away. A refresh that read it at its registered
name — or at any bare name — would fail at the source; it must still build, from the replica.
"""

# Requirements: REQ-135, REQ-826, REQ-1912

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pytest.importorskip("duckdb")

_ISOLATED_ORG = "jp_refresh_e2e"
_CONFIG = "tests/fixtures/join_pattern_refresh_config.yaml"
_SCHEMA = "jp_refresh_e2e"
_MV = "auto-mv-jp-orders-customer"  # the view the loader builds for the relationship
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
        await conn.execute(f"CREATE TABLE {_SCHEMA}.jp_customers (id int PRIMARY KEY, name text)")
        await conn.execute(
            f"CREATE TABLE {_SCHEMA}.jp_orders (id int PRIMARY KEY, customer_id int, amount int)"
        )
        await conn.executemany(
            f"INSERT INTO {_SCHEMA}.jp_customers VALUES ($1, $2)", [(1, "ada"), (2, "grace")]
        )
        await conn.executemany(
            f"INSERT INTO {_SCHEMA}.jp_orders VALUES ($1, $2, $3)",
            [(10, 1, 5), (11, 1, 7), (12, 2, 9)],
        )
    finally:
        await conn.close()


async def _take_orders_upstream_away() -> None:
    """Rename the floored input's upstream table: a live read of it now fails at the source."""
    conn = await _pg()
    try:
        await conn.execute(f"ALTER TABLE {_SCHEMA}.jp_orders RENAME TO jp_orders_gone")
    finally:
        await conn.close()


async def _drop() -> None:
    conn = await _pg()
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    finally:
        await conn.close()


async def _admin(client: httpx.AsyncClient, query: str) -> dict:
    resp = await client.post(
        "/admin/graphql", json={"query": query}, headers={"X-Provisa-Role": _ROLE}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


async def _view(client: httpx.AsyncClient) -> dict:
    data = await _admin(client, "query { mvList { id status rowCount lastError } }")
    return next(mv for mv in data["mvList"] if mv["id"] == _MV)


@pytest_asyncio.fixture(scope="module", params=["duckdb", "trino"])
async def server(request):
    from tests.integration.isolated_server import IsolatedServer

    await _seed()
    store_dir = tempfile.TemporaryDirectory()
    org = f"{_ISOLATED_ORG}_{request.param}"
    if request.param == "duckdb":
        srv = IsolatedServer(
            org,
            engine="duckdb",
            config=_CONFIG,
            control_plane="sqlite",
            materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
        )
    else:
        srv = IsolatedServer(org, engine="trino", config=_CONFIG)
    srv.start()
    try:
        async with httpx.AsyncClient(base_url=srv.base_url, timeout=120.0) as client:
            # The first read of the floored table builds its replica.
            first = await client.post(
                "/data/sql", json={"sql": "SELECT id FROM jp_orders ORDER BY id", "role": _ROLE}
            )
            assert first.status_code == 200, first.text
        await _take_orders_upstream_away()
        yield srv
    finally:
        srv.stop_process()
        store_dir.cleanup()
        await _drop()


async def test_a_join_pattern_view_refreshes_from_a_live_input_and_a_replica(server):
    async with httpx.AsyncClient(base_url=server.base_url, timeout=180.0) as client:
        result = await _admin(
            client, f'mutation {{ refreshMv(mvId: "{_MV}") {{ success message }} }}'
        )
        assert result["refreshMv"]["success"], result
        view = await _view(client)

    # Built: three orders, each joined to its customer. The orders came from the replica (their
    # upstream table is gone); the customers were read live.
    assert view["lastError"] is None, view
    assert view["status"] == "fresh", view
    assert view["rowCount"] == 3, view
