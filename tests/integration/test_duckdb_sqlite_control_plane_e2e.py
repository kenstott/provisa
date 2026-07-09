# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: a fully DuckDB + SQLite stack, queried over GraphQL.

Answers the question end-to-end, through the real HTTP GraphQL interface (not SQL):

  * federation engine   — DuckDB (``PROVISA_ENGINE=duckdb``)
  * data source         — a SQLite file (``inquiries.sqlite``), federated in place by the
                          DuckDB sqlite connector (``Mechanism.ATTACH_RW``)
  * control plane       — SQLite (``TENANT/PLATFORM_DATABASE_URL=sqlite+aiosqlite``), so the
                          source is CONSUMED (registered) into a SQLite registry — no Postgres
  * materialize store   — DuckDB (``materialize_store_url=duckdb:///...``)

A real Provisa server is booted with exactly that wiring; the SQLite source is then queried
by POSTing a GraphQL query to ``/data/graphql`` and asserting GraphQL-shaped rows come back.
The dedicated server + its SQLite/DuckDB files are torn down afterwards.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pytest.importorskip("duckdb")
pytest.importorskip("aiosqlite")

_ISOLATED_ORG = "duckdb_sqlite_cp"
_CONFIG = "tests/fixtures/duckdb_sqlite_config.yaml"


@pytest_asyncio.fixture(scope="module")
async def duckdb_sqlite_server():
    """A dedicated Provisa server: DuckDB engine, SQLite control plane, DuckDB materialize store."""
    from tests.integration.isolated_server import IsolatedServer

    store_dir = tempfile.TemporaryDirectory()
    store_url = f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}"
    server = IsolatedServer(
        _ISOLATED_ORG,
        engine="duckdb",
        config=_CONFIG,
        control_plane="sqlite",
        materialize_store_url=store_url,
    )
    server.start()
    try:
        yield server
    finally:
        server.stop_process()
        store_dir.cleanup()


async def test_sqlite_source_queryable_over_graphql_on_duckdb(duckdb_sqlite_server):
    server = duckdb_sqlite_server

    async with httpx.AsyncClient(base_url=server.base_url, timeout=60.0) as client:
        # Sanity: the server is up (SQLite control plane + DuckDB engine booted, no Postgres).
        health = await client.get("/health")
        assert health.status_code == 200

        # Query the SQLite source THROUGH GRAPHQL (not SQL): the DuckDB engine ATTACHes the
        # sqlite file and the request is served by the real /data/graphql pipeline.
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ inquiries { petId userId status } }"},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body or not body["errors"], body
    rows = body["data"]["inquiries"]
    assert isinstance(rows, list) and len(rows) > 0
    first = rows[0]
    # GraphQL-shaped result — keyed by the camelCase field names, not raw SQL columns.
    assert {"petId", "userId", "status"} <= set(first)
