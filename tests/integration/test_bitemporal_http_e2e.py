# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1162 + REQ-1163: HTTP end-to-end for a bitemporal materialized view.

Drives the full live path through the real app: register a MATERIALIZED bitemporal view over GraphQL
(config persists to the DB), rebuild + refresh so its append log materializes, then query it through
/data/sql — current-by-default and with the X-Provisa-As-Of header. The materialize store is pointed
at an embedded DuckDB file so the in-process test app needs no external store (the isolated Postgres
is on an ephemeral port, not :5432)."""

import os
import tempfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    # Materialize into an embedded DuckDB file — no external store needed for the in-process app.
    _store = os.path.join(tempfile.mkdtemp(prefix="bt_mat_"), "mat.duckdb")
    os.environ["PROVISA_MATERIALIZE_URL"] = f"duckdb:///{_store}"
    from provisa.api.app import create_app

    app = create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c
    os.environ.pop("PROVISA_MATERIALIZE_URL", None)


async def _admin(client, query: str):
    r = await client.post("/admin/graphql", json={"query": query})
    assert r.status_code == 200, r.text
    return r.json()


async def _sql(client, sql: str, **headers):
    return await client.post("/data/sql", json={"sql": sql, "role": "admin"}, headers=headers or None)


async def test_bitemporal_view_http_end_to_end(client):
    await _admin(client, 'mutation { createDomain(input: { id: "bt", description: "x" }) { success } }')

    reg = await _admin(
        client,
        """
        mutation {
            registerTable(input: {
                sourceId: "__provisa__",
                domainId: "bt",
                schemaName: "views",
                tableName: "bt_view",
                alias: "bt_view",
                viewSql: "SELECT 1 AS id, 10 AS amount",
                materialize: true,
                mvBitemporalMode: "delta",
                mvBitemporalKey: ["id"],
                columns: [{ name: "id", visibleTo: ["public"] }, { name: "amount", visibleTo: ["public"] }]
            }) { success message }
        }
        """,
    )
    assert reg["data"]["registerTable"]["success"], reg

    # (1) Persistence round-trip: the bitemporal config survives GraphQL -> DB -> read-back.
    tables = await _admin(client, "query { tables { tableName mvBitemporalMode mvBitemporalKey } }")
    row = next(t for t in tables["data"]["tables"] if t["tableName"] == "bt_view")
    assert row["mvBitemporalMode"] == "delta"
    assert row["mvBitemporalKey"] == ["id"]

    # Rebuild so the view's read is wired to the reconstruction, then materialize the append log.
    rb = await _admin(client, "mutation { rebuildSchemas { success } }")
    assert rb["data"]["rebuildSchemas"]["success"], rb
    from provisa.api.app import state
    from provisa.mv.refresh import refresh_mv

    mv = state.mv_registry.get("view-bt_view")
    assert mv is not None and mv.bitemporal is not None  # REQ-1162: spec survived the reload
    assert mv.bitemporal.mode == "delta"
    await refresh_mv(state.federation_engine, mv, state.mv_registry)

    # (2) Current-by-default read through the real endpoint reconstructs current state from the log.
    cur = await _sql(client, 'SELECT id, amount FROM "bt"."bt_view"')
    assert cur.status_code == 200, cur.text
    assert cur.json() == {"data": {"sql": [{"id": 1, "amount": 10}]}}, cur.text

    # (3) X-Provisa-As-Of after the refresh → the reconstructed current row (time-travel path).
    fut = await _sql(client, 'SELECT id, amount FROM "bt"."bt_view"', **{"X-Provisa-As-Of": "2999-01-01T00:00:00"})
    assert fut.status_code == 200, fut.text
    assert fut.json() == {"data": {"sql": [{"id": 1, "amount": 10}]}}, fut.text

    # (4) X-Provisa-As-Of BEFORE the first refresh → no version was effective yet → empty.
    past = await _sql(client, 'SELECT id, amount FROM "bt"."bt_view"', **{"X-Provisa-As-Of": "2000-01-01T00:00:00"})
    assert past.status_code == 200, past.text
    assert past.json() == {"data": {"sql": []}}, past.text

    # (5) A malformed X-Provisa-As-Of is rejected at the endpoint before execution → HTTP 400.
    bad = await _sql(client, 'SELECT id FROM "bt"."bt_view"', **{"X-Provisa-As-Of": "not-a-timestamp"})
    assert bad.status_code == 400, bad.text
    assert "as-of" in bad.text.lower() or "as_of" in bad.text.lower()
