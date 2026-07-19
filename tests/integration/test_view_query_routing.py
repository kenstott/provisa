# Copyright (c) 2026 Kenneth Stott
# Canary: a7a76d7c-e08f-4141-a5a7-0bb43b3423b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-135 regression: querying a __provisa__ view through /data/sql must inline-expand the view and
route through the engine. Previously routing bound the view's virtual source to no native driver, fell
back to a real source on the DIRECT route, and executed the un-expanded view ref → KeyError in the
native pool (surfaced to the client as {"detail": "'<source-id>'"})."""

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    from provisa.api.app import create_app

    app = create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def test_select_from_provisa_view_routes_through_engine(client):
    await client.post(
        "/admin/graphql",
        json={"query": 'mutation { createDomain(input: { id: "vq", description: "x" }) { success } }'},
    )
    reg = await client.post(
        "/admin/graphql",
        json={
            "query": """
                mutation {
                    registerTable(input: {
                        sourceId: "__provisa__",
                        domainId: "vq",
                        schemaName: "views",
                        tableName: "vq_view",
                        alias: "vq_view",
                        viewSql: "SELECT 1 AS n",
                        columns: [{ name: "n", visibleTo: ["public"] }]
                    }) { success message }
                }
            """
        },
    )
    assert reg.json()["data"]["registerTable"]["success"], reg.text
    # Rebuild so the view enters view_sql_map (inline-expansion source).
    rb = await client.post("/admin/graphql", json={"query": "mutation { rebuildSchemas { success } }"})
    assert rb.json()["data"]["rebuildSchemas"]["success"], rb.text

    resp = await client.post(
        "/data/sql",
        json={"sql": 'SELECT * FROM "vq"."vq_view"', "role": "admin"},
    )
    # The regression: this used to be a 400 {"detail": "'<source-id>'"} (KeyError in the driver pool).
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"data": {"sql": [{"n": 1}]}}, resp.text
