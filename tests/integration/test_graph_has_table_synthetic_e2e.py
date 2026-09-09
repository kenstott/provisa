# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e5d19-2c4a-4f86-a9e1-6d0f8b27c4e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1631 live round trip: the admin GraphQL ``allRelationships`` resolver of a booted app
returns one synthetic HAS_TABLE row per non-meta registered table, each linking that table to the
``registered_tables`` meta definition -- the rows the graph explorer's drop-onto-canvas expansion
(``graph-drop.ts``) reads to wire a dropped table node to its data-instance nodes."""

from __future__ import annotations

import os
import tempfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    _store = os.path.join(tempfile.mkdtemp(prefix="has_table_mat_"), "mat.duckdb")
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
    body = r.json()
    assert not body.get("errors"), body
    return body["data"]


async def test_all_relationships_carries_one_has_table_row_per_registered_table(client):
    tables = (await _admin(client, "{ tables { id tableName domainId } }"))["tables"]
    data_tables = [t for t in tables if t["domainId"] != "meta"]
    assert data_tables, "the booted app registers data tables"

    rels = (
        await _admin(
            client,
            "{ allRelationships { id alias sourceTableId targetTableName sourceColumn "
            "targetColumn cardinality } }",
        )
    )["allRelationships"]
    has_table = [r for r in rels if r["alias"] == "HAS_TABLE"]

    assert {r["sourceTableId"] for r in has_table} == {t["id"] for t in data_tables}
    for row in has_table:
        assert row["id"] == f"meta:has_table:{row['sourceTableId']}"
        assert row["targetTableName"] == "registered_tables"
        assert (row["sourceColumn"], row["targetColumn"]) == ("__table_id__", "id")
        assert row["cardinality"] == "many-to-one"
