# Copyright (c) 2026 Kenneth Stott
# Canary: 5e9a2c7d-1b4f-4d3e-a8c6-7f0b3d9e1a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Register Table on a neo4j source through the admin GraphQL surface (REQ-1670).

The path the UI drives: ``createSource`` (a neo4j Source row), ``neo4jPreview`` (rows + inferred
column types from a live Neo4j), ``registerTable`` with the Cypher as ``queryTemplate``, then the
``tables`` query returning that Cypher. The registration lands the same api_endpoints row config
load writes (REQ-1668), so a restart serves the table without re-registering.
"""

from __future__ import annotations

import json
import os

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.integration.connector_source_harness import create_domain, create_source

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_neo4j,
    pytest.mark.asyncio(loop_scope="session"),
]

_SOURCE_ID = "neo4j_ui_it"
_DOMAIN = "graph_ui_it"
_TABLE = "ui_adopter"
_CYPHER = (
    "MATCH (a:UiAdopter) RETURN a.adopter_id AS adopter_id, a.name AS name ORDER BY a.adopter_id"
)
_SEED = [(1, "Sara Kim"), (2, "Tom Evans")]


def _neo4j_port() -> int:
    return int(os.environ["NEO4J_HTTP_PORT"])


@pytest.fixture(autouse=True)
def _seed_neo4j():
    tx = f"http://localhost:{_neo4j_port()}/db/neo4j/tx/commit"
    create = " ".join(f"CREATE (:UiAdopter {{adopter_id: {i}, name: '{n}'}})" for i, n in _SEED)
    with httpx.Client(timeout=60) as client:
        for stmt in ("MATCH (a:UiAdopter) DETACH DELETE a", create):
            resp = client.post(tx, json={"statements": [{"statement": stmt}]})
            assert resp.status_code == 200 and resp.json()["errors"] == [], resp.text
        yield
        client.post(tx, json={"statements": [{"statement": "MATCH (a:UiAdopter) DETACH DELETE a"}]})


@pytest_asyncio.fixture(loop_scope="session")
async def admin_client():
    """An in-process app on the default (native) engine — a neo4j table needs no Trino."""
    import provisa.api.app as app_mod

    os.environ.setdefault("PG_PASSWORD", "provisa")
    app = app_mod.create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test", timeout=180) as c:
            yield c
            # Leave nothing behind for the next module.
            listing = await c.post(
                "/admin/graphql", json={"query": "query { tables { id tableName } }"}
            )
            for t in listing.json()["data"]["tables"]:
                if t["tableName"].startswith(_TABLE):
                    await c.post(
                        "/admin/graphql",
                        json={"query": f"mutation {{ deleteTable(id: {t['id']}) {{ success }} }}"},
                    )
            await c.post(
                "/admin/graphql",
                json={"query": f'mutation {{ deleteSource(id: "{_SOURCE_ID}") {{ success }} }}'},
            )


async def _gql(client: AsyncClient, query: str) -> dict:
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, resp.text
    return body["data"]


async def test_preview_then_register_then_tables_reports_the_cypher(admin_client):
    await create_domain(admin_client, _DOMAIN)
    await create_source(
        admin_client,
        source_id=_SOURCE_ID,
        source_type="neo4j",
        host="localhost",
        port=_neo4j_port(),
        database="neo4j",
    )

    preview = (
        await _gql(
            admin_client,
            f'query {{ neo4jPreview(sourceId: "{_SOURCE_ID}", cypher: {json.dumps(_CYPHER)}) '
            "{ rows columns { name dataType } error } }",
        )
    )["neo4jPreview"]
    assert preview["error"] is None, preview
    assert [(c["name"], c["dataType"]) for c in preview["columns"]] == [
        ("adopter_id", "integer"),
        ("name", "text"),
    ]
    assert [(r["adopter_id"], r["name"]) for r in preview["rows"]] == _SEED

    cols = ", ".join(
        f'{{ name: "{c["name"]}", dataType: "{c["dataType"]}", visibleTo: ["org_admin"] }}'
        for c in preview["columns"]
    )
    result = (
        await _gql(
            admin_client,
            f'mutation {{ registerTable(input: {{ sourceId: "{_SOURCE_ID}", domainId: "{_DOMAIN}", '
            f'schemaName: "neo4j", tableName: "{_TABLE}", queryTemplate: {json.dumps(_CYPHER)}, '
            f"columns: [{cols}] }}) {{ success message }} }}",
        )
    )["registerTable"]
    assert result["success"], result

    tables = (await _gql(admin_client, "query { tables { tableName queryTemplate } }"))["tables"]
    mine = [t for t in tables if t["tableName"] == _TABLE]
    assert mine == [{"tableName": _TABLE, "queryTemplate": _CYPHER}]

    # Persisted, not process-lifetime: the endpoint row is what a restart hydrates from.
    from provisa.api.app import state

    assert state.tenant_db is not None
    async with state.tenant_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT body_encoding, query_template, response_normalizer FROM api_endpoints "
            f"WHERE table_name = '{_TABLE}'"
        )
    assert [dict(r) for r in rows] == [
        {
            "body_encoding": "neo4j_tx",
            "query_template": _CYPHER,
            "response_normalizer": "neo4j_tabular",
        }
    ]


async def test_register_without_cypher_is_refused(admin_client):
    await create_domain(admin_client, _DOMAIN)
    await create_source(
        admin_client,
        source_id=_SOURCE_ID,
        source_type="neo4j",
        host="localhost",
        port=_neo4j_port(),
        database="neo4j",
    )
    result = (
        await _gql(
            admin_client,
            f'mutation {{ registerTable(input: {{ sourceId: "{_SOURCE_ID}", domainId: "{_DOMAIN}", '
            f'schemaName: "neo4j", tableName: "{_TABLE}_nocypher", '
            'columns: [{ name: "adopter_id", dataType: "integer", visibleTo: ["org_admin"] }] }) '
            "{ success message code } }",
        )
    )["registerTable"]
    assert result["success"] is False
    assert result["code"] == "schema.neo4j_query_required", result
