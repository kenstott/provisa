# Copyright (c) 2026 Kenneth Stott
# Canary: 7415d461-89b7-427e-b4f5-d2bd39b91f01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests: Admin GraphQL CRUD operations as exercised by the UI.

Tests the exact same API calls the UI makes: fetchSources, createSource,
deleteSource, fetchTables, fetchRelationships, fetchRoles, fetchRlsRules,
fetchSdl, and executeQuery.

Requires Docker Compose stack (PG + Trino) and loaded config.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def _gql(client, query: str, variables: dict | None = None):
    """Helper: send GraphQL to /admin/graphql, return parsed data or raise."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = await client.post(
        "/admin/graphql",
        json=payload,
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert "errors" not in body, f"GraphQL errors: {body['errors']}"
    return body["data"]


class TestAdminSourcesCRUD:
    """Tests the exact API calls SourcesPage.tsx makes."""

    async def test_fetch_sources(self, client):
        """UI calls: { sources { id type host port database username dialect } }"""
        data = await _gql(
            client,
            "{ sources { id type host port database username dialect } }",
        )
        assert "sources" in data
        assert len(data["sources"]) >= 1
        src = data["sources"][0]
        assert "id" in src
        assert "type" in src
        assert "host" in src
        assert "port" in src

    async def test_create_source(self, client):
        """UI calls createSource mutation with SourceInput."""
        data = await _gql(
            client,
            'mutation { createSource(input: {id: "ui-test-src", type: "postgresql", '
            'host: "localhost", port: 5432, database: "testdb", '
            'username: "testuser", password: "testpass"}) { success message } }',
        )
        assert data["createSource"]["success"] is True

        # Verify it appears in list
        data = await _gql(client, "{ sources { id } }")
        ids = [s["id"] for s in data["sources"]]
        assert "ui-test-src" in ids

    async def test_delete_source(self, client):
        """UI calls deleteSource mutation."""
        # Create first
        await _gql(
            client,
            'mutation { createSource(input: {id: "ui-delete-me", type: "postgresql", '
            'host: "localhost", port: 5432, database: "testdb", '
            'username: "testuser", password: "testpass"}) { success } }',
        )

        # Delete
        data = await _gql(
            client,
            'mutation { deleteSource(id: "ui-delete-me") { success message } }',
        )
        assert data["deleteSource"]["success"] is True

        # Verify gone
        data = await _gql(client, "{ sources { id } }")
        ids = [s["id"] for s in data["sources"]]
        assert "ui-delete-me" not in ids

    async def test_delete_nonexistent_source(self, client):
        """Deleting nonexistent source returns success=false."""
        data = await _gql(
            client,
            'mutation { deleteSource(id: "does-not-exist") { success message } }',
        )
        assert data["deleteSource"]["success"] is False


class TestAdminTablesQuery:
    """Tests the exact API calls TablesPage.tsx makes."""

    async def test_fetch_tables(self, client):
        data = await _gql(
            client,
            "{ tables { id sourceId domainId schemaName tableName governance "
            "columns { id columnName visibleTo } } }",
        )
        assert "tables" in data
        assert len(data["tables"]) >= 1
        tbl = data["tables"][0]
        assert "id" in tbl
        assert "tableName" in tbl
        assert "columns" in tbl
        assert len(tbl["columns"]) >= 1


class TestAdminRelationshipsQuery:
    """Tests the exact API calls RelationshipsPage.tsx makes."""

    async def test_fetch_relationships(self, client):
        data = await _gql(
            client,
            "{ relationships { id sourceTableId targetTableId "
            "sourceColumn targetColumn cardinality } }",
        )
        assert "relationships" in data
        assert len(data["relationships"]) >= 1
        rel = data["relationships"][0]
        assert "sourceColumn" in rel
        assert "targetColumn" in rel


class TestAdminRolesAndSecurity:
    """Tests the exact API calls SecurityPage.tsx makes."""

    async def test_fetch_roles(self, client):
        data = await _gql(
            client,
            "{ roles { id capabilities domainAccess } }",
        )
        assert "roles" in data
        assert len(data["roles"]) >= 1
        role = data["roles"][0]
        assert "id" in role
        assert "capabilities" in role

    async def test_fetch_rls_rules(self, client):
        data = await _gql(
            client,
            "{ rlsRules { id tableId roleId filterExpr } }",
        )
        assert "rlsRules" in data
        assert len(data["rlsRules"]) >= 1


class TestSDLEndpointForUI:
    """Tests the exact API call SchemaExplorer.tsx makes."""

    async def test_fetch_sdl(self, client):
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        sdl = resp.text
        assert "type Query" in sdl
        # Voyager will parse this — verify it's valid
        from graphql import build_schema

        schema = build_schema(sdl)
        assert schema.query_type is not None


class TestDataQueryForUI:
    """Tests the exact API call QueryPage.tsx makes."""

    async def test_execute_query(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id region status } }"},
            headers={"X-Role": "analyst"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        assert "sales_analytics__orders" in body["data"]
        assert len(body["data"]["sales_analytics__orders"]) > 0

    async def test_execute_query_with_filter(self, client):
        """UI sends queries with WHERE filters."""
        resp = await client.post(
            "/data/graphql",
            json={"query": '{ sales_analytics__orders(where: {region: {eq: "us-east"}}) { id region } }'},
            headers={"X-Role": "analyst"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body
        for order in body["data"]["sales_analytics__orders"]:
            assert order["region"] == "us-east"
