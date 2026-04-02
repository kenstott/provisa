# Copyright (c) 2025 Kenneth Stott
# Canary: 32911be2-e2d6-48ac-910a-8ccbb24a3329
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Admin GraphQL API (Strawberry) against real PG."""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestQuerySources:
    async def test_list_sources(self, client):
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ sources { id type host port } }"},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert "sources" in data
        assert len(data["sources"]) > 0
        assert data["sources"][0]["id"]

    async def test_get_source(self, client):
        resp = await client.post(
            "/admin/graphql",
            json={"query": '{ source(id: "sales-pg") { id type host } }'},
        )
        assert resp.status_code == 200
        source = resp.json()["data"]["source"]
        assert source["id"] == "sales-pg"
        assert source["type"] == "postgresql"


class TestQueryDomains:
    async def test_list_domains(self, client):
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ domains { id description } }"},
        )
        assert resp.status_code == 200
        domains = resp.json()["data"]["domains"]
        assert len(domains) > 0


class TestQueryTables:
    async def test_list_tables_with_columns(self, client):
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ tables { id tableName columns { columnName visibleTo } } }"},
        )
        assert resp.status_code == 200
        tables = resp.json()["data"]["tables"]
        assert len(tables) > 0
        assert len(tables[0]["columns"]) > 0


class TestQueryRoles:
    async def test_list_roles(self, client):
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ roles { id capabilities domainAccess } }"},
        )
        assert resp.status_code == 200
        roles = resp.json()["data"]["roles"]
        assert len(roles) > 0
        admin = next(r for r in roles if r["id"] == "admin")
        assert "admin" in admin["capabilities"]


class TestQueryRelationships:
    async def test_list_relationships(self, client):
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ relationships { id sourceColumn targetColumn cardinality } }"},
        )
        assert resp.status_code == 200
        rels = resp.json()["data"]["relationships"]
        assert len(rels) > 0


class TestMutations:
    async def test_create_and_delete_domain(self, client):
        # Create
        resp = await client.post(
            "/admin/graphql",
            json={
                "query": """
                    mutation {
                        createDomain(input: { id: "test-domain", description: "Test" }) {
                            success message
                        }
                    }
                """,
            },
        )
        assert resp.status_code == 200
        result = resp.json()["data"]["createDomain"]
        assert result["success"]

        # Verify exists
        resp2 = await client.post(
            "/admin/graphql",
            json={"query": "{ domains { id } }"},
        )
        domain_ids = [d["id"] for d in resp2.json()["data"]["domains"]]
        assert "test-domain" in domain_ids

    async def test_create_role(self, client):
        resp = await client.post(
            "/admin/graphql",
            json={
                "query": """
                    mutation {
                        createRole(input: {
                            id: "test-role",
                            capabilities: ["query_development"],
                            domainAccess: ["sales-analytics"]
                        }) { success }
                    }
                """,
            },
        )
        assert resp.status_code == 200
        assert resp.json()["data"]["createRole"]["success"]
