# Copyright (c) 2025 Kenneth Stott
# Canary: 28830b45-4f25-4f9d-9cc4-a19cab250dc5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E admin flow tests — config changes through admin API."""

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


class TestGraphiQL:
    async def test_graphiql_accessible(self, client):
        """GraphiQL explorer should be served at the admin endpoint."""
        resp = await client.get("/admin/graphql", headers={"Accept": "text/html"})
        # Strawberry serves GraphiQL on GET with text/html accept
        assert resp.status_code == 200


class TestAdminQuery:
    async def test_sources_and_roles(self, client):
        """Query both sources and roles in one request."""
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ sources { id type } roles { id capabilities } }"},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert len(data["sources"]) > 0
        assert len(data["roles"]) > 0

    async def test_rls_rules(self, client):
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ rlsRules { id tableId roleId filterExpr } }"},
        )
        assert resp.status_code == 200
        rules = resp.json()["data"]["rlsRules"]
        assert len(rules) > 0
        assert rules[0]["roleId"] == "analyst"
