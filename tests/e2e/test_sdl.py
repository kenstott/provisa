# Copyright (c) 2026 Kenneth Stott
# Canary: deb99226-64c3-4401-a454-7ac0407ee4fe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests: GET /data/sdl returns role-aware GraphQL SDL (REQ-076).

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


class TestSDLEndpoint:
    async def test_sdl_returns_text(self, client):
        """SDL endpoint returns text/plain with valid SDL content."""
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        assert "type Query" in resp.text

    async def test_sdl_contains_registered_tables(self, client):
        """SDL includes types derived from registered tables in config."""
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        assert resp.status_code == 200
        sdl = resp.text
        # Config has orders, customers, products tables
        assert "Orders" in sdl or "Customers" in sdl

    async def test_sdl_unknown_role_404(self, client):
        """Unknown role returns 404."""
        resp = await client.get("/data/sdl", headers={"X-Role": "nonexistent"})
        assert resp.status_code == 404

    async def test_sdl_missing_role_header_422(self, client):
        """Missing X-Role header returns 422."""
        resp = await client.get("/data/sdl")
        assert resp.status_code == 422

    async def test_sdl_different_roles_may_differ(self, client):
        """Different roles see potentially different schemas."""
        resp_analyst = await client.get(
            "/data/sdl", headers={"X-Role": "analyst"}
        )
        resp_admin = await client.get(
            "/data/sdl", headers={"X-Role": "admin"}
        )
        assert resp_analyst.status_code == 200
        assert resp_admin.status_code == 200
        # Both should contain valid SDL
        assert "type Query" in resp_analyst.text
        assert "type Query" in resp_admin.text

    async def test_sdl_is_valid_graphql(self, client):
        """SDL can be parsed by graphql-core as valid schema."""
        from graphql import build_schema

        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        assert resp.status_code == 200
        # This will raise if SDL is invalid
        schema = build_schema(resp.text)
        assert schema.query_type is not None
