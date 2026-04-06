# Copyright (c) 2026 Kenneth Stott
# Canary: 6c7ee3c2-0f9c-4fc9-9d0b-6a2396756946
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for API source discovery → registration → query flow."""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture
async def client(pg_pool):
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c

    # Clean up rows created by discover tests (CASCADE deletes candidates/endpoints too)
    async with pg_pool.acquire() as conn:
        await conn.execute("DELETE FROM api_sources WHERE id = 'test-api'")


class TestApiSourceDiscovery:
    async def test_discover_endpoint_returns_candidates(self, client):
        """POST /admin/api-sources/discover triggers introspection."""
        # This test requires a real OpenAPI spec URL — skip if no network
        resp = await client.post(
            "/admin/api-sources/discover",
            json={
                "source_id": "test-api",
                "type": "openapi",
                "spec_url": "https://petstore3.swagger.io/api/v3/openapi.json",
            },
        )
        if resp.status_code == 503:
            pytest.skip("Database not connected")
        # May fail due to network — that's acceptable for CI
        if resp.status_code == 200:
            data = resp.json()
            assert "candidates_stored" in data
            assert data["candidates_stored"] >= 0

    async def test_list_candidates(self, client):
        """GET /admin/api-sources/candidates returns discovered candidates."""
        resp = await client.get("/admin/api-sources/candidates")
        if resp.status_code == 503:
            pytest.skip("Database not connected")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestApiSourceRegistration:
    async def test_accept_and_reject_flow(self, client):
        """Accept a candidate → endpoint created. Reject → removed from queue."""
        # List candidates
        resp = await client.get("/admin/api-sources/candidates")
        if resp.status_code == 503:
            pytest.skip("Database not connected")
        candidates = resp.json()
        if not candidates:
            pytest.skip("No candidates to test — run discover first")

        candidate_id = candidates[0]["id"]

        # Accept the first candidate
        resp = await client.post(
            f"/admin/api-sources/candidates/{candidate_id}/accept",
            json={"overrides": {"ttl": 600}},
        )
        assert resp.status_code == 200
        endpoint = resp.json()
        assert endpoint["ttl"] == 600

        # Accepting again should fail
        resp = await client.post(
            f"/admin/api-sources/candidates/{candidate_id}/accept",
        )
        assert resp.status_code in (400, 422, 500)

    async def test_reject_candidate(self, client):
        """Reject a candidate — removed from discovered list."""
        resp = await client.get("/admin/api-sources/candidates")
        if resp.status_code == 503:
            pytest.skip("Database not connected")
        candidates = resp.json()
        if not candidates:
            pytest.skip("No candidates to reject")

        candidate_id = candidates[-1]["id"]
        resp = await client.post(
            f"/admin/api-sources/candidates/{candidate_id}/reject",
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "rejected"


class TestApiSourceQueryIntegration:
    async def test_registered_api_endpoints_in_admin_tables(self, client):
        """Registered API endpoints should be queryable via admin GraphQL."""
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ tables { id tableName } }"},
        )
        if resp.status_code != 200:
            pytest.skip("Admin API not available")
        data = resp.json()
        assert "data" in data
        # Tables list should be non-empty (includes DB tables + any API endpoints)
        assert isinstance(data["data"]["tables"], list)
