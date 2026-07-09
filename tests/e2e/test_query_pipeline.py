# Copyright (c) 2026 Kenneth Stott
# Canary: cbb4ddc4-a350-49b0-a47f-83c2965a93d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests: POST /data/graphql → JSON response through full pipeline.

Requires Docker Compose stack (PG + Trino) and loaded config.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture(scope="module")
async def client():
    """Create test client against the FastAPI app with full startup."""
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestHealth:
    async def test_health(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestFieldSelection:
    async def test_select_orders(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id amount } }", "role": "admin"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        assert "sa__orders" in data["data"]
        rows = data["data"]["sa__orders"]
        assert len(rows) > 0
        assert "id" in rows[0]
        assert "amount" in rows[0]

    async def test_select_customers(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__customers { id name email } }", "role": "admin"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sa__customers"]
        assert len(rows) > 0
        assert "name" in rows[0]


class TestWhereFilter:
    async def test_filter_by_region(self, client):
        resp = await client.post(
            "/data/graphql",
            json={
                "query": '{ sa__orders(where: { region: { eq: "us-east" } }) { id region } }',
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sa__orders"]
        for row in rows:
            assert row["region"] == "us-east"

    async def test_filter_limit(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders(limit: 2) { id } }", "role": "admin"},
        )
        assert resp.status_code == 200
        assert len(resp.json()["data"]["sa__orders"]) <= 2


class TestNestedRelationship:
    async def test_orders_with_customer(self, client):
        resp = await client.post(
            "/data/graphql",
            json={
                "query": "{ sa__orders { id amount customer { name email } } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sa__orders"]
        assert len(rows) > 0
        # At least some orders have customers (many-to-one → singular 'customer')
        has_customer = any(r.get("customer") is not None for r in rows)
        assert has_customer
        for row in rows:
            if row["customer"] is not None:
                assert "name" in row["customer"]
                assert "email" in row["customer"]


class TestValidationErrors:
    async def test_unknown_field(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id bogus_field } }", "role": "admin"},
        )
        assert resp.status_code == 400

    async def test_unknown_role(self, client):
        # Dev mode resolves the role from the x-provisa-role header (REQ-273/535); an unknown
        # role is default-denied by the rate-limit middleware (403) before the endpoint runs.
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id } }"},
            headers={"x-provisa-role": "nonexistent"},
        )
        assert resp.status_code == 403

    async def test_syntax_error(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id", "role": "admin"},
        )
        assert resp.status_code in (400, 422, 500)


class TestRoleHeader:
    async def test_role_via_header(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id } }"},
            headers={"X-Provisa-Role": "admin"},
        )
        assert resp.status_code == 200
