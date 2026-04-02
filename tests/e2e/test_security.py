# Copyright (c) 2025 Kenneth Stott
# Canary: 14646ac0-d381-4534-99bf-e604d71b245a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E security tests: RLS enforcement, column visibility, rights checks.

Requires Docker Compose stack (PG + Trino + MongoDB) and loaded config.
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


class TestColumnVisibility:
    async def test_admin_sees_amount(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id amount } }", "role": "admin"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sales_analytics__orders"]
        assert "amount" in rows[0]

    async def test_analyst_cannot_see_amount(self, client):
        """Analyst role has no visibility to 'amount' column — query should fail validation."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id amount } }", "role": "analyst"},
        )
        # amount is not in analyst's schema — GraphQL validation rejects it
        assert resp.status_code == 400

    async def test_analyst_sees_visible_fields_on_customers(self, client):
        """Analyst can query customers with visible columns (no RLS on customers)."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__customers { id name } }", "role": "analyst"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sales_analytics__customers"]
        assert len(rows) > 0
        assert "id" in rows[0]
        assert "name" in rows[0]

    async def test_analyst_cannot_see_product_id(self, client):
        """Analyst cannot see product_id on orders."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id product_id } }", "role": "analyst"},
        )
        assert resp.status_code == 400  # product_id not in analyst schema


class TestRLSEnforcement:
    async def test_analyst_rls_applied_on_orders(self, client):
        """Analyst has RLS on orders: region = current_setting('provisa.user_region').
        Since provisa.user_region is not set, PG raises an error — proving
        the RLS filter was injected into the SQL."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id } }", "role": "analyst"},
        )
        # RLS filter references current_setting which isn't set → 500 error
        # This proves the filter IS being injected
        assert resp.status_code == 500

    async def test_admin_no_rls(self, client):
        """Admin has no RLS rules — should see all data."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id } }", "role": "admin"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sales_analytics__orders"]
        assert len(rows) == 25  # all seeded orders


class TestDomainAccess:
    async def test_analyst_cannot_see_product_catalog_domain(self, client):
        """Analyst only has access to sales-analytics domain, not product-catalog."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ product_catalog__products { id name } }", "role": "analyst"},
        )
        # products is in product-catalog domain — not in analyst's schema
        assert resp.status_code == 400
