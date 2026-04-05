# Copyright (c) 2026 Kenneth Stott
# Canary: bbd9a487-ff56-464c-93bb-13b458e218e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests: verify routing — single-source queries route direct, results correct.

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


class TestDirectRouting:
    """Single-source PG queries should route direct and return correct data."""

    async def test_simple_query_returns_data(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id amount } }", "role": "admin"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sales_analytics__orders"]
        assert len(rows) > 0
        assert "id" in rows[0]

    async def test_filtered_query(self, client):
        resp = await client.post(
            "/data/graphql",
            json={
                "query": '{ sales_analytics__orders(where: { region: { eq: "us-east" } }) { id region } }',
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        for row in resp.json()["data"]["sales_analytics__orders"]:
            assert row["region"] == "us-east"

    async def test_nested_join_same_source(self, client):
        """Join within same source routes direct."""
        resp = await client.post(
            "/data/graphql",
            json={
                "query": "{ sales_analytics__orders { id sales_analytics__customers { name } } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sales_analytics__orders"]
        has_customer = any(r.get("sales_analytics__customers") is not None for r in rows)
        assert has_customer

    async def test_pagination(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders(limit: 2) { id } }", "role": "admin"},
        )
        assert resp.status_code == 200
        assert len(resp.json()["data"]["sales_analytics__orders"]) <= 2

    async def test_customers_query(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__customers { id name email } }", "role": "admin"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sales_analytics__customers"]
        assert len(rows) > 0


class TestTrinoRouting:
    """Cross-source queries should route through Trino."""

    async def test_cross_source_pg_to_pg(self, client):
        """customers (sales-pg) + customer_segments (analytics-pg) → Trino."""
        resp = await client.post(
            "/data/graphql",
            json={
                "query": "{ sales_analytics__customers { id name customer_insights__customer_segments { segment } } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sales_analytics__customers"]
        assert len(rows) > 0
        has_segment = any(
            r.get("customer_insights__customer_segments") is not None
            and len(r["customer_insights__customer_segments"]) > 0
            for r in rows
        )
        assert has_segment

    async def test_cross_source_pg_to_mongodb(self, client):
        """orders (sales-pg) + product_reviews (reviews-mongo) → Trino."""
        resp = await client.post(
            "/data/graphql",
            json={
                "query": "{ sales_analytics__orders(limit: 5) { id amount product_catalog__product_reviews { reviewer rating } } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sales_analytics__orders"]
        assert len(rows) > 0
        # At least some orders have reviews (products 1-7 have reviews)
        has_review = any(
            r.get("product_catalog__product_reviews") is not None
            and len(r["product_catalog__product_reviews"]) > 0
            for r in rows
        )
        assert has_review
