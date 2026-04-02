# Copyright (c) 2025 Kenneth Stott
# Canary: aabd860a-861a-45e8-b68b-fa2273557fc6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E mutation tests: INSERT/UPDATE/DELETE via GraphQL against PG.

Requires Docker Compose stack.
Uses the customers table (simpler constraints than orders).
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


class TestInsert:
    async def test_insert_customer(self, client):
        resp = await client.post(
            "/data/graphql",
            json={
                "query": """
                    mutation {
                        insert_sales_analytics__customers(input: {
                            id: 9999, name: "Test User", email: "test@example.com", region: "test"
                        }) { affected_rows }
                    }
                """,
                "role": "admin",
            },
        )
        assert resp.status_code == 200, resp.json()
        data = resp.json()["data"]["insert_sales_analytics__customers"]
        assert data["affected_rows"] == 1

        # Verify the row exists
        resp2 = await client.post(
            "/data/graphql",
            json={
                "query": '{ sales_analytics__customers(where: { id: { eq: 9999 } }) { id name region } }',
                "role": "admin",
            },
        )
        assert resp2.status_code == 200
        rows = resp2.json()["data"]["sales_analytics__customers"]
        assert len(rows) == 1
        assert rows[0]["name"] == "Test User"


class TestUpdate:
    async def test_update_customer(self, client):
        resp = await client.post(
            "/data/graphql",
            json={
                "query": """
                    mutation {
                        update_sales_analytics__customers(
                            set: { name: "Updated User" },
                            where: { id: { eq: 9999 } }
                        ) { affected_rows }
                    }
                """,
                "role": "admin",
            },
        )
        assert resp.status_code == 200, resp.json()
        data = resp.json()["data"]["update_sales_analytics__customers"]
        assert data["affected_rows"] >= 1

        # Verify update
        resp2 = await client.post(
            "/data/graphql",
            json={
                "query": '{ sales_analytics__customers(where: { id: { eq: 9999 } }) { name } }',
                "role": "admin",
            },
        )
        rows = resp2.json()["data"]["sales_analytics__customers"]
        assert rows[0]["name"] == "Updated User"


class TestDelete:
    async def test_delete_customer(self, client):
        resp = await client.post(
            "/data/graphql",
            json={
                "query": """
                    mutation {
                        delete_sales_analytics__customers(where: { id: { eq: 9999 } }) { affected_rows }
                    }
                """,
                "role": "admin",
            },
        )
        assert resp.status_code == 200, resp.json()
        data = resp.json()["data"]["delete_sales_analytics__customers"]
        assert data["affected_rows"] >= 1

        # Verify deletion
        resp2 = await client.post(
            "/data/graphql",
            json={
                "query": '{ sales_analytics__customers(where: { id: { eq: 9999 } }) { id } }',
                "role": "admin",
            },
        )
        rows = resp2.json()["data"]["sales_analytics__customers"]
        assert len(rows) == 0
