# Copyright (c) 2026 Kenneth Stott
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


@pytest.fixture(scope="module")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(
            # REQ-273: the header carries the role; a body role must match it.
            transport=transport,
            base_url="http://test",
            headers={"X-Provisa-Role": "org_admin"},
        ) as c:
            yield c


class TestColumnVisibility:
    async def test_admin_sees_amount(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id amount } }", "role": "org_admin"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sa__orders"]
        assert "amount" in rows[0]

    async def test_analyst_cannot_see_amount(self, client):
        """Analyst role has no visibility to 'amount' column — query should fail validation."""
        # Dev mode resolves the role from the x-provisa-role header (REQ-273/535); a body
        # `role` that differs from it is refused.
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id amount } }"},
            headers={"x-provisa-role": "analyst"},
        )
        # amount is not in analyst's schema — GraphQL validation rejects it
        assert resp.status_code == 400

    async def test_analyst_sees_visible_fields_on_customers(self, client):
        """Analyst can query customers with visible columns (no RLS on customers)."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__customers { id name } }"},
            headers={"x-provisa-role": "analyst"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sa__customers"]
        assert len(rows) > 0
        assert "id" in rows[0]
        assert "name" in rows[0]

    async def test_analyst_cannot_see_product_id(self, client):
        """Analyst cannot see product_id on orders."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id product_id } }"},
            headers={"x-provisa-role": "analyst"},
        )
        assert resp.status_code == 400  # product_id not in analyst schema


_SESSION_REGION = {"x-provisa-session-user_region": "us-east"}


async def _graphql_orders(client, role: str, session: dict | None = None) -> list[dict]:
    resp = await client.post(
        "/data/graphql",
        json={"query": "{ sa__orders { id region } }"},
        headers={"x-provisa-role": role, **(session or {})},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sa__orders"]


async def _sql_orders(client, role: str, session: dict | None = None) -> list[dict]:
    resp = await client.post(
        "/data/sql",
        json={"sql": "SELECT id, region FROM sales_analytics.orders"},
        headers={"x-provisa-role": role, **(session or {})},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


async def _rest_orders(client, role: str, session: dict | None = None) -> list[dict]:
    resp = await client.get(
        "/data/rest/sales-analytics/orders",
        headers={"x-provisa-role": role, **(session or {})},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]


_READS = {"graphql": _graphql_orders, "sql_http": _sql_orders, "rest": _rest_orders}


class TestRLSEnforcement:
    """The analyst's row filter on orders is ``region = current_setting('provisa.user_region')``.

    The session variable is bound per request from the acting identity (REQ-1682; in an
    unsecured deployment, from the ``x-provisa-session-<name>`` header). The assertions are on the
    ROWS the analyst gets — a status code says nothing about which rows a role was shown."""

    @pytest.mark.parametrize("transport", sorted(_READS))
    async def test_analyst_with_no_region_bound_sees_no_orders(self, client, transport):
        """A variable bound nowhere resolves to NULL: the filter matches no row. Deny by
        default — never the unfiltered table."""
        read = _READS[transport]
        assert len(await read(client, "org_admin")) >= 25
        assert await read(client, "analyst") == []

    @pytest.mark.parametrize("transport", sorted(_READS))
    async def test_analyst_sees_exactly_the_orders_of_the_bound_region(self, client, transport):
        read = _READS[transport]
        everything = await read(client, "org_admin")
        expected = sorted(r["id"] for r in everything if r["region"] == "us-east")
        assert expected and len(expected) < len(everything)  # the filter has something to remove

        rows = await read(client, "analyst", _SESSION_REGION)
        assert sorted(r["id"] for r in rows) == expected
        assert {r["region"] for r in rows} == {"us-east"}

    @pytest.mark.parametrize("transport", sorted(_READS))
    async def test_a_bound_region_does_not_outlive_its_request(self, client, transport):
        """The same read with no region bound, right after one that bound it, is empty again:
        neither a kept plan nor a cached result carries one request's rows to the next."""
        read = _READS[transport]
        assert await read(client, "analyst", _SESSION_REGION) != []
        assert await read(client, "analyst") == []

    async def test_the_governed_sql_carries_the_analysts_filter(self, client):
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sa__orders { id region } }"},
            headers={"x-provisa-role": "analyst"},
        )
        assert resp.status_code == 200, resp.text
        compiled = resp.json()["compiled"][0]
        assert compiled["enforcement"]["rls_filters_applied"] == [
            "region = current_setting('provisa.user_region')"
        ]
        assert "WHERE" in compiled["sql"] and "provisa.user_region" in compiled["sql"]

    async def test_admin_no_rls(self, client):
        """Admin has no RLS rules — should see all data."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__orders { id } }", "role": "org_admin"},
        )
        assert resp.status_code == 200
        rows = resp.json()["data"]["sa__orders"]
        assert len(rows) >= 25  # all seeded orders (CDC tests may add rows)


class TestDomainAccess:
    async def test_analyst_cannot_see_product_catalog_domain(self, client):
        """Analyst only has access to sales-analytics domain, not product-catalog."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ product_catalog__products { id name } }"},
            headers={"x-provisa-role": "analyst"},
        )
        # products is in product-catalog domain — not in analyst's schema
        assert resp.status_code == 400
