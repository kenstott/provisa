# Copyright (c) 2026 Kenneth Stott
# Canary: bb37c402-b825-40e9-aaef-92ba292dff7b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for /data/sdl endpoint — tests actual HTTP via ASGI transport.

No PG/Trino required. Injects schemas directly into app state.
"""

import pytest
from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    print_schema,
)
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.asyncio


def _make_schema() -> GraphQLSchema:
    """Build a minimal GraphQL schema for testing."""
    order_type = GraphQLObjectType(
        "Orders",
        lambda: {
            "id": GraphQLField(GraphQLInt),
            "customer_id": GraphQLField(GraphQLInt),
            "total": GraphQLField(GraphQLString),
            "customer": GraphQLField(customer_type),
        },
    )
    customer_type = GraphQLObjectType(
        "Customers",
        lambda: {
            "id": GraphQLField(GraphQLInt),
            "name": GraphQLField(GraphQLString),
            "orders": GraphQLField(GraphQLList(order_type)),
        },
    )
    return GraphQLSchema(
        query=GraphQLObjectType(
            "Query",
            {
                "orders": GraphQLField(
                    GraphQLList(order_type),
                    args={"limit": GraphQLArgument(GraphQLInt)},
                ),
                "customers": GraphQLField(GraphQLList(customer_type)),
            },
        )
    )


@pytest.fixture
async def client():
    """Create test client with injected schemas — no PG/Trino needed."""
    import provisa.api.app as app_mod

    # Build app without lifespan (skip _load_and_build entirely)
    from provisa.api.app import create_app

    the_app = create_app()

    # Inject test schemas directly into app state
    test_schema = _make_schema()
    app_mod.state.schemas = {
        "analyst": test_schema,
        "steward": test_schema,
    }

    transport = ASGITransport(app=the_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

    # Clean up state
    app_mod.state.schemas = {}


class TestSDLReturnsValidSDL:
    async def test_returns_200_text_plain(self, client):
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]

    async def test_contains_query_type(self, client):
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        assert "type Query" in resp.text

    async def test_contains_table_types(self, client):
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        sdl = resp.text
        assert "type Orders" in sdl
        assert "type Customers" in sdl

    async def test_contains_fields(self, client):
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        sdl = resp.text
        assert "id: Int" in sdl
        assert "name: String" in sdl
        assert "customer: Customers" in sdl

    async def test_contains_arguments(self, client):
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        sdl = resp.text
        assert "limit: Int" in sdl

    async def test_sdl_matches_print_schema(self, client):
        """SDL output matches what graphql-core print_schema produces."""
        resp = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        expected = print_schema(_make_schema())
        assert resp.text == expected


class TestSDLRoleRouting:
    async def test_unknown_role_404(self, client):
        resp = await client.get("/data/sdl", headers={"X-Role": "nonexistent"})
        assert resp.status_code == 404
        assert "nonexistent" in resp.json()["detail"]

    async def test_missing_role_header_returns_error(self, client):
        resp = await client.get("/data/sdl")
        assert resp.status_code in (400, 422)

    async def test_different_roles_get_their_schema(self, client):
        resp_analyst = await client.get("/data/sdl", headers={"X-Role": "analyst"})
        resp_steward = await client.get("/data/sdl", headers={"X-Role": "steward"})
        assert resp_analyst.status_code == 200
        assert resp_steward.status_code == 200
        # Both use same test schema, but the endpoint correctly routes by role
        assert "type Query" in resp_analyst.text
        assert "type Query" in resp_steward.text


class TestSDLRoleScopedSchemas:
    """Test that different roles see different schemas when configured differently."""

    @pytest.fixture
    async def scoped_client(self):
        import provisa.api.app as app_mod
        from provisa.api.app import create_app

        the_app = create_app()

        # Analyst sees orders only
        analyst_schema = GraphQLSchema(
            query=GraphQLObjectType(
                "Query",
                {"orders": GraphQLField(GraphQLList(GraphQLObjectType(
                    "Orders", {"id": GraphQLField(GraphQLInt)},
                )))},
            )
        )
        # Steward sees orders + customers
        steward_schema = GraphQLSchema(
            query=GraphQLObjectType(
                "Query",
                {
                    "orders": GraphQLField(GraphQLList(GraphQLObjectType(
                        "Orders", {"id": GraphQLField(GraphQLInt)},
                    ))),
                    "customers": GraphQLField(GraphQLList(GraphQLObjectType(
                        "Customers", {"id": GraphQLField(GraphQLInt), "name": GraphQLField(GraphQLString)},
                    ))),
                },
            )
        )

        app_mod.state.schemas = {
            "analyst": analyst_schema,
            "steward": steward_schema,
        }

        transport = ASGITransport(app=the_app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c

        app_mod.state.schemas = {}

    async def test_analyst_sees_only_orders(self, scoped_client):
        resp = await scoped_client.get("/data/sdl", headers={"X-Role": "analyst"})
        assert resp.status_code == 200
        assert "type Orders" in resp.text
        assert "type Customers" not in resp.text

    async def test_steward_sees_orders_and_customers(self, scoped_client):
        resp = await scoped_client.get("/data/sdl", headers={"X-Role": "steward"})
        assert resp.status_code == 200
        assert "type Orders" in resp.text
        assert "type Customers" in resp.text

    async def test_analyst_and_steward_differ(self, scoped_client):
        resp_a = await scoped_client.get("/data/sdl", headers={"X-Role": "analyst"})
        resp_s = await scoped_client.get("/data/sdl", headers={"X-Role": "steward"})
        assert resp_a.text != resp_s.text
