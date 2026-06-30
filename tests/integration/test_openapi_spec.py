# Copyright (c) 2026 Kenneth Stott
# Canary: d3e6f0a4-b5c7-8901-2def-012345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration tests for REST OpenAPI 3.1 spec endpoint GET /data/rest/openapi.json (REQ-804)."""

from __future__ import annotations

import pytest
from graphql import (
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _make_app_and_state(role_id: str = "admin"):
    from fastapi import FastAPI

    from provisa.api.app import AppState
    from provisa.api.rest.generator import create_rest_router
    from provisa.compiler.rls import RLSContext

    order_type = GraphQLObjectType(
        "Order",
        lambda: {
            "id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "region": GraphQLField(GraphQLString),  # type: ignore[arg-type]
            "amount": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {"orders": GraphQLField(GraphQLList(order_type))},  # type: ignore[arg-type]
    )
    schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]

    state = AppState()
    state.schemas = {role_id: schema}
    state.contexts = {}
    state.rls_contexts = {role_id: RLSContext.empty()}
    state.masking_rules = {}
    state.table_path_maps = {
        role_id: {
            "orders": {
                "schema_name": "public",
                "table_name": "orders",
                "domain_id": "default",
                "table_description": "Sales orders",
                "domain_description": "Default domain",
            }
        }
    }

    app = FastAPI()
    app.include_router(create_rest_router(state))
    return app, state


@pytest.fixture
def client():
    import httpx

    app, _ = _make_app_and_state()
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


class TestSpecEndpoint:
    async def test_returns_200(self, client):
        async with client:
            resp = await client.get("/data/rest/openapi.json?role=admin")
        assert resp.status_code == 200

    async def test_content_type_is_json(self, client):
        async with client:
            resp = await client.get("/data/rest/openapi.json?role=admin")
        assert "application/json" in resp.headers["content-type"]

    async def test_openapi_version(self, client):
        async with client:
            resp = await client.get("/data/rest/openapi.json?role=admin")
        assert resp.json()["openapi"] == "3.1.0"

    async def test_paths_include_table(self, client):
        async with client:
            resp = await client.get("/data/rest/openapi.json?role=admin")
        assert "/default/orders" in resp.json()["paths"]

    async def test_unknown_role_returns_empty_paths(self, client):
        async with client:
            resp = await client.get("/data/rest/openapi.json?role=ghost")
        assert resp.status_code == 200
        assert resp.json()["paths"] == {}

    async def test_domain_filter_restricts_paths(self):
        import httpx

        order_type = GraphQLObjectType(
            "Order",
            lambda: {"id": GraphQLField(GraphQLInt)},  # type: ignore[arg-type]
        )
        product_type = GraphQLObjectType(
            "Product",
            lambda: {"sku": GraphQLField(GraphQLString)},  # type: ignore[arg-type]
        )
        query_type = GraphQLObjectType(
            "Query",
            {
                "orders": GraphQLField(GraphQLList(order_type)),  # type: ignore[arg-type]
                "products": GraphQLField(GraphQLList(product_type)),  # type: ignore[arg-type]
            },
        )
        schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]

        from fastapi import FastAPI

        from provisa.api.app import AppState
        from provisa.api.rest.generator import create_rest_router
        from provisa.compiler.rls import RLSContext

        state = AppState()
        state.schemas = {"admin": schema}
        state.contexts = {}
        state.rls_contexts = {"admin": RLSContext.empty()}
        state.masking_rules = {}
        state.table_path_maps = {
            "admin": {
                "orders": {
                    "schema_name": "public",
                    "table_name": "orders",
                    "domain_id": "sales",
                    "table_description": None,
                    "domain_description": None,
                },
                "products": {
                    "schema_name": "public",
                    "table_name": "products",
                    "domain_id": "catalog",
                    "table_description": None,
                    "domain_description": None,
                },
            }
        }

        app = FastAPI()
        app.include_router(create_rest_router(state))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/data/rest/openapi.json?role=admin&domains=sales")

        paths = resp.json()["paths"]
        assert "/sales/orders" in paths
        assert "/catalog/products" not in paths

    async def test_download_flag_sets_content_disposition(self, client):
        async with client:
            resp = await client.get("/data/rest/openapi.json?role=admin&download=1")
        assert "attachment" in resp.headers.get("content-disposition", "")

    async def test_components_schemas_present(self, client):
        async with client:
            resp = await client.get("/data/rest/openapi.json?role=admin")
        body = resp.json()
        assert "components" in body
        assert "schemas" in body["components"]
        assert "Order" in body["components"]["schemas"]

    async def test_get_operation_has_all_parameters(self, client):
        async with client:
            resp = await client.get("/data/rest/openapi.json?role=admin")
        params = {p["name"] for p in resp.json()["paths"]["/default/orders"]["get"]["parameters"]}
        assert {"limit", "offset", "fields", "filter", "orderBy"}.issubset(params)
