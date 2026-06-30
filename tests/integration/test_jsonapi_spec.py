# Copyright (c) 2026 Kenneth Stott
# Canary: e4f7a1b5-c6d8-9012-3ef0-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration tests for JSON:API OpenAPI 3.1 spec endpoint GET /data/jsonapi/openapi.json (REQ-805)."""

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
    from provisa.api.jsonapi.generator import create_jsonapi_router
    from provisa.compiler.rls import RLSContext

    pet_type = GraphQLObjectType(
        "Pet",
        lambda: {
            "id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "name": GraphQLField(GraphQLString),  # type: ignore[arg-type]
            "age": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {"pets": GraphQLField(GraphQLList(pet_type))},  # type: ignore[arg-type]
    )
    schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]

    state = AppState()
    state.schemas = {role_id: schema}
    state.contexts = {}
    state.rls_contexts = {role_id: RLSContext.empty()}
    state.masking_rules = {}
    state.table_path_maps = {
        role_id: {
            "pets": {
                "schema_name": "public",
                "table_name": "Pets",
                "domain_id": "store",
                "table_description": "Pet records",
                "domain_description": "Pet store domain",
            }
        }
    }
    state.schema_build_cache = {"domains": [{"id": "store", "description": "Pet store domain"}]}

    app = FastAPI()
    app.include_router(create_jsonapi_router(state))
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
            resp = await client.get("/data/jsonapi/openapi.json?role=admin")
        assert resp.status_code == 200

    async def test_openapi_version(self, client):
        async with client:
            resp = await client.get("/data/jsonapi/openapi.json?role=admin")
        assert resp.json()["openapi"] == "3.1.0"

    async def test_paths_include_table(self, client):
        async with client:
            resp = await client.get("/data/jsonapi/openapi.json?role=admin")
        assert "/store/Pets" in resp.json()["paths"]

    async def test_unknown_role_returns_empty_paths(self, client):
        async with client:
            resp = await client.get("/data/jsonapi/openapi.json?role=ghost")
        assert resp.json()["paths"] == {}

    async def test_response_content_type_is_json(self, client):
        async with client:
            resp = await client.get("/data/jsonapi/openapi.json?role=admin")
        assert "application/json" in resp.headers["content-type"]

    async def test_parameters_include_page_size(self, client):
        async with client:
            resp = await client.get("/data/jsonapi/openapi.json?role=admin")
        params = {p["name"] for p in resp.json()["paths"]["/store/Pets"]["get"]["parameters"]}
        assert "page[size]" in params

    async def test_parameters_include_sparse_fieldsets(self, client):
        async with client:
            resp = await client.get("/data/jsonapi/openapi.json?role=admin")
        params = {p["name"] for p in resp.json()["paths"]["/store/Pets"]["get"]["parameters"]}
        assert "fields[Pets]" in params

    async def test_parameters_include_filter_operators(self, client):
        async with client:
            resp = await client.get("/data/jsonapi/openapi.json?role=admin")
        params = {p["name"] for p in resp.json()["paths"]["/store/Pets"]["get"]["parameters"]}
        assert "filter[name]" in params
        assert "filter[name][like]" in params

    async def test_domain_filter_restricts_paths(self):
        import httpx

        cat_type = GraphQLObjectType(
            "Cat",
            lambda: {"id": GraphQLField(GraphQLInt)},  # type: ignore[arg-type]
        )
        dog_type = GraphQLObjectType(
            "Dog",
            lambda: {"id": GraphQLField(GraphQLInt)},  # type: ignore[arg-type]
        )
        query_type = GraphQLObjectType(
            "Query",
            {
                "cats": GraphQLField(GraphQLList(cat_type)),  # type: ignore[arg-type]
                "dogs": GraphQLField(GraphQLList(dog_type)),  # type: ignore[arg-type]
            },
        )
        schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]

        from fastapi import FastAPI

        from provisa.api.app import AppState
        from provisa.api.jsonapi.generator import create_jsonapi_router
        from provisa.compiler.rls import RLSContext

        state = AppState()
        state.schemas = {"admin": schema}
        state.contexts = {}
        state.rls_contexts = {"admin": RLSContext.empty()}
        state.masking_rules = {}
        state.table_path_maps = {
            "admin": {
                "cats": {
                    "schema_name": "public",
                    "table_name": "Cats",
                    "domain_id": "feline",
                    "table_description": None,
                    "domain_description": None,
                },
                "dogs": {
                    "schema_name": "public",
                    "table_name": "Dogs",
                    "domain_id": "canine",
                    "table_description": None,
                    "domain_description": None,
                },
            }
        }
        state.schema_build_cache = {"domains": []}

        app = FastAPI()
        app.include_router(create_jsonapi_router(state))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/data/jsonapi/openapi.json?role=admin&domains=feline")

        paths = resp.json()["paths"]
        assert "/feline/Cats" in paths
        assert "/canine/Dogs" not in paths

    async def test_response_schema_has_data_array(self, client):
        async with client:
            resp = await client.get("/data/jsonapi/openapi.json?role=admin")
        content = resp.json()["paths"]["/store/Pets"]["get"]["responses"]["200"]["content"]
        schema = content["application/vnd.api+json"]["schema"]
        assert schema["properties"]["data"]["type"] == "array"
