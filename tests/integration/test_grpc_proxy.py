# Copyright (c) 2026 Kenneth Stott
# Canary: c2d5e9f3-a4b6-7890-1cde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration tests for HTTP→gRPC proxy endpoint (REQ-803).

Tests JSON→GraphQL translation, read_mask filtering, and filter argument
extraction at the endpoint boundary. Query execution is mocked — this
boundary is about translation logic, not DB execution.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _make_schema_and_state(role_id: str = "analyst"):
    from provisa.compiler.rls import RLSContext

    pet_type = GraphQLObjectType(
        "Pet",
        lambda: {
            "id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "name": GraphQLField(GraphQLString),  # type: ignore[arg-type]
            "age": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "weight": GraphQLField(GraphQLFloat),  # type: ignore[arg-type]
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {
            "pets": GraphQLField(
                GraphQLList(pet_type),  # type: ignore[arg-type]
                args={
                    "limit": GraphQLArgument(GraphQLInt),  # type: ignore[arg-type]
                },
            )
        },
    )
    schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]

    from provisa.api.app import AppState

    state = AppState()
    state.schemas = {role_id: schema}
    state.contexts = {role_id: MagicMock()}
    state.rls_contexts = {role_id: RLSContext.empty()}
    state.masking_rules = {}
    return state


def _make_app(state):
    from fastapi import FastAPI
    from provisa.api.data.endpoint_grpc_proxy import router

    app = FastAPI()

    # Patch the module-level state reference
    import provisa.api.app as app_module

    app_module.state = state
    app.include_router(router)
    return app


@pytest.fixture
def state():
    return _make_schema_and_state()


@pytest.fixture
def client(state):
    import httpx

    app = _make_app(state)
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


class TestMissingRole:
    async def test_missing_role_header_and_body_returns_400(self, client):
        async with client:
            resp = await client.post("/data/grpc/Pet", json={})
        assert resp.status_code == 400
        assert "role" in resp.json()["detail"].lower()


class TestUnknownRole:
    async def test_unknown_role_returns_404(self, client):
        async with client:
            resp = await client.post("/data/grpc/Pet", json={"role_id": "ghost"})
        assert resp.status_code == 404


class TestUnknownType:
    async def test_unknown_type_name_returns_404(self, client):
        async with client:
            resp = await client.post("/data/grpc/UnknownType", json={"role_id": "analyst"})
        assert resp.status_code == 404


class TestTranslation:
    async def _post(self, client, body: dict):

        mock_result = MagicMock()
        mock_result.rows = [{"id": 1, "name": "Fido", "age": 3, "weight": 10.5}]

        fake_compiled = SimpleNamespace(
            sql="SELECT ...",
            params=None,
            columns=["id", "name", "age", "weight"],
        )

        with (
            patch(
                "provisa.api.data.endpoint_grpc_proxy.parse_query",
                return_value=MagicMock(),
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy.compile_query",
                return_value=[fake_compiled],
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy._govern_and_route_compiled",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy._execute_plan",
                new_callable=AsyncMock,
                return_value=mock_result,
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy.serialize_rows",
                return_value={"data": {"pets": [{"id": 1, "name": "Fido"}]}},
            ),
        ):
            async with client:
                return await client.post("/data/grpc/Pet", json=body)

    async def test_valid_request_returns_200(self, client, state):
        app = _make_app(state)

        import httpx

        transport = httpx.ASGITransport(app=app)
        c = httpx.AsyncClient(transport=transport, base_url="http://test")

        mock_result = MagicMock()
        mock_result.rows = []
        fake_compiled = SimpleNamespace(sql="SELECT ...", params=None, columns=["id", "name"])

        with (
            patch("provisa.api.data.endpoint_grpc_proxy.parse_query", return_value=MagicMock()),
            patch(
                "provisa.api.data.endpoint_grpc_proxy.compile_query",
                return_value=[fake_compiled],
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy._govern_and_route_compiled",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy._execute_plan",
                new_callable=AsyncMock,
                return_value=mock_result,
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy.serialize_rows",
                return_value={"data": {"pets": []}},
            ),
        ):
            async with c:
                resp = await c.post(
                    "/data/grpc/Pet",
                    json={"role_id": "analyst", "limit": 10},
                )
        assert resp.status_code == 200

    async def test_response_is_list(self, state):
        app = _make_app(state)
        import httpx

        transport = httpx.ASGITransport(app=app)
        c = httpx.AsyncClient(transport=transport, base_url="http://test")

        mock_result = MagicMock()
        mock_result.rows = []
        fake_compiled = SimpleNamespace(sql="SELECT ...", params=None, columns=["id"])

        with (
            patch("provisa.api.data.endpoint_grpc_proxy.parse_query", return_value=MagicMock()),
            patch(
                "provisa.api.data.endpoint_grpc_proxy.compile_query",
                return_value=[fake_compiled],
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy._govern_and_route_compiled",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy._execute_plan",
                new_callable=AsyncMock,
                return_value=mock_result,
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy.serialize_rows",
                return_value={"data": {"pets": [{"id": 1}]}},
            ),
        ):
            async with c:
                resp = await c.post(
                    "/data/grpc/Pet",
                    json={"role_id": "analyst"},
                )
        assert isinstance(resp.json(), list)

    async def test_governance_denial_returns_403(self, state):
        app = _make_app(state)
        import httpx

        transport = httpx.ASGITransport(app=app)
        c = httpx.AsyncClient(transport=transport, base_url="http://test")

        fake_compiled = SimpleNamespace(sql="SELECT ...", params=None, columns=["id"])

        with (
            patch("provisa.api.data.endpoint_grpc_proxy.parse_query", return_value=MagicMock()),
            patch(
                "provisa.api.data.endpoint_grpc_proxy.compile_query",
                return_value=[fake_compiled],
            ),
            patch(
                "provisa.api.data.endpoint_grpc_proxy._govern_and_route_compiled",
                new_callable=AsyncMock,
                side_effect=PermissionError("Access denied"),
            ),
        ):
            async with c:
                resp = await c.post(
                    "/data/grpc/Pet",
                    json={"role_id": "analyst"},
                )
        assert resp.status_code == 403
