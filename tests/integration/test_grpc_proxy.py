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


def _make_app(state, monkeypatch):
    from fastapi import FastAPI
    from provisa.api.data.endpoint_grpc_proxy import router

    app = FastAPI()

    # Swap the module-level state reference for this test only. A bare assignment would leave this
    # stub AppState installed process-wide, and every later module in the session would then read an
    # empty org runtime (no tenant_db) through provisa.api.app.state.
    import provisa.api.app as app_module

    monkeypatch.setattr(app_module, "state", state)
    app.include_router(router)
    return app


@pytest.fixture
def state():
    return _make_schema_and_state()


@pytest.fixture
def client(state, monkeypatch):
    import httpx

    app = _make_app(state, monkeypatch)
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
    """The proxy lowers the request to a semantic SELECT, governs+routes it, executes the plan, and
    keys each row by its proto field name. These patch the real pipeline seams (semantic-SQL → govern
    → execute), not the retired GraphQL round-trip."""

    @staticmethod
    def _patches(*, result=None, govern_side_effect=None):
        semantic = patch(
            "provisa.api.data.endpoint_grpc_proxy.grpc_table_to_semantic_sql",
            return_value="SELECT id, name FROM pets",
        )
        govern_kwargs = (
            {"side_effect": govern_side_effect}
            if govern_side_effect is not None
            else {"return_value": MagicMock()}
        )
        govern = patch(
            "provisa.api.data.endpoint_grpc_proxy._govern_and_route_compiled",
            new_callable=AsyncMock,
            **govern_kwargs,
        )
        execute = patch(
            "provisa.api.data.endpoint_grpc_proxy._execute_plan",
            new_callable=AsyncMock,
            return_value=result,
        )
        return semantic, govern, execute

    async def test_valid_request_returns_200(self, state, monkeypatch):
        app = _make_app(state, monkeypatch)
        import httpx

        transport = httpx.ASGITransport(app=app)
        c = httpx.AsyncClient(transport=transport, base_url="http://test")

        result = MagicMock()
        result.column_names = ["id", "name"]
        result.rows = []

        semantic, govern, execute = self._patches(result=result)
        with semantic, govern, execute:
            async with c:
                resp = await c.post("/data/grpc/Pet", json={"role_id": "analyst", "limit": 10})
        assert resp.status_code == 200

    async def test_response_is_list(self, state, monkeypatch):
        app = _make_app(state, monkeypatch)
        import httpx

        transport = httpx.ASGITransport(app=app)
        c = httpx.AsyncClient(transport=transport, base_url="http://test")

        result = MagicMock()
        result.column_names = ["id", "name"]
        result.rows = [[1, "Fido"]]

        semantic, govern, execute = self._patches(result=result)
        with semantic, govern, execute:
            async with c:
                resp = await c.post("/data/grpc/Pet", json={"role_id": "analyst"})
        assert isinstance(resp.json(), list)

    async def test_governance_denial_returns_403(self, state, monkeypatch):
        app = _make_app(state, monkeypatch)
        import httpx

        transport = httpx.ASGITransport(app=app)
        c = httpx.AsyncClient(transport=transport, base_url="http://test")

        semantic, govern, execute = self._patches(
            govern_side_effect=PermissionError("Access denied")
        )
        with semantic, govern, execute:
            async with c:
                resp = await c.post("/data/grpc/Pet", json={"role_id": "analyst"})
        assert resp.status_code == 403


class TestGroupByIncludeNodes:
    """REQ-1401/REQ-1408: the proxy is the gRPC Explorer's transport, so a body carrying
    include_nodes/include must produce the same nodes-bearing rows the native servicer yields —
    dropping them made the Explorer answer a narrower query than the one it displayed."""

    @staticmethod
    def _column(**kw):
        col = MagicMock()
        for k, v in kw.items():
            setattr(col, k, v)
        return col

    async def _post(self, state, monkeypatch, body):
        import httpx

        app = _make_app(state, monkeypatch)
        c = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")

        compiled = MagicMock()
        compiled.sql = "SELECT 1"
        compiled.params = None
        compiled.columns = ["status", "count"]
        compiled.nodes_sql = "SELECT 2"
        compiled.nodes_params = None
        compiled.nodes_columns = [
            self._column(nested_in="__join_key__", field_name="status"),
            self._column(nested_in=None, field_name="id"),
            self._column(nested_in=None, field_name="user"),
        ]

        agg_result = MagicMock(rows=[["open", 2]])
        nodes_result = MagicMock(rows=[["open", 7, {"email": "a@b.c"}]])

        gql_text = patch(
            "provisa.api.data.endpoint_grpc_proxy.grpc_table_to_group_by_graphql_text",
            return_value="{ inquiries_group_by(by: [status]) { nodes { id user { email } } } }",
        )
        parse = patch("provisa.compiler.parser.parse_query", return_value=MagicMock())
        compile_ = patch("provisa.compiler.sql_gen.compile_query", return_value=[compiled])
        split_gb = patch(
            "provisa.api.data.endpoint_grpc_proxy.split_group_by_columns",
            return_value=([self._column(column="status")], [0], ["count"], [1]),
        )
        split_agg = patch(
            "provisa.api.data.endpoint_grpc_proxy.split_agg_columns",
            return_value=({"count": 2}, {}),
        )
        govern = patch(
            "provisa.api.data.endpoint_grpc_proxy._govern_and_route_compiled",
            new_callable=AsyncMock,
            return_value=MagicMock(),
        )
        execute = patch(
            "provisa.api.data.endpoint_grpc_proxy._execute_plan",
            new_callable=AsyncMock,
            side_effect=[agg_result, nodes_result],
        )
        with gql_text as gql_mock, parse, compile_, split_gb, split_agg, govern, execute:
            async with c:
                resp = await c.post("/data/grpc/InquiriesGroupBy", json=body)
        return resp, gql_mock

    async def test_include_flags_reach_the_graphql_synthesis(self, state, monkeypatch):
        resp, gql_mock = await self._post(
            state,
            monkeypatch,
            {
                "role_id": "analyst",
                "by": ["status"],
                "include_nodes": True,
                "include": ["user.email"],
            },
        )
        assert resp.status_code == 200
        assert gql_mock.call_args.kwargs["include_nodes"] is True
        assert gql_mock.call_args.kwargs["include"] == ["user.email"]

    async def test_nodes_are_joined_onto_their_group(self, state, monkeypatch):
        resp, _ = await self._post(
            state,
            monkeypatch,
            {"role_id": "analyst", "by": ["status"], "include_nodes": True},
        )
        assert resp.status_code == 200
        assert resp.json() == [
            {
                "group_key": {"status": "open"},
                "aggregate": {"count": 2},
                "nodes": [{"id": 7, "user": {"email": "a@b.c"}}],
            }
        ]
