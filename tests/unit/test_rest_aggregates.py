# Copyright (c) 2026 Kenneth Stott
# Canary: b4e7f1a2-9c3d-4e5f-8a6b-1d2c3e4f5a6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for REST aggregate/groupBy support (REQ-1359)."""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from starlette.testclient import TestClient

from provisa.api.rest.generator import (
    _build_aggregate_graphql_query,
    _build_group_by_graphql_query,
    _parse_aggregate_param,
    _parse_group_by_param,
    create_rest_router,
)
from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.executor.result import QueryResult


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx(enable_aggregates: bool = True, enable_group_by: bool = True):
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "enable_aggregates": enable_aggregates,
            "enable_group_by": enable_group_by,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
            _col("status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
    }
    role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


class TestParseAggregateParam:
    def test_absent_returns_none(self):
        assert _parse_aggregate_param({}) is None

    def test_bare_true_returns_all(self):
        assert _parse_aggregate_param({"aggregate": "true"}) == []

    def test_empty_value_returns_all(self):
        assert _parse_aggregate_param({"aggregate": ""}) == []

    def test_explicit_funcs(self):
        assert _parse_aggregate_param({"aggregate": "count,sum,avg"}) == [
            "count",
            "sum",
            "avg",
        ]

    def test_unknown_func_filtered(self):
        assert _parse_aggregate_param({"aggregate": "count,banana"}) == ["count"]


class TestParseGroupByParam:
    def test_absent_returns_empty(self):
        assert _parse_group_by_param({}) == []

    def test_single_column(self):
        assert _parse_group_by_param({"groupBy": "region"}) == ["region"]

    def test_multiple_columns(self):
        assert _parse_group_by_param({"groupBy": "region, status"}) == ["region", "status"]


class TestBuildAggregateGraphqlQuery:
    def test_default_selection_all_funcs(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_aggregate_graphql_query(schema, "orders", [], {}, None)
        assert q.startswith("{ orders_aggregate")
        assert "count" in q
        assert "sum {" in q
        assert "avg {" in q
        assert "min {" in q
        assert "max {" in q

    def test_explicit_funcs_only(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_aggregate_graphql_query(schema, "orders", ["count", "sum"], {}, None)
        assert "count" in q
        assert "sum {" in q
        assert "avg {" not in q

    def test_with_where(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_aggregate_graphql_query(
            schema, "orders", ["count"], {"region": {"eq": "US"}}, None
        )
        assert 'where: {region: {eq: "US"}}' in q

    def test_field_filter_restricts_columns(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_aggregate_graphql_query(schema, "orders", ["sum"], {}, ["amount"])
        assert "sum { amount }" in q

    def test_query_parses_against_schema(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_aggregate_graphql_query(schema, "orders", [], {}, None)
        document = parse_query(schema, q)
        assert document is not None


class TestBuildGroupByGraphqlQuery:
    def test_basic_group_by(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_group_by_graphql_query(
            schema, "orders", ["region"], [], {}, [], None, None, None
        )
        assert q.startswith("{ orders_group_by(by: [region])")
        assert "groupKey" in q
        assert "aggregate {" in q

    def test_with_limit_offset_order(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_group_by_graphql_query(
            schema,
            "orders",
            ["region"],
            ["count"],
            {},
            [{"field": "region", "dir": "asc"}],
            5,
            10,
            None,
        )
        assert "limit: 5" in q
        assert "offset: 10" in q
        assert "order_by: {region: asc}" in q

    def test_query_parses_against_schema(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_group_by_graphql_query(
            schema, "orders", ["region"], [], {}, [], None, None, None
        )
        document = parse_query(schema, q)
        assert document is not None


def _make_state(enable_aggregates: bool = True, enable_group_by: bool = True):
    schema, ctx = _build_schema_and_ctx(enable_aggregates, enable_group_by)
    path_map = {
        "orders": {
            "domain_id": "sales",
            "table_name": "orders",
        }
    }
    return SimpleNamespace(
        schemas={"admin": schema},
        contexts={"admin": ctx},
        table_path_maps={"admin": path_map},
    )


def _make_app(state) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def set_role(request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.role = "admin"
        return await call_next(request)

    app.include_router(create_rest_router(state))
    return app


def _patch_pipeline(monkeypatch, rows, redirect=None):
    async def fake_govern(*args, **kwargs):
        return "fake-plan"

    async def fake_execute(plan, state):
        return QueryResult(rows=rows, column_names=[], redirect=redirect)

    monkeypatch.setattr(
        "provisa.pgwire._pipeline._govern_and_route_compiled", fake_govern
    )
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", fake_execute)


class TestAggregateEndpointResponseShape:
    def test_aggregate_response_is_flat(self, monkeypatch):
        state = _make_state()
        schema, ctx = state.schemas["admin"], state.contexts["admin"]
        q = _build_aggregate_graphql_query(schema, "orders", ["count"], {}, None)
        document = parse_query(schema, q)
        from provisa.compiler.sql_gen import compile_query

        compiled = compile_query(document, ctx)[0]
        row = tuple(1 for _ in compiled.columns)
        _patch_pipeline(monkeypatch, [row])

        client = TestClient(_make_app(state))
        resp = client.get("/data/rest/sales/orders", params={"aggregate": "count"})
        assert resp.status_code == 200
        body = resp.json()
        assert set(body.keys()) == {"data"}
        assert "count" in body["data"]

    def test_group_by_response_is_flat_list(self, monkeypatch):
        state = _make_state()
        schema, ctx = state.schemas["admin"], state.contexts["admin"]
        q = _build_group_by_graphql_query(
            schema, "orders", ["region"], ["count"], {}, [], None, None, None
        )
        document = parse_query(schema, q)
        from provisa.compiler.sql_gen import compile_query

        compiled = compile_query(document, ctx)[0]
        row = tuple("x" if i == 0 else 1 for i, _ in enumerate(compiled.columns))
        _patch_pipeline(monkeypatch, [row])

        client = TestClient(_make_app(state))
        resp = client.get(
            "/data/rest/sales/orders", params={"groupBy": "region", "aggregate": "count"}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert set(body.keys()) == {"data"}
        assert isinstance(body["data"], list)
        assert len(body["data"]) == 1
        assert "groupKey" in body["data"][0]
        assert "aggregate" in body["data"][0]


class TestDisabledTableReturns400:
    def test_aggregate_on_disabled_table_is_400(self, monkeypatch):
        state = _make_state(enable_aggregates=False, enable_group_by=False)
        _patch_pipeline(monkeypatch, [])
        client = TestClient(_make_app(state))
        resp = client.get("/data/rest/sales/orders", params={"aggregate": "count"})
        assert resp.status_code == 400

    def test_group_by_on_disabled_table_is_400(self, monkeypatch):
        state = _make_state(enable_aggregates=False, enable_group_by=False)
        _patch_pipeline(monkeypatch, [])
        client = TestClient(_make_app(state))
        resp = client.get("/data/rest/sales/orders", params={"groupBy": "region"})
        assert resp.status_code == 400
