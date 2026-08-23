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

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from provisa.api.rest.generator import (
    _build_aggregate_graphql_query,
    _build_group_by_graphql_query,
    _parse_aggregate_param,
    _parse_group_by_param,
    _parse_include_nodes,
    create_rest_router,
)
from provisa.compiler import naming as _naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import GraphQLValidationError, parse_query
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


def _build_schema_and_ctx_with_relationship(enable_group_by: bool = True):
    """``orders`` group-by joined to a ``customers`` relationship, for includeNodes dot-path tests."""
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "enable_aggregates": True,
            "enable_group_by": enable_group_by,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
                {"column_name": "home_region_id", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 3,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "home_regions",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
            ],
        },
    ]
    relationships = [
        {
            "id": "ord-cust",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
        {
            "id": "cust-region",
            "source_table_id": 2,
            "target_table_id": 3,
            "source_column": "home_region_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
        ],
        2: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
            _col("email", "varchar(200)"),
            _col("home_region_id", "integer"),
        ],
        3: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
        ],
    }
    role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


class TestParseIncludeNodes:
    def test_absent_returns_false(self):
        assert _parse_include_nodes({}) is False

    def test_true_returns_true(self):
        assert _parse_include_nodes({"includeNodes": "true"}) is True
        assert _parse_include_nodes({"includeNodes": "1"}) is True

    def test_bare_comma_list(self):
        assert _parse_include_nodes({"includeNodes": "user_id,user.email"}) == [
            "user_id",
            "user.email",
        ]

    def test_json_array(self):
        assert _parse_include_nodes({"includeNodes": '["user_id","user.email"]'}) == [
            "user_id",
            "user.email",
        ]

    def test_malformed_json_array_returns_false(self):
        assert _parse_include_nodes({"includeNodes": "[not valid json"}) is False


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

    def test_by_columns_translated_to_gql_convention(self):
        """REQ-1361: ?groupBy= takes API-native (physical) column names; the
        synthesized GraphQL text must use the schema's GQL-convention spelling."""
        schema, _ = _build_schema_and_ctx()
        _naming.configure(gql="apollo_graphql")
        try:
            q = _build_group_by_graphql_query(
                schema, "orders", ["user_id"], [], {}, [], None, None, None
            )
        finally:
            _naming.configure(gql="snake")
        assert "by: [userId]" in q
        assert "user_id" not in q

    def test_include_nodes_adds_nodes_selection(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_group_by_graphql_query(
            schema, "orders", ["region"], ["count"], {}, [], None, None, None, True
        )
        assert "nodes {" in q
        document = parse_query(schema, q)
        assert document is not None

    def test_without_include_nodes_omits_nodes_selection(self):
        schema, _ = _build_schema_and_ctx()
        q = _build_group_by_graphql_query(
            schema, "orders", ["region"], ["count"], {}, [], None, None, None, False
        )
        assert "nodes {" not in q

    def test_include_nodes_projection_restricts_scalar_fields(self):
        """REQ-1402: a bare (no-dot) includeNodes path selects just that scalar field."""
        schema, _ = _build_schema_and_ctx()
        q = _build_group_by_graphql_query(
            schema, "orders", ["region"], ["count"], {}, [], None, None, None, ["amount"]
        )
        assert "nodes { amount }" in q
        assert "status" not in q
        document = parse_query(schema, q)
        assert document is not None

    def test_include_nodes_projection_unknown_field_rejected_by_compiler(self):
        """REQ-1402: no local schema check — an unknown path is still emitted (translated via
        apply_gql_name) and left for parse_query's own validation to reject, same as by_cols."""
        schema, _ = _build_schema_and_ctx()
        q = _build_group_by_graphql_query(
            schema, "orders", ["region"], ["count"], {}, [], None, None, None, ["not_a_field"]
        )
        assert "nodes { not_a_field }" in q
        with pytest.raises(GraphQLValidationError):
            parse_query(schema, q)

    def test_include_nodes_dot_path_selects_relationship_scalar(self):
        """REQ-1402: "customer.email" projects nodes { customer { email } } — one level deep."""
        schema, _ = _build_schema_and_ctx_with_relationship()
        q = _build_group_by_graphql_query(
            schema,
            "orders",
            ["region"],
            ["count"],
            {},
            [],
            None,
            None,
            None,
            ["customer.email"],
        )
        assert "nodes { customer { email } }" in q
        document = parse_query(schema, q)
        assert document is not None

    def test_include_nodes_dot_path_unknown_relationship_rejected_by_compiler(self):
        schema, _ = _build_schema_and_ctx_with_relationship()
        q = _build_group_by_graphql_query(
            schema,
            "orders",
            ["region"],
            ["count"],
            {},
            [],
            None,
            None,
            None,
            ["vendor.email"],
        )
        assert "nodes { vendor { email } }" in q
        with pytest.raises(GraphQLValidationError):
            parse_query(schema, q)

    def test_include_nodes_dot_path_unknown_nested_field_rejected_by_compiler(self):
        schema, _ = _build_schema_and_ctx_with_relationship()
        q = _build_group_by_graphql_query(
            schema,
            "orders",
            ["region"],
            ["count"],
            {},
            [],
            None,
            None,
            None,
            ["customer.not_a_field"],
        )
        assert "nodes { customer { not_a_field } }" in q
        with pytest.raises(GraphQLValidationError):
            parse_query(schema, q)

    def test_include_nodes_dot_path_two_levels_deep(self):
        """REQ-1402: depth is bounded only by the schema's own relationships, not by this code —
        "customer.home_region.name" resolves through two hops (orders -> customers -> home_regions)."""
        schema, _ = _build_schema_and_ctx_with_relationship()
        q = _build_group_by_graphql_query(
            schema,
            "orders",
            ["region"],
            ["count"],
            {},
            [],
            None,
            None,
            None,
            ["customer.home_region.name"],
        )
        assert "nodes { customer { home_region { name } } }" in q
        document = parse_query(schema, q)
        assert document is not None

    def test_include_nodes_mixes_base_and_relationship_paths(self):
        schema, _ = _build_schema_and_ctx_with_relationship()
        q = _build_group_by_graphql_query(
            schema,
            "orders",
            ["region"],
            ["count"],
            {},
            [],
            None,
            None,
            None,
            ["amount", "customer.email", "customer.name"],
        )
        assert "nodes { amount customer { email name } }" in q
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

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", fake_govern)
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
