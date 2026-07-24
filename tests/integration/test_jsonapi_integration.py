# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7f2d9a-c104-4e8e-b5a1-6d0c8f3e9217
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for JSON:API auto-generated endpoints.

Tests the generator→serializer component boundary with real components.
No mocks at the boundary under test; DB/execution mocked where noted.

Covered REQ-IDs:
  JSON:API Endpoints: REQ-257
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------


def _make_orders_schema():
    """Build a minimal GraphQL schema with an 'orders' root field."""
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

    order_type = GraphQLObjectType(
        "Order",
        lambda: {
            "id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "region": GraphQLField(GraphQLString),  # type: ignore[arg-type]
            "amount": GraphQLField(GraphQLFloat),  # type: ignore[arg-type]
            "created_at": GraphQLField(GraphQLString),  # type: ignore[arg-type]
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {
            "orders": GraphQLField(
                GraphQLList(order_type),
                args={
                    "limit": GraphQLArgument(GraphQLInt),
                    "offset": GraphQLArgument(GraphQLInt),
                },
            )
        },
    )
    return GraphQLSchema(query=query_type)  # type: ignore[arg-type]


def _make_orders_schema_with_customer():
    """Build a schema with orders→customer relationship."""
    from graphql import (
        GraphQLField,
        GraphQLFloat,
        GraphQLInt,
        GraphQLList,
        GraphQLObjectType,
        GraphQLSchema,
        GraphQLString,
    )

    customer_type = GraphQLObjectType(
        "Customer",
        {
            "id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "name": GraphQLField(GraphQLString),  # type: ignore[arg-type]
        },
    )
    order_type = GraphQLObjectType(
        "Order",
        lambda: {
            "id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "region": GraphQLField(GraphQLString),  # type: ignore[arg-type]
            "amount": GraphQLField(GraphQLFloat),  # type: ignore[arg-type]
            "customer_id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "customer": GraphQLField(customer_type),  # type: ignore[arg-type]
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {"orders": GraphQLField(GraphQLList(order_type))},
    )
    return GraphQLSchema(query=query_type)  # type: ignore[arg-type]


def _make_app_state(schema=None):
    """Build a minimal AppState mock for JSON:API tests.

    # integration: mock-justified — AppState is a config-populated data struct,
    # not a docker-compose service. MagicMock scaffolds only the schema/context
    # fields; the real generator and serializer run unmodified.
    """
    from provisa.compiler.sql_gen import CompilationContext, TableMeta
    from provisa.compiler.rls import RLSContext

    if schema is None:
        schema = _make_orders_schema()

    ctx = CompilationContext(
        tables={
            "orders": TableMeta(
                table_id=1,
                field_name="orders",
                type_name="Order",
                source_id="test-pg",
                catalog_name="postgresql",
                schema_name="public",
                table_name="orders",
                domain_id="default",
            )
        }
    )

    state = MagicMock()
    state.schemas = {"admin": schema}
    state.contexts = {"admin": ctx}
    state.rls_contexts = {"admin": RLSContext.empty()}
    state.roles = {
        "admin": {
            "id": "admin",
            "capabilities": ["full_results", "ad_hoc_query"],
            "domain_access": ["*"],
        }
    }
    state.masking_rules = {}
    state.source_types = {"test-pg": "postgresql"}
    state.source_dialects = {"test-pg": "postgres"}
    state.source_pools = MagicMock()
    state.engine_conn = None
    state.schema_build_cache = {"column_types": {1: []}, "tables": []}
    state.tables = []
    # Domain-scoped path map (REQ-799..804): gql field name → {domain_id, table_name}
    state.table_path_maps = {
        "admin": {
            "orders": {
                "domain_id": "default",
                "table_name": "orders",
                "schema_name": "public",
            }
        }
    }
    return state


# ---------------------------------------------------------------------------
# TestJSONAPISerializer — row_to_resource / rows_to_jsonapi boundary
# ---------------------------------------------------------------------------


class TestJSONAPISerializer:
    """REQ-257: Serializer produces compliant JSON:API resource objects."""

    def test_row_to_resource_basic_shape(self):
        # REQ-257: resource objects must have type, id, attributes
        from provisa.api.jsonapi.serializer import row_to_resource

        row = {"id": 1, "region": "US", "amount": 99.5}
        resource = row_to_resource(row, "orders")

        assert resource["type"] == "orders"
        assert resource["id"] == "1"
        assert "attributes" in resource
        assert resource["attributes"]["region"] == "US"
        assert resource["attributes"]["amount"] == 99.5

    def test_row_to_resource_id_excluded_from_attributes(self):
        # REQ-257: id must not appear in attributes
        from provisa.api.jsonapi.serializer import row_to_resource

        row = {"id": 42, "region": "EU"}
        resource = row_to_resource(row, "orders")

        assert "id" not in resource["attributes"]

    def test_row_to_resource_relationship_fields(self):
        # REQ-257: FK columns become relationship objects, not attributes
        from provisa.api.jsonapi.serializer import row_to_resource

        row = {"id": 1, "customer_id": 7, "amount": 50.0}
        resource = row_to_resource(row, "orders", relationship_fields={"customer_id": "customers"})

        assert "customer_id" not in resource["attributes"]
        assert "relationships" in resource
        assert "customer" in resource["relationships"]
        rel_data = resource["relationships"]["customer"]["data"]
        assert rel_data["type"] == "customers"
        assert rel_data["id"] == "7"

    def test_row_to_resource_null_relationship(self):
        # REQ-257: null FK produces null relationship data
        from provisa.api.jsonapi.serializer import row_to_resource

        row = {"id": 1, "customer_id": None, "amount": 10.0}
        resource = row_to_resource(row, "orders", relationship_fields={"customer_id": "customers"})

        assert resource["relationships"]["customer"]["data"] is None

    def test_rows_to_jsonapi_document_structure(self):
        # REQ-257: document must have data list and meta.total
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        rows = [{"id": 1, "region": "US"}, {"id": 2, "region": "EU"}]
        doc = rows_to_jsonapi(rows, "orders")

        assert "data" in doc
        assert isinstance(doc["data"], list)
        assert len(doc["data"]) == 2
        assert doc["meta"]["total"] == 2

    def test_rows_to_jsonapi_compound_document_with_included(self):
        # REQ-257: included sideloaded resources in compound documents
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        rows = [{"id": 1, "region": "US", "customer_id": 10}]
        included_rows = {"customer": [{"id": 10, "name": "Alice"}]}
        doc = rows_to_jsonapi(
            rows,
            "orders",
            relationship_fields={"customer_id": "customers"},
            included_rows=included_rows,
        )

        assert "included" in doc
        assert len(doc["included"]) == 1
        inc = doc["included"][0]
        assert inc["type"] == "customer"
        assert inc["id"] == "10"
        assert inc["attributes"]["name"] == "Alice"

    def test_rows_to_jsonapi_empty(self):
        # REQ-257: empty result set returns empty data list
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        doc = rows_to_jsonapi([], "orders")

        assert doc["data"] == []
        assert doc["meta"]["total"] == 0


# ---------------------------------------------------------------------------
# TestJSONAPIPagination — pagination helpers at the boundary
# ---------------------------------------------------------------------------


class TestJSONAPIPagination:
    """REQ-257: Pagination helpers compute correct limit/offset and links."""

    def test_parse_page_params_defaults(self):
        # REQ-257: defaults to page 1, size 25
        from provisa.api.jsonapi.pagination import DEFAULT_PAGE_SIZE, parse_page_params

        page = parse_page_params({})

        assert page["number"] == 1
        assert page["size"] == DEFAULT_PAGE_SIZE

    def test_parse_page_params_explicit(self):
        # REQ-257: page[number] and page[size] parsed correctly
        from provisa.api.jsonapi.pagination import parse_page_params

        page = parse_page_params({"page[number]": "3", "page[size]": "10"})

        assert page["number"] == 3
        assert page["size"] == 10

    def test_parse_page_params_clamped_min(self):
        # REQ-257: page number below 1 is clamped to 1
        from provisa.api.jsonapi.pagination import parse_page_params

        page = parse_page_params({"page[number]": "0", "page[size]": "0"})

        assert page["number"] == 1
        assert page["size"] == 1

    def test_parse_page_params_clamped_max_size(self):
        # REQ-257: page size above MAX_PAGE_SIZE is clamped
        from provisa.api.jsonapi.pagination import MAX_PAGE_SIZE, parse_page_params

        page = parse_page_params({"page[size]": "9999"})

        assert page["size"] == MAX_PAGE_SIZE

    def test_page_to_limit_offset_page1(self):
        # REQ-257: first page has offset 0
        from provisa.api.jsonapi.pagination import page_to_limit_offset

        limit, offset = page_to_limit_offset({"number": 1, "size": 25})

        assert limit == 25
        assert offset == 0

    def test_page_to_limit_offset_page2(self):
        # REQ-257: second page of size 10 has offset 10
        from provisa.api.jsonapi.pagination import page_to_limit_offset

        limit, offset = page_to_limit_offset({"number": 2, "size": 10})

        assert limit == 10
        assert offset == 10

    def test_page_to_limit_offset_page3(self):
        # REQ-257: third page of size 25 has offset 50
        from provisa.api.jsonapi.pagination import page_to_limit_offset

        limit, offset = page_to_limit_offset({"number": 3, "size": 25})

        assert limit == 25
        assert offset == 50

    def test_build_pagination_links_first_page_no_prev(self):
        # REQ-257: first page has no prev link
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links("/data/jsonapi/orders", 1, 25, 25)

        assert links["prev"] is None
        assert links["first"] is not None
        assert links["self"] is not None

    def test_build_pagination_links_has_next_when_full_page(self):
        # REQ-257: next link present when total > page_size (more pages exist)
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links("/data/jsonapi/orders", 1, 25, 50)

        assert links["next"] is not None
        assert "page%5Bnumber%5D=2" in links["next"] or "page[number]=2" in links["next"]

    def test_build_pagination_links_no_next_on_last_page(self):
        # REQ-257: next is None when result_count < page_size (last page)
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links("/data/jsonapi/orders", 2, 25, 10)

        assert links["next"] is None

    def test_build_pagination_links_prev_on_later_page(self):
        # REQ-257: prev link present on page > 1
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links("/data/jsonapi/orders", 3, 10, 10)

        assert links["prev"] is not None


# ---------------------------------------------------------------------------
# TestJSONAPIGeneratorRoutes — router registration
# ---------------------------------------------------------------------------


class TestJSONAPIGeneratorRoutes:
    """REQ-257: Router registers /data/jsonapi/{domain_id}/{table_name} for each schema table."""

    def test_router_registered_for_orders(self):
        # REQ-257 / REQ-799..804: domain-scoped route /data/jsonapi/{domain_id}/{table_name}
        from provisa.api.jsonapi.generator import create_jsonapi_router

        state = _make_app_state()
        router = create_jsonapi_router(state)

        paths = [route.path for route in router.routes]  # type: ignore[attr-defined]
        assert any("{domain_id}" in p and "{table_name}" in p for p in paths)

    def test_router_has_prefix(self):
        # REQ-257: router is mounted under /data/jsonapi
        from provisa.api.jsonapi.generator import create_jsonapi_router

        state = _make_app_state()
        router = create_jsonapi_router(state)

        assert router.prefix == "/data/jsonapi"

    def test_router_returns_api_router(self):
        # REQ-257: return value is a FastAPI APIRouter
        from fastapi import APIRouter
        from provisa.api.jsonapi.generator import create_jsonapi_router

        state = _make_app_state()
        router = create_jsonapi_router(state)

        assert isinstance(router, APIRouter)


# ---------------------------------------------------------------------------
# TestJSONAPISparseFieldsets — _parse_sparse_fieldsets
# ---------------------------------------------------------------------------


class TestJSONAPISparseFieldsets:
    """REQ-257: Sparse fieldsets parsed from ?fields[orders]=amount."""

    def test_parse_sparse_returns_requested_fields(self):
        # REQ-257: only requested fields returned from sparse fieldset param
        from provisa.api.jsonapi.generator import _parse_sparse_fieldsets

        result = _parse_sparse_fieldsets({"fields[orders]": "amount,region"}).get("orders")

        assert result == ["amount", "region"]

    def test_parse_sparse_single_field(self):
        # REQ-257: sparse fieldset with a single field
        from provisa.api.jsonapi.generator import _parse_sparse_fieldsets

        result = _parse_sparse_fieldsets({"fields[orders]": "amount"}).get("orders")

        assert result == ["amount"]

    def test_parse_sparse_none_when_absent(self):
        # REQ-257: no sparse param → None (all fields)
        from provisa.api.jsonapi.generator import _parse_sparse_fieldsets

        result = _parse_sparse_fieldsets({}).get("orders")

        assert result is None

    def test_parse_sparse_strips_whitespace(self):
        # REQ-257: whitespace around field names is stripped
        from provisa.api.jsonapi.generator import _parse_sparse_fieldsets

        result = _parse_sparse_fieldsets({"fields[orders]": " amount , region "}).get("orders")

        assert result == ["amount", "region"]

    def test_serializer_respects_sparse_fields(self):
        # REQ-257: serializer attributes only contain requested fields
        from provisa.api.jsonapi.serializer import row_to_resource

        row = {"id": 1, "amount": 50.0, "region": "US", "created_at": "2026-01-01"}
        # Sparse fieldset: only amount requested — caller selects which fields are in row
        sparse_row = {"id": row["id"], "amount": row["amount"]}
        resource = row_to_resource(sparse_row, "orders")

        assert "amount" in resource["attributes"]
        assert "region" not in resource["attributes"]
        assert "created_at" not in resource["attributes"]


# ---------------------------------------------------------------------------
# TestJSONAPIFiltering — _parse_filters
# ---------------------------------------------------------------------------


class TestJSONAPIFiltering:
    """REQ-257: Filter params parsed and propagated into WHERE clause."""

    def test_parse_filters_simple_eq(self):
        # REQ-257: filter[col]=val maps to {col: {eq: val}}
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"filter[region]": "US"})

        assert result == {"region": {"eq": "US"}}

    def test_parse_filters_operator_syntax(self):
        # REQ-257: filter[col][op]=val maps to {col: {op: val}}
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"filter[amount][gt]": "100"})

        assert result == {"amount": {"gt": "100"}}

    def test_parse_filters_in_operator_splits_csv(self):
        # REQ-257: filter[col][in]=a,b,c maps to list value
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"filter[region][in]": "US,EU,APAC"})

        assert result == {"region": {"in": ["US", "EU", "APAC"]}}

    def test_parse_filters_ignores_non_filter_params(self):
        # REQ-257: non-filter params are ignored
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"sort": "-created_at", "page[number]": "1"})

        assert result == {}

    def test_build_graphql_query_includes_where_clause(self):
        # REQ-257: filter translates into GraphQL where argument
        from provisa.api.jsonapi.generator import _build_graphql_query

        gql = _build_graphql_query(
            "orders",
            ["id", "region"],
            {"region": {"eq": "US"}},
            [],
            None,
            None,
        )

        assert "where" in gql
        assert "region" in gql
        assert "US" in gql


# ---------------------------------------------------------------------------
# TestJSONAPISorting — _parse_sort
# ---------------------------------------------------------------------------


class TestJSONAPISorting:
    """REQ-257: Sort param parsed; descending prefix '-' respected."""

    def test_parse_sort_descending(self):
        # REQ-257: sort=-created_at → desc direction
        from provisa.api.jsonapi.generator import _parse_sort

        result = _parse_sort("-created_at")

        assert result == [{"field": "created_at", "dir": "desc"}]

    def test_parse_sort_ascending(self):
        # REQ-257: sort=amount → asc direction
        from provisa.api.jsonapi.generator import _parse_sort

        result = _parse_sort("amount")

        assert result == [{"field": "amount", "dir": "asc"}]

    def test_parse_sort_multiple_fields(self):
        # REQ-257: comma-separated sort fields produce ordered list
        from provisa.api.jsonapi.generator import _parse_sort

        result = _parse_sort("-created_at,amount")

        assert result == [
            {"field": "created_at", "dir": "desc"},
            {"field": "amount", "dir": "asc"},
        ]

    def test_parse_sort_none_returns_empty(self):
        # REQ-257: absent sort param returns empty list
        from provisa.api.jsonapi.generator import _parse_sort

        result = _parse_sort(None)

        assert result == []

    def test_build_graphql_query_includes_order_by(self):
        # REQ-257: sort translates into GraphQL order_by argument
        from provisa.api.jsonapi.generator import _build_graphql_query

        gql = _build_graphql_query(
            "orders",
            ["id", "created_at"],
            {},
            [{"field": "created_at", "dir": "desc"}],
            None,
            None,
        )

        assert "order_by" in gql
        assert "created_at" in gql
        assert "desc" in gql


# ---------------------------------------------------------------------------
# TestJSONAPIPaginationHTTP — full HTTP round-trip via ASGI transport
# ---------------------------------------------------------------------------


class TestJSONAPIPaginationHTTP:
    """REQ-257: Paginated HTTP GET returns data list + pagination links/meta."""

    def _build_app_with_stub_execution(self):
        """Build a FastAPI app with the JSON:API router; stub the pipeline.

        # integration: mock-justified — the database and pipeline are external
        # services not under test here. The generator→serializer boundary is real.
        """
        from fastapi import FastAPI
        from provisa.api.jsonapi.generator import create_jsonapi_router
        from provisa.auth.middleware import AuthMiddleware

        state = _make_app_state()
        router = create_jsonapi_router(state)

        app = FastAPI()
        app.add_middleware(AuthMiddleware)
        app.include_router(router)
        return app, state

    @pytest.mark.anyio
    async def test_pagination_response_has_data_and_links(self, monkeypatch):
        # REQ-257: paginated response includes data list and pagination links
        import httpx

        stub_rows = [
            {"id": 1, "region": "US", "amount": 100.0, "created_at": "2026-01-01"},
            {"id": 2, "region": "EU", "amount": 200.0, "created_at": "2026-01-02"},
        ]

        # integration: mock-justified — pipeline execution is an external boundary;
        # real generator+serializer run against the stub rows below.
        async def _fake_govern(sql, role_id, exec_params, state, deliver=None):
            # The count query wraps the compiled SELECT in COUNT(*); tag the plan so the
            # fake terminal returns a scalar count row rather than the full data rows.
            plan = MagicMock()
            plan._is_count = "COUNT(*)" in sql
            return plan

        async def _fake_execute(plan, state):
            result = MagicMock()
            result.redirect = None  # non-materialized: real QueryResult.redirect defaults None
            if getattr(plan, "_is_count", False):
                result.rows = [[len(stub_rows)]]  # engine-side COUNT(*) → single scalar row
            else:
                result.rows = [list(r.values()) for r in stub_rows]
                result.columns = list(stub_rows[0].keys())
            return result

        monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
        monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)

        # Also stub serialize_rows to return predictable data
        def _fake_serialize(rows, columns, table):
            col_names = [getattr(c, "field_name", c) for c in columns]
            col_rows = [dict(zip(col_names, r)) for r in rows]
            return {"data": {table: col_rows}}

        monkeypatch.setattr("provisa.executor.serialize.serialize_rows", _fake_serialize)

        app, _ = self._build_app_with_stub_execution()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                "/data/jsonapi/default/orders",
                params={"page[number]": "1", "page[size]": "2"},
                headers={"accept": "application/vnd.api+json"},
            )

        assert response.status_code == 200
        body = response.json()
        assert "data" in body
        assert isinstance(body["data"], list)
        assert "links" in body

    @pytest.mark.anyio
    async def test_pagination_links_contain_page_params(self, monkeypatch):
        # REQ-257: pagination links include page[number] and page[size]
        stub_rows = [
            {"id": 1, "region": "US", "amount": 10.0, "created_at": "2026-01-01"},
            {"id": 2, "region": "EU", "amount": 20.0, "created_at": "2026-01-02"},
        ]

        async def _fake_govern(sql, role_id, exec_params, state, deliver=None):
            # The count query wraps the compiled SELECT in COUNT(*); tag the plan so the
            # fake terminal returns a scalar count row rather than the full data rows.
            plan = MagicMock()
            plan._is_count = "COUNT(*)" in sql
            return plan

        async def _fake_execute(plan, state):
            result = MagicMock()
            result.redirect = None  # non-materialized: real QueryResult.redirect defaults None
            if getattr(plan, "_is_count", False):
                result.rows = [[len(stub_rows)]]  # engine-side COUNT(*) → single scalar row
            else:
                result.rows = [list(r.values()) for r in stub_rows]
                result.columns = list(stub_rows[0].keys())
            return result

        monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
        monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)

        def _fake_serialize(rows, columns, table):
            col_names = [getattr(c, "field_name", c) for c in columns]
            col_rows = [dict(zip(col_names, r)) for r in rows]
            return {"data": {table: col_rows}}

        monkeypatch.setattr("provisa.executor.serialize.serialize_rows", _fake_serialize)

        import httpx
        from fastapi import FastAPI
        from provisa.api.jsonapi.generator import create_jsonapi_router

        from provisa.auth.middleware import AuthMiddleware

        state = _make_app_state()
        router = create_jsonapi_router(state)
        app = FastAPI()
        app.add_middleware(AuthMiddleware)
        app.include_router(router)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                "/data/jsonapi/default/orders",
                params={"page[number]": "1", "page[size]": "2"},
                headers={"accept": "application/vnd.api+json"},
            )

        body = response.json()
        links = body.get("links", {})
        self_link = links.get("self", "")
        assert "page" in self_link

    @pytest.mark.anyio
    async def test_content_type_is_jsonapi(self, monkeypatch):
        # REQ-257: response Content-Type is application/vnd.api+json
        stub_rows = [{"id": 1, "region": "US", "amount": 10.0, "created_at": "2026-01-01"}]

        async def _fake_govern(sql, role_id, exec_params, state, deliver=None):
            # The count query wraps the compiled SELECT in COUNT(*); tag the plan so the
            # fake terminal returns a scalar count row rather than the full data rows.
            plan = MagicMock()
            plan._is_count = "COUNT(*)" in sql
            return plan

        async def _fake_execute(plan, state):
            result = MagicMock()
            result.redirect = None  # non-materialized: real QueryResult.redirect defaults None
            if getattr(plan, "_is_count", False):
                result.rows = [[len(stub_rows)]]  # engine-side COUNT(*) → single scalar row
            else:
                result.rows = [list(r.values()) for r in stub_rows]
                result.columns = list(stub_rows[0].keys())
            return result

        monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
        monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)

        def _fake_serialize(rows, columns, table):
            col_names = [getattr(c, "field_name", c) for c in columns]
            col_rows = [dict(zip(col_names, r)) for r in rows]
            return {"data": {table: col_rows}}

        monkeypatch.setattr("provisa.executor.serialize.serialize_rows", _fake_serialize)

        import httpx
        from fastapi import FastAPI
        from provisa.api.jsonapi.generator import create_jsonapi_router

        state = _make_app_state()
        router = create_jsonapi_router(state)
        app = FastAPI()
        app.include_router(router)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                "/data/jsonapi/default/orders",
                headers={"accept": "application/vnd.api+json"},
            )

        assert "vnd.api+json" in response.headers.get("content-type", "")

    @pytest.mark.anyio
    async def test_unknown_table_returns_404(self, monkeypatch):
        # REQ-257: unknown resource type returns JSON:API 404 error object
        import httpx
        from fastapi import FastAPI
        from provisa.api.jsonapi.generator import create_jsonapi_router
        from provisa.auth.middleware import AuthMiddleware

        state = _make_app_state()
        router = create_jsonapi_router(state)
        app = FastAPI()
        app.add_middleware(AuthMiddleware)
        app.include_router(router)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                "/data/jsonapi/default/nonexistent",
                headers={"accept": "application/vnd.api+json"},
            )

        assert response.status_code == 404
        body = response.json()
        assert "errors" in body
        assert body["errors"][0]["status"] == "404"

    @pytest.mark.anyio
    async def test_meta_contains_total(self, monkeypatch):
        # REQ-257: response meta.total reflects row count
        stub_rows = [
            {"id": 1, "region": "US", "amount": 10.0, "created_at": "2026-01-01"},
            {"id": 2, "region": "EU", "amount": 20.0, "created_at": "2026-01-02"},
        ]

        async def _fake_govern(sql, role_id, exec_params, state, deliver=None):
            # The count query wraps the compiled SELECT in COUNT(*); tag the plan so the
            # fake terminal returns a scalar count row rather than the full data rows.
            plan = MagicMock()
            plan._is_count = "COUNT(*)" in sql
            return plan

        async def _fake_execute(plan, state):
            result = MagicMock()
            result.redirect = None  # non-materialized: real QueryResult.redirect defaults None
            if getattr(plan, "_is_count", False):
                result.rows = [[len(stub_rows)]]  # engine-side COUNT(*) → single scalar row
            else:
                result.rows = [list(r.values()) for r in stub_rows]
                result.columns = list(stub_rows[0].keys())
            return result

        monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _fake_govern)
        monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)

        def _fake_serialize(rows, columns, table):
            col_names = [getattr(c, "field_name", c) for c in columns]
            col_rows = [dict(zip(col_names, r)) for r in rows]
            return {"data": {table: col_rows}}

        monkeypatch.setattr("provisa.executor.serialize.serialize_rows", _fake_serialize)

        import httpx
        from fastapi import FastAPI
        from provisa.api.jsonapi.generator import create_jsonapi_router

        from provisa.auth.middleware import AuthMiddleware

        state = _make_app_state()
        router = create_jsonapi_router(state)
        app = FastAPI()
        app.add_middleware(AuthMiddleware)
        app.include_router(router)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                "/data/jsonapi/default/orders",
                headers={"accept": "application/vnd.api+json"},
            )

        body = response.json()
        assert body["meta"]["total"] == 2
