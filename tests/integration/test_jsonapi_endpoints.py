# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for JSON:API compliant endpoints (Phase AB6).

Tests are organised in two layers:
  1. Pure Python API — exercises the serializer, pagination helpers, and
     error formatting modules directly.  No PG, no HTTP required.
  2. HTTP via httpx.AsyncClient — tests the full JSON:API FastAPI handler.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


_SAMPLE_ROWS = [
    {"id": 1, "region": "us-east", "amount": 500, "customer_id": 10},
    {"id": 2, "region": "eu-west", "amount": 200, "customer_id": 20},
    {"id": 3, "region": "us-east", "amount": 750, "customer_id": 10},
]

_SAMPLE_ROW_NO_REL = [
    {"id": 1, "region": "us-east", "amount": 500},
]


# ---------------------------------------------------------------------------
# Serializer tests
# ---------------------------------------------------------------------------


class TestRowToResource:
    """Unit tests for the row_to_resource serializer function."""

    async def test_basic_resource_structure(self):
        from provisa.api.jsonapi.serializer import row_to_resource

        resource = row_to_resource({"id": 1, "region": "us-east"}, "orders")
        assert resource["type"] == "orders"
        assert resource["id"] == "1"
        assert "attributes" in resource
        assert resource["attributes"]["region"] == "us-east"

    async def test_id_excluded_from_attributes(self):
        from provisa.api.jsonapi.serializer import row_to_resource

        resource = row_to_resource({"id": 42, "amount": 100}, "orders")
        assert "id" not in resource["attributes"]

    async def test_fk_column_becomes_relationship(self):
        from provisa.api.jsonapi.serializer import row_to_resource

        resource = row_to_resource(
            {"id": 1, "customer_id": 10, "amount": 100},
            "orders",
            relationship_fields={"customer_id": "customers"},
        )
        assert "relationships" in resource
        assert "customer" in resource["relationships"]
        rel = resource["relationships"]["customer"]
        assert rel["data"]["type"] == "customers"
        assert rel["data"]["id"] == "10"

    async def test_null_fk_creates_null_relationship_data(self):
        from provisa.api.jsonapi.serializer import row_to_resource

        resource = row_to_resource(
            {"id": 1, "customer_id": None},
            "orders",
            relationship_fields={"customer_id": "customers"},
        )
        assert resource["relationships"]["customer"]["data"] is None

    async def test_no_relationships_key_when_none(self):
        from provisa.api.jsonapi.serializer import row_to_resource

        resource = row_to_resource({"id": 1, "amount": 99}, "orders")
        assert "relationships" not in resource


class TestRowsToJsonapi:
    """Unit tests for the rows_to_jsonapi document builder."""

    async def test_jsonapi_list_response_structure(self):
        """Response has data and meta top-level keys."""
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        doc = rows_to_jsonapi(_SAMPLE_ROWS, "orders")
        assert "data" in doc
        assert "meta" in doc

    async def test_jsonapi_resource_type_matches_table(self):
        """type field in each resource matches the table name."""
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        doc = rows_to_jsonapi(_SAMPLE_ROWS, "orders")
        for resource in doc["data"]:
            assert resource["type"] == "orders"

    async def test_meta_total_equals_row_count(self):
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        doc = rows_to_jsonapi(_SAMPLE_ROWS, "orders")
        assert doc["meta"]["total"] == len(_SAMPLE_ROWS)

    async def test_jsonapi_relationship_included(self):
        """included array is present when included_rows are provided."""
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        included_rows = {
            "customers": [
                {"id": 10, "name": "Alice"},
                {"id": 20, "name": "Bob"},
            ]
        }
        doc = rows_to_jsonapi(
            _SAMPLE_ROWS,
            "orders",
            relationship_fields={"customer_id": "customers"},
            included_rows=included_rows,
        )
        assert "included" in doc
        types_in_included = {r["type"] for r in doc["included"]}
        assert "customers" in types_in_included

    async def test_empty_rows_produces_empty_data_list(self):
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        doc = rows_to_jsonapi([], "orders")
        assert doc["data"] == []
        assert doc["meta"]["total"] == 0

    async def test_no_included_key_when_no_sideloads(self):
        from provisa.api.jsonapi.serializer import rows_to_jsonapi

        doc = rows_to_jsonapi(_SAMPLE_ROW_NO_REL, "orders")
        assert "included" not in doc


# ---------------------------------------------------------------------------
# Pagination tests
# ---------------------------------------------------------------------------


class TestPaginationHelpers:
    """Unit tests for JSON:API pagination link building."""

    async def test_jsonapi_pagination_links_present(self):
        """links.next is present when more pages exist (result_count >= page_size)."""
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links(
            base_path="/data/jsonapi/orders",
            page_number=1,
            page_size=2,
            result_count=2,  # exactly page_size — indicates there may be more
        )
        assert "next" in links
        assert links["next"] is not None

    async def test_no_next_on_last_page(self):
        """links.next is None when result_count < page_size."""
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links(
            base_path="/data/jsonapi/orders",
            page_number=1,
            page_size=25,
            result_count=3,  # fewer than page_size — last page
        )
        assert links["next"] is None

    async def test_first_link_always_present(self):
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links("/data/jsonapi/orders", 2, 10, 10)
        assert "first" in links
        assert links["first"] is not None

    async def test_prev_link_absent_on_page_one(self):
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links("/data/jsonapi/orders", 1, 10, 5)
        assert links.get("prev") is None

    async def test_prev_link_present_on_page_two(self):
        from provisa.api.jsonapi.pagination import build_pagination_links

        links = build_pagination_links("/data/jsonapi/orders", 2, 10, 10)
        assert links.get("prev") is not None

    async def test_parse_page_params_defaults(self):
        from provisa.api.jsonapi.pagination import parse_page_params, DEFAULT_PAGE_SIZE

        page_num, page_size = parse_page_params({})
        assert page_num == 1
        assert page_size == DEFAULT_PAGE_SIZE

    async def test_parse_page_params_custom(self):
        from provisa.api.jsonapi.pagination import parse_page_params

        page_num, page_size = parse_page_params({"page[number]": "3", "page[size]": "5"})
        assert page_num == 3
        assert page_size == 5

    async def test_page_to_limit_offset_page_one(self):
        from provisa.api.jsonapi.pagination import page_to_limit_offset

        limit, offset = page_to_limit_offset(1, 10)
        assert limit == 10
        assert offset == 0

    async def test_page_to_limit_offset_page_two(self):
        from provisa.api.jsonapi.pagination import page_to_limit_offset

        limit, offset = page_to_limit_offset(2, 10)
        assert limit == 10
        assert offset == 10


# ---------------------------------------------------------------------------
# Error formatting tests
# ---------------------------------------------------------------------------


class TestJsonapiErrors:
    """Unit tests for JSON:API error formatting."""

    async def test_jsonapi_error_format_on_bad_request(self):
        """errors array has title and status fields."""
        from provisa.api.jsonapi.errors import jsonapi_error, error_response

        err = jsonapi_error(400, "Bad Request", "Missing required parameter")
        doc = error_response([err])
        assert "errors" in doc
        assert len(doc["errors"]) == 1
        assert doc["errors"][0]["title"] == "Bad Request"
        assert doc["errors"][0]["status"] == "400"

    async def test_error_includes_detail_when_provided(self):
        from provisa.api.jsonapi.errors import jsonapi_error

        err = jsonapi_error(422, "Unprocessable Entity", "field 'region' is required")
        assert err["detail"] == "field 'region' is required"

    async def test_error_includes_source_parameter(self):
        from provisa.api.jsonapi.errors import jsonapi_error

        err = jsonapi_error(
            400, "Invalid Filter", "Unknown field",
            source_parameter="filter[bogus]",
        )
        assert "source" in err
        assert err["source"]["parameter"] == "filter[bogus]"

    async def test_error_omits_source_when_not_given(self):
        from provisa.api.jsonapi.errors import jsonapi_error

        err = jsonapi_error(500, "Internal Server Error")
        assert "source" not in err

    async def test_multiple_errors_in_response(self):
        from provisa.api.jsonapi.errors import jsonapi_error, error_response

        errs = [
            jsonapi_error(400, "Bad Request", "param A"),
            jsonapi_error(400, "Bad Request", "param B"),
        ]
        doc = error_response(errs)
        assert len(doc["errors"]) == 2


# ---------------------------------------------------------------------------
# Filter parsing tests (generator layer)
# ---------------------------------------------------------------------------


class TestJsonapiFilterParsing:
    """Unit tests for JSON:API filter parameter parsing in the generator."""

    async def test_simple_filter_becomes_eq(self):
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"filter[region]": "us-east"})
        assert result == {"region": {"eq": "us-east"}}

    async def test_operator_filter_parsed(self):
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"filter[amount][gt]": "100"})
        assert result["amount"]["gt"] == "100"

    async def test_in_operator_splits_on_comma(self):
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"filter[region][in]": "us-east,eu-west"})
        assert result["region"]["in"] == ["us-east", "eu-west"]

    async def test_unknown_op_ignored(self):
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"filter[region][INVALID]": "x"})
        assert result == {}

    async def test_non_filter_params_ignored(self):
        from provisa.api.jsonapi.generator import _parse_filters

        result = _parse_filters({"sort": "-amount", "page[size]": "10"})
        assert result == {}


class TestJsonapiSortParsing:
    """Unit tests for JSON:API sort parameter parsing."""

    async def test_ascending_sort(self):
        from provisa.api.jsonapi.generator import _parse_sort

        result = _parse_sort("amount")
        assert result == [{"field": "amount", "dir": "asc"}]

    async def test_descending_sort(self):
        from provisa.api.jsonapi.generator import _parse_sort

        result = _parse_sort("-created_at")
        assert result == [{"field": "created_at", "dir": "desc"}]

    async def test_multiple_sort_fields(self):
        from provisa.api.jsonapi.generator import _parse_sort

        result = _parse_sort("-created_at,amount")
        assert len(result) == 2
        assert result[0] == {"field": "created_at", "dir": "desc"}
        assert result[1] == {"field": "amount", "dir": "asc"}

    async def test_empty_sort_returns_empty_list(self):
        from provisa.api.jsonapi.generator import _parse_sort

        assert _parse_sort(None) == []
        assert _parse_sort("") == []


class TestJsonapiSparseFieldsets:
    """Unit tests for JSON:API sparse fieldset parsing."""

    async def test_sparse_fieldset_parsed(self):
        from provisa.api.jsonapi.generator import _parse_sparse_fieldsets

        result = _parse_sparse_fieldsets({"fields[orders]": "amount,region"}, "orders")
        assert result == ["amount", "region"]

    async def test_missing_fieldset_returns_none(self):
        from provisa.api.jsonapi.generator import _parse_sparse_fieldsets

        result = _parse_sparse_fieldsets({}, "orders")
        assert result is None

    async def test_strips_whitespace(self):
        from provisa.api.jsonapi.generator import _parse_sparse_fieldsets

        result = _parse_sparse_fieldsets({"fields[orders]": " amount , region "}, "orders")
        assert "amount" in result
        assert "region" in result


# ---------------------------------------------------------------------------
# HTTP integration tests (require running PG + full AppState)
# ---------------------------------------------------------------------------


class TestJsonapiEndpointsHTTP:
    """HTTP-level JSON:API tests using httpx.AsyncClient."""

    async def _make_client(self, pg_pool):
        httpx = pytest.importorskip("httpx")
        from provisa.api.jsonapi.generator import create_jsonapi_router
        from provisa.api.app import AppState
        from provisa.compiler.rls import RLSContext
        from fastapi import FastAPI

        try:
            from provisa.compiler.schema_gen import SchemaInput, generate_schema
            from provisa.compiler.introspect import ColumnMetadata
            from provisa.compiler.sql_gen import build_context

            def _col(name, dtype="varchar(100)", nullable=True):
                return ColumnMetadata(column_name=name, data_type=dtype, is_nullable=nullable)

            tables = [{
                "id": 1,
                "source_id": "test-pg",
                "domain_id": "default",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "region", "visible_to": ["admin"]},
                    {"column_name": "amount", "visible_to": ["admin"]},
                ],
            }]
            column_types = {
                1: [
                    _col("id", "integer", nullable=False),
                    _col("region", "varchar(50)"),
                    _col("amount", "decimal(10,2)"),
                ]
            }
            role = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
            si = SchemaInput(
                tables=tables,
                relationships=[],
                column_types=column_types,
                naming_rules=[],
                role=role,
                domains=[{"id": "default", "description": "Default"}],
                source_types={"test-pg": "postgresql"},
            )
            schema = generate_schema(si)
            ctx = build_context(si)
        except Exception:
            raise

        from provisa.executor.pool import SourcePool
        import os

        source_pool = SourcePool()
        await source_pool.add(
            "test-pg",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
        )

        app_state = AppState()
        app_state.schemas = {"admin": schema}
        app_state.contexts = {"admin": ctx}
        app_state.rls_contexts = {"admin": RLSContext.empty()}
        app_state.source_pools = source_pool
        app_state.source_types = {"test-pg": "postgresql"}
        app_state.source_dialects = {"test-pg": "postgres"}
        app_state.masking_rules = {}

        app = FastAPI()
        app.include_router(create_jsonapi_router(app_state))

        transport = httpx.ASGITransport(app=app)
        client = httpx.AsyncClient(transport=transport, base_url="http://test")
        return client, source_pool

    async def test_jsonapi_content_type_header(self, pg_pool):
        """Response Content-Type is application/vnd.api+json."""
        client, pool = await self._make_client(pg_pool)

        try:
            async with client:
                response = await client.get(
                    "/data/jsonapi/orders",
                    headers={"Accept": "application/vnd.api+json"},
                )
            ct = response.headers.get("content-type", "")
            assert "application/vnd.api+json" in ct
        finally:
            await pool.close_all()

    async def test_jsonapi_list_response_has_data_meta_links(self, pg_pool):
        """Full HTTP response has data, meta, and links top-level keys."""
        client, pool = await self._make_client(pg_pool)

        try:
            async with client:
                response = await client.get(
                    "/data/jsonapi/orders",
                    headers={"Accept": "application/vnd.api+json"},
                )
            assert response.status_code == 200
            body = response.json()
            assert "data" in body
            assert "meta" in body
            assert "links" in body
        finally:
            await pool.close_all()

    async def test_jsonapi_resource_type_matches_table_http(self, pg_pool):
        """type field in resource matches table name via HTTP."""
        client, pool = await self._make_client(pg_pool)

        try:
            async with client:
                response = await client.get(
                    "/data/jsonapi/orders",
                    params={"page[size]": "1"},
                    headers={"Accept": "application/vnd.api+json"},
                )
            body = response.json()
            resources = body.get("data", [])
            if resources:
                assert resources[0]["type"] == "orders"
        finally:
            await pool.close_all()

    async def test_jsonapi_not_acceptable_without_correct_header(self):
        """406 is returned when Accept does not include application/vnd.api+json."""
        httpx = pytest.importorskip("httpx")
        from provisa.api.jsonapi.generator import create_jsonapi_router
        from provisa.api.app import AppState
        from provisa.compiler.rls import RLSContext
        from graphql import (
            GraphQLField,
            GraphQLInt,
            GraphQLList,
            GraphQLObjectType,
            GraphQLSchema,
        )
        from fastapi import FastAPI

        order_type = GraphQLObjectType("Order", {"id": GraphQLField(GraphQLInt)})
        query_type = GraphQLObjectType("Query", {"orders": GraphQLField(GraphQLList(order_type))})
        schema = GraphQLSchema(query=query_type)

        try:
            from provisa.compiler.sql_gen import CompilationContext
            ctx = CompilationContext()
        except Exception:
            raise

        app_state = AppState()
        app_state.schemas = {"admin": schema}
        app_state.contexts = {"admin": ctx}
        app_state.rls_contexts = {"admin": RLSContext.empty()}

        app = FastAPI()
        app.include_router(create_jsonapi_router(app_state))

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/data/jsonapi/orders",
                headers={"Accept": "text/html"},
            )
        assert response.status_code == 406

    async def test_jsonapi_not_found_for_unknown_table(self):
        """404 error response for an unregistered table."""
        httpx = pytest.importorskip("httpx")
        from provisa.api.jsonapi.generator import create_jsonapi_router
        from provisa.api.app import AppState
        from provisa.compiler.rls import RLSContext
        from graphql import (
            GraphQLField,
            GraphQLInt,
            GraphQLList,
            GraphQLObjectType,
            GraphQLSchema,
        )
        from fastapi import FastAPI

        order_type = GraphQLObjectType("Order", {"id": GraphQLField(GraphQLInt)})
        query_type = GraphQLObjectType("Query", {"orders": GraphQLField(GraphQLList(order_type))})
        schema = GraphQLSchema(query=query_type)

        try:
            from provisa.compiler.sql_gen import CompilationContext
            ctx = CompilationContext()
        except Exception:
            raise

        app_state = AppState()
        app_state.schemas = {"admin": schema}
        app_state.contexts = {"admin": ctx}
        app_state.rls_contexts = {"admin": RLSContext.empty()}

        app = FastAPI()
        app.include_router(create_jsonapi_router(app_state))

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/data/jsonapi/no_such_table",
                headers={"Accept": "application/vnd.api+json"},
            )
        assert response.status_code == 404
        body = response.json()
        assert "errors" in body
        assert body["errors"][0]["status"] == "404"
