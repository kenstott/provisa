# Copyright (c) 2025 Kenneth Stott
# Canary: 8140c31e-6ba3-4e10-9278-a7b06863f3f9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for JSON:API auto-generation (Phase AB6)."""

import pytest

from provisa.api.jsonapi.errors import error_response, jsonapi_error
from provisa.api.jsonapi.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    build_pagination_links,
    page_to_limit_offset,
    parse_page_params,
)
from provisa.api.jsonapi.serializer import row_to_resource, rows_to_jsonapi
from provisa.api.jsonapi.generator import (
    _build_graphql_query,
    _get_scalar_fields,
    _parse_filters,
    _parse_sort,
    _parse_sparse_fieldsets,
)
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_test_schema():
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
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
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("created_at", "timestamp"),
        ],
        2: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
            _col("email", "varchar(200)"),
        ],
    }
    role = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}
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


# ── Error objects ──


class TestJsonApiErrors:
    def test_basic_error(self):
        err = jsonapi_error(404, "Not Found")
        assert err["status"] == "404"
        assert err["title"] == "Not Found"
        assert "detail" not in err

    def test_error_with_detail(self):
        err = jsonapi_error(400, "Bad Request", detail="Missing field")
        assert err["detail"] == "Missing field"

    def test_error_with_source(self):
        err = jsonapi_error(
            400, "Invalid Filter",
            source_parameter="filter[foo]",
        )
        assert err["source"]["parameter"] == "filter[foo]"

    def test_error_response_envelope(self):
        resp = error_response([jsonapi_error(500, "Error")])
        assert "errors" in resp
        assert len(resp["errors"]) == 1


# ── Pagination ──


class TestPagination:
    def test_default_params(self):
        pn, ps = parse_page_params({})
        assert pn == 1
        assert ps == DEFAULT_PAGE_SIZE

    def test_custom_params(self):
        pn, ps = parse_page_params({"page[number]": "3", "page[size]": "50"})
        assert pn == 3
        assert ps == 50

    def test_clamps_page_size(self):
        _, ps = parse_page_params({"page[size]": "99999"})
        assert ps == MAX_PAGE_SIZE

    def test_clamps_negative_page(self):
        pn, _ = parse_page_params({"page[number]": "-5"})
        assert pn == 1

    def test_invalid_values_use_defaults(self):
        pn, ps = parse_page_params({"page[number]": "abc", "page[size]": "xyz"})
        assert pn == 1
        assert ps == DEFAULT_PAGE_SIZE

    def test_limit_offset_conversion(self):
        limit, offset = page_to_limit_offset(1, 25)
        assert limit == 25
        assert offset == 0

        limit, offset = page_to_limit_offset(3, 10)
        assert limit == 10
        assert offset == 20

    def test_pagination_links_first_page(self):
        links = build_pagination_links("/data/jsonapi/orders", 1, 10, 10)
        assert links["prev"] is None
        assert links["next"] is not None
        assert "page%5Bnumber%5D=2" in links["next"]

    def test_pagination_links_middle_page(self):
        links = build_pagination_links("/data/jsonapi/orders", 3, 10, 10)
        assert links["prev"] is not None
        assert links["next"] is not None

    def test_pagination_links_last_page(self):
        links = build_pagination_links("/data/jsonapi/orders", 5, 10, 3)
        assert links["prev"] is not None
        assert links["next"] is None  # result_count < page_size


# ── Serializer ──


class TestSerializer:
    def test_basic_resource(self):
        row = {"id": 42, "amount": 99.5, "region": "US"}
        resource = row_to_resource(row, "orders")
        assert resource["type"] == "orders"
        assert resource["id"] == "42"
        assert resource["attributes"] == {"amount": 99.5, "region": "US"}
        assert "relationships" not in resource

    def test_resource_with_relationship(self):
        row = {"id": 1, "amount": 50, "customer_id": 7}
        resource = row_to_resource(
            row, "orders",
            relationship_fields={"customer_id": "customers"},
        )
        assert resource["attributes"] == {"amount": 50}
        assert resource["relationships"]["customer"]["data"]["type"] == "customers"
        assert resource["relationships"]["customer"]["data"]["id"] == "7"

    def test_null_relationship(self):
        row = {"id": 1, "customer_id": None}
        resource = row_to_resource(
            row, "orders",
            relationship_fields={"customer_id": "customers"},
        )
        assert resource["relationships"]["customer"]["data"] is None

    def test_rows_to_jsonapi_document(self):
        rows = [
            {"id": 1, "amount": 10},
            {"id": 2, "amount": 20},
        ]
        doc = rows_to_jsonapi(rows, "orders")
        assert len(doc["data"]) == 2
        assert doc["meta"]["total"] == 2
        assert doc["data"][0]["type"] == "orders"
        assert doc["data"][0]["id"] == "1"

    def test_rows_to_jsonapi_with_included(self):
        rows = [{"id": 1, "customer_id": 7}]
        included = {"customers": [{"id": 7, "name": "Alice"}]}
        doc = rows_to_jsonapi(
            rows, "orders",
            relationship_fields={"customer_id": "customers"},
            included_rows=included,
        )
        assert "included" in doc
        assert doc["included"][0]["type"] == "customers"
        assert doc["included"][0]["id"] == "7"


# ── Filter parsing ──


class TestParseFilters:
    def test_simple_filter(self):
        result = _parse_filters({"filter[region]": "US"})
        assert result == {"region": {"eq": "US"}}

    def test_operator_filter(self):
        result = _parse_filters({"filter[amount][gt]": "100"})
        assert result == {"amount": {"gt": "100"}}

    def test_multiple_filters(self):
        result = _parse_filters({
            "filter[region]": "US",
            "filter[amount][gte]": "50",
        })
        assert "region" in result
        assert "amount" in result
        assert result["amount"]["gte"] == "50"

    def test_in_filter(self):
        result = _parse_filters({"filter[region][in]": "US,EU"})
        assert result == {"region": {"in": ["US", "EU"]}}

    def test_invalid_op_ignored(self):
        result = _parse_filters({"filter[x][banana]": "1"})
        assert result == {}

    def test_non_filter_ignored(self):
        result = _parse_filters({"sort": "-id", "filter[x]": "1"})
        assert result == {"x": {"eq": "1"}}


# ── Sort parsing ──


class TestParseSort:
    def test_single_asc(self):
        result = _parse_sort("amount")
        assert result == [{"field": "amount", "dir": "asc"}]

    def test_single_desc(self):
        result = _parse_sort("-created_at")
        assert result == [{"field": "created_at", "dir": "desc"}]

    def test_multiple(self):
        result = _parse_sort("-created_at,amount")
        assert result == [
            {"field": "created_at", "dir": "desc"},
            {"field": "amount", "dir": "asc"},
        ]

    def test_none(self):
        assert _parse_sort(None) == []

    def test_empty_string(self):
        assert _parse_sort("") == []


# ── Sparse fieldsets ──


class TestSparseFieldsets:
    def test_no_fieldset(self):
        assert _parse_sparse_fieldsets({}, "orders") is None

    def test_basic_fieldset(self):
        result = _parse_sparse_fieldsets(
            {"fields[orders]": "amount,created_at"}, "orders",
        )
        assert result == ["amount", "created_at"]

    def test_wrong_table_ignored(self):
        result = _parse_sparse_fieldsets(
            {"fields[customers]": "name"}, "orders",
        )
        assert result is None


# ── GraphQL query building ──


class TestBuildGraphQLQuery:
    def test_simple(self):
        q = _build_graphql_query("orders", ["id", "amount"], {}, [], None, None)
        assert q == "{ orders { id amount } }"

    def test_with_pagination(self):
        q = _build_graphql_query("orders", ["id"], {}, [], 10, 20)
        assert "limit: 10" in q
        assert "offset: 20" in q

    def test_with_filters(self):
        q = _build_graphql_query(
            "orders", ["id"],
            {"region": {"eq": "US"}}, [], None, None,
        )
        assert "where:" in q
        assert 'region: {eq: "US"}' in q

    def test_with_sort(self):
        q = _build_graphql_query(
            "orders", ["id"], {},
            [{"field": "created_at", "dir": "desc"}], None, None,
        )
        assert "order_by:" in q
        assert "created_at: desc" in q


# ── Schema integration ──


class TestScalarFields:
    def test_returns_scalars_only(self):
        schema, _ = _build_test_schema()
        fields = _get_scalar_fields(schema, "orders")
        assert "id" in fields
        assert "amount" in fields
        assert "customer" not in fields

    def test_unknown_table(self):
        schema, _ = _build_test_schema()
        assert _get_scalar_fields(schema, "nope") == []
