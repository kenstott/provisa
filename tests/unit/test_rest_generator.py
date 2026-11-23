# Copyright (c) 2026 Kenneth Stott
# Canary: 5d8a69b8-3715-46ee-a34b-5d545df1f868
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for REST auto-generation (Phase AB5, REQ-222)."""

import pytest

from provisa.api._query_helpers import NO_VARIABLES, RequestRefused, parse_params, request_variables
from provisa.api.rest.generator import (
    _build_graphql_query,
    _get_scalar_fields,
    _parse_order_by_params,
    _parse_where_params,
)
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context


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
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "created_at", "visible_to": ["admin"]},
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
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
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
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


class TestParseWhereParams:
    def test_single_eq(self):
        result = _parse_where_params(
            {"filter": '[{"field": "region", "comparator": "eq", "value": "US"}]'}
        )
        assert result == {"region": {"eq": "US"}}

    def test_multiple_ops(self):
        result = _parse_where_params(
            {
                "filter": (
                    '[{"field": "amount", "comparator": "gt", "value": "100"},'
                    '{"field": "amount", "comparator": "lt", "value": "500"},'
                    '{"field": "region", "comparator": "eq", "value": "US"}]'
                )
            }
        )
        assert result == {
            "amount": {"gt": "100", "lt": "500"},
            "region": {"eq": "US"},
        }

    def test_in_operator(self):
        result = _parse_where_params(
            {"filter": '[{"field": "region", "comparator": "in", "value": ["US", "EU", "APAC"]}]'}
        )
        assert result == {"region": {"in": ["US", "EU", "APAC"]}}

    def test_invalid_op_ignored(self):
        result = _parse_where_params(
            {"filter": '[{"field": "region", "comparator": "banana", "value": "US"}]'}
        )
        assert result == {}

    def test_non_where_ignored(self):
        result = _parse_where_params(
            {"limit": "10", "filter": '[{"field": "x", "comparator": "eq", "value": "1"}]'}
        )
        assert result == {"x": {"eq": "1"}}


class TestParseOrderBy:
    def test_single_order(self):
        result = _parse_order_by_params(
            {"orderBy": '[{"field": "created_at", "direction": "desc"}]'}
        )
        assert result == [{"field": "created_at", "dir": "desc"}]

    def test_multiple_orders(self):
        result = _parse_order_by_params(
            {
                "orderBy": (
                    '[{"field": "created_at", "direction": "desc"},'
                    '{"field": "amount", "direction": "asc"}]'
                )
            }
        )
        assert len(result) == 2
        fields = {o["field"] for o in result}
        assert fields == {"created_at", "amount"}

    def test_invalid_direction_defaults_asc(self):
        result = _parse_order_by_params({"orderBy": '[{"field": "id", "direction": "banana"}]'})
        assert result == [{"field": "id", "dir": "asc"}]


class TestBuildGraphQLQuery:
    def test_simple_query(self):
        q = _build_graphql_query("orders", ["id", "amount"], NO_VARIABLES, [], None, None)
        assert q == "{ orders { id amount } }"

    def test_with_limit_offset(self):
        q = _build_graphql_query("orders", ["id"], NO_VARIABLES, [], 10, 20)
        assert "limit: 10" in q
        assert "offset: 20" in q

    def test_a_filter_is_a_variable_typed_by_the_where_argument(self):
        schema, _ = _build_test_schema()
        variables = request_variables(schema, "orders", {"region": {"eq": "US"}})
        q = _build_graphql_query("orders", ["id"], variables, [], None, None)
        assert q == "query($where: OrdersWhere) { orders(where: $where) { id } }"
        assert variables.values == {"where": {"region": {"eq": "US"}}}

    def test_a_numeric_filter_reads_as_its_column_type(self):
        schema, _ = _build_test_schema()
        variables = request_variables(
            schema, "orders", {"amount": {"gt": "100"}, "id": {"in": ["1", "2"]}}
        )
        # REQ-1961: a sized numeric's filter value is its exact digits (compared as a number by
        # the compiler); an integer's is the integer.
        assert variables.values == {"where": {"amount": {"gt": "100"}, "id": {"in": [1, 2]}}}

    def test_a_filter_value_with_a_quote_is_never_written_into_the_text(self):
        schema, _ = _build_test_schema()
        variables = request_variables(schema, "orders", {"region": {"eq": 'say "hi"'}})
        q = _build_graphql_query("orders", ["id"], variables, [], None, None)
        assert '"' not in q
        assert variables.values == {"where": {"region": {"eq": 'say "hi"'}}}

    def test_a_filter_value_its_type_refuses_is_refused(self):
        schema, _ = _build_test_schema()
        with pytest.raises(RequestRefused, match="filter"):
            request_variables(schema, "orders", {"id": {"eq": "abc"}})

    def test_a_parameter_the_table_does_not_take_is_refused_naming_it(self):
        schema, _ = _build_test_schema()
        with pytest.raises(RequestRefused, match="'region'"):
            request_variables(schema, "orders", {}, {"region": "US"})

    def test_with_order_by(self):
        q = _build_graphql_query(
            "orders",
            ["id"],
            NO_VARIABLES,
            [{"field": "created_at", "dir": "desc"}],
            None,
            None,
        )
        assert "order_by:" in q
        assert "created_at: desc" in q

    def test_combined(self):
        schema, _ = _build_test_schema()
        q = _build_graphql_query(
            "orders",
            ["id", "amount"],
            request_variables(schema, "orders", {"region": {"eq": "US"}}),
            [{"field": "amount", "dir": "desc"}],
            5,
            10,
        )
        assert "limit: 5" in q
        assert "offset: 10" in q
        assert "where: $where" in q
        assert "order_by:" in q


class TestParseParams:
    def test_absent_is_no_parameters(self):
        assert parse_params(None) == {}

    def test_a_json_object(self):
        assert parse_params('{"region": "EU", "n": 3}') == {"region": "EU", "n": 3}

    def test_not_json_is_refused(self):
        with pytest.raises(RequestRefused, match="not JSON"):
            parse_params("region=EU")

    def test_not_an_object_is_refused(self):
        with pytest.raises(RequestRefused, match="JSON object"):
            parse_params('["EU"]')


class TestGetScalarFields:
    def test_returns_scalar_fields(self):
        schema, _ = _build_test_schema()
        fields = _get_scalar_fields(schema, "orders")
        assert "id" in fields
        assert "amount" in fields
        assert "region" in fields
        # customer relationship field should not be scalar
        assert "customer" not in fields

    def test_unknown_table_returns_empty(self):
        schema, _ = _build_test_schema()
        fields = _get_scalar_fields(schema, "nonexistent")
        assert fields == []
