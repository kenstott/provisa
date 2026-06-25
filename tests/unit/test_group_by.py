# Copyright (c) 2026 Kenneth Stott
# Canary: a3f1b8e2-5c94-4d7f-8e21-6b9a3d0f1c42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for REQ-653, REQ-654, REQ-655: group_by root field, GroupByRow type, FILTER/HAVING."""

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler import naming as _naming
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx(enable_group_by: bool = True, enable_aggregates: bool = False):
    _naming.configure(gql="snake")
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
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


@pytest.fixture
def schema_and_ctx():
    return _build_schema_and_ctx()


class TestGroupBySchema:
    """REQ-653/654: Schema structure for _group_by field."""

    def test_group_by_field_present_when_enabled(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        assert "orders_group_by" in schema.query_type.fields

    def test_group_by_field_absent_when_disabled(self):
        schema, _ = _build_schema_and_ctx(enable_group_by=False)
        assert "orders_group_by" not in schema.query_type.fields

    def test_group_by_field_absent_by_default(self):
        _naming.configure(gql="snake")
        tables = [
            {
                "id": 1,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
            },
        ]
        column_types = {1: [_col("id", "integer")]}
        role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=column_types,
            naming_rules=[],
            role=role,
            domains=[{"id": "sales", "description": "Sales"}],
        )
        schema = generate_schema(si)
        assert "orders_group_by" not in schema.query_type.fields

    def test_group_by_return_type_is_list(self, schema_and_ctx):
        from graphql import GraphQLList, GraphQLNonNull

        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        # NonNull(List(NonNull(GroupByRow)))
        assert isinstance(field.type, GraphQLNonNull)
        assert isinstance(field.type.of_type, GraphQLList)

    def test_group_by_row_has_group_key(self, schema_and_ctx):
        from graphql import GraphQLNonNull

        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        row_type = field.type.of_type.of_type.of_type
        assert "groupKey" in row_type.fields
        gk = row_type.fields["groupKey"]
        assert isinstance(gk.type, GraphQLNonNull)

    def test_group_by_row_has_aggregates(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        row_type = field.type.of_type.of_type.of_type
        assert "aggregates" in row_type.fields

    def test_aggregates_field_has_where_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        row_type = field.type.of_type.of_type.of_type
        agg_field = row_type.fields["aggregates"]
        assert "where" in agg_field.args

    def test_group_by_root_has_by_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "by" in field.args

    def test_group_by_root_has_where_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "where" in field.args

    def test_group_by_root_has_having_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "having" in field.args

    def test_group_by_root_has_limit_offset(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "limit" in field.args
        assert "offset" in field.args

    def test_group_by_root_has_order_by_arg(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        field = schema.query_type.fields["orders_group_by"]
        assert "order_by" in field.args

    def test_enable_aggregates_independent(self):
        # enable_group_by=True, enable_aggregates=False → group_by present, _aggregate absent
        schema, _ = _build_schema_and_ctx(enable_group_by=True, enable_aggregates=False)
        assert "orders_group_by" in schema.query_type.fields
        assert "orders_aggregate" not in schema.query_type.fields

    def test_both_flags_independent(self):
        schema, _ = _build_schema_and_ctx(enable_group_by=True, enable_aggregates=True)
        assert "orders_group_by" in schema.query_type.fields
        assert "orders_aggregate" in schema.query_type.fields

    def test_group_by_query_validates(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates {
                        count
                        sum { amount }
                    }
                }
            }
        """)
        errors = validate(schema, doc)
        assert errors == []

    def test_group_by_query_with_having_validates(self, schema_and_ctx):
        schema, _ = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(
                    by: [region],
                    having: { count: { gt: 5 } }
                ) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        errors = validate(schema, doc)
        assert errors == []


class TestGroupBySQLGen:
    """REQ-654: SQL generation for _group_by field."""

    def test_group_by_generates_group_by_clause(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        assert len(compiled) == 1
        sql = compiled[0].sql
        assert 'GROUP BY "region"' in sql

    def test_group_by_selects_group_columns(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        col_refs = compiled[0].columns
        group_key_refs = [c for c in col_refs if c.nested_in == "groupKey"]
        assert any(c.field_name == "region" for c in group_key_refs)

    def test_group_by_count_nested_in_aggregates(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        col_refs = compiled[0].columns
        agg_refs = [c for c in col_refs if c.nested_in == "aggregates"]
        assert any(c.field_name == "count" for c in agg_refs)

    def test_group_by_sum_nested_in_aggregates_sum(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { sum { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        col_refs = compiled[0].columns
        sum_refs = [c for c in col_refs if c.nested_in == "aggregates.sum"]
        assert any(c.field_name == "amount" for c in sum_refs)

    def test_group_by_sum_sql_fragment(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { sum { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'SUM("amount")' in sql

    def test_group_by_multiple_columns(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region, status]) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'GROUP BY "region", "status"' in sql
        assert compiled[0].is_group_by is True
        assert compiled[0].group_by_columns == ["region", "status"]

    def test_group_by_where_clause(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region], where: { status: { eq: "active" } }) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "WHERE" in sql
        assert "$1" in sql
        assert compiled[0].params == ["active"]

    def test_group_by_limit_offset(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region], limit: 10, offset: 5) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "LIMIT $1" in sql
        assert "OFFSET $2" in sql
        assert compiled[0].params == [10, 5]

    def test_group_by_is_group_by_flag(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        assert compiled[0].is_group_by is True

    def test_group_by_group_by_columns_field(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        assert compiled[0].group_by_columns == ["region"]

    def test_group_by_avg_sql_fragment(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { avg { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'AVG("amount")' in sql

    def test_group_by_min_max_sql_fragment(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { min { amount } max { amount } }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'MIN("amount")' in sql
        assert 'MAX("amount")' in sql


class TestGroupByFilterHaving:
    """REQ-655: FILTER (WHERE ...) on aggregates and HAVING clause."""

    def test_filter_where_on_aggregates(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates(where: { status: { eq: "active" } }) {
                        count
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "FILTER (WHERE" in sql
        assert "$1" in sql
        assert compiled[0].params[0] == "active"

    def test_filter_where_applied_to_count(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates(where: { status: { eq: "active" } }) {
                        count
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "COUNT(*) FILTER (WHERE" in sql

    def test_filter_where_applied_to_sum(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates(where: { status: { eq: "active" } }) {
                        sum { amount }
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert 'SUM("amount") FILTER (WHERE' in sql

    def test_having_count_gt(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region], having: { count: { gt: 5 } }) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "HAVING" in sql
        assert "COUNT(*) > $1" in sql
        assert compiled[0].params == [5]

    def test_having_sum_gte(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region], having: { sum: { amount: { gte: 1000 } } }) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "HAVING" in sql
        assert 'SUM("amount") >= $1' in sql
        assert compiled[0].params == [1000]

    def test_having_not_emitted_when_empty(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(by: [region]) {
                    groupKey
                    aggregates { count }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "HAVING" not in sql

    def test_filter_and_root_where_both_present(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(
                    by: [region],
                    where: { status: { eq: "active" } }
                ) {
                    groupKey
                    aggregates(where: { region: { eq: "US" } }) {
                        count
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        assert "FILTER (WHERE" in sql
        assert "WHERE" in sql
        assert len(compiled[0].params) == 2

    def test_having_with_filter_param_ordering(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            query {
                orders_group_by(
                    by: [region],
                    having: { count: { gt: 3 } }
                ) {
                    groupKey
                    aggregates(where: { status: { eq: "active" } }) {
                        count
                    }
                }
            }
        """)
        compiled = compile_query(doc, ctx, variables=None)
        sql = compiled[0].sql
        params = compiled[0].params
        # FILTER params come first (status="active" $1), then HAVING ($2)
        assert "FILTER (WHERE" in sql
        assert "HAVING" in sql
        assert len(params) == 2
