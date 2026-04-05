# Copyright (c) 2025 Kenneth Stott
# Canary: 6befc172-96f9-4237-be6b-e5f8a2849346
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Aggregate and alias tests for sql_gen — split from test_sql_gen.py for size."""

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import (
    CompilationContext,
    build_context,
    compile_query,
)


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_and_ctx(
    tables=None, relationships=None, role_id="admin", naming_rules=None
):
    if tables is None:
        tables = [
            {
                "id": 1,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin", "analyst"]},
                    {"column_name": "customer_id", "visible_to": ["admin", "analyst"]},
                    {"column_name": "amount", "visible_to": ["admin"]},
                    {"column_name": "region", "visible_to": ["admin"]},
                    {"column_name": "status", "visible_to": ["admin"]},
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
    if relationships is None:
        relationships = [
            {
                "id": "r1",
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            }
        ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(20)"),
            _col("status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
        2: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
            _col("email", "varchar(200)"),
        ],
    }
    role = {"id": role_id, "capabilities": [], "domain_access": ["*"]}
    domains = [{"id": "sales", "description": "Sales"}]
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=naming_rules or [],
        role=role,
        domains=domains,
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


@pytest.fixture
def schema_and_ctx():
    return _build_schema_and_ctx()


class TestAggregate:
    def test_aggregate_schema_generated(self, schema_and_ctx):
        """orders_aggregate root field exists in the schema."""
        schema, ctx = schema_and_ctx
        query_type = schema.query_type
        assert "orders_aggregate" in query_type.fields

    def test_aggregate_count_compiles(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate {
                aggregate { count }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        assert len(results) == 1
        q = results[0]
        assert q.root_field == "orders_aggregate"
        assert "COUNT(*)" in q.sql
        assert 'FROM "public"."orders"' in q.sql

    def test_aggregate_sum_only_numeric(self, schema_and_ctx):
        """sum fields should only include numeric columns (amount), not varchar (region)."""
        schema, ctx = schema_and_ctx
        agg_type = schema.query_type.fields["orders_aggregate"].type
        agg_fields_type = agg_type.fields["aggregate"].type
        sum_type = agg_fields_type.fields["sum"].type
        sum_field_names = set(sum_type.fields.keys())
        assert "amount" in sum_field_names
        assert "id" in sum_field_names
        assert "region" not in sum_field_names
        assert "status" not in sum_field_names

    def test_aggregate_avg_only_numeric(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        agg_type = schema.query_type.fields["orders_aggregate"].type
        agg_fields_type = agg_type.fields["aggregate"].type
        avg_type = agg_fields_type.fields["avg"].type
        avg_field_names = set(avg_type.fields.keys())
        assert "amount" in avg_field_names
        assert "region" not in avg_field_names

    def test_aggregate_min_max_include_comparable(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        agg_type = schema.query_type.fields["orders_aggregate"].type
        agg_fields_type = agg_type.fields["aggregate"].type
        min_type = agg_fields_type.fields["min"].type
        min_field_names = set(min_type.fields.keys())
        assert "amount" in min_field_names
        assert "region" in min_field_names
        assert "created_at" in min_field_names

    def test_aggregate_sum_avg_sql(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate {
                aggregate {
                    count
                    sum { amount }
                    avg { amount }
                }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        q = results[0]
        assert "COUNT(*)" in q.sql
        assert 'SUM("amount")' in q.sql
        assert 'AVG("amount")' in q.sql

    def test_aggregate_min_max_sql(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate {
                aggregate {
                    min { amount region }
                    max { amount region }
                }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        q = results[0]
        assert 'MIN("amount")' in q.sql
        assert 'MIN("region")' in q.sql
        assert 'MAX("amount")' in q.sql
        assert 'MAX("region")' in q.sql

    def test_aggregate_where_clause(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate(where: { region: { eq: "us-east" } }) {
                aggregate { count }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        q = results[0]
        assert "COUNT(*)" in q.sql
        assert 'WHERE "region" = $1' in q.sql
        assert q.params == ["us-east"]

    def test_aggregate_nodes_field(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        agg_type = schema.query_type.fields["orders_aggregate"].type
        assert "nodes" in agg_type.fields

    def test_aggregate_role_gating(self):
        tables = [
            {
                "id": 1, "source_id": "pg", "domain_id": "d",
                "schema_name": "public", "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin", "limited"]},
                    {"column_name": "amount", "visible_to": ["admin"]},
                    {"column_name": "status", "visible_to": ["admin", "limited"]},
                ],
            },
        ]
        col_types = {
            1: [_col("id", "integer"), _col("amount", "decimal(10,2)"), _col("status", "varchar")],
        }
        si = SchemaInput(
            tables=tables, relationships=[], column_types=col_types,
            naming_rules=[],
            role={"id": "limited", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema = generate_schema(si)
        agg_type = schema.query_type.fields["orders_aggregate"].type
        agg_fields_type = agg_type.fields["aggregate"].type
        sum_type = agg_fields_type.fields["sum"].type
        assert "id" in sum_type.fields
        assert "amount" not in sum_type.fields

    def test_aggregate_where_compiles_correct_sql(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate(where: { status: { eq: "shipped" }, region: { eq: "eu" } }) {
                aggregate { count sum { amount } }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        q = results[0]
        assert "COUNT(*)" in q.sql
        assert 'SUM("amount")' in q.sql
        assert "$1" in q.sql
        assert "$2" in q.sql
        assert len(q.params) == 2
        assert "shipped" in q.params
        assert "eu" in q.params

    def test_aggregate_no_numeric_columns_only_count(self):
        tables = [
            {
                "id": 1, "source_id": "pg", "domain_id": "d",
                "schema_name": "public", "table_name": "tags",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "name", "visible_to": ["admin"]},
                    {"column_name": "category", "visible_to": ["admin"]},
                ],
            },
        ]
        col_types = {
            1: [_col("name", "varchar(50)"), _col("category", "varchar(50)")],
        }
        si = SchemaInput(
            tables=tables, relationships=[], column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema = generate_schema(si)
        agg_type = schema.query_type.fields["tags_aggregate"].type
        agg_fields_type = agg_type.fields["aggregate"].type
        assert "count" in agg_fields_type.fields
        assert "sum" not in agg_fields_type.fields
        assert "avg" not in agg_fields_type.fields
        assert "min" in agg_fields_type.fields
        assert "max" in agg_fields_type.fields
        ctx = build_context(si)
        doc = parse("{ tags_aggregate { aggregate { count } } }")
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        assert "COUNT(*)" in results[0].sql

    def test_aggregate_nodes_returns_rows_alongside_aggregate(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate {
                aggregate { count sum { amount } }
                nodes { id amount region }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        q = results[0]
        assert "COUNT(*)" in q.sql
        assert 'SUM("amount")' in q.sql
        agg_cols = [c for c in q.columns if c.nested_in and c.nested_in.startswith("aggregate")]
        assert len(agg_cols) >= 2

    def test_aggregate_multiple_functions_in_one_query(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate {
                aggregate {
                    count
                    sum { amount id }
                    avg { amount }
                }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        q = results[0]
        assert "COUNT(*)" in q.sql
        assert 'SUM("amount")' in q.sql
        assert 'SUM("id")' in q.sql
        assert 'AVG("amount")' in q.sql
        select_part = q.sql.split("FROM")[0]
        assert select_part.count(",") >= 3

    def test_aggregate_excludes_relationship_fields_from_sum_avg(self):
        tables = [
            {
                "id": 1, "source_id": "pg", "domain_id": "d",
                "schema_name": "public", "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "customer_id", "visible_to": ["admin"]},
                    {"column_name": "amount", "visible_to": ["admin"]},
                ],
            },
            {
                "id": 2, "source_id": "pg", "domain_id": "d",
                "schema_name": "public", "table_name": "customers",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "name", "visible_to": ["admin"]},
                ],
            },
        ]
        rels = [{
            "id": "r1", "source_table_id": 1, "target_table_id": 2,
            "source_column": "customer_id", "target_column": "id",
            "cardinality": "many-to-one",
        }]
        col_types = {
            1: [_col("id", "integer"), _col("customer_id", "integer"), _col("amount", "decimal(10,2)")],
            2: [_col("id", "integer"), _col("name", "varchar")],
        }
        si = SchemaInput(
            tables=tables, relationships=rels, column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema = generate_schema(si)
        agg_type = schema.query_type.fields["orders_aggregate"].type
        agg_fields_type = agg_type.fields["aggregate"].type
        sum_type = agg_fields_type.fields["sum"].type
        sum_field_names = set(sum_type.fields.keys())
        assert "customers" not in sum_field_names
        assert "id" in sum_field_names
        assert "customer_id" in sum_field_names
        assert "amount" in sum_field_names

    def test_aggregate_nodes_compiles_plain_select(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate {
                aggregate { count max { rating: amount } }
                nodes { id amount region }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        q = results[0]
        assert q.nodes_sql is not None
        assert "COUNT(*)" not in q.nodes_sql
        assert 'SELECT' in q.nodes_sql
        assert 'FROM "public"."orders"' in q.nodes_sql
        assert q.nodes_columns is not None
        node_field_names = {c.field_name for c in q.nodes_columns}
        assert "id" in node_field_names
        assert "amount" in node_field_names
        assert "region" in node_field_names

    def test_aggregate_nodes_sql_respects_where_filter(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate(where: { region: { eq: "us-east" } }) {
                aggregate { count }
                nodes { id region }
            } }
        """)
        errors = validate(schema, doc)
        assert not errors
        results = compile_query(doc, ctx)
        q = results[0]
        assert q.nodes_sql is not None
        assert 'WHERE "region" = $' in q.nodes_sql

    def test_aggregate_only_no_nodes_sql(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            { orders_aggregate {
                aggregate { count }
            } }
        """)
        results = compile_query(doc, ctx)
        q = results[0]
        assert q.nodes_sql is None
        assert q.nodes_columns is None

    def test_aggregate_per_role_gating_admin_vs_analyst(self):
        tables = [
            {
                "id": 1, "source_id": "pg", "domain_id": "d",
                "schema_name": "public", "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin", "analyst"]},
                    {"column_name": "amount", "visible_to": ["admin"]},
                    {"column_name": "cost", "visible_to": ["admin"]},
                    {"column_name": "region", "visible_to": ["admin", "analyst"]},
                ],
            },
        ]
        col_types = {
            1: [
                _col("id", "integer"),
                _col("amount", "decimal(10,2)"),
                _col("cost", "decimal(10,2)"),
                _col("region", "varchar(50)"),
            ],
        }
        si_admin = SchemaInput(
            tables=tables, relationships=[], column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema_admin = generate_schema(si_admin)
        agg_admin = schema_admin.query_type.fields["orders_aggregate"].type
        sum_admin = agg_admin.fields["aggregate"].type.fields["sum"].type
        admin_sum_fields = set(sum_admin.fields.keys())
        assert "id" in admin_sum_fields
        assert "amount" in admin_sum_fields
        assert "cost" in admin_sum_fields

        si_analyst = SchemaInput(
            tables=tables, relationships=[], column_types=col_types,
            naming_rules=[],
            role={"id": "analyst", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema_analyst = generate_schema(si_analyst)
        agg_analyst = schema_analyst.query_type.fields["orders_aggregate"].type
        agg_fields_analyst = agg_analyst.fields["aggregate"].type
        sum_analyst = agg_fields_analyst.fields["sum"].type
        analyst_sum_fields = set(sum_analyst.fields.keys())
        assert "id" in analyst_sum_fields
        assert "amount" not in analyst_sum_fields
        assert "cost" not in analyst_sum_fields
        assert len(analyst_sum_fields) < len(admin_sum_fields)


def test_field_alias_used_as_response_key():
    """root_field on CompiledQuery must equal the alias when one is present."""
    schema, ctx = _build_schema_and_ctx()
    doc = parse("{ my_alias: orders { id } }")
    results = compile_query(doc, ctx)
    assert len(results) == 1
    assert results[0].root_field == "my_alias"


def test_aggregate_alias_used_as_response_key():
    """_aggregate root_field must equal the alias when one is present."""
    schema, ctx = _build_schema_and_ctx()
    doc = parse("{ my_agg: orders_aggregate { aggregate { count } } }")
    results = compile_query(doc, ctx)
    assert len(results) == 1
    assert results[0].root_field == "my_agg"


def test_aggregate_aliases_at_all_levels():
    """Aliases on aggregate/func/column keys propagate to ColumnRef nested_in and field_name."""
    schema, ctx = _build_schema_and_ctx()
    doc = parse("""
        {
            test: orders_aggregate {
                derived: aggregate {
                    total: sum {
                        rev: amount
                    }
                }
            }
        }
    """)
    results = compile_query(doc, ctx)
    assert len(results) == 1
    q = results[0]
    assert q.root_field == "test"
    assert q.agg_alias == "derived"
    sum_cols = [c for c in q.columns if c.nested_in and "total" in c.nested_in]
    assert len(sum_cols) == 1
    assert sum_cols[0].field_name == "rev"
    assert sum_cols[0].nested_in == "derived.total"
