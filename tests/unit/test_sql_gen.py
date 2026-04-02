# Copyright (c) 2025 Kenneth Stott
# Canary: 6befc172-96f9-4237-be6b-e5f8a2849346
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for sql_gen — GraphQL AST → PG-style SQL compilation.

Uses fixture-based pairs from tests/fixtures/graphql_queries.py where possible,
plus direct tests for WHERE operators, variables, and edge cases.
"""

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
    """Build a schema + compilation context from minimal test data."""
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
                    {"column_name": "id", "visible_to": ["admin", "analyst"]},
                    {"column_name": "name", "visible_to": ["admin", "analyst"]},
                    {"column_name": "email", "visible_to": ["admin", "analyst"]},
                ],
            },
        ]
    if relationships is None:
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
            _col("status", "varchar(20)"),
            _col("created_at", "timestamp"),
        ],
        2: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
            _col("email", "varchar(200)"),
        ],
    }

    role = {"id": role_id, "capabilities": ["query_development"], "domain_access": ["*"]}
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


class TestSimpleSelect:
    def test_select_fields(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders { id amount status } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        assert len(results) == 1
        q = results[0]
        assert q.root_field == "orders"
        assert q.sql == 'SELECT "id", "amount", "status" FROM "public"."orders"'
        assert q.params == []

    def test_single_field(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders { id } }")
        results = compile_query(doc, ctx)
        assert results[0].sql == 'SELECT "id" FROM "public"."orders"'

    def test_sources_tracked(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders { id } }")
        results = compile_query(doc, ctx)
        assert results[0].sources == {"sales-pg"}


class TestWhereClause:
    def test_eq_filter(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse('{ orders(where: { region: { eq: "us-east" } }) { id amount } }')
        results = compile_query(doc, ctx)
        q = results[0]
        assert 'WHERE "region" = $1' in q.sql
        assert q.params == ["us-east"]

    def test_multiple_filters(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse('{ orders(where: { region: { eq: "us" }, status: { eq: "done" } }) { id } }')
        results = compile_query(doc, ctx)
        q = results[0]
        assert "$1" in q.sql
        assert "$2" in q.sql
        assert len(q.params) == 2

    def test_in_filter(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse('{ orders(where: { region: { in: ["us", "eu"] } }) { id } }')
        results = compile_query(doc, ctx)
        q = results[0]
        assert "IN ($1, $2)" in q.sql
        assert q.params == ["us", "eu"]

    def test_is_null_filter(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders(where: { region: { is_null: true } }) { id } }")
        results = compile_query(doc, ctx)
        assert "IS NULL" in results[0].sql

    def test_neq_filter(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse('{ orders(where: { status: { neq: "cancelled" } }) { id } }')
        results = compile_query(doc, ctx)
        assert '!= $1' in results[0].sql

    def test_like_filter(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse('{ orders(where: { region: { like: "%east%" } }) { id } }')
        results = compile_query(doc, ctx)
        assert "LIKE $1" in results[0].sql


class TestPagination:
    def test_limit(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders(limit: 10) { id } }")
        results = compile_query(doc, ctx)
        assert results[0].sql.endswith("LIMIT 10")

    def test_offset(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders(limit: 10, offset: 20) { id } }")
        results = compile_query(doc, ctx)
        assert "LIMIT 10 OFFSET 20" in results[0].sql

    def test_order_by(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders(order_by: [{ field: CREATED_AT, direction: DESC }]) { id } }")
        results = compile_query(doc, ctx)
        # Enum value resolves to column name (may be uppercase from enum definition)
        sql = results[0].sql
        assert "ORDER BY" in sql
        assert "DESC" in sql

    def test_full_pagination(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse(
            "{ orders(order_by: [{ field: ID, direction: ASC }], limit: 5, offset: 10) { id amount } }"
        )
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "ORDER BY" in sql
        assert "ASC" in sql
        assert "LIMIT 5" in sql
        assert "OFFSET 10" in sql


class TestNestedRelationship:
    def test_many_to_one_join(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders { id amount customers { name email } } }")
        results = compile_query(doc, ctx)
        q = results[0]
        assert '"t0"."id"' in q.sql
        assert '"t0"."amount"' in q.sql
        assert '"t1"."name"' in q.sql
        assert '"t1"."email"' in q.sql
        assert 'LEFT JOIN "public"."customers" "t1"' in q.sql
        assert '"t0"."customer_id" = "t1"."id"' in q.sql

    def test_join_tracks_multiple_sources(self):
        """When join crosses sources, both source_ids are tracked."""
        tables = [
            {
                "id": 1,
                "source_id": "src-a",
                "domain_id": "d",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "cust_id", "visible_to": ["admin"]},
                ],
            },
            {
                "id": 2,
                "source_id": "src-b",
                "domain_id": "d",
                "schema_name": "public",
                "table_name": "customers",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "name", "visible_to": ["admin"]},
                ],
            },
        ]
        rels = [
            {
                "id": "r1",
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "cust_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            }
        ]
        col_types = {
            1: [_col("id", "integer"), _col("cust_id", "integer")],
            2: [_col("id", "integer"), _col("name", "varchar")],
        }

        si = SchemaInput(
            tables=tables,
            relationships=rels,
            column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders { id customers { name } } }")
        results = compile_query(doc, ctx)
        assert results[0].sources == {"src-a", "src-b"}

    def test_columns_metadata_for_nested(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders { id customers { name } } }")
        results = compile_query(doc, ctx)
        cols = results[0].columns
        root_cols = [c for c in cols if c.nested_in is None]
        nested_cols = [c for c in cols if c.nested_in == "customers"]
        assert len(root_cols) == 1
        assert root_cols[0].field_name == "id"
        assert len(nested_cols) == 1
        assert nested_cols[0].field_name == "name"


class TestJoinTypeCast:
    """CAST is added to JOIN ON only when column types are incompatible."""

    def test_no_cast_for_same_types(self, schema_and_ctx):
        """integer = integer → no CAST."""
        schema, ctx = schema_and_ctx
        doc = parse("{ orders { id customers { name } } }")
        results = compile_query(doc, ctx)
        sql = results[0].sql
        # Should be plain column refs, no CAST
        assert "CAST" not in sql
        assert '"t0"."customer_id" = "t1"."id"' in sql

    def test_no_cast_for_compatible_numeric_types(self):
        """integer JOIN bigint → no CAST (same numeric group)."""
        tables = [
            {
                "id": 1, "source_id": "s1", "domain_id": "d",
                "schema_name": "public", "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "product_id", "visible_to": ["admin"]},
                ],
            },
            {
                "id": 2, "source_id": "s2", "domain_id": "d",
                "schema_name": "public", "table_name": "reviews",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "product_id", "visible_to": ["admin"]},
                    {"column_name": "rating", "visible_to": ["admin"]},
                ],
            },
        ]
        rels = [{
            "id": "r1", "source_table_id": 1, "target_table_id": 2,
            "source_column": "product_id", "target_column": "product_id",
            "cardinality": "many-to-one",
        }]
        col_types = {
            1: [_col("id", "integer"), _col("product_id", "integer")],
            2: [_col("product_id", "bigint"), _col("rating", "integer")],
        }
        si = SchemaInput(
            tables=tables, relationships=rels, column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders { id reviews { rating } } }")
        results = compile_query(doc, ctx)
        assert "CAST" not in results[0].sql

    def test_cast_for_incompatible_types(self):
        """varchar JOIN integer → CAST both to VARCHAR."""
        tables = [
            {
                "id": 1, "source_id": "s1", "domain_id": "d",
                "schema_name": "public", "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "ext_ref", "visible_to": ["admin"]},
                ],
            },
            {
                "id": 2, "source_id": "s2", "domain_id": "d",
                "schema_name": "public", "table_name": "externals",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "ref_id", "visible_to": ["admin"]},
                    {"column_name": "label", "visible_to": ["admin"]},
                ],
            },
        ]
        rels = [{
            "id": "r1", "source_table_id": 1, "target_table_id": 2,
            "source_column": "ext_ref", "target_column": "ref_id",
            "cardinality": "many-to-one",
        }]
        col_types = {
            1: [_col("id", "integer"), _col("ext_ref", "varchar(50)")],
            2: [_col("ref_id", "integer"), _col("label", "varchar")],
        }
        si = SchemaInput(
            tables=tables, relationships=rels, column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema = generate_schema(si)
        ctx = build_context(si)
        doc = parse("{ orders { id externals { label } } }")
        results = compile_query(doc, ctx)
        sql = results[0].sql
        # varchar side stays as-is, integer side gets CAST
        assert "CAST" in sql
        assert "VARCHAR" in sql


class TestVariables:
    def test_variable_in_where(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse(
            "query Q($r: String) { orders(where: { region: { eq: $r } }) { id } }"
        )
        results = compile_query(doc, ctx, variables={"r": "us-west"})
        assert results[0].params == ["us-west"]

    def test_missing_variable_raises(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse(
            "query Q($r: String) { orders(where: { region: { eq: $r } }) { id } }"
        )
        with pytest.raises(ValueError, match="Variable"):
            compile_query(doc, ctx, variables={})


class TestUnknownField:
    def test_unknown_root_field(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        # Parse against a schema that doesn't have "bogus" — validation catches it
        doc = parse("{ bogus { id } }")
        errors = validate(schema, doc)
        assert errors  # GraphQL validation catches unknown root fields


class TestMultipleRootFields:
    def test_two_root_fields(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders { id } customers { id name } }")
        results = compile_query(doc, ctx)
        assert len(results) == 2
        assert results[0].root_field == "orders"
        assert results[1].root_field == "customers"


class TestRelationshipVisibility:
    def test_relationship_hidden_when_join_column_not_visible(self):
        """If the join column (customer_id) is not visible to a role,
        the relationship should not appear in the schema."""
        tables = [
            {
                "id": 1, "source_id": "pg", "domain_id": "d",
                "schema_name": "public", "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin", "limited"]},
                    {"column_name": "customer_id", "visible_to": ["admin"]},  # NOT visible to 'limited'
                    {"column_name": "amount", "visible_to": ["admin", "limited"]},
                ],
            },
            {
                "id": 2, "source_id": "pg", "domain_id": "d",
                "schema_name": "public", "table_name": "customers",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin", "limited"]},
                    {"column_name": "name", "visible_to": ["admin", "limited"]},
                ],
            },
        ]
        rels = [{
            "id": "r1", "source_table_id": 1, "target_table_id": 2,
            "source_column": "customer_id", "target_column": "id",
            "cardinality": "many-to-one",
        }]
        col_types = {
            1: [_col("id", "integer"), _col("customer_id", "integer"), _col("amount", "decimal")],
            2: [_col("id", "integer"), _col("name", "varchar")],
        }

        # Admin can see customer_id → relationship visible
        si_admin = SchemaInput(
            tables=tables, relationships=rels, column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema_admin = generate_schema(si_admin)
        doc = parse("{ orders { id customers { name } } }")
        errors = validate(schema_admin, doc)
        assert not errors  # admin can traverse the relationship

        # Limited cannot see customer_id → relationship hidden
        si_limited = SchemaInput(
            tables=tables, relationships=rels, column_types=col_types,
            naming_rules=[],
            role={"id": "limited", "capabilities": [], "domain_access": ["*"]},
            domains=[{"id": "d", "description": "D"}],
        )
        schema_limited = generate_schema(si_limited)
        doc2 = parse("{ orders { id customers { name } } }")
        errors2 = validate(schema_limited, doc2)
        assert errors2  # 'customers' field should not exist on Orders for limited role
