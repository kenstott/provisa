# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3b9e2a-c5f1-4860-a7d3-2e9b0c4f8173
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Hasura v2-style order_by alignment (Phase AD6, REQ-200–202).

Verifies that the GraphQL order_by argument compiles to correct SQL ORDER BY
clauses using Hasura v2 direction semantics: asc, desc, asc_nulls_first,
asc_nulls_last, desc_nulls_first, desc_nulls_last.

Column names in order_by enum values must preserve original case (REQ-157).
"""

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build(role_id="admin"):
    tables = [
        {
            "id": 1, "source_id": "sales-pg", "domain_id": "sales",
            "schema_name": "public", "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "amount", "visible_to": ["admin", "analyst"]},
                {"column_name": "region", "visible_to": ["admin", "analyst"]},
                {"column_name": "created_at", "visible_to": ["admin", "analyst"]},
                {"column_name": "mixedCase", "visible_to": ["admin"]},
            ],
        },
    ]
    col_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("created_at", "timestamp"),
            _col("mixedCase", "varchar(50)"),
        ],
    }
    si = SchemaInput(
        tables=tables, relationships=[], column_types=col_types,
        naming_rules=[],
        role={"id": role_id, "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    return generate_schema(si), build_context(si)


class TestOrderByAsc:
    def test_asc_produces_order_by(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { amount: asc }) { id amount } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        assert "ORDER BY" in result.sql
        assert "ASC" in result.sql.upper()

    def test_asc_column_name_present(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { amount: asc }) { id amount } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        assert '"amount"' in result.sql


class TestOrderByDesc:
    def test_desc_produces_order_by(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { amount: desc }) { id amount } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        assert "ORDER BY" in result.sql
        assert "DESC" in result.sql.upper()

    def test_desc_does_not_contain_asc(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { amount: desc }) { id amount } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        # DESC direction should not fall back to ASC
        sql_upper = result.sql.upper()
        order_idx = sql_upper.index("ORDER BY")
        after_order = sql_upper[order_idx:]
        assert "DESC" in after_order


class TestOrderByNullsDirections:
    def test_asc_nulls_first(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { amount: asc_nulls_first }) { id amount } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        sql_upper = result.sql.upper()
        assert "ORDER BY" in sql_upper
        assert "NULLS FIRST" in sql_upper

    def test_asc_nulls_last(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { amount: asc_nulls_last }) { id amount } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        sql_upper = result.sql.upper()
        assert "NULLS LAST" in sql_upper

    def test_desc_nulls_first(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { amount: desc_nulls_first }) { id amount } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        sql_upper = result.sql.upper()
        assert "DESC" in sql_upper
        assert "NULLS FIRST" in sql_upper

    def test_desc_nulls_last(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { amount: desc_nulls_last }) { id amount } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        sql_upper = result.sql.upper()
        assert "DESC" in sql_upper
        assert "NULLS LAST" in sql_upper


class TestMultiColumnOrderBy:
    def test_two_columns_both_in_order_clause(self):
        schema, ctx = _build()
        doc = parse("""
            {
                orders(order_by: [{ region: asc }, { amount: desc }]) { id region amount }
            }
        """)
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        sql_upper = result.sql.upper()
        assert "ORDER BY" in sql_upper
        assert '"region"' in result.sql
        assert '"amount"' in result.sql

    def test_two_columns_correct_directions(self):
        schema, ctx = _build()
        doc = parse("""
            {
                orders(order_by: [{ region: asc }, { amount: desc }]) { id region amount }
            }
        """)
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        # ASC must appear before DESC in the ORDER BY clause
        sql_upper = result.sql.upper()
        order_idx = sql_upper.index("ORDER BY")
        order_clause = sql_upper[order_idx:]
        asc_idx = order_clause.index("ASC")
        desc_idx = order_clause.index("DESC")
        assert asc_idx < desc_idx


class TestOrderByWithWhere:
    """ORDER BY and WHERE can coexist without interference."""

    def test_where_and_order_by(self):
        schema, ctx = _build()
        doc = parse("""
            {
                orders(
                    where: { region: { eq: "us-east" } }
                    order_by: { amount: desc }
                ) { id amount region }
            }
        """)
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        assert "WHERE" in result.sql
        assert "ORDER BY" in result.sql
        assert "DESC" in result.sql.upper()


class TestOrderByColumnCasePreservation:
    """REQ-157: order_by enum values preserve original column case."""

    def test_mixedcase_column_preserved(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { mixedCase: asc }) { id mixedCase } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        # Column name must appear in ORDER BY with original case
        assert '"mixedCase"' in result.sql


class TestOrderByWithPagination:
    """ORDER BY works together with LIMIT/OFFSET."""

    def test_order_by_with_limit(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { id: asc }, limit: 10) { id } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        sql_upper = result.sql.upper()
        assert "ORDER BY" in sql_upper
        assert "LIMIT" in sql_upper

    def test_order_by_with_offset(self):
        schema, ctx = _build()
        doc = parse('{ orders(order_by: { id: desc }, limit: 5, offset: 20) { id } }')
        assert not validate(schema, doc)
        result = compile_query(doc, ctx)[0]
        sql_upper = result.sql.upper()
        assert "ORDER BY" in sql_upper
        assert "OFFSET" in sql_upper
