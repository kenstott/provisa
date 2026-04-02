# Copyright (c) 2025 Kenneth Stott
# Canary: af870f23-c0e4-4042-b071-c6474037eec2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for mutation SQL generation."""

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import (
    compile_mutation,
    inject_rls_into_mutation,
)
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build():
    tables = [
        {
            "id": 1, "source_id": "sales-pg", "domain_id": "sales",
            "schema_name": "public", "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
    ]
    col_types = {
        1: [_col("id", "integer"), _col("amount", "decimal(10,2)"), _col("region", "varchar(50)")],
    }
    si = SchemaInput(
        tables=tables, relationships=[], column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


class TestInsertMutation:
    def test_basic_insert(self):
        schema, ctx = _build()
        doc = parse("""
            mutation { insert_orders(input: { amount: 42.0, region: "us-east" }) { affected_rows } }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert len(results) == 1
        m = results[0]
        assert m.mutation_type == "insert"
        assert "INSERT INTO" in m.sql
        assert '"amount"' in m.sql
        assert '"region"' in m.sql
        assert "$1" in m.sql
        assert m.params == [42.0, "us-east"]

    def test_insert_source_id(self):
        schema, ctx = _build()
        doc = parse('mutation { insert_orders(input: { region: "x" }) { affected_rows } }')
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        assert results[0].source_id == "sales-pg"


class TestUpdateMutation:
    def test_basic_update(self):
        schema, ctx = _build()
        doc = parse("""
            mutation { update_orders(set: { amount: 99.0 }, where: { id: { eq: 1 } }) { affected_rows } }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        assert m.mutation_type == "update"
        assert "UPDATE" in m.sql
        assert "SET" in m.sql
        assert "WHERE" in m.sql
        assert m.params == [99.0, 1]


class TestDeleteMutation:
    def test_basic_delete(self):
        schema, ctx = _build()
        doc = parse("""
            mutation { delete_orders(where: { id: { eq: 5 } }) { affected_rows } }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        assert m.mutation_type == "delete"
        assert "DELETE FROM" in m.sql
        assert "WHERE" in m.sql
        assert m.params == [5]


class TestNoSQLRejection:
    def test_nosql_source_rejected(self):
        schema, ctx = _build()
        # Override source type to mongodb
        doc = parse('mutation { insert_orders(input: { region: "x" }) { affected_rows } }')
        with pytest.raises(ValueError, match="NoSQL"):
            compile_mutation(doc, ctx, {"sales-pg": "mongodb"})


class TestRLSOnMutation:
    def test_rls_injected_into_update(self):
        schema, ctx = _build()
        doc = parse("""
            mutation { update_orders(set: { amount: 1.0 }, where: { id: { eq: 1 } }) { affected_rows } }
        """)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        m = inject_rls_into_mutation(m, 1, {1: "region = 'us'"})
        assert "region = 'us'" in m.sql
        assert "AND" in m.sql

    def test_rls_injected_into_delete(self):
        schema, ctx = _build()
        doc = parse('mutation { delete_orders(where: { id: { eq: 1 } }) { affected_rows } }')
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        m = inject_rls_into_mutation(m, 1, {1: "region = 'us'"})
        assert "region = 'us'" in m.sql

    def test_rls_not_injected_into_insert(self):
        schema, ctx = _build()
        doc = parse('mutation { insert_orders(input: { region: "x" }) { affected_rows } }')
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        m = inject_rls_into_mutation(m, 1, {1: "region = 'us'"})
        assert "region = 'us'" not in m.sql  # INSERT has no WHERE

    def test_no_rls_when_no_rule(self):
        schema, ctx = _build()
        doc = parse('mutation { delete_orders(where: { id: { eq: 1 } }) { affected_rows } }')
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        original_sql = m.sql
        m = inject_rls_into_mutation(m, 1, {})  # no rules
        assert m.sql == original_sql
