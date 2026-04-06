# Copyright (c) 2026 Kenneth Stott
# Canary: b82d4f1e-9c37-4a50-8f71-6e3ab0c1d852
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for upsert SQL generation and DISTINCT ON (REQ-212, REQ-213)."""

from __future__ import annotations

import pytest
from graphql import parse, validate
from graphql.language.ast import (
    ArgumentNode,
    FieldNode,
    IntValueNode,
    ListValueNode,
    NameNode,
    ObjectFieldNode,
    ObjectValueNode,
    StringValueNode,
)

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import MutationResult, compile_mutation, compile_upsert
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import TableMeta, build_context, compile_query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _table_meta(
    schema_name: str = "public",
    table_name: str = "orders",
    source_id: str = "sales-pg",
    table_id: int = 1,
) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id.replace("-", "_"),
        schema_name=schema_name,
        table_name=table_name,
    )


def _build():
    """Return (schema, ctx) for a simple orders table with id/amount/region."""
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
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
    ]
    col_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
        ],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    return generate_schema(si), build_context(si)


# ---------------------------------------------------------------------------
# AST builders for direct compile_upsert tests
# ---------------------------------------------------------------------------


def _name(s: str) -> NameNode:
    return NameNode(value=s)


def _str_val(s: str) -> StringValueNode:
    return StringValueNode(value=s)


def _int_val(i: int) -> IntValueNode:
    return IntValueNode(value=str(i))


def _obj_val(fields: dict) -> ObjectValueNode:
    """Build an ObjectValueNode from a plain Python dict of {name: AST value node}."""
    obj_fields = [
        ObjectFieldNode(name=_name(k), value=v)
        for k, v in fields.items()
    ]
    return ObjectValueNode(fields=tuple(obj_fields))


def _list_val(items: list) -> ListValueNode:
    return ListValueNode(values=tuple(items))


def _field_node(arguments: list[ArgumentNode]) -> FieldNode:
    """Build a minimal FieldNode with the given arguments."""
    return FieldNode(
        name=_name("upsert_orders"),
        arguments=tuple(arguments),
        selection_set=None,
        directives=(),
        alias=None,
    )


def _arg(name: str, value) -> ArgumentNode:
    return ArgumentNode(name=_name(name), value=value)


# ---------------------------------------------------------------------------
# TestCompileUpsert — direct unit tests on compile_upsert()
# ---------------------------------------------------------------------------


class TestCompileUpsert:
    def test_basic_upsert_generates_insert_on_conflict(self):
        table = _table_meta()
        field = _field_node([
            _arg("input", _obj_val({"id": _int_val(1), "amount": _str_val("42.0")})),
            _arg("on_conflict", _list_val([_str_val("id")])),
        ])
        result = compile_upsert(field, table, None)
        assert "INSERT INTO" in result.sql
        assert "ON CONFLICT" in result.sql

    def test_conflict_column_in_sql(self):
        table = _table_meta()
        field = _field_node([
            _arg("input", _obj_val({"id": _int_val(5), "region": _str_val("eu")})),
            _arg("on_conflict", _list_val([_str_val("id")])),
        ])
        result = compile_upsert(field, table, None)
        assert '"id"' in result.sql
        assert "ON CONFLICT" in result.sql

    def test_do_update_set_for_non_conflict_columns(self):
        table = _table_meta()
        field = _field_node([
            _arg("input", _obj_val({
                "id": _int_val(1),
                "amount": _str_val("99.9"),
                "region": _str_val("us"),
            })),
            _arg("on_conflict", _list_val([_str_val("id")])),
        ])
        result = compile_upsert(field, table, None)
        assert "DO UPDATE SET" in result.sql
        assert "EXCLUDED" in result.sql
        # Non-conflict columns should appear in SET clause
        assert '"amount"' in result.sql
        assert '"region"' in result.sql

    def test_missing_input_raises_value_error(self):
        table = _table_meta()
        field = _field_node([
            _arg("on_conflict", _list_val([_str_val("id")])),
        ])
        with pytest.raises(ValueError, match="input"):
            compile_upsert(field, table, None)

    def test_missing_on_conflict_raises_value_error(self):
        table = _table_meta()
        field = _field_node([
            _arg("input", _obj_val({"id": _int_val(1)})),
        ])
        with pytest.raises(ValueError, match="on_conflict"):
            compile_upsert(field, table, None)

    def test_string_on_conflict_auto_wrapped_in_list(self):
        """A bare string on_conflict is internally coerced to a list."""
        table = _table_meta()
        # Simulate a case where _extract_value returns a string (single enum value)
        # by providing a StringValueNode directly (not wrapped in a list).
        field = _field_node([
            _arg("input", _obj_val({"id": _int_val(3), "region": _str_val("ap")})),
            _arg("on_conflict", _str_val("id")),
        ])
        result = compile_upsert(field, table, None)
        # Should not crash; conflict column used
        assert "ON CONFLICT" in result.sql

    def test_returns_mutation_result_with_upsert_type(self):
        table = _table_meta()
        field = _field_node([
            _arg("input", _obj_val({"id": _int_val(1), "region": _str_val("us")})),
            _arg("on_conflict", _list_val([_str_val("id")])),
        ])
        result = compile_upsert(field, table, None)
        assert isinstance(result, MutationResult)
        assert result.mutation_type == "upsert"

    def test_all_conflict_columns_generates_do_nothing(self):
        """When every input column is a conflict column, emit DO NOTHING."""
        table = _table_meta()
        field = _field_node([
            _arg("input", _obj_val({"id": _int_val(7)})),
            _arg("on_conflict", _list_val([_str_val("id")])),
        ])
        result = compile_upsert(field, table, None)
        assert "DO NOTHING" in result.sql
        assert "DO UPDATE" not in result.sql

    def test_returning_clause_present(self):
        table = _table_meta()
        field = _field_node([
            _arg("input", _obj_val({"id": _int_val(1), "amount": _str_val("10")})),
            _arg("on_conflict", _list_val([_str_val("id")])),
        ])
        result = compile_upsert(field, table, None)
        assert "RETURNING" in result.sql

    def test_source_id_on_result(self):
        table = _table_meta(source_id="crm-pg")
        field = _field_node([
            _arg("input", _obj_val({"id": _int_val(1), "region": _str_val("us")})),
            _arg("on_conflict", _list_val([_str_val("id")])),
        ])
        result = compile_upsert(field, table, None)
        assert result.source_id == "crm-pg"


# ---------------------------------------------------------------------------
# TestCompileUpsertViaGraphQL — higher-level tests through compile_mutation
# ---------------------------------------------------------------------------


class TestCompileUpsertViaGraphQL:
    """These tests mirror the pattern from test_mutation_sql.py but focus on
    structural SQL correctness for multi-column conflict targets and param
    ordering, which the existing tests don't fully cover."""

    def test_multi_column_conflict_key(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                upsert_orders(
                    input: { id: 1, amount: 10.0, region: "us" }
                    on_conflict: [id, region]
                ) { affected_rows }
            }
        """)
        assert not validate(schema, doc)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        # Both conflict columns in ON CONFLICT clause
        assert '"id"' in m.sql
        assert '"region"' in m.sql
        # Only non-conflict column (amount) in DO UPDATE SET
        assert "DO UPDATE SET" in m.sql
        assert '"amount"' in m.sql

    def test_params_in_insertion_order(self):
        schema, ctx = _build()
        doc = parse("""
            mutation {
                upsert_orders(
                    input: { id: 2, amount: 55.5, region: "ap" }
                    on_conflict: [id]
                ) { affected_rows }
            }
        """)
        results = compile_mutation(doc, ctx, {"sales-pg": "postgresql"})
        m = results[0]
        assert m.params == [2, 55.5, "ap"]


# ---------------------------------------------------------------------------
# TestDistinctOn — SQL generation with distinct_on argument
# ---------------------------------------------------------------------------


@pytest.fixture
def schema_and_ctx():
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
            ],
        },
    ]
    col_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
        ],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


class TestDistinctOn:
    def test_distinct_on_single_column_in_sql(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders(distinct_on: [customer_id]) { id customer_id } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "DISTINCT ON" in sql
        assert "customer_id" in sql

    def test_distinct_on_appears_immediately_after_select(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders(distinct_on: [customer_id]) { id customer_id } }")
        results = compile_query(doc, ctx)
        sql = results[0].sql
        # The DISTINCT ON prefix must be right after SELECT
        stripped = sql.lstrip()
        assert stripped.upper().startswith("SELECT DISTINCT ON")

    def test_distinct_on_multiple_columns_comma_separated(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders(distinct_on: [customer_id, region]) { id customer_id region } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "DISTINCT ON" in sql
        assert "customer_id" in sql
        assert "region" in sql
        # Comma between them
        distinct_start = sql.index("DISTINCT ON")
        close_paren = sql.index(")", distinct_start)
        distinct_clause = sql[distinct_start:close_paren + 1]
        assert "," in distinct_clause

    def test_distinct_on_with_order_by_present(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("""
            {
                orders(
                    distinct_on: [customer_id]
                    order_by: [{ customer_id: asc }]
                ) { id customer_id }
            }
        """)
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "DISTINCT ON" in sql
        assert "ORDER BY" in sql

    def test_without_distinct_on_no_distinct_keyword(self, schema_and_ctx):
        schema, ctx = schema_and_ctx
        doc = parse("{ orders { id customer_id } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "DISTINCT" not in sql
