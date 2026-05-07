# Copyright (c) 2026 Kenneth Stott
#
# Tests asserting layer-to-layer type contracts in the compiler pipeline.
# REQ-009, REQ-040, REQ-041, REQ-066

"""Layer contract tests: sql_gen → rls → executor type boundaries."""

import dataclasses

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import (
    CompilationContext,
    CompiledQuery,
    TableMeta,
    build_context,
    compile_query,
)
from provisa.executor.trino import QueryResult


# --- TableMeta field contract ---

class TestTableMetaContract:
    def test_has_table_id(self):
        field_names = {f.name for f in dataclasses.fields(TableMeta)}
        assert "table_id" in field_names

    def test_has_field_name(self):
        field_names = {f.name for f in dataclasses.fields(TableMeta)}
        assert "field_name" in field_names

    def test_has_source_id(self):
        field_names = {f.name for f in dataclasses.fields(TableMeta)}
        assert "source_id" in field_names

    def test_has_catalog_name(self):
        field_names = {f.name for f in dataclasses.fields(TableMeta)}
        assert "catalog_name" in field_names

    def test_has_schema_name(self):
        field_names = {f.name for f in dataclasses.fields(TableMeta)}
        assert "schema_name" in field_names

    def test_has_table_name(self):
        field_names = {f.name for f in dataclasses.fields(TableMeta)}
        assert "table_name" in field_names


# --- CompilationContext field contract ---

class TestCompilationContextContract:
    def test_has_tables_dict(self):
        ctx = CompilationContext()
        assert isinstance(ctx.tables, dict)

    def test_has_joins_dict(self):
        ctx = CompilationContext()
        assert isinstance(ctx.joins, dict)

    def test_tables_field_exists(self):
        field_names = {f.name for f in dataclasses.fields(CompilationContext)}
        assert "tables" in field_names

    def test_joins_field_exists(self):
        field_names = {f.name for f in dataclasses.fields(CompilationContext)}
        assert "joins" in field_names


# --- RLSContext field contract ---

class TestRLSContextContract:
    def test_has_rules_attribute(self):
        rls = RLSContext(rules={})
        assert hasattr(rls, "rules")

    def test_rules_is_dict(self):
        rls = RLSContext(rules={3: "tenant_id = 42"})
        assert isinstance(rls.rules, dict)

    def test_rules_maps_int_to_str(self):
        rules = {1: "user_id = 1", 2: "region = 'eu'"}
        rls = RLSContext(rules=rules)
        assert rls.rules[1] == "user_id = 1"
        assert rls.rules[2] == "region = 'eu'"

    def test_empty_factory(self):
        rls = RLSContext.empty()
        assert rls.rules == {}


# --- QueryResult field contract ---

class TestQueryResultContract:
    def test_has_rows(self):
        field_names = {f.name for f in dataclasses.fields(QueryResult)}
        assert "rows" in field_names

    def test_has_column_names(self):
        field_names = {f.name for f in dataclasses.fields(QueryResult)}
        assert "column_names" in field_names

    def test_rows_is_list(self):
        qr = QueryResult(rows=[(1, "a")], column_names=["id", "name"])
        assert isinstance(qr.rows, list)

    def test_column_names_is_list(self):
        qr = QueryResult(rows=[], column_names=["id"])
        assert isinstance(qr.column_names, list)


# --- CompiledQuery output contract ---

class TestCompiledQueryContract:
    def test_has_sql_attribute(self):
        field_names = {f.name for f in dataclasses.fields(CompiledQuery)}
        assert "sql" in field_names

    def test_sql_is_str(self):
        cq = CompiledQuery(
            sql="SELECT 1",
            params=[],
            root_field="test",
            columns=[],
            sources=set(),
        )
        assert isinstance(cq.sql, str)

    def test_has_params_attribute(self):
        field_names = {f.name for f in dataclasses.fields(CompiledQuery)}
        assert "params" in field_names


# --- Minimal fixture helpers (mirrors test_sql_gen._build_schema_and_ctx) ---

def _col(name: str, data_type: str = "varchar(100)") -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=False)


def _build_minimal_ctx():
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
            ],
        }
    ]
    column_types = {
        1: [_col("id", "integer"), _col("amount", "decimal(10,2)")]
    }
    role = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}
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


# --- Round-trip: CompilationContext → compile_query → CompiledQuery → inject_rls ---

class TestRoundTripContracts:
    def test_compile_produces_compiled_query(self):
        schema, ctx = _build_minimal_ctx()
        doc = parse("{ orders { id amount } }")
        assert not validate(schema, doc)
        results = compile_query(doc, ctx)
        assert len(results) == 1
        assert isinstance(results[0], CompiledQuery)

    def test_compiled_query_sql_is_str(self):
        schema, ctx = _build_minimal_ctx()
        doc = parse("{ orders { id amount } }")
        results = compile_query(doc, ctx)
        assert isinstance(results[0].sql, str)

    def test_compiled_query_sql_nonempty(self):
        schema, ctx = _build_minimal_ctx()
        doc = parse("{ orders { id amount } }")
        results = compile_query(doc, ctx)
        assert len(results[0].sql) > 0

    def test_inject_rls_accepts_compiled_query_output(self):
        schema, ctx = _build_minimal_ctx()
        doc = parse("{ orders { id amount } }")
        results = compile_query(doc, ctx)
        compiled = results[0]
        rls = RLSContext.empty()
        # inject_rls must accept CompiledQuery + CompilationContext + RLSContext
        result = inject_rls(compiled, ctx, rls)
        assert isinstance(result, CompiledQuery)

    def test_inject_rls_output_sql_is_str(self):
        schema, ctx = _build_minimal_ctx()
        doc = parse("{ orders { id amount } }")
        results = compile_query(doc, ctx)
        compiled = results[0]
        rls = RLSContext(rules={1: "amount > 0"})
        result = inject_rls(compiled, ctx, rls)
        assert isinstance(result.sql, str)

    def test_inject_rls_with_rule_modifies_sql(self):
        schema, ctx = _build_minimal_ctx()
        doc = parse("{ orders { id amount } }")
        results = compile_query(doc, ctx)
        compiled = results[0]
        rls = RLSContext(rules={1: "amount > 0"})
        result = inject_rls(compiled, ctx, rls)
        assert "amount > 0" in result.sql

    def test_inject_rls_empty_rules_returns_same_sql(self):
        schema, ctx = _build_minimal_ctx()
        doc = parse("{ orders { id amount } }")
        results = compile_query(doc, ctx)
        compiled = results[0]
        rls = RLSContext.empty()
        result = inject_rls(compiled, ctx, rls)
        assert result.sql == compiled.sql
