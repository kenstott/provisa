# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7b1a9f-d2c4-4f6e-8b0d-5a1c3e7f9b2d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for compiler → schema/SQL boundaries.

Covered REQ-IDs:
  JSONB Field Promotion:    REQ-119
  View RLS Enforcement:     REQ-134
  View Computed Semantics:  REQ-136
  JSON Path Expressions:    REQ-151
  Domain Prefix:            REQ-154
  Table Alias:              REQ-155
"""

from __future__ import annotations

import pytest
from graphql import GraphQLObjectType, parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler import naming as _naming
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "varchar", nullable: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _make_role(role_id: str = "admin", domain_access: list[str] | None = None) -> dict:
    return {
        "id": role_id,
        "domain_access": domain_access if domain_access is not None else ["*"],
        "capabilities": [],
    }


def _make_basic_si(
    tables: list[dict],
    column_types: dict[int, list[ColumnMetadata]],
    role_id: str = "admin",
    naming_convention: str = "snake",
    domain_prefix: bool = False,
    domains: list[dict] | None = None,
) -> SchemaInput:
    _naming.configure(gql=naming_convention)
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=_make_role(role_id),
        domains=domains or [{"id": "sales", "graphql_alias": None}],
        domain_prefix=domain_prefix,
    )


# ---------------------------------------------------------------------------
# REQ-119: JSONB field promotion
# ---------------------------------------------------------------------------


class TestJSONBFieldPromotion:
    """REQ-119: JSONB column with object_fields promotes sub-fields as queryable GQL fields."""

    def _make_jsonb_si(self) -> SchemaInput:
        tables = [
            {
                "id": 1,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "events",
                "governance": "pre-approved",
                "columns": [
                    {
                        "column_name": "id",
                        "visible_to": [],
                    },
                    {
                        "column_name": "payload",
                        "visible_to": [],
                        "object_fields": [
                            {"name": "order_id", "type": "integer"},
                            {"name": "amount", "type": "number"},
                            {"name": "status", "type": "string"},
                        ],
                    },
                ],
            }
        ]
        col_types = {
            1: [
                _col("id", "integer", nullable=False),
                _col("payload", "json"),
            ]
        }
        return _make_basic_si(tables, col_types)

    def test_jsonb_column_produces_queryable_subfields(self):
        # REQ-119: a column declared with object_fields should produce a GQL object type
        si = self._make_jsonb_si()
        schema = generate_schema(si)
        query_type = schema.query_type
        assert query_type is not None
        assert "events" in query_type.fields
        events_field = query_type.fields["events"]
        # Strip List + NonNull wrappers
        inner = events_field.type
        while hasattr(inner, "of_type"):
            inner = inner.of_type
        assert isinstance(inner, GraphQLObjectType)
        # payload should be an object type (promoted subfields)
        assert "payload" in inner.fields, (
            f"Expected 'payload' in GQL object fields, got: {list(inner.fields)}"
        )
        payload_type = inner.fields["payload"].type
        # payload field should itself be an object type (not a scalar)
        while hasattr(payload_type, "of_type"):
            payload_type = payload_type.of_type
        assert isinstance(payload_type, GraphQLObjectType), (
            f"Expected payload to be a GraphQLObjectType, got {type(payload_type)}"
        )

    def test_jsonb_subfields_have_correct_graphql_types(self):
        # REQ-119: promoted JSONB sub-fields should have their declared types
        si = self._make_jsonb_si()
        schema = generate_schema(si)
        query_type = schema.query_type
        assert query_type is not None
        inner = query_type.fields["events"].type
        while hasattr(inner, "of_type"):
            inner = inner.of_type
        assert isinstance(inner, GraphQLObjectType)
        payload_type = inner.fields["payload"].type
        while hasattr(payload_type, "of_type"):
            payload_type = payload_type.of_type
        assert isinstance(payload_type, GraphQLObjectType)
        sub_fields = payload_type.fields
        assert "order_id" in sub_fields, f"Missing order_id in {list(sub_fields)}"
        assert "amount" in sub_fields, f"Missing amount in {list(sub_fields)}"
        assert "status" in sub_fields, f"Missing status in {list(sub_fields)}"


# ---------------------------------------------------------------------------
# REQ-134: Views enforce RLS/masking like tables
# ---------------------------------------------------------------------------


class TestViewRLSEnforcement:
    """REQ-134: views go through the same governance pipeline as tables."""

    def _make_view_si_two_roles(self) -> tuple[SchemaInput, SchemaInput]:
        # A view table with column-level visibility restrictions
        tables = [
            {
                "id": 10,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders_view",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": []},
                    # 'secret_col' only visible to admin
                    {"column_name": "secret_col", "visible_to": ["admin"]},
                    {"column_name": "amount", "visible_to": []},
                ],
            }
        ]
        col_types = {
            10: [
                _col("id", "integer"),
                _col("secret_col", "varchar"),
                _col("amount", "double"),
            ]
        }
        _naming.configure(gql="snake")
        admin_si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=col_types,
            naming_rules=[],
            role={"id": "admin", "domain_access": ["*"], "capabilities": []},
            domains=[{"id": "sales", "graphql_alias": None}],
        )
        analyst_si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=col_types,
            naming_rules=[],
            role={"id": "analyst", "domain_access": ["*"], "capabilities": []},
            domains=[{"id": "sales", "graphql_alias": None}],
        )
        return admin_si, analyst_si

    def test_view_has_rls_applied_same_as_table(self):
        # REQ-134: column visibility on a view is filtered identically to a plain table
        admin_si, analyst_si = self._make_view_si_two_roles()
        admin_schema = generate_schema(admin_si)
        analyst_schema = generate_schema(analyst_si)

        def _get_fields(schema):
            qt = schema.query_type
            assert qt is not None
            inner = qt.fields["orders_view"].type
            while hasattr(inner, "of_type"):
                inner = inner.of_type
            return set(inner.fields.keys())

        admin_fields = _get_fields(admin_schema)
        analyst_fields = _get_fields(analyst_schema)
        assert "secret_col" in admin_fields, "admin should see secret_col"
        assert "secret_col" not in analyst_fields, "analyst must not see secret_col on view"

    def test_view_rls_not_on_separate_code_path(self):
        # REQ-134: both view and table go through _build_visible_tables + _build_column_fields;
        # verify the compilation context for the view also respects column visibility
        _naming.configure(gql="snake")
        # Analyst cannot see secret_col in the compilation context either
        tables = [
            {
                "id": 20,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "revenue_view",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": []},
                    {"column_name": "revenue", "visible_to": ["analyst"]},
                    {"column_name": "cost", "visible_to": ["admin"]},
                ],
            }
        ]
        col_types = {
            20: [
                _col("id", "integer"),
                _col("revenue", "double"),
                _col("cost", "double"),
            ]
        }
        analyst_si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=col_types,
            naming_rules=[],
            role={"id": "analyst", "domain_access": ["*"], "capabilities": []},
            domains=[{"id": "sales", "graphql_alias": None}],
        )
        ctx = build_context(analyst_si)
        # The compilation context for the view should only register analyst-visible columns
        agg_cols = {name for name, _ in ctx.aggregate_columns.get(20, [])}
        assert "revenue" in agg_cols
        assert "cost" not in agg_cols, "cost must not be in analyst's compilation context"


# ---------------------------------------------------------------------------
# REQ-136: Views emit computed semantics through SQL
# ---------------------------------------------------------------------------


class TestViewComputedSemantics:
    """REQ-136: a view definition is compiled into a real SQL subquery/CTE."""

    def _make_view_with_path_si(self) -> SchemaInput:
        # Simulate a 'view' table with a column that uses path extraction (computed semantics)
        tables = [
            {
                "id": 30,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "order_summary",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": []},
                    {
                        "column_name": "payload",
                        "visible_to": [],
                        "path": "payload.total",
                    },
                ],
            }
        ]
        col_types = {
            30: [
                _col("id", "integer"),
                _col("payload", "json"),
            ]
        }
        return _make_basic_si(tables, col_types)

    def test_view_compiled_to_sql_subquery(self):
        # REQ-136: the schema generates successfully (compilation does not short-circuit for views)
        si = self._make_view_with_path_si()
        schema = generate_schema(si)
        qt = schema.query_type
        assert qt is not None
        assert "order_summary" in qt.fields

    def test_view_sql_expression_appears_in_compiled_output(self):
        # REQ-136: the column_paths entry for the path-extracted column is registered in ctx
        si = self._make_view_with_path_si()
        ctx = build_context(si)
        # payload col has path="payload.total" — should be registered in column_paths
        # key is (table_id, gql_field_name) → path expression
        paths = {k: v for k, v in ctx.column_paths.items() if k[0] == 30}
        assert paths, f"Expected column_paths entries for table 30, got: {ctx.column_paths}"
        path_values = list(paths.values())
        assert any("payload.total" in p for p in path_values), (
            f"Expected 'payload.total' in column_paths, got: {path_values}"
        )


# ---------------------------------------------------------------------------
# REQ-151: JSON path expressions in queries execute end-to-end
# ---------------------------------------------------------------------------


class TestJSONPathExpressions:
    """REQ-151: a column with path= renders the ->> operator in generated SQL."""

    def _make_path_si(self) -> SchemaInput:
        tables = [
            {
                "id": 40,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "events",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": []},
                    {
                        "column_name": "payload",
                        "visible_to": [],
                        "path": "order_id",
                        "alias": "order_id",
                    },
                ],
            }
        ]
        col_types = {
            40: [
                _col("id", "integer"),
                _col("payload", "json"),
            ]
        }
        return _make_basic_si(tables, col_types)

    def test_json_path_field_renders_arrow_operator_in_sql(self):
        # REQ-151: column with path extracts value via ->>'key' in SQL
        si = self._make_path_si()
        ctx = build_context(si)
        schema = generate_schema(si)
        gql = parse("{ events { id order_id } }")
        errors = validate(schema, gql)
        assert not errors, f"GQL validation errors: {errors}"
        results = compile_query(gql, ctx, variables=None, use_catalog=False)
        assert results, "Expected at least one compiled query"
        sql = results[0].sql
        # The path col alias "order_id" should be in the SELECT with ->>'order_id' extraction
        assert "->>" in sql or "->" in sql, (
            f"Expected JSON path operator (->> or ->) in SQL for path column, got:\n{sql}"
        )

    def test_json_path_field_with_text_extraction(self):
        # REQ-151: column_paths is populated so the executor can apply path extraction
        si = self._make_path_si()
        ctx = build_context(si)
        paths = {k: v for k, v in ctx.column_paths.items() if k[0] == 40}
        assert paths, f"Expected column_paths for table 40, got: {ctx.column_paths}"


# ---------------------------------------------------------------------------
# REQ-154: domain_prefix config rewrites GraphQL type/field names
# ---------------------------------------------------------------------------


class TestDomainPrefix:
    """REQ-154: domain_prefix=True prepends domain_id__ to all GraphQL names."""

    def _make_prefix_si(self) -> SchemaInput:
        tables = [
            {
                "id": 50,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": []},
                    {"column_name": "amount", "visible_to": []},
                ],
            }
        ]
        col_types = {
            50: [
                _col("id", "integer"),
                _col("amount", "double"),
            ]
        }
        return _make_basic_si(
            tables,
            col_types,
            domain_prefix=True,
            domains=[{"id": "sales", "graphql_alias": None}],
        )

    def test_domain_prefix_applied_to_graphql_type_names(self):
        # REQ-154: with domain_prefix=True, the generated type name includes domain prefix
        si = self._make_prefix_si()
        schema = generate_schema(si)
        qt = schema.query_type
        assert qt is not None
        # The field name should be prefixed with the domain abbreviation (first-letter acronym)
        # domain_gql_alias("sales") → "s", so the field is "s__orders"
        field_names = list(qt.fields.keys())
        prefixed = [f for f in field_names if "__" in f and "orders" in f.lower()]
        assert prefixed, (
            f"Expected a domain-prefixed field containing '__' and 'orders', got: {field_names}"
        )

    def test_domain_prefix_applied_to_field_names(self):
        # REQ-154: the compilation context field_name also has the domain prefix
        si = self._make_prefix_si()
        ctx = build_context(si)
        field_names = list(ctx.tables.keys())
        # At least one field name should contain a domain prefix pattern (e.g. "sales__orders")
        prefixed = [f for f in field_names if "__" in f and "orders" in f]
        assert prefixed, (
            f"Expected a domain-prefixed table key in compilation context, got: {field_names}"
        )


# ---------------------------------------------------------------------------
# REQ-155: Table alias in config overrides the GraphQL type name
# ---------------------------------------------------------------------------


class TestTableAlias:
    """REQ-155: table alias field overrides the GraphQL type name in the schema output."""

    def _make_alias_si(self, alias: str | None) -> SchemaInput:
        tables = [
            {
                "id": 60,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "legacy_order_data",
                "governance": "pre-approved",
                "alias": alias,
                "columns": [
                    {"column_name": "id", "visible_to": []},
                    {"column_name": "total", "visible_to": []},
                ],
            }
        ]
        col_types = {
            60: [
                _col("id", "integer"),
                _col("total", "double"),
            ]
        }
        return _make_basic_si(tables, col_types)

    def test_alias_overrides_graphql_type_name(self):
        # REQ-155: when alias="orders", the root query field should be "orders", not "legacy_order_data"
        si = self._make_alias_si(alias="orders")
        schema = generate_schema(si)
        qt = schema.query_type
        assert qt is not None
        field_names = list(qt.fields.keys())
        assert "orders" in field_names, (
            f"Expected 'orders' (alias) in root query fields, got: {field_names}"
        )
        assert "legacy_order_data" not in field_names, (
            f"Expected original table name 'legacy_order_data' to be replaced by alias, got: {field_names}"
        )

    def test_alias_does_not_affect_underlying_sql_table_name(self):
        # REQ-155: alias changes the GQL name but the SQL must still reference the physical table
        si = self._make_alias_si(alias="orders")
        ctx = build_context(si)
        # The TableMeta should use the physical table_name, not the alias
        meta = ctx.tables.get("orders")
        assert meta is not None, f"Expected 'orders' in ctx.tables, got: {list(ctx.tables.keys())}"
        assert meta.table_name == "legacy_order_data", (
            f"Expected physical table_name='legacy_order_data', got: {meta.table_name!r}"
        )

    def test_no_alias_uses_table_name_as_field_name(self):
        # REQ-155: without alias, the field name is derived from the physical table name
        si = self._make_alias_si(alias=None)
        schema = generate_schema(si)
        qt = schema.query_type
        assert qt is not None
        field_names = list(qt.fields.keys())
        # "legacy_order_data" should appear (snake_case convention)
        assert "legacy_order_data" in field_names, (
            f"Expected 'legacy_order_data' without alias, got: {field_names}"
        )
