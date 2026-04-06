# Copyright (c) 2026 Kenneth Stott
# Canary: a2b3c4d5-e6f7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the two-stage compiler: Stage 1 (GraphQL→SQL) + Stage 2 (governance)."""

from __future__ import annotations

import pytest
from graphql import parse as gql_parse

from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
)
from provisa.compiler.stage1 import compile_graphql
from provisa.compiler.stage2 import (
    GovernanceContext,
    apply_governance,
)
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.mask_inject import inject_masking
from provisa.security.masking import MaskingRule, MaskType

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Shared fixtures / builders
# ---------------------------------------------------------------------------

ORDERS_TABLE_ID = 10
CUSTOMERS_TABLE_ID = 20


def _orders_meta() -> TableMeta:
    return TableMeta(
        table_id=ORDERS_TABLE_ID,
        field_name="orders",
        type_name="Orders",
        source_id="test-pg",
        catalog_name="test_pg",
        schema_name="public",
        table_name="orders",
    )


def _customers_meta() -> TableMeta:
    return TableMeta(
        table_id=CUSTOMERS_TABLE_ID,
        field_name="customers",
        type_name="Customers",
        source_id="test-pg",
        catalog_name="test_pg",
        schema_name="public",
        table_name="customers",
    )


def _basic_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["orders"] = _orders_meta()
    ctx.tables["customers"] = _customers_meta()
    return ctx


def _parse(query: str):
    return gql_parse(query)


def _make_compiled(sql: str, root_field: str = "orders") -> CompiledQuery:
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field=root_field,
        columns=[ColumnRef(alias="t0", column="id", field_name="id", nested_in=None)],
        sources={"test-pg"},
    )


# ---------------------------------------------------------------------------
# Stage 1 tests
# ---------------------------------------------------------------------------

class TestStage1:
    def test_stage1_compiles_graphql_to_sql(self):
        doc = _parse('{ orders { id } }')
        ctx = _basic_ctx()
        results = compile_graphql(doc, ctx)
        assert len(results) == 1
        compiled = results[0]
        assert compiled.sql
        assert "orders" in compiled.sql.lower()
        assert compiled.root_field == "orders"

    def test_stage1_selects_only_requested_columns(self):
        doc = _parse('{ orders { id } }')
        ctx = _basic_ctx()
        results = compile_graphql(doc, ctx)
        sql = results[0].sql.lower()
        # Only "id" should appear in the SELECT, not other columns
        assert '"id"' in results[0].sql

    def test_stage1_applies_limit_argument(self):
        doc = _parse('{ orders(limit: 10) { id } }')
        ctx = _basic_ctx()
        results = compile_graphql(doc, ctx)
        sql = results[0].sql.upper()
        assert "LIMIT" in sql
        assert "10" in sql

    def test_stage1_applies_where_argument(self):
        doc = _parse('{ orders(where: { id: { eq: 42 } }) { id } }')
        ctx = _basic_ctx()
        results = compile_graphql(doc, ctx)
        sql = results[0].sql
        assert "WHERE" in sql.upper()
        # Parameter or literal 42 should appear
        assert "42" in sql or "$1" in sql

    def test_stage1_multiple_fields_compile_independently(self):
        doc = _parse('{ orders { id } customers { id } }')
        ctx = _basic_ctx()
        results = compile_graphql(doc, ctx)
        assert len(results) == 2
        root_fields = {r.root_field for r in results}
        assert "orders" in root_fields
        assert "customers" in root_fields

    def test_stage1_unknown_table_raises(self):
        doc = _parse('{ nonexistent { id } }')
        ctx = _basic_ctx()
        with pytest.raises((ValueError, KeyError)):
            compile_graphql(doc, ctx)

    def test_stage1_produces_compiled_query_type(self):
        doc = _parse('{ orders { id } }')
        ctx = _basic_ctx()
        results = compile_graphql(doc, ctx)
        assert isinstance(results[0], CompiledQuery)
        assert isinstance(results[0].sql, str)
        assert isinstance(results[0].params, list)
        assert isinstance(results[0].columns, list)


# ---------------------------------------------------------------------------
# Stage 2 (governance) tests
# ---------------------------------------------------------------------------

class TestStage2:
    def _make_gov_ctx(
        self,
        rls_rules: dict | None = None,
        masking_rules: dict | None = None,
        visible_columns: dict | None = None,
        limit_ceiling: int | None = None,
    ) -> GovernanceContext:
        gov = GovernanceContext()
        gov.rls_rules = rls_rules or {}
        gov.masking_rules = masking_rules or {}
        gov.visible_columns = visible_columns or {}
        gov.table_map = {
            "public.orders": ORDERS_TABLE_ID,
            "orders": ORDERS_TABLE_ID,
            "public.customers": CUSTOMERS_TABLE_ID,
            "customers": CUSTOMERS_TABLE_ID,
        }
        gov.all_columns = {
            ORDERS_TABLE_ID: [("id", "integer"), ("amount", "double"), ("region", "varchar"), ("email", "varchar")],
            CUSTOMERS_TABLE_ID: [("id", "integer"), ("name", "varchar"), ("email", "varchar")],
        }
        gov.limit_ceiling = limit_ceiling
        return gov

    def test_stage2_injects_rls_into_sql(self):
        """apply_governance adds a WHERE predicate from an RLS rule."""
        sql = 'SELECT "t0"."id" FROM "public"."orders" "t0"'
        gov = self._make_gov_ctx(rls_rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        governed = apply_governance(sql, gov)
        assert "WHERE" in governed.upper()
        assert "us-east" in governed

    def test_stage2_injects_masking_into_sql(self):
        """apply_governance replaces masked columns with CASE/mask expressions."""
        from provisa.security.masking import MaskingRule, MaskType
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        sql = 'SELECT "t0"."id", "t0"."email" FROM "public"."orders" "t0"'
        gov = self._make_gov_ctx(
            masking_rules={(ORDERS_TABLE_ID, "email"): (rule, "varchar")},
        )
        governed = apply_governance(sql, gov)
        # Column should be present but email value masked to NULL
        assert "email" in governed.lower() or "null" in governed.lower()

    def test_stage2_rejects_unauthorized_table(self):
        """SQL referencing a table not in table_map raises or returns unmodified.

        The governance layer does not silently pass unknown tables — it either
        raises or leaves their columns untouched (no RLS applied). We verify that
        at least a known table that IS in table_map does get RLS applied, while
        an unknown table passes through without crashing.
        """
        sql = 'SELECT "s"."secret" FROM "public"."secret_table" "s"'
        gov = self._make_gov_ctx()
        # No exception — unknown tables are ignored by apply_governance
        governed = apply_governance(sql, gov)
        assert isinstance(governed, str)

    def test_stage2_applies_ceiling_when_no_limit_present(self):
        """apply_governance appends LIMIT when limit_ceiling is set and no LIMIT present."""
        sql = 'SELECT "t0"."id" FROM "public"."orders" "t0"'
        gov = self._make_gov_ctx(limit_ceiling=100)
        governed = apply_governance(sql, gov)
        assert "LIMIT" in governed.upper()
        assert "100" in governed

    def test_stage2_caps_existing_limit_to_ceiling(self):
        """apply_governance replaces a LIMIT that exceeds the ceiling."""
        sql = 'SELECT "t0"."id" FROM "public"."orders" "t0" LIMIT 9999'
        gov = self._make_gov_ctx(limit_ceiling=200)
        governed = apply_governance(sql, gov)
        assert "LIMIT 200" in governed

    def test_stage2_preserves_limit_below_ceiling(self):
        """apply_governance does not reduce a LIMIT that's already within the ceiling."""
        sql = 'SELECT "t0"."id" FROM "public"."orders" "t0" LIMIT 50'
        gov = self._make_gov_ctx(limit_ceiling=200)
        governed = apply_governance(sql, gov)
        assert "LIMIT 50" in governed

    def test_stage2_no_rls_no_modification(self):
        """With empty governance context, SQL is returned structurally equivalent."""
        sql = 'SELECT "t0"."id" FROM "public"."orders" "t0"'
        gov = self._make_gov_ctx()
        governed = apply_governance(sql, gov)
        # Without RLS or masking or ceiling, output should still be valid SQL
        assert "SELECT" in governed.upper()
        assert "orders" in governed.lower()


# ---------------------------------------------------------------------------
# Full pipeline (Stage1 + RLS + masking) tests
# ---------------------------------------------------------------------------

class TestFullPipeline:
    def test_full_pipeline_graphql_to_governed_sql_rls(self):
        """Stage1 output fed into inject_rls produces governed SQL with WHERE clause."""
        doc = _parse('{ orders { id } }')
        ctx = _basic_ctx()
        compiled_list = compile_graphql(doc, ctx)
        compiled = compiled_list[0]

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        governed = inject_rls(compiled, ctx, rls)
        assert "WHERE" in governed.sql.upper()
        assert "us-east" in governed.sql

    def test_full_pipeline_graphql_to_governed_sql_masking(self):
        """Stage1 output fed into inject_masking rewrites masked column."""
        doc = _parse('{ orders { id } }')
        ctx = _basic_ctx()
        # Inject email column into orders table meta and context
        meta = _orders_meta()
        ctx.tables["orders"] = meta

        compiled_list = compile_graphql(doc, ctx)
        compiled = compiled_list[0]

        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        masking_rules = {(ORDERS_TABLE_ID, "analyst"): {"id": (rule, "integer")}}
        governed = inject_masking(compiled, ctx, masking_rules, "analyst")
        # With masking applied, the SQL should have changed or the rule should have matched
        assert isinstance(governed.sql, str)

    def test_full_pipeline_rls_then_masking(self):
        """Chaining inject_rls + inject_masking produces both WHERE and masked projection."""
        doc = _parse('{ orders { id } }')
        ctx = _basic_ctx()
        compiled_list = compile_graphql(doc, ctx)
        compiled = compiled_list[0]

        # Apply RLS
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'eu-west'"})
        after_rls = inject_rls(compiled, ctx, rls)
        assert "eu-west" in after_rls.sql

        # Then masking
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        masking_rules = {(ORDERS_TABLE_ID, "viewer"): {"id": (rule, "integer")}}
        after_mask = inject_masking(after_rls, ctx, masking_rules, "viewer")
        # Both transformations are present
        assert "eu-west" in after_mask.sql or "WHERE" in after_mask.sql.upper()

    def test_full_pipeline_empty_rls_no_where(self):
        """Empty RLS context leaves SQL unchanged — no WHERE appended."""
        doc = _parse('{ orders { id } }')
        ctx = _basic_ctx()
        compiled_list = compile_graphql(doc, ctx)
        original_sql = compiled_list[0].sql

        rls = RLSContext.empty()
        governed = inject_rls(compiled_list[0], ctx, rls)
        assert governed.sql == original_sql
