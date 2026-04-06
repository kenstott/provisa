# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4f2a71-c3e9-4b50-af16-9e2b7d05c183
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Stage 2 SQL governance transformer — complex scenarios (REQ-263–266)."""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

import pytest

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import (
    GovernanceContext,
    apply_governance,
    build_governance_context,
    extract_sources,
)
from provisa.security.masking import MaskType, MaskingRule


# --------------------------------------------------------------------------- #
# Shared helpers                                                               #
# --------------------------------------------------------------------------- #


def _gov(**kwargs) -> GovernanceContext:
    """Build a GovernanceContext with safe defaults for unspecified fields."""
    defaults: dict = dict(
        rls_rules={},
        masking_rules={},
        visible_columns={},
        table_map={},
        all_columns={},
        limit_ceiling=None,
        sample_size=None,
    )
    defaults.update(kwargs)
    return GovernanceContext(**defaults)


def _meta(
    field_name: str,
    table_name: str,
    table_id: int,
    source_id: str = "src-1",
    schema_name: str = "public",
    catalog_name: str = "main",
) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
    )


def _ctx(*metas: TableMeta) -> CompilationContext:
    """Build a CompilationContext keyed by field_name."""
    ctx = CompilationContext()
    for m in metas:
        ctx.tables[m.field_name] = m
    return ctx


def _col_names_from_sql(sql: str) -> list[str]:
    """Parse governed SQL and return the projection column/alias names."""
    ast = sqlglot.parse_one(sql, read="postgres")
    names = []
    for expr in ast.expressions:
        if isinstance(expr, exp.Alias):
            names.append(expr.alias)
        elif isinstance(expr, exp.Column):
            names.append(expr.name)
        elif isinstance(expr, (exp.Anonymous, exp.Func)):
            pass  # masking expression — not a simple name
        else:
            names.append(str(expr))
    return names


def _where_text(sql: str) -> str:
    """Return the WHERE clause text from governed SQL (empty string if absent)."""
    ast = sqlglot.parse_one(sql, read="postgres")
    where = ast.args.get("where")
    return where.sql(dialect="postgres") if where else ""


# --------------------------------------------------------------------------- #
# TestApplyGovernanceSelectStar                                                #
# --------------------------------------------------------------------------- #


class TestApplyGovernanceSelectStar:
    def test_select_star_no_restrictions_all_columns_preserved(self):
        gov = _gov(
            table_map={"orders": 1},
            all_columns={1: [("id", "integer"), ("amount", "numeric"), ("status", "varchar")]},
            visible_columns={1: None},  # None = all visible
        )
        result = apply_governance("SELECT * FROM orders", gov)
        assert "id" in result
        assert "amount" in result
        assert "status" in result

    def test_select_star_with_visibility_only_visible_columns(self):
        gov = _gov(
            table_map={"orders": 1},
            all_columns={1: [("id", "integer"), ("amount", "numeric"), ("secret", "varchar")]},
            visible_columns={1: frozenset({"id", "amount"})},
        )
        result = apply_governance("SELECT * FROM orders", gov)
        assert "id" in result
        assert "amount" in result
        assert "secret" not in result

    def test_select_star_from_subquery_alias_qualified(self):
        gov = _gov(
            table_map={"orders": 1},
            all_columns={1: [("id", "integer"), ("total", "numeric")]},
            visible_columns={1: None},
        )
        sql = "SELECT * FROM (SELECT id, total FROM orders) AS sub"
        result = apply_governance(sql, gov)
        # The outer * should not be modified (no table_id match for alias "sub")
        # but the inner query should be governed
        assert "id" in result
        assert "total" in result

    def test_single_table_mixed_visible_invisible(self):
        gov = _gov(
            table_map={"customers": 2},
            all_columns={2: [("id", "integer"), ("name", "varchar"), ("ssn", "varchar")]},
            visible_columns={2: frozenset({"id", "name"})},
        )
        result = apply_governance("SELECT * FROM customers", gov)
        assert "ssn" not in result
        assert "name" in result
        assert "id" in result


# --------------------------------------------------------------------------- #
# TestApplyGovernanceRLS                                                       #
# --------------------------------------------------------------------------- #


class TestApplyGovernanceRLS:
    def test_rls_injected_as_where_when_no_existing_where(self):
        gov = _gov(
            rls_rules={1: "tenant_id = 42"},
            table_map={"orders": 1},
        )
        result = apply_governance("SELECT id FROM orders", gov)
        assert "WHERE" in result
        assert "tenant_id" in result
        assert "42" in result

    def test_rls_anded_with_existing_where(self):
        gov = _gov(
            rls_rules={1: "region = 'eu'"},
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders WHERE status = 'open'"
        result = apply_governance(sql, gov)
        assert "region = 'eu'" in result
        assert "status = 'open'" in result
        assert "AND" in result

    def test_rls_multiple_tables_multiple_filters(self):
        gov = _gov(
            rls_rules={1: "region = 'us'", 2: "active = TRUE"},
            table_map={"orders": 1, "customers": 2},
        )
        sql = "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id"
        result = apply_governance(sql, gov)
        assert "region = 'us'" in result
        assert "active" in result.lower()

    def test_empty_rls_context_sql_unchanged_where(self):
        gov = _gov(table_map={"orders": 1})
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "WHERE" not in result

    def test_rls_complex_filter_expression(self):
        gov = _gov(
            rls_rules={1: "department_id IN (10, 20) AND active = TRUE"},
            table_map={"employees": 1},
        )
        result = apply_governance("SELECT id FROM employees", gov)
        assert "department_id" in result
        assert "10" in result
        assert "20" in result
        assert "active" in result.lower()


# --------------------------------------------------------------------------- #
# TestApplyGovernanceMasking                                                   #
# --------------------------------------------------------------------------- #


class TestApplyGovernanceMasking:
    def test_masked_column_replaced_with_masking_expression(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
        gov = _gov(
            masking_rules={(1, "email"): (rule, "varchar")},
            table_map={"users": 1},
        )
        result = apply_governance("SELECT email FROM users", gov)
        assert "REGEXP_REPLACE" in result
        assert "email" in result

    def test_multiple_masked_columns(self):
        email_rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
        phone_rule = MaskingRule(mask_type=MaskType.constant, value="***-****")
        gov = _gov(
            masking_rules={
                (1, "email"): (email_rule, "varchar"),
                (1, "phone"): (phone_rule, "varchar"),
            },
            table_map={"users": 1},
        )
        result = apply_governance("SELECT email, phone FROM users", gov)
        assert "REGEXP_REPLACE" in result
        assert "***-****" in result

    def test_unmasked_columns_preserved_unchanged(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
        gov = _gov(
            masking_rules={(1, "email"): (rule, "varchar")},
            table_map={"users": 1},
        )
        result = apply_governance("SELECT id FROM users", gov)
        assert "REGEXP_REPLACE" not in result
        assert "id" in result

    def test_null_masking_constant_produces_null(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        gov = _gov(
            masking_rules={(1, "ssn"): (rule, "varchar")},
            table_map={"customers": 1},
        )
        result = apply_governance("SELECT ssn FROM customers", gov)
        assert "NULL" in result

    def test_empty_string_masking_constant(self):
        rule = MaskingRule(mask_type=MaskType.constant, value="")
        gov = _gov(
            masking_rules={(1, "notes"): (rule, "varchar")},
            table_map={"records": 1},
        )
        result = apply_governance("SELECT notes FROM records", gov)
        # Constant '' should appear as an empty string literal
        assert "records" in result  # query still valid


# --------------------------------------------------------------------------- #
# TestApplyGovernanceLimitCeiling                                              #
# --------------------------------------------------------------------------- #


class TestApplyGovernanceLimitCeiling:
    def test_query_without_limit_ceiling_added(self):
        gov = _gov(limit_ceiling=500)
        result = apply_governance("SELECT id FROM orders", gov)
        assert "LIMIT 500" in result

    def test_query_with_limit_below_ceiling_preserved(self):
        gov = _gov(limit_ceiling=500)
        result = apply_governance("SELECT id FROM orders LIMIT 10", gov)
        assert "LIMIT 10" in result
        assert "LIMIT 500" not in result

    def test_query_with_limit_above_ceiling_capped(self):
        gov = _gov(limit_ceiling=100)
        result = apply_governance("SELECT id FROM orders LIMIT 9999", gov)
        assert "LIMIT 100" in result
        assert "LIMIT 9999" not in result

    def test_ceiling_none_no_limit_added(self):
        gov = _gov(limit_ceiling=None)
        result = apply_governance("SELECT id FROM orders", gov)
        assert "LIMIT" not in result

    def test_limit_zero_edge_case_preserved(self):
        # LIMIT 0 is below any positive ceiling — original preserved
        gov = _gov(limit_ceiling=100)
        result = apply_governance("SELECT id FROM orders LIMIT 0", gov)
        assert "LIMIT 0" in result
        assert "LIMIT 100" not in result


# --------------------------------------------------------------------------- #
# TestExtractSources                                                           #
# --------------------------------------------------------------------------- #


class TestExtractSources:
    def test_simple_select_extracts_source_id(self):
        m = _meta("orders", "orders", table_id=1, source_id="postgres-prod")
        ctx = _ctx(m)
        gov = _gov(table_map={"orders": 1})
        sources = extract_sources("SELECT id FROM orders", gov, ctx)
        assert "postgres-prod" in sources

    def test_multi_table_select_extracts_all_source_ids(self):
        m1 = _meta("orders", "orders", table_id=1, source_id="pg-main")
        m2 = _meta("customers", "customers", table_id=2, source_id="pg-crm")
        ctx = _ctx(m1, m2)
        gov = _gov(table_map={"orders": 1, "customers": 2})
        sql = "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id"
        sources = extract_sources(sql, gov, ctx)
        assert "pg-main" in sources
        assert "pg-crm" in sources

    def test_unknown_table_not_included_in_sources(self):
        m = _meta("orders", "orders", table_id=1, source_id="pg-main")
        ctx = _ctx(m)
        gov = _gov(table_map={"orders": 1})
        sources = extract_sources("SELECT id FROM unknown_table", gov, ctx)
        assert "pg-main" not in sources
        assert len(sources) == 0

    def test_cte_with_source_tables_extracted(self):
        m = _meta("orders", "orders", table_id=1, source_id="pg-main")
        ctx = _ctx(m)
        gov = _gov(table_map={"orders": 1})
        sql = "WITH recent AS (SELECT id FROM orders WHERE created_at > '2025-01-01') SELECT * FROM recent"
        sources = extract_sources(sql, gov, ctx)
        assert "pg-main" in sources


# --------------------------------------------------------------------------- #
# TestBuildGovernanceContext                                                   #
# --------------------------------------------------------------------------- #


class _FakeRLSContext:
    """Minimal stand-in for a real RLSContext."""

    def __init__(self, rules: dict[int, str]) -> None:
        self.rules = rules


class TestBuildGovernanceContext:
    def test_empty_masking_and_rls_produces_basic_context(self):
        ctx = _ctx(_meta("orders", "orders", table_id=1))
        tables = [{"id": 1, "columns": [{"column_name": "id", "data_type": "integer"}]}]
        rls = _FakeRLSContext({})
        gov = build_governance_context("role-a", rls, {}, ctx, tables)
        assert isinstance(gov, GovernanceContext)
        assert gov.rls_rules == {}
        assert gov.masking_rules == {}

    def test_rls_rules_populated_from_context(self):
        ctx = _ctx(_meta("orders", "orders", table_id=1))
        tables = [{"id": 1, "columns": [{"column_name": "id"}]}]
        rls = _FakeRLSContext({1: "tenant_id = 7"})
        gov = build_governance_context("role-a", rls, {}, ctx, tables)
        assert gov.rls_rules == {1: "tenant_id = 7"}

    def test_masking_rules_for_matching_role_populated(self):
        ctx = _ctx(_meta("users", "users", table_id=2))
        tables = [{"id": 2, "columns": [{"column_name": "email", "data_type": "varchar"}]}]
        rls = _FakeRLSContext({})
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
        masking = {(2, "role-a"): {"email": (rule, "varchar")}}
        gov = build_governance_context("role-a", rls, masking, ctx, tables)
        assert (2, "email") in gov.masking_rules
        assert gov.masking_rules[(2, "email")][0] is rule

    def test_masking_rules_for_other_role_excluded(self):
        ctx = _ctx(_meta("users", "users", table_id=2))
        tables = [{"id": 2, "columns": [{"column_name": "email", "data_type": "varchar"}]}]
        rls = _FakeRLSContext({})
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
        masking = {(2, "role-b"): {"email": (rule, "varchar")}}
        gov = build_governance_context("role-a", rls, masking, ctx, tables)
        assert (2, "email") not in gov.masking_rules

    def test_visibility_restrictions_visible_columns_populated(self):
        ctx = _ctx(_meta("orders", "orders", table_id=1))
        tables = [
            {
                "id": 1,
                "columns": [
                    {"column_name": "id", "visible_to": ["role-a", "role-b"]},
                    {"column_name": "secret", "visible_to": ["role-admin"]},
                ],
            }
        ]
        rls = _FakeRLSContext({})
        gov = build_governance_context("role-a", rls, {}, ctx, tables)
        vis = gov.visible_columns.get(1)
        assert vis is not None  # not all-visible
        assert "id" in vis
        assert "secret" not in vis

    def test_all_columns_visible_when_no_visible_to_restriction(self):
        ctx = _ctx(_meta("orders", "orders", table_id=1))
        tables = [
            {
                "id": 1,
                "columns": [
                    {"column_name": "id"},
                    {"column_name": "amount"},
                ],
            }
        ]
        rls = _FakeRLSContext({})
        gov = build_governance_context("role-a", rls, {}, ctx, tables)
        # visible_to=None on all columns → all_visible=True → None sentinel
        assert gov.visible_columns.get(1) is None

    def test_table_map_populated_from_compilation_context(self):
        m = _meta("orders", "orders", table_id=1, schema_name="public")
        ctx = _ctx(m)
        tables = [{"id": 1, "columns": []}]
        rls = _FakeRLSContext({})
        gov = build_governance_context("role-a", rls, {}, ctx, tables)
        assert gov.table_map.get("orders") == 1
        assert gov.table_map.get("public.orders") == 1
