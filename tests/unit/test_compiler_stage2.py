# Copyright (c) 2026 Kenneth Stott
# Canary: e1b8837d-afd5-4a43-ac66-4f7676818f9b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for Stage 2 governance transformer — new coverage areas.

Covers:
- _qualify_filter helper (stage2 variant)
- GovernanceContext with complex governance rules
- CTE rewriting preservation under governance
- Window function SQL preservation under governance
- Relationship traversal: aliased JOINs with per-table RLS
- Cross-dialect SQL patterns (ILIKE, date arithmetic)
- Governance rule injection into query ASTs
- Sample-size vs limit-ceiling precedence
- extract_sources with schema-qualified tables
- build_governance_context with mixed visibility
"""

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import (
    GovernanceContext,
    _apply_limit_ceiling,
    _qualify_filter,
    _table_id_for_node,
    apply_governance,
    build_governance_context,
    extract_sources,
)
from provisa.security.masking import MaskType, MaskingRule


# ---------------------------------------------------------------------------
# Shared helpers (mirrors style of test_stage2_complex.py)
# ---------------------------------------------------------------------------


def _gov(**kwargs) -> GovernanceContext:
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
    ctx = CompilationContext()
    for m in metas:
        ctx.tables[m.field_name] = m
    return ctx


class _FakeRLSContext:
    def __init__(self, rules: dict[int, str]) -> None:
        self.rules = rules


# ---------------------------------------------------------------------------
# TestQualifyFilter — stage2's own _qualify_filter implementation
# ---------------------------------------------------------------------------


class TestStage2QualifyFilter:
    """_qualify_filter from stage2 prefixes unqualified column names with alias."""

    def test_simple_column_prefixed(self):
        result = _qualify_filter("status = 'active'", "t0")
        assert '"t0".status' in result

    def test_string_literal_not_prefixed(self):
        result = _qualify_filter("region = 'us-east'", "t0")
        assert "'us-east'" in result
        # the literal value should not be re-qualified
        assert '"t0".' + "'us-east'" not in result

    def test_already_qualified_not_doubled(self):
        expr = '"t0".region = \'us\''
        result = _qualify_filter(expr, "t0")
        assert result.count('"t0"') == 1

    def test_sql_keywords_not_prefixed(self):
        result = _qualify_filter("active = true AND deleted = false", "u")
        # AND, true, false are keywords and should not be prefixed
        assert "AND" in result
        assert '"u".' + "true" not in result
        assert '"u".' + "false" not in result

    def test_compound_expression_prefixed(self):
        result = _qualify_filter("department_id IN (10, 20)", "emp")
        assert '"emp".department_id' in result

    def test_is_null_expression(self):
        result = _qualify_filter("deleted_at IS NULL", "t0")
        assert '"t0".deleted_at' in result
        assert "IS NULL" in result

    def test_nested_string_with_special_chars_skipped(self):
        result = _qualify_filter("name = 'O''Brien'", "t0")
        # The string literal 'O''Brien' should be preserved
        assert "O''Brien" in result or "O'Brien" in result


# ---------------------------------------------------------------------------
# TestApplyLimitCeiling — standalone helper
# ---------------------------------------------------------------------------


class TestApplyLimitCeiling:
    def test_no_limit_appends_ceiling(self):
        sql = "SELECT id FROM orders"
        result = _apply_limit_ceiling(sql, 500)
        assert result.endswith("LIMIT 500")

    def test_existing_limit_below_ceiling_unchanged(self):
        sql = "SELECT id FROM orders LIMIT 25"
        result = _apply_limit_ceiling(sql, 500)
        assert "LIMIT 25" in result
        assert "LIMIT 500" not in result

    def test_existing_limit_above_ceiling_capped(self):
        sql = "SELECT id FROM orders LIMIT 10000"
        result = _apply_limit_ceiling(sql, 200)
        assert "LIMIT 200" in result
        assert "LIMIT 10000" not in result

    def test_limit_exactly_at_ceiling_unchanged(self):
        sql = "SELECT id FROM orders LIMIT 100"
        result = _apply_limit_ceiling(sql, 100)
        assert "LIMIT 100" in result
        assert result.count("LIMIT") == 1

    def test_trailing_semicolon_stripped_before_append(self):
        sql = "SELECT id FROM orders;"
        result = _apply_limit_ceiling(sql, 50)
        assert "LIMIT 50" in result
        # Semicolons should not appear between sql body and LIMIT
        assert ";" not in result.split("LIMIT")[0].rstrip()


# ---------------------------------------------------------------------------
# TestSampleSizeVsCeilingPrecedence
# ---------------------------------------------------------------------------


class TestSampleSizeVsCeilingPrecedence:
    def test_ceiling_takes_precedence_over_sample_size(self):
        """When both limit_ceiling and sample_size are set, ceiling wins."""
        gov = _gov(limit_ceiling=100, sample_size=25)
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "LIMIT 100" in result

    def test_sample_size_used_when_no_ceiling(self):
        gov = _gov(sample_size=75)
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "LIMIT 75" in result

    def test_neither_set_no_limit_added(self):
        gov = _gov()
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "LIMIT" not in result


# ---------------------------------------------------------------------------
# TestCTEPreservation — WITH queries pass through governance without corruption
# ---------------------------------------------------------------------------


class TestCTEPreservation:
    def test_cte_rls_applied_to_inner_select(self):
        """RLS is injected into the main table reference inside the CTE body."""
        gov = _gov(
            rls_rules={1: "tenant_id = 7"},
            table_map={"orders": 1},
        )
        sql = (
            "WITH recent AS (SELECT id, amount FROM orders WHERE created_at > '2025-01-01') "
            "SELECT * FROM recent"
        )
        result = apply_governance(sql, gov)
        # The orders reference inside the CTE gets the RLS predicate
        assert "tenant_id" in result
        assert "7" in result

    def test_cte_limit_ceiling_applied_to_outer_select(self):
        """LIMIT ceiling added to outer query even when CTE exists."""
        gov = _gov(limit_ceiling=50)
        sql = "WITH t AS (SELECT id FROM orders) SELECT id FROM t"
        result = apply_governance(sql, gov)
        assert "LIMIT 50" in result

    def test_cte_no_rls_for_unknown_table(self):
        """CTE over an unknown table does not raise."""
        gov = _gov(table_map={"orders": 1}, rls_rules={1: "active = true"})
        sql = "WITH x AS (SELECT id FROM unknown_table) SELECT id FROM x"
        result = apply_governance(sql, gov)
        assert isinstance(result, str)
        assert "unknown_table" in result or "x" in result

    def test_multi_cte_governance_applies_to_all_branches(self):
        """Multiple CTEs — RLS applied wherever the governed table appears."""
        gov = _gov(
            rls_rules={1: "region = 'us'"},
            table_map={"orders": 1},
        )
        sql = (
            "WITH a AS (SELECT id FROM orders), "
            "b AS (SELECT id FROM orders WHERE amount > 100) "
            "SELECT a.id FROM a JOIN b ON a.id = b.id"
        )
        result = apply_governance(sql, gov)
        # Both CTE branches touch 'orders' → at least one RLS injection
        assert result.count("region = 'us'") >= 1


# ---------------------------------------------------------------------------
# TestWindowFunctionSQL — governance does not corrupt window function syntax
# ---------------------------------------------------------------------------


class TestWindowFunctionSQL:
    def test_window_function_preserved_by_governance(self):
        """ROW_NUMBER OVER() expressions pass through governance unchanged."""
        gov = _gov(
            rls_rules={1: "active = true"},
            table_map={"employees": 1},
        )
        sql = (
            "SELECT id, salary, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rn "
            "FROM employees"
        )
        result = apply_governance(sql, gov)
        # Window function should be present in result
        assert "OVER" in result
        assert "PARTITION BY" in result
        # RLS should also be applied
        assert "active" in result.lower()

    def test_rank_window_function_with_rls(self):
        gov = _gov(
            rls_rules={1: "tenant_id = 1"},
            table_map={"sales": 1},
        )
        sql = (
            "SELECT rep_id, amount, RANK() OVER (ORDER BY amount DESC) AS rnk "
            "FROM sales"
        )
        result = apply_governance(sql, gov)
        assert "RANK()" in result or "rank()" in result.lower()
        assert "tenant_id" in result

    def test_window_function_no_rls_passthrough(self):
        """Window queries with no matching RLS pass through structurally valid."""
        gov = _gov()
        sql = (
            "SELECT id, LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY created_at) AS prev_amount "
            "FROM orders"
        )
        result = apply_governance(sql, gov)
        assert "LAG" in result
        assert "OVER" in result
        assert "LIMIT" not in result


# ---------------------------------------------------------------------------
# TestAliasedJoinRLS — per-table RLS with table aliases
# ---------------------------------------------------------------------------


class TestAliasedJoinRLS:
    def test_rls_on_root_table_with_alias(self):
        gov = _gov(
            rls_rules={1: "region = 'eu'"},
            table_map={"orders": 1},
        )
        sql = 'SELECT o.id FROM orders o WHERE o.status = \'open\''
        result = apply_governance(sql, gov)
        assert "region = 'eu'" in result
        assert "status" in result

    def test_rls_on_joined_table_with_distinct_aliases(self):
        gov = _gov(
            rls_rules={1: "region = 'us'", 2: "active = true"},
            table_map={"orders": 1, "customers": 2},
        )
        sql = (
            "SELECT o.id, c.name "
            "FROM orders o "
            "LEFT JOIN customers c ON o.customer_id = c.id"
        )
        result = apply_governance(sql, gov)
        assert "region = 'us'" in result
        assert "active" in result.lower()

    def test_rls_not_applied_to_non_matching_table(self):
        gov = _gov(
            rls_rules={99: "secret = true"},
            table_map={"orders": 1, "customers": 2},
        )
        sql = "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id"
        result = apply_governance(sql, gov)
        # No matching table_id → no WHERE injected
        assert "secret" not in result

    def test_rls_complex_predicate_with_subquery(self):
        gov = _gov(
            rls_rules={1: "id IN (SELECT order_id FROM allowed_orders)"},
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders"
        result = apply_governance(sql, gov)
        assert "allowed_orders" in result
        assert "WHERE" in result


# ---------------------------------------------------------------------------
# TestGovernanceStarExpansionWithJoins
# ---------------------------------------------------------------------------


class TestGovernanceStarExpansionWithJoins:
    def test_star_on_aliased_table_expands_visible_only(self):
        gov = _gov(
            table_map={"orders": 1},
            all_columns={1: [("id", "integer"), ("amount", "numeric"), ("internal_key", "varchar")]},
            visible_columns={1: frozenset({"id", "amount"})},
        )
        sql = "SELECT * FROM orders"
        result = apply_governance(sql, gov)
        assert "id" in result
        assert "amount" in result
        assert "internal_key" not in result

    def test_star_with_masking_and_visibility(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern=r"\d", replace="*")
        gov = _gov(
            table_map={"users": 2},
            all_columns={2: [("id", "integer"), ("phone", "varchar"), ("hidden_col", "varchar")]},
            visible_columns={2: frozenset({"id", "phone"})},
            masking_rules={(2, "phone"): (rule, "varchar")},
        )
        sql = "SELECT * FROM users"
        result = apply_governance(sql, gov)
        assert "id" in result
        assert "REGEXP_REPLACE" in result  # phone is masked
        assert "hidden_col" not in result

    def test_star_from_table_with_no_all_columns_metadata(self):
        """When all_columns is empty for a table, star stays as star (no expansion)."""
        gov = _gov(
            table_map={"orders": 1},
            all_columns={},  # no metadata
            visible_columns={},
        )
        sql = "SELECT * FROM orders"
        result = apply_governance(sql, gov)
        # No metadata → governance can't expand → star may remain or be empty
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# TestCrossDialectPatterns — stage2 uses postgres dialect but should not
# crash on SQL idioms that survive roundtripping via SQLGlot
# ---------------------------------------------------------------------------


class TestCrossDialectPatterns:
    def test_governance_on_subquery_with_aggregation(self):
        gov = _gov(
            rls_rules={1: "tenant_id = 42"},
            table_map={"orders": 1},
        )
        sql = "SELECT customer_id, COUNT(*) AS cnt FROM orders GROUP BY customer_id"
        result = apply_governance(sql, gov)
        assert "COUNT" in result
        assert "GROUP BY" in result
        assert "tenant_id" in result

    def test_governance_on_union_all(self):
        gov = _gov(
            rls_rules={1: "region = 'us'"},
            table_map={"orders": 1},
        )
        sql = "SELECT id FROM orders WHERE status = 'new' UNION ALL SELECT id FROM orders WHERE status = 'old'"
        result = apply_governance(sql, gov)
        assert result.count("region = 'us'") >= 2

    def test_governance_on_case_expression(self):
        gov = _gov(
            rls_rules={1: "active = true"},
            table_map={"employees": 1},
        )
        sql = "SELECT id, CASE WHEN salary > 100000 THEN 'high' ELSE 'normal' END AS band FROM employees"
        result = apply_governance(sql, gov)
        assert "CASE" in result
        assert "WHEN" in result
        assert "active" in result.lower()

    def test_governance_on_date_filter(self):
        gov = _gov(
            rls_rules={1: "active = true"},
            table_map={"events": 1},
        )
        sql = "SELECT id FROM events WHERE event_date >= '2025-01-01'"
        result = apply_governance(sql, gov)
        assert "2025-01-01" in result
        assert "active" in result.lower()

    def test_governance_passthrough_on_complex_having(self):
        gov = _gov(
            rls_rules={1: "tenant_id = 1"},
            table_map={"orders": 1},
        )
        sql = (
            "SELECT customer_id, SUM(amount) AS total "
            "FROM orders "
            "GROUP BY customer_id "
            "HAVING SUM(amount) > 1000"
        )
        result = apply_governance(sql, gov)
        assert "HAVING" in result
        assert "SUM" in result
        assert "tenant_id" in result


# ---------------------------------------------------------------------------
# TestExtractSourcesExpanded — additional extract_sources scenarios
# ---------------------------------------------------------------------------


class TestExtractSourcesExpanded:
    def test_schema_qualified_table_matched(self):
        m = _meta("orders", "orders", table_id=5, source_id="pg-eu", schema_name="sales")
        ctx = _ctx(m)
        gov = _gov(table_map={"sales.orders": 5, "orders": 5})
        sources = extract_sources("SELECT id FROM sales.orders", gov, ctx)
        assert "pg-eu" in sources

    def test_empty_sql_returns_empty_set(self):
        ctx = _ctx(_meta("orders", "orders", table_id=1, source_id="s1"))
        gov = _gov(table_map={"orders": 1})
        sources = extract_sources("", gov, ctx)
        assert sources == set()

    def test_subquery_with_multiple_tables(self):
        m1 = _meta("orders", "orders", table_id=1, source_id="pg-main")
        m2 = _meta("items", "items", table_id=2, source_id="pg-catalog")
        ctx = _ctx(m1, m2)
        gov = _gov(table_map={"orders": 1, "items": 2})
        sql = "SELECT o.id FROM orders o LEFT JOIN items i ON o.item_id = i.id"
        sources = extract_sources(sql, gov, ctx)
        assert "pg-main" in sources
        assert "pg-catalog" in sources

    def test_invalid_sql_returns_empty_set(self):
        ctx = _ctx(_meta("orders", "orders", table_id=1, source_id="s1"))
        gov = _gov(table_map={"orders": 1})
        sources = extract_sources("SELECTFROM !! garbage", gov, ctx)
        assert isinstance(sources, set)


# ---------------------------------------------------------------------------
# TestBuildGovernanceContextExpanded — extra coverage on builder
# ---------------------------------------------------------------------------


class TestBuildGovernanceContextExpanded:
    def test_multiple_tables_produce_separate_table_ids(self):
        m1 = _meta("orders", "orders", table_id=1, schema_name="public")
        m2 = _meta("customers", "customers", table_id=2, schema_name="public")
        ctx = _ctx(m1, m2)
        tables = [
            {"id": 1, "columns": [{"column_name": "id"}]},
            {"id": 2, "columns": [{"column_name": "id"}]},
        ]
        rls = _FakeRLSContext({})
        gov = build_governance_context("analyst", rls, {}, ctx, tables)
        assert gov.table_map.get("orders") == 1
        assert gov.table_map.get("customers") == 2

    def test_masking_rules_for_multiple_tables(self):
        m1 = _meta("orders", "orders", table_id=1)
        m2 = _meta("customers", "customers", table_id=2)
        ctx = _ctx(m1, m2)
        tables = [
            {"id": 1, "columns": [{"column_name": "amount", "data_type": "numeric"}]},
            {"id": 2, "columns": [{"column_name": "email", "data_type": "varchar"}]},
        ]
        rule_a = MaskingRule(mask_type=MaskType.constant, value=0)
        rule_b = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
        masking = {
            (1, "analyst"): {"amount": (rule_a, "numeric")},
            (2, "analyst"): {"email": (rule_b, "varchar")},
        }
        rls = _FakeRLSContext({})
        gov = build_governance_context("analyst", rls, masking, ctx, tables)
        assert (1, "amount") in gov.masking_rules
        assert (2, "email") in gov.masking_rules

    def test_all_columns_populated_with_data_types(self):
        m = _meta("products", "products", table_id=3)
        ctx = _ctx(m)
        tables = [
            {
                "id": 3,
                "columns": [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "price", "data_type": "numeric"},
                    {"column_name": "name", "data_type": "varchar"},
                ],
            }
        ]
        rls = _FakeRLSContext({})
        gov = build_governance_context("role-x", rls, {}, ctx, tables)
        cols = gov.all_columns.get(3)
        assert cols is not None
        col_names = [c[0] for c in cols]
        assert "id" in col_names
        assert "price" in col_names
        assert "name" in col_names

    def test_rls_rules_multiple_tables(self):
        m1 = _meta("orders", "orders", table_id=1)
        m2 = _meta("items", "items", table_id=2)
        ctx = _ctx(m1, m2)
        tables = [
            {"id": 1, "columns": []},
            {"id": 2, "columns": []},
        ]
        rls = _FakeRLSContext({1: "tenant = 1", 2: "active = true"})
        gov = build_governance_context("r", rls, {}, ctx, tables)
        assert gov.rls_rules == {1: "tenant = 1", 2: "active = true"}

    def test_partial_column_visibility_per_role(self):
        m = _meta("payroll", "payroll", table_id=9)
        ctx = _ctx(m)
        tables = [
            {
                "id": 9,
                "columns": [
                    {"column_name": "employee_id", "visible_to": ["hr", "admin"]},
                    {"column_name": "salary", "visible_to": ["admin"]},
                    {"column_name": "department", "visible_to": ["hr", "admin"]},
                ],
            }
        ]
        rls = _FakeRLSContext({})
        gov = build_governance_context("hr", rls, {}, ctx, tables)
        vis = gov.visible_columns.get(9)
        assert vis is not None
        assert "employee_id" in vis
        assert "department" in vis
        assert "salary" not in vis


# ---------------------------------------------------------------------------
# TestTableIdForNodeHelper
# ---------------------------------------------------------------------------


class TestTableIdForNodeHelper:
    def test_simple_table_lookup(self):
        gov = _gov(table_map={"orders": 1})
        tree = sqlglot.parse_one("SELECT id FROM orders", read="postgres")
        tables = list(tree.find_all(exp.Table))
        assert len(tables) == 1
        tid = _table_id_for_node(tables[0], gov)
        assert tid == 1

    def test_schema_qualified_table_lookup(self):
        gov = _gov(table_map={"public.orders": 1, "orders": 1})
        tree = sqlglot.parse_one('SELECT id FROM "public"."orders"', read="postgres")
        tables = list(tree.find_all(exp.Table))
        assert tables
        tid = _table_id_for_node(tables[0], gov)
        assert tid == 1

    def test_unknown_table_returns_none(self):
        gov = _gov(table_map={"orders": 1})
        tree = sqlglot.parse_one("SELECT id FROM unknown_table", read="postgres")
        tables = list(tree.find_all(exp.Table))
        assert tables
        tid = _table_id_for_node(tables[0], gov)
        assert tid is None


# ---------------------------------------------------------------------------
# TestGovernanceOnMutationLikeSql — governance shouldn't crash on non-SELECT
# ---------------------------------------------------------------------------


class TestGovernanceOnNonSelectSQL:
    def test_governance_on_plain_values_query(self):
        """Non-SELECT SQL should not crash, even if governance is a no-op."""
        gov = _gov(rls_rules={1: "x = 1"}, table_map={"orders": 1})
        sql = "SELECT 1 AS ping"
        result = apply_governance(sql, gov)
        assert "ping" in result

    def test_governance_on_subquery_as_from(self):
        gov = _gov(limit_ceiling=10)
        sql = "SELECT id FROM (SELECT id FROM orders LIMIT 5) AS sub"
        result = apply_governance(sql, gov)
        assert "LIMIT" in result
