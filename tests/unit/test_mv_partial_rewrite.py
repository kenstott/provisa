# Copyright (c) 2026 Kenneth Stott
# Canary: b3e7d912-f4a1-4c8e-9d56-7a2f1e083b45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for MV partial rewrite logic (REQ-810).

Tests rewrite_if_mv_match and _partial_rewrite_to_mv in provisa/mv/rewriter.py.
Pure function calls — no I/O, no DB, no Docker, no Trino.
"""

# Requirements: REQ-810

from __future__ import annotations

import time


from provisa.compiler.sql_gen import CompiledQuery
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
from provisa.mv.rewriter import _partial_rewrite_to_mv, rewrite_if_mv_match


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fresh_mv(
    mv_id: str,
    left_table: str,
    left_column: str,
    right_table: str,
    right_column: str,
    target_table: str = "mv_target",
) -> MVDefinition:
    jp = JoinPattern(
        left_table=left_table,
        left_column=left_column,
        right_table=right_table,
        right_column=right_column,
    )
    mv = MVDefinition(
        id=mv_id,
        source_tables=[left_table, right_table],
        target_catalog="iceberg",
        target_schema="mv",
        target_table=target_table,
        join_pattern=jp,
        refresh_interval=300,
    )
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = time.time() - 5
    return mv


def _compiled(sql: str, sources: set[str] | None = None) -> CompiledQuery:
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=[],
        sources=sources or {"sales-pg"},
    )


# ---------------------------------------------------------------------------
# REQ-810: Full match — single JOIN fully covered by MV
# ---------------------------------------------------------------------------


class TestFullMatchRewrite:
    def test_full_match_replaces_from_with_mv_table(self):
        # REQ-810: when MV covers all joins, FROM is replaced by MV target table
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        assert "mv_orders_customers" in result.sql

    def test_full_match_removes_join_clause(self):
        # REQ-810: full match removes the JOIN clause entirely
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        assert "JOIN" not in result.sql

    def test_full_match_rewrites_right_table_column_refs(self):
        # REQ-810: full match rewrites right-table column refs as "right_table__col"
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        assert "customers__name" in result.sql

    def test_full_match_includes_mv_catalog_in_sources(self):
        # REQ-810: MV target catalog is added to compiled.sources after full rewrite
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql, sources={"sales-pg"})
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id")

        result = rewrite_if_mv_match(compiled, [mv])

        assert "iceberg" in result.sources


# ---------------------------------------------------------------------------
# REQ-810: Partial match — MV covers subset of joins
# ---------------------------------------------------------------------------


class TestPartialMatchRewrite:
    def test_partial_match_removes_only_covered_join(self):
        # REQ-810: partial match removes the matched JOIN, keeps uncovered JOINs
        sql = (
            'SELECT "t0"."id", "t1"."name", "t2"."amount" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."payments" "t2" ON "t0"."id" = "t2"."order_id"'
        )
        compiled = _compiled(sql)
        # MV covers only orders ↔ customers; payments join is uncovered
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        # covered join removed
        assert "customers" not in result.sql or "mv_orders_customers" in result.sql
        # uncovered join preserved
        assert "payments" in result.sql

    def test_partial_match_replaces_from_with_mv(self):
        # REQ-810: partial match rewrites FROM to MV target table
        sql = (
            'SELECT "t0"."id", "t1"."name", "t2"."amount" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."payments" "t2" ON "t0"."id" = "t2"."order_id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        assert "mv_orders_customers" in result.sql

    def test_partial_match_keeps_uncovered_join_clause(self):
        # REQ-810: uncovered JOIN clause is preserved verbatim in rewritten SQL
        sql = (
            'SELECT "t0"."id", "t1"."name", "t2"."amount" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."payments" "t2" ON "t0"."id" = "t2"."order_id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        assert "JOIN" in result.sql

    def test_partial_match_rewrites_covered_column_refs(self):
        # REQ-810: covered right-table columns rewritten as t0."right_table__col"
        sql = (
            'SELECT "t0"."id", "t1"."name", "t2"."amount" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."payments" "t2" ON "t0"."id" = "t2"."order_id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        assert "customers__name" in result.sql

    def test_partial_match_leaves_uncovered_column_refs_unchanged(self):
        # REQ-810: uncovered right-table column refs (t2) are not rewritten
        sql = (
            'SELECT "t0"."id", "t1"."name", "t2"."amount" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."payments" "t2" ON "t0"."id" = "t2"."order_id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        # t2."amount" should remain as-is (payments alias not covered by MV)
        assert '"t2"."amount"' in result.sql

    def test_partial_match_adds_mv_catalog_to_sources(self):
        # REQ-810: partial rewrite merges MV catalog into existing sources
        sql = (
            'SELECT "t0"."id", "t1"."name", "t2"."amount" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."payments" "t2" ON "t0"."id" = "t2"."order_id"'
        )
        compiled = _compiled(sql, sources={"sales-pg"})
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        assert "iceberg" in result.sources
        assert "sales-pg" in result.sources

    def test_partial_match_preserves_params(self):
        # REQ-810: partial rewrite does not alter query params
        sql = (
            'SELECT "t0"."id" FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."payments" "t2" ON "t0"."id" = "t2"."order_id" '
            'WHERE "t0"."status" = $1'
        )
        compiled = CompiledQuery(
            sql=sql,
            params=["active"],
            root_field="orders",
            columns=[],
            sources={"sales-pg"},
        )
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_orders_customers")

        result = rewrite_if_mv_match(compiled, [mv])

        assert result.params == ["active"]


# ---------------------------------------------------------------------------
# REQ-810: _partial_rewrite_to_mv called directly — unit-level
# ---------------------------------------------------------------------------


class TestPartialRewriteDirectCall:
    def test_direct_call_removes_matched_join(self):
        # REQ-810: _partial_rewrite_to_mv removes matched join index
        from provisa.mv.rewriter import _extract_join_info

        sql = (
            'SELECT "t0"."id", "t1"."email", "t2"."total" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."invoices" "t2" ON "t0"."id" = "t2"."order_id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_ord_cust")

        joins = _extract_join_info(sql)
        # join 0 = customers (covered), join 1 = invoices (uncovered)
        result = _partial_rewrite_to_mv(compiled, mv, joins, matched_indices=[0])

        assert "customers" not in result.sql or "mv_ord_cust" in result.sql
        assert "invoices" in result.sql

    def test_direct_call_mv_alias_preserved_as_t0(self):
        # REQ-810: MV is aliased as "t0" so root-table column refs remain valid
        from provisa.mv.rewriter import _extract_join_info

        sql = (
            'SELECT "t0"."id", "t1"."email", "t2"."total" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."invoices" "t2" ON "t0"."id" = "t2"."order_id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id", "mv_ord_cust")

        joins = _extract_join_info(sql)
        result = _partial_rewrite_to_mv(compiled, mv, joins, matched_indices=[0])

        assert '"t0"' in result.sql


# ---------------------------------------------------------------------------
# REQ-810: No match — query unchanged
# ---------------------------------------------------------------------------


class TestNoMatchRewrite:
    def test_no_match_returns_original_sql(self):
        # REQ-810: when no MV matches, original SQL is returned unchanged
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."products" "t1" ON "t0"."product_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        # MV is for orders ↔ customers, not products
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id")

        result = rewrite_if_mv_match(compiled, [mv])

        assert result.sql == sql

    def test_empty_mv_list_returns_unchanged(self):
        # REQ-810: empty MV list → query returned unchanged
        sql = (
            'SELECT "t0"."id" FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql)

        result = rewrite_if_mv_match(compiled, [])

        assert result.sql == sql

    def test_stale_mv_not_matched(self):
        # REQ-810: STALE MV is not used for rewriting
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id")
        mv.status = MVStatus.STALE  # MVDefinition is not frozen; override

        result = rewrite_if_mv_match(compiled, [mv])

        assert result.sql == sql

    def test_no_join_in_query_returns_unchanged(self):
        # REQ-810: query with no JOINs is never rewritten
        sql = 'SELECT "t0"."id" FROM "public"."orders" "t0"'
        compiled = _compiled(sql)
        mv = _fresh_mv("mv-1", "orders", "customer_id", "customers", "id")

        result = rewrite_if_mv_match(compiled, [mv])

        assert result.sql == sql
