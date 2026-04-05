# Copyright (c) 2026 Kenneth Stott
# Canary: fcea896c-848b-4202-a2f0-65d617fcba99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""End-to-end tests for materialized view optimization pipeline.

Tests the full flow: MV registration → query compilation → rewrite → stale
invalidation → refresh → re-optimization. Uses mock execution layer.
"""

from __future__ import annotations

from provisa.compiler.sql_gen import ColumnRef, CompiledQuery
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus, SDLConfig
from provisa.mv.registry import MVRegistry
from provisa.mv.rewriter import rewrite_if_mv_match


def _compiled(sql, sources=None):
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
        ],
        sources=sources or {"pg"},
    )


def _mv_fresh():
    mv = MVDefinition(
        id="mv-orders-customers",
        source_tables=["orders", "customers"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
            join_type="left",
        ),
        refresh_interval=300,
    )
    mv.status = MVStatus.FRESH
    return mv


JOIN_SQL = (
    'SELECT "t0"."id", "t1"."name" '
    'FROM "public"."orders" "t0" '
    'LEFT JOIN "public"."customers" "t1" '
    'ON "t0"."customer_id" = "t1"."id"'
)


class TestFreshMVOptimization:
    """Query with fresh MV → rewritten to MV target table."""

    def test_fresh_mv_rewrites_query(self):
        compiled = _compiled(JOIN_SQL)
        mv = _mv_fresh()
        result = rewrite_if_mv_match(compiled, [mv])
        assert "mv_cache" in result.sql
        assert "JOIN" not in result.sql

    def test_sources_updated_to_mv_catalog(self):
        compiled = _compiled(JOIN_SQL, sources={"pg", "pg2"})
        mv = _mv_fresh()
        result = rewrite_if_mv_match(compiled, [mv])
        assert "postgresql" in result.sources


class TestStaleMVFallback:
    """Stale MV → original SQL executed, no silent stale data (REQ-064)."""

    def test_stale_mv_not_used(self):
        compiled = _compiled(JOIN_SQL)
        mv = _mv_fresh()
        mv.status = MVStatus.STALE
        result = rewrite_if_mv_match(compiled, [mv])
        assert result.sql == JOIN_SQL

    def test_disabled_mv_not_used(self):
        compiled = _compiled(JOIN_SQL)
        mv = _mv_fresh()
        mv.status = MVStatus.DISABLED
        mv.enabled = False
        result = rewrite_if_mv_match(compiled, [mv])
        assert result.sql == JOIN_SQL


class TestMutationInvalidation:
    """Mutation on source table → MV marked stale → next query executes normally (REQ-084)."""

    def test_mutation_marks_mv_stale(self):
        registry = MVRegistry()
        mv = _mv_fresh()
        registry.register(mv)
        assert mv.status == MVStatus.FRESH

        # Simulate mutation on orders table
        affected = registry.mark_stale("orders")
        assert "mv-orders-customers" in affected
        assert mv.status == MVStatus.STALE

        # Next query should not use stale MV
        compiled = _compiled(JOIN_SQL)
        result = rewrite_if_mv_match(compiled, registry.get_fresh())
        assert result.sql == JOIN_SQL  # unchanged

    def test_refresh_re_enables_optimization(self):
        registry = MVRegistry()
        mv = _mv_fresh()
        registry.register(mv)

        # Stale → refresh → fresh
        registry.mark_stale("orders")
        assert mv.status == MVStatus.STALE
        registry.mark_refreshed("mv-orders-customers", row_count=500)
        assert mv.status == MVStatus.FRESH

        # Query should use MV again
        compiled = _compiled(JOIN_SQL)
        result = rewrite_if_mv_match(compiled, registry.get_fresh())
        assert "mv_cache" in result.sql


class TestRLSOnMVQueries:
    """RLS WHERE clauses are preserved after MV rewrite (REQ-085)."""

    def test_where_clause_preserved(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id" '
            'WHERE "t0"."region" = $1'
        )
        compiled = CompiledQuery(
            sql=sql, params=["us"], root_field="orders",
            columns=[], sources={"pg"},
        )
        mv = _mv_fresh()
        result = rewrite_if_mv_match(compiled, [mv])
        assert "WHERE" in result.sql
        assert '"region"' in result.sql
        assert result.params == ["us"]

    def test_order_by_preserved(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id" '
            'ORDER BY "t0"."id" ASC'
        )
        compiled = _compiled(sql)
        mv = _mv_fresh()
        result = rewrite_if_mv_match(compiled, [mv])
        assert "ORDER BY" in result.sql
        assert '"id"' in result.sql

    def test_limit_offset_preserved(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id" '
            'LIMIT 10 OFFSET 20'
        )
        compiled = _compiled(sql)
        mv = _mv_fresh()
        result = rewrite_if_mv_match(compiled, [mv])
        assert "LIMIT 10" in result.sql
        assert "OFFSET 20" in result.sql


class TestPartialMVMatch:
    """Partial match: MV covers subset of JOINs, rest preserved (REQ-083)."""

    def test_partial_match_rewrites_covered_join(self):
        sql = (
            'SELECT "t0"."id", "t1"."name", "t2"."product_name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."products" "t2" '
            'ON "t0"."product_id" = "t2"."id"'
        )
        compiled = CompiledQuery(
            sql=sql, params=[], root_field="orders",
            columns=[], sources={"pg"},
        )
        mv = _mv_fresh()
        result = rewrite_if_mv_match(compiled, [mv])

        # MV used for orders+customers
        assert "mv_cache" in result.sql
        # Products JOIN preserved
        assert '"products"' in result.sql
        assert '"t2"' in result.sql
        # Covered right-table columns rewritten
        assert '"t0"."customers__name"' in result.sql

    def test_partial_match_preserves_uncovered_join_on_clause(self):
        sql = (
            'SELECT "t0"."id", "t1"."name", "t2"."product_name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."products" "t2" '
            'ON "t0"."product_id" = "t2"."id"'
        )
        compiled = CompiledQuery(
            sql=sql, params=[], root_field="orders",
            columns=[], sources={"pg"},
        )
        mv = _mv_fresh()
        result = rewrite_if_mv_match(compiled, [mv])

        # Uncovered JOIN ON clause still references t0 and t2
        assert '"t0"."product_id"' in result.sql
        assert '"t2"."id"' in result.sql


class TestSDLExposure:
    """Exposed MVs have sdl_config for schema registration (REQ-086)."""

    def test_exposed_mv_has_sdl_config(self):
        mv = MVDefinition(
            id="mv-customer-stats",
            source_tables=["orders", "customers"],
            target_catalog="postgresql",
            target_schema="mv_cache",
            sql="SELECT ...",
            expose_in_sdl=True,
            sdl_config=SDLConfig(
                domain_id="sales-analytics",
                governance="pre-approved",
                columns=[
                    {"name": "customer_id", "visible_to": ["admin", "analyst"]},
                    {"name": "order_count", "visible_to": ["admin", "analyst"]},
                    {"name": "avg_amount", "visible_to": ["admin"]},
                ],
            ),
        )
        assert mv.expose_in_sdl is True
        assert mv.sdl_config.domain_id == "sales-analytics"
        assert len(mv.sdl_config.columns) == 3

    def test_non_exposed_mv_no_sdl_config(self):
        mv = MVDefinition(
            id="mv-transparent",
            source_tables=["orders", "customers"],
            target_catalog="postgresql",
            target_schema="mv_cache",
            join_pattern=JoinPattern(
                left_table="orders", left_column="customer_id",
                right_table="customers", right_column="id",
            ),
        )
        assert mv.expose_in_sdl is False
        assert mv.sdl_config is None


class TestNoJoinQueryUnchanged:
    """Single-table queries pass through without modification."""

    def test_no_join_unchanged(self):
        sql = 'SELECT "id", "amount" FROM "public"."orders"'
        compiled = _compiled(sql)
        mv = _mv_fresh()
        result = rewrite_if_mv_match(compiled, [mv])
        assert result.sql == sql

    def test_empty_mv_list_unchanged(self):
        compiled = _compiled(JOIN_SQL)
        result = rewrite_if_mv_match(compiled, [])
        assert result.sql == JOIN_SQL
