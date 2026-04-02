# Copyright (c) 2025 Kenneth Stott
# Canary: d36d0e03-e74f-498c-9516-39f0c2d10d23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for materialized view SQL rewriter."""

from provisa.compiler.sql_gen import ColumnRef, CompiledQuery
from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
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


def _mv(
    mv_id="mv-orders-customers",
    left="orders", left_col="customer_id",
    right="customers", right_col="id",
    status=MVStatus.FRESH,
):
    mv = MVDefinition(
        id=mv_id,
        source_tables=[left, right],
        target_catalog="postgresql",
        target_schema="mv_cache",
        join_pattern=JoinPattern(
            left_table=left,
            left_column=left_col,
            right_table=right,
            right_column=right_col,
            join_type="left",
        ),
        refresh_interval=300,
    )
    mv.status = status
    return mv


class TestRewriteIfMvMatch:
    def test_matching_join_rewrites(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        mv = _mv()
        result = rewrite_if_mv_match(compiled, [mv])
        assert "mv_cache" in result.sql
        assert "mv_orders_customers" in result.sql
        assert "JOIN" not in result.sql

    def test_stale_mv_not_used(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        mv = _mv(status=MVStatus.STALE)
        result = rewrite_if_mv_match(compiled, [mv])
        assert result.sql == sql  # unchanged

    def test_no_matching_join_unchanged(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."products" "t1" '
            'ON "t0"."product_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        mv = _mv()  # covers orders↔customers, not orders↔products
        result = rewrite_if_mv_match(compiled, [mv])
        assert result.sql == sql  # unchanged

    def test_no_joins_unchanged(self):
        sql = 'SELECT "id", "amount" FROM "public"."orders"'
        compiled = _compiled(sql, sources={"pg"})
        mv = _mv()
        result = rewrite_if_mv_match(compiled, [mv])
        assert result.sql == sql

    def test_empty_mv_list(self):
        sql = 'SELECT "id" FROM "public"."orders"'
        compiled = _compiled(sql)
        result = rewrite_if_mv_match(compiled, [])
        assert result.sql == sql

    def test_where_preserved_after_rewrite(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id" '
            'WHERE "t0"."region" = $1'
        )
        compiled = _compiled(sql)
        compiled = CompiledQuery(
            sql=sql, params=["us"], root_field="orders",
            columns=compiled.columns, sources=compiled.sources,
        )
        mv = _mv()
        result = rewrite_if_mv_match(compiled, [mv])
        assert "WHERE" in result.sql
        assert '"region"' in result.sql
        assert result.params == ["us"]

    def test_sources_updated_to_mv_target(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql, sources={"pg", "pg2"})
        mv = _mv()
        result = rewrite_if_mv_match(compiled, [mv])
        assert "postgresql" in result.sources

    def test_aliases_removed_after_rewrite(self):
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _compiled(sql)
        mv = _mv()
        result = rewrite_if_mv_match(compiled, [mv])
        # Table aliases should be removed
        assert '"t0".' not in result.sql
        assert '"t1".' not in result.sql
        # Left-table columns keep their name, right-table columns get prefixed
        assert '"id"' in result.sql
        assert '"customers__name"' in result.sql


class TestMVRegistry:
    def test_get_fresh_returns_only_fresh(self):
        from provisa.mv.registry import MVRegistry
        reg = MVRegistry()
        mv1 = _mv(mv_id="mv1", status=MVStatus.FRESH)
        mv2 = _mv(mv_id="mv2", status=MVStatus.STALE)
        reg.register(mv1)
        reg.register(mv2)
        fresh = reg.get_fresh()
        assert len(fresh) == 1
        assert fresh[0].id == "mv1"

    def test_mark_stale_by_table(self):
        from provisa.mv.registry import MVRegistry
        reg = MVRegistry()
        mv = _mv(status=MVStatus.FRESH)
        reg.register(mv)
        affected = reg.mark_stale("orders")
        assert affected == ["mv-orders-customers"]
        assert mv.status == MVStatus.STALE

    def test_mark_stale_unrelated_table(self):
        from provisa.mv.registry import MVRegistry
        reg = MVRegistry()
        mv = _mv(status=MVStatus.FRESH)
        reg.register(mv)
        affected = reg.mark_stale("products")
        assert affected == []
        assert mv.status == MVStatus.FRESH

    def test_mark_refreshed(self):
        from provisa.mv.registry import MVRegistry
        reg = MVRegistry()
        mv = _mv(status=MVStatus.STALE)
        reg.register(mv)
        reg.mark_refreshed("mv-orders-customers", row_count=1000)
        assert mv.status == MVStatus.FRESH
        assert mv.row_count == 1000

    def test_get_due_for_refresh(self):
        from provisa.mv.registry import MVRegistry
        reg = MVRegistry()
        mv = _mv(status=MVStatus.STALE)
        reg.register(mv)
        due = reg.get_due_for_refresh()
        assert len(due) == 1
