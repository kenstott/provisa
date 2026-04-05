# Copyright (c) 2026 Kenneth Stott
# Canary: 3d56c839-336d-436b-a37f-a937d682a902
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for RLS WHERE clause injection."""

from provisa.compiler.rls import (
    RLSContext,
    build_rls_context,
    inject_rls,
    _inject_where,
    _qualify_filter,
)
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,
    TableMeta,
)


def _meta(table_id=1, field_name="orders", table_name="orders", source_id="pg"):
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name=field_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        table_name=table_name,
    )


def _compiled(sql, root_field="orders", sources=None):
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field=root_field,
        columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources=sources or {"pg"},
    )


def _ctx(tables=None, joins=None):
    ctx = CompilationContext()
    ctx.tables = tables or {"orders": _meta()}
    ctx.joins = joins or {}
    return ctx


class TestBuildRLSContext:
    def test_filters_by_role(self):
        rules = [
            {"table_id": 1, "role_id": "analyst", "filter_expr": "region = 'us'"},
            {"table_id": 1, "role_id": "admin", "filter_expr": "1=1"},
            {"table_id": 2, "role_id": "analyst", "filter_expr": "active = true"},
        ]
        rls = build_rls_context(rules, "analyst")
        assert rls.rules == {1: "region = 'us'", 2: "active = true"}

    def test_empty_for_no_rules(self):
        rls = build_rls_context([], "admin")
        assert not rls.has_rules()

    def test_empty_for_unmatched_role(self):
        rules = [{"table_id": 1, "role_id": "other", "filter_expr": "x = 1"}]
        rls = build_rls_context(rules, "admin")
        assert not rls.has_rules()


class TestInjectRLS:
    def test_no_rules_returns_unchanged(self):
        compiled = _compiled('SELECT "id" FROM "public"."orders"')
        ctx = _ctx()
        result = inject_rls(compiled, ctx, RLSContext.empty())
        assert result.sql == compiled.sql

    def test_injects_where_on_simple_query(self):
        compiled = _compiled('SELECT "id" FROM "public"."orders"')
        ctx = _ctx()
        rls = RLSContext(rules={1: "region = 'us'"})
        result = inject_rls(compiled, ctx, rls)
        assert "WHERE" in result.sql
        assert "region = 'us'" in result.sql

    def test_ands_with_existing_where(self):
        compiled = _compiled('SELECT "id" FROM "public"."orders" WHERE "status" = $1')
        compiled = CompiledQuery(
            sql=compiled.sql, params=["active"],
            root_field="orders",
            columns=compiled.columns,
            sources=compiled.sources,
        )
        ctx = _ctx()
        rls = RLSContext(rules={1: "region = 'us'"})
        result = inject_rls(compiled, ctx, rls)
        assert "AND" in result.sql
        assert "region = 'us'" in result.sql
        assert '"status" = $1' in result.sql

    def test_injects_before_order_by(self):
        compiled = _compiled(
            'SELECT "id" FROM "public"."orders" ORDER BY "id"'
        )
        ctx = _ctx()
        rls = RLSContext(rules={1: "region = 'us'"})
        result = inject_rls(compiled, ctx, rls)
        assert result.sql.index("WHERE") < result.sql.index("ORDER BY")

    def test_injects_before_limit(self):
        compiled = _compiled(
            'SELECT "id" FROM "public"."orders" LIMIT 10'
        )
        ctx = _ctx()
        rls = RLSContext(rules={1: "region = 'us'"})
        result = inject_rls(compiled, ctx, rls)
        assert result.sql.index("WHERE") < result.sql.index("LIMIT")

    def test_no_rule_for_table_unchanged(self):
        compiled = _compiled('SELECT "id" FROM "public"."orders"')
        ctx = _ctx()
        rls = RLSContext(rules={99: "x = 1"})  # table_id 99 doesn't match
        result = inject_rls(compiled, ctx, rls)
        assert result.sql == compiled.sql

    def test_preserves_params(self):
        compiled = CompiledQuery(
            sql='SELECT "id" FROM "public"."orders" WHERE "x" = $1',
            params=["val"],
            root_field="orders",
            columns=[],
            sources={"pg"},
        )
        ctx = _ctx()
        rls = RLSContext(rules={1: "region = 'us'"})
        result = inject_rls(compiled, ctx, rls)
        assert result.params == ["val"]


class TestInjectWhere:
    def test_no_existing_where(self):
        sql = 'SELECT 1 FROM t'
        result = _inject_where(sql, "x = 1")
        assert result == "SELECT 1 FROM t WHERE x = 1"

    def test_existing_where(self):
        sql = 'SELECT 1 FROM t WHERE y = 2'
        result = _inject_where(sql, "x = 1")
        assert "x = 1 AND" in result
        assert "y = 2" in result

    def test_before_order_by(self):
        sql = 'SELECT 1 FROM t ORDER BY id'
        result = _inject_where(sql, "x = 1")
        assert "WHERE x = 1" in result
        assert result.index("WHERE") < result.index("ORDER BY")

    def test_before_limit(self):
        sql = 'SELECT 1 FROM t LIMIT 5'
        result = _inject_where(sql, "x = 1")
        assert "WHERE x = 1" in result
        assert result.index("WHERE") < result.index("LIMIT")


class TestQualifyFilter:
    def test_qualifies_column(self):
        result = _qualify_filter("region = 'us'", "t0")
        assert '"t0".region' in result

    def test_does_not_qualify_string_literal(self):
        result = _qualify_filter("region = 'us'", "t0")
        # 'us' should not be qualified
        assert "'us'" in result

    def test_does_not_double_qualify(self):
        result = _qualify_filter('"t0".region = \'us\'', "t0")
        # Should not add another t0 prefix
        assert result.count('"t0"') == 1
