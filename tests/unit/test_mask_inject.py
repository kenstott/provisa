# Copyright (c) 2026 Kenneth Stott
# Canary: 82e1dc51-1990-41c2-b6c9-32e720d5434d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for masking injection into compiled SQL."""

from provisa.compiler.mask_inject import inject_masking, MaskingRules
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,
    TableMeta,
)
from provisa.security.masking import MaskType, MaskingRule


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


def _ctx(tables=None, joins=None):
    ctx = CompilationContext()
    ctx.tables = tables or {"orders": _meta()}
    ctx.joins = joins or {}
    return ctx


class TestInjectMasking:
    def test_no_rules_returns_unchanged(self):
        compiled = CompiledQuery(
            sql='SELECT "email" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[ColumnRef(alias=None, column="email", field_name="email", nested_in=None)],
            sources={"pg"},
        )
        ctx = _ctx(tables={"customers": _meta(table_id=2, field_name="customers", table_name="customers")})
        result = inject_masking(compiled, ctx, {}, "analyst")
        assert result.sql == compiled.sql

    def test_regex_mask_on_simple_select(self):
        compiled = CompiledQuery(
            sql='SELECT "email" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[ColumnRef(alias=None, column="email", field_name="email", nested_in=None)],
            sources={"pg"},
        )
        ctx = _ctx(tables={"customers": _meta(table_id=2, field_name="customers", table_name="customers")})
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.regex, pattern="^(.{2}).*(@.*)$", replace="$1***$2"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert "REGEXP_REPLACE" in result.sql
        assert 'AS "email"' in result.sql
        assert result.params == []

    def test_constant_mask_replaces_column(self):
        compiled = CompiledQuery(
            sql='SELECT "amount" FROM "public"."orders"',
            params=[],
            root_field="orders",
            columns=[ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None)],
            sources={"pg"},
        )
        ctx = _ctx()
        rules: MaskingRules = {
            (1, "viewer"): {
                "amount": (
                    MaskingRule(mask_type=MaskType.constant, value=0),
                    "integer",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "viewer")
        assert '0 AS "amount"' in result.sql

    def test_truncate_mask(self):
        compiled = CompiledQuery(
            sql='SELECT "t0"."created_at" FROM "public"."orders" "t0"',
            params=[],
            root_field="orders",
            columns=[ColumnRef(alias="t0", column="created_at", field_name="created_at", nested_in=None)],
            sources={"pg"},
        )
        ctx = _ctx()
        rules: MaskingRules = {
            (1, "analyst"): {
                "created_at": (
                    MaskingRule(mask_type=MaskType.truncate, precision="month"),
                    "timestamp",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert "DATE_TRUNC('month'" in result.sql
        assert 'AS "created_at"' in result.sql

    def test_aliased_column_with_join(self):
        orders_meta = _meta(table_id=1, field_name="orders", table_name="orders")
        customers_meta = _meta(table_id=2, field_name="customers", table_name="customers")
        ctx = _ctx(
            tables={"orders": orders_meta},
            joins={
                ("Orders", "customers"): JoinMeta(
                    source_column="customer_id",
                    target_column="id",
                    source_column_type="integer",
                    target_column_type="integer",
                    target=customers_meta,
                    cardinality="many-to-one",
                ),
            },
        )
        compiled = CompiledQuery(
            sql='SELECT "t0"."id", "t1"."email" FROM "public"."orders" "t0" LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"',
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
                ColumnRef(alias="t1", column="email", field_name="email", nested_in="customers"),
            ],
            sources={"pg"},
        )
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.regex, pattern="^(.{2}).*(@.*)$", replace="$1***$2"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert "REGEXP_REPLACE" in result.sql
        assert 'AS "email"' in result.sql
        # Root column should be unchanged
        assert '"t0"."id"' in result.sql

    def test_multiple_masked_columns(self):
        compiled = CompiledQuery(
            sql='SELECT "email", "name" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
                ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = _ctx(tables={"customers": _meta(table_id=2, field_name="customers", table_name="customers")})
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="***@***.***"),
                    "varchar",
                ),
                "name": (
                    MaskingRule(mask_type=MaskType.regex, pattern="^(.).* (.).*$", replace="$1. $2."),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert "'***@***.***' AS \"email\"" in result.sql
        assert "REGEXP_REPLACE" in result.sql
        assert 'AS "name"' in result.sql

    def test_unmasked_columns_unchanged(self):
        compiled = CompiledQuery(
            sql='SELECT "id", "email", "region" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
                ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = _ctx(tables={"customers": _meta(table_id=2, field_name="customers", table_name="customers")})
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="HIDDEN"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert "'HIDDEN' AS \"email\"" in result.sql
        # id and region should be untouched
        assert '"id"' in result.sql
        assert '"region"' in result.sql

    def test_no_masking_for_role_without_rules(self):
        compiled = CompiledQuery(
            sql='SELECT "email" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[ColumnRef(alias=None, column="email", field_name="email", nested_in=None)],
            sources={"pg"},
        )
        ctx = _ctx(tables={"customers": _meta(table_id=2, field_name="customers", table_name="customers")})
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="HIDDEN"),
                    "varchar",
                ),
            }
        }
        # admin has no masking rules
        result = inject_masking(compiled, ctx, rules, "admin")
        assert result.sql == compiled.sql

    def test_where_clause_preserved(self):
        compiled = CompiledQuery(
            sql='SELECT "email", "region" FROM "public"."customers" WHERE "region" = $1',
            params=["us"],
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
                ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
            ],
            sources={"pg"},
        )
        ctx = _ctx(tables={"customers": _meta(table_id=2, field_name="customers", table_name="customers")})
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="HIDDEN"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert 'WHERE "region" = $1' in result.sql
        assert "'HIDDEN' AS \"email\"" in result.sql
        assert result.params == ["us"]

    def test_params_preserved(self):
        compiled = CompiledQuery(
            sql='SELECT "email" FROM "public"."customers" WHERE "id" = $1',
            params=[42],
            root_field="customers",
            columns=[ColumnRef(alias=None, column="email", field_name="email", nested_in=None)],
            sources={"pg"},
        )
        ctx = _ctx(tables={"customers": _meta(table_id=2, field_name="customers", table_name="customers")})
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="X"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert result.params == [42]
        assert result.columns == compiled.columns
        assert result.sources == compiled.sources
