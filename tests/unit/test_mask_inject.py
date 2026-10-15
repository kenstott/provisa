# Copyright (c) 2026 Kenneth Stott
# Canary: 82e1dc51-1990-41c2-b6c9-32e720d5434d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column masks applied to compiled SQL by the one governed pipeline (compiler/stage2)."""

from provisa.compiler.mask_inject import MaskingRules
from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.security.masking import MaskType, MaskingRule
from tests.helpers import governed_for_role

_REGEX = "REGEXP_REPLACE({}, '^(.{{2}}).*(@.*)$', '$1***$2')"
_EMAIL_MASK = _REGEX.format('"email"')
_T1_EMAIL_MASK = _REGEX.format('"t1"."email"')


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


def _customers_ctx():
    return _ctx(
        tables={"customers": _meta(table_id=2, field_name="customers", table_name="customers")}
    )


def _email_regex() -> MaskingRule:
    return MaskingRule(mask_type=MaskType.regex, pattern="^(.{2}).*(@.*)$", replace="$1***$2")


class TestInjectMasking:
    def test_no_rules_returns_unchanged(self):
        sql = 'SELECT "email" FROM "public"."customers"'
        assert governed_for_role(sql, _customers_ctx(), "analyst", masking_rules={}) == sql

    def test_regex_mask_on_simple_select(self):
        rules: MaskingRules = {(2, "analyst"): {"email": (_email_regex(), "varchar")}}
        result = governed_for_role(
            'SELECT "email" FROM "public"."customers"',
            _customers_ctx(),
            "analyst",
            masking_rules=rules,
        )
        assert result == (f'SELECT {_EMAIL_MASK} AS "email" FROM "public"."customers"')

    def test_constant_mask_replaces_column(self):
        rules: MaskingRules = {
            (1, "viewer"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
            }
        }
        result = governed_for_role(
            'SELECT "amount" FROM "public"."orders"', _ctx(), "viewer", masking_rules=rules
        )
        assert result == 'SELECT 0 AS "amount" FROM "public"."orders"'

    def test_truncate_mask(self):
        rules: MaskingRules = {
            (1, "analyst"): {
                "created_at": (
                    MaskingRule(mask_type=MaskType.truncate, precision="month"),
                    "timestamp",
                ),
            }
        }
        result = governed_for_role(
            'SELECT "t0"."created_at" FROM "public"."orders" "t0"',
            _ctx(),
            "analyst",
            masking_rules=rules,
        )
        # sqlglot canonicalizes the DATE_TRUNC unit to upper-case (same as transpile output).
        assert result == (
            'SELECT DATE_TRUNC(\'MONTH\', "t0"."created_at") AS "created_at" '
            'FROM "public"."orders" AS "t0"'
        )

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
        rules: MaskingRules = {(2, "analyst"): {"email": (_email_regex(), "varchar")}}
        result = governed_for_role(
            'SELECT "t0"."id", "t1"."email" FROM "public"."orders" "t0" LEFT JOIN '
            '"public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"',
            ctx,
            "analyst",
            masking_rules=rules,
        )
        # The joined table's column is masked; the root column is unchanged.
        assert result == (
            f'SELECT "t0"."id", {_T1_EMAIL_MASK} '
            'AS "email" FROM "public"."orders" AS "t0" LEFT JOIN "public"."customers" AS "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )

    def test_multiple_masked_columns(self):
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="***@***.***"),
                    "varchar",
                ),
                "name": (
                    MaskingRule(
                        mask_type=MaskType.regex, pattern="^(.).* (.).*$", replace="$1. $2."
                    ),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(
            'SELECT "email", "name" FROM "public"."customers"',
            _customers_ctx(),
            "analyst",
            masking_rules=rules,
        )
        assert result == (
            "SELECT '***@***.***' AS \"email\", "
            "REGEXP_REPLACE(\"name\", '^(.).* (.).*$', '$1. $2.') AS \"name\" "
            'FROM "public"."customers"'
        )

    def test_unmasked_columns_unchanged(self):
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="HIDDEN"), "varchar"),
            }
        }
        result = governed_for_role(
            'SELECT "id", "email", "region" FROM "public"."customers"',
            _customers_ctx(),
            "analyst",
            masking_rules=rules,
        )
        assert result == ('SELECT "id", \'HIDDEN\' AS "email", "region" FROM "public"."customers"')

    def test_no_masking_for_role_without_rules(self):
        sql = 'SELECT "email" FROM "public"."customers"'
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="HIDDEN"), "varchar"),
            }
        }
        # admin has no masking rules
        assert governed_for_role(sql, _customers_ctx(), "admin", masking_rules=rules) == sql

    def test_where_clause_preserved(self):
        rules: MaskingRules = {
            (2, "analyst"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="HIDDEN"), "varchar"),
            }
        }
        result = governed_for_role(
            'SELECT "email", "region" FROM "public"."customers" WHERE "region" = $1',
            _customers_ctx(),
            "analyst",
            masking_rules=rules,
        )
        assert result == (
            'SELECT \'HIDDEN\' AS "email", "region" FROM "public"."customers" WHERE "region" = $1'
        )
