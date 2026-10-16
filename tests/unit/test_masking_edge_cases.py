# Copyright (c) 2026 Kenneth Stott
# Canary: 232a593b-255d-4ef9-8e09-604d80c291a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column masking edge-case tests (REQ-087 through REQ-091).

These tests cover scenarios not addressed in tests/unit/test_mask_inject.py
(which covers: no rules unchanged, regex/constant/truncate on simple SELECT,
aliased JOIN columns, multiple masked columns, unmasked columns unchanged,
WHERE clause preserved) and tests/unit/test_masking.py
(which covers: validate_masking_rule and build_mask_expression for each type).
Masks are applied by the one governed pipeline (compiler/stage2); a reference to a
masked column outside the projection reads the mask (REQ-740), covered in
tests/unit/test_governance_whole_statement.py.

New coverage here:
  - A row filter and a mask on the same table coexist: the projection is masked and
    the row filter reads the physical column.
  - A masked column on a joined table: the projection is masked, a JOIN ON over
    unmasked keys is left as written, and the root table's columns are untouched.
  - Masking across SQL dialects — build_mask_expression output is dialect-agnostic
    (produces ANSI REGEXP_REPLACE / DATE_TRUNC / literal forms) regardless of
    the underlying source dialect.
  - NULL constant mask on nullable column emits exactly "NULL".
  - Boolean constant mask emits TRUE / FALSE (not 'True'/'False' Python strings).
  - ORDER BY and LIMIT survive masking of the projection.
  - Masked column with a parameterised type (varchar(255)) is handled correctly.
"""

from __future__ import annotations


from provisa.compiler.mask_inject import MaskingRules
from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.security.masking import MaskType, MaskingRule, build_mask_expression
from tests.helpers import governed_for_role

# ---------------------------------------------------------------------------
# Shared table IDs and helper builders
# ---------------------------------------------------------------------------

ORDERS_TABLE_ID = 1
CUSTOMERS_TABLE_ID = 2


def _table_meta(
    table_id: int = ORDERS_TABLE_ID,
    field_name: str = "orders",
    table_name: str = "orders",
    source_id: str = "pg",
) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name=field_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        table_name=table_name,
    )


def _ctx(
    tables: dict[str, TableMeta] | None = None,
    joins: dict | None = None,
) -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = tables or {"orders": _table_meta()}
    ctx.joins = joins or {}
    return ctx


# ---------------------------------------------------------------------------
# Row filter and mask on the same table
# ---------------------------------------------------------------------------


class TestMaskedColumnInWhereClause:
    def test_rls_where_and_masking_coexist(self):
        """A role with a row filter and a mask on one table gets both: the projection is
        masked and the row filter, which reads the physical column, is intact."""
        ctx = _ctx(tables={"customers": _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")})
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="***"),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(
            'SELECT "t0"."email", "t0"."region" FROM "public"."customers" "t0"',
            ctx,
            "analyst",
            rls={CUSTOMERS_TABLE_ID: "region = 'us-east'"},
            masking_rules=rules,
        )
        assert result == (
            'SELECT \'***\' AS "email", "t0"."region" FROM "public"."customers" AS "t0" '
            'WHERE ("t0"."region" = \'us-east\')'
        )


# ---------------------------------------------------------------------------
# Masked column in JOIN ON condition — only SELECT is rewritten
# ---------------------------------------------------------------------------


class TestMaskedColumnInJoinCondition:
    def test_join_on_predicate_not_rewritten_when_join_column_masked(self):
        """A masked column of the joined table is masked in the SELECT; the JOIN ON
        predicate, over unmasked keys, is left as written. (A masked key in JOIN ON reads
        the mask — tests/unit/test_governance_whole_statement.py.)
        """
        orders_meta = _table_meta(ORDERS_TABLE_ID, "orders", "orders")
        customers_meta = _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")
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
        sql = (
            'SELECT "t0"."id", "t1"."email" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="***"),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(sql, ctx, "analyst", masking_rules=rules)

        assert result == (
            'SELECT "t0"."id", REGEXP_REPLACE("t1"."email", \'.*\', \'***\', \'g\') AS "email" '
            'FROM "public"."orders" AS "t0" LEFT JOIN "public"."customers" AS "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )

    def test_join_root_column_not_masked_when_only_target_has_rule(self):
        """The root table's column in SELECT must not be masked when masking rules
        only apply to the joined table's column."""
        orders_meta = _table_meta(ORDERS_TABLE_ID, "orders", "orders")
        customers_meta = _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")
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
        sql = (
            'SELECT "t0"."amount", "t1"."email" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id"'
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="HIDDEN"),
                    "varchar",
                ),
            }
        }
        result = governed_for_role(sql, ctx, "analyst", masking_rules=rules)

        # email masked; amount untouched in SELECT
        assert result == (
            'SELECT "t0"."amount", \'HIDDEN\' AS "email" FROM "public"."orders" AS "t0" '
            'LEFT JOIN "public"."customers" AS "t1" ON "t0"."customer_id" = "t1"."id"'
        )


# ---------------------------------------------------------------------------
# Consistent masking across SQL dialects
# ---------------------------------------------------------------------------


class TestMaskingDialectConsistency:
    """build_mask_expression produces ANSI-style SQL that is dialect-independent.

    The masking layer does not receive a dialect hint — it always emits:
      REGEXP_REPLACE, DATE_TRUNC, and numeric/string literals.
    These forms work across PostgreSQL, MySQL, Snowflake, BigQuery etc.
    (Dialect-specific transpilation happens downstream via SQLGlot.)
    """

    def test_regex_mask_expression_is_ansi_regexp_replace(self):
        rule = MaskingRule(mask_type=MaskType.regex, pattern="^(.{2}).*$", replace="$1***")
        expr = build_mask_expression(rule, '"col"', "varchar")
        assert expr.startswith("REGEXP_REPLACE(")

    def test_truncate_mask_expression_is_ansi_date_trunc(self):
        rule = MaskingRule(mask_type=MaskType.truncate, precision="day")
        expr = build_mask_expression(rule, '"ts"', "timestamp")
        assert expr.startswith("DATE_TRUNC(")
        assert "'day'" in expr

    def test_constant_integer_mask_is_bare_literal(self):
        """Constant integer mask must emit a bare numeric literal (no quotes)."""
        rule = MaskingRule(mask_type=MaskType.constant, value=42)
        expr = build_mask_expression(rule, '"amount"', "integer")
        assert expr == "42"

    def test_constant_string_mask_is_single_quoted_literal(self):
        """Constant string mask must use ANSI single-quoted literals."""
        rule = MaskingRule(mask_type=MaskType.constant, value="redacted")
        expr = build_mask_expression(rule, '"name"', "varchar")
        assert expr == "'redacted'"

    def test_constant_null_mask_emits_null_keyword(self):
        """Constant NULL mask must emit the SQL keyword NULL (not quoted)."""
        rule = MaskingRule(mask_type=MaskType.constant, value=None)
        expr = build_mask_expression(rule, '"email"', "varchar")
        assert expr == "NULL"

    def test_constant_boolean_true_emits_true_keyword(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=True)
        expr = build_mask_expression(rule, '"flag"', "boolean")
        assert expr == "TRUE"

    def test_constant_boolean_false_emits_false_keyword(self):
        rule = MaskingRule(mask_type=MaskType.constant, value=False)
        expr = build_mask_expression(rule, '"flag"', "boolean")
        assert expr == "FALSE"

    def test_regex_mask_on_parameterized_varchar_uses_regexp_replace(self):
        """varchar(255) must be treated as varchar for REGEXP_REPLACE emission."""
        rule = MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="X")
        expr = build_mask_expression(rule, '"col"', "varchar(255)")
        assert expr.startswith("REGEXP_REPLACE(")

    def test_truncate_mask_preserves_precision_token(self):
        """The precision token must be preserved exactly in the DATE_TRUNC call."""
        for precision in ("year", "month", "day", "hour"):
            rule = MaskingRule(mask_type=MaskType.truncate, precision=precision)
            expr = build_mask_expression(rule, '"ts"', "timestamp")
            assert f"'{precision}'" in expr, f"Precision '{precision}' not found in: {expr}"

    def test_max_constant_resolves_per_integer_subtype(self):
        """MAX sentinel resolves to the correct bound for each integer subtype."""
        cases = [
            ("tinyint", "127"),
            ("smallint", "32767"),
            ("integer", "2147483647"),
            ("bigint", "9223372036854775807"),
        ]
        for data_type, expected in cases:
            rule = MaskingRule(mask_type=MaskType.constant, value="MAX")
            expr = build_mask_expression(rule, '"col"', data_type)
            assert expr == expected, f"MAX for {data_type}: expected {expected}, got {expr}"

    def test_min_constant_resolves_per_integer_subtype(self):
        """MIN sentinel resolves to the correct lower bound for each integer subtype."""
        cases = [
            ("tinyint", "-128"),
            ("smallint", "-32768"),
            ("integer", "-2147483648"),
            ("bigint", "-9223372036854775808"),
        ]
        for data_type, expected in cases:
            rule = MaskingRule(mask_type=MaskType.constant, value="MIN")
            expr = build_mask_expression(rule, '"col"', data_type)
            assert expr == expected, f"MIN for {data_type}: expected {expected}, got {expr}"


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT preserved when the projection is masked
# ---------------------------------------------------------------------------


class TestMaskingInjectionSideEffects:
    def test_order_by_preserved_after_masking(self):
        """ORDER BY on an unmasked column survives masking of the projection unchanged."""
        rules: MaskingRules = {
            (ORDERS_TABLE_ID, "viewer"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
            }
        }
        result = governed_for_role(
            'SELECT "id", "amount" FROM "public"."orders" ORDER BY "id" DESC',
            _ctx(),
            "viewer",
            masking_rules=rules,
        )
        assert result == 'SELECT "id", 0 AS "amount" FROM "public"."orders" ORDER BY "id" DESC'

    def test_limit_clause_preserved_after_masking(self):
        """LIMIT clause must not be removed or altered by masking."""
        ctx = _ctx(tables={"customers": _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")})
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="X"), "varchar"),
            }
        }
        result = governed_for_role(
            'SELECT "email" FROM "public"."customers" LIMIT 100',
            ctx,
            "analyst",
            masking_rules=rules,
        )
        assert result == 'SELECT \'X\' AS "email" FROM "public"."customers" LIMIT 100'

    def test_single_quote_in_constant_mask_value_escaped(self):
        """A constant mask value containing a single quote must be SQL-escaped."""
        rule = MaskingRule(mask_type=MaskType.constant, value="it's redacted")
        expr = build_mask_expression(rule, '"name"', "varchar")
        assert expr == "'it''s redacted'"

    def test_mask_expression_for_float_constant(self):
        """A float constant mask value must be emitted as a numeric literal."""
        rule = MaskingRule(mask_type=MaskType.constant, value=3.14)
        expr = build_mask_expression(rule, '"score"', "double")
        assert expr == "3.14"
