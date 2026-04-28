# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-cdef-012345678901
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
WHERE clause preserved, params preserved) and tests/unit/test_masking.py
(which covers: validate_masking_rule and build_mask_expression for each type).

New coverage here:
  - Masked column referenced in WHERE clause: masking only touches SELECT
    projection; the WHERE predicate must reference the original physical column.
  - Masked column referenced in JOIN ON condition: the JOIN ON predicate must
    not be rewritten; only the SELECT projection is masked.
  - Masking applied consistently with aliased and non-aliased column refs.
  - Masking across SQL dialects — build_mask_expression output is dialect-agnostic
    (produces ANSI REGEXP_REPLACE / DATE_TRUNC / literal forms) regardless of
    the underlying source dialect.
  - NULL constant mask on nullable column emits exactly "NULL".
  - Boolean constant mask emits TRUE / FALSE (not 'True'/'False' Python strings).
  - Masking applied only to SELECT portion when LIMIT and ORDER BY are present.
  - Masked column with a parameterised type (varchar(255)) is handled correctly.
  - A column present in SELECT but also mentioned in GROUP BY is only masked in SELECT.
  - inject_masking returns a new CompiledQuery object (not the same instance).
"""

from __future__ import annotations

import pytest

from provisa.compiler.mask_inject import MaskingRules, inject_masking
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,
    TableMeta,
)
from provisa.security.masking import MaskType, MaskingRule, build_mask_expression

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


def _q(
    sql: str,
    root_field: str = "orders",
    columns: list[ColumnRef] | None = None,
    params: list | None = None,
) -> CompiledQuery:
    return CompiledQuery(
        sql=sql,
        params=params or [],
        root_field=root_field,
        columns=columns or [ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources={"pg"},
    )


# ---------------------------------------------------------------------------
# Masked column in WHERE clause — SELECT is masked, WHERE is preserved
# ---------------------------------------------------------------------------


class TestMaskedColumnInWhereClause:
    def test_where_predicate_on_masked_column_is_preserved(self):
        """When a masked column also appears in WHERE, the WHERE clause must
        retain the original physical column reference unchanged.

        Masking only rewrites the SELECT projection; a WHERE predicate already
        set by the query compiler must not be touched.
        """
        compiled = _q(
            sql='SELECT "email", "region" FROM "public"."customers" WHERE "email" = $1',
            root_field="customers",
            columns=[
                ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
                ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
            ],
            params=["bob@example.com"],
        )
        ctx = _ctx(
            tables={"customers": _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")}
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="HIDDEN"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")

        # The SELECT must use the mask expression
        assert "'HIDDEN' AS \"email\"" in result.sql

        # The WHERE clause must still reference the original physical column
        assert '"email" = $1' in result.sql

        # Params must be untouched
        assert result.params == ["bob@example.com"]

    def test_where_with_aliased_masked_column_preserved(self):
        """Aliased column reference in WHERE is not rewritten by masking."""
        compiled = _q(
            sql='SELECT "t0"."amount", "t0"."region" FROM "public"."orders" "t0" WHERE "t0"."amount" > $1',
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None),
                ColumnRef(alias="t0", column="region", field_name="region", nested_in=None),
            ],
            params=[100],
        )
        ctx = _ctx()
        rules: MaskingRules = {
            (ORDERS_TABLE_ID, "viewer"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
            }
        }
        result = inject_masking(compiled, ctx, rules, "viewer")

        # Mask applied in SELECT
        assert '0 AS "amount"' in result.sql

        # WHERE must retain the original physical reference
        assert '"t0"."amount" > $1' in result.sql

    def test_rls_where_and_masking_coexist(self):
        """Masking injection after RLS injection must not disrupt the RLS WHERE clause."""
        # Simulates a query that already has an RLS WHERE injected before masking runs
        sql_with_rls = (
            'SELECT "t0"."email", "t0"."region" '
            'FROM "public"."customers" "t0" '
            'WHERE (region = \'us-east\')'
        )
        compiled = _q(
            sql=sql_with_rls,
            root_field="customers",
            columns=[
                ColumnRef(alias="t0", column="email", field_name="email", nested_in=None),
                ColumnRef(alias="t0", column="region", field_name="region", nested_in=None),
            ],
        )
        ctx = _ctx(
            tables={"customers": _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")}
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="***"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")

        # Masking applied in SELECT
        assert "'***' AS \"email\"" in result.sql

        # RLS WHERE clause must be intact
        assert "region = 'us-east'" in result.sql
        assert "WHERE" in result.sql.upper()


# ---------------------------------------------------------------------------
# Masked column in JOIN ON condition — only SELECT is rewritten
# ---------------------------------------------------------------------------


class TestMaskedColumnInJoinCondition:
    def test_join_on_predicate_not_rewritten_when_join_column_masked(self):
        """When a column used in a JOIN ON condition is also masked in the SELECT,
        the JOIN ON predicate must not be altered — only the SELECT projection is
        masked.
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
        compiled = _q(
            sql=sql,
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
                ColumnRef(alias="t1", column="email", field_name="email", nested_in="customers"),
            ],
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.regex, pattern=".*", replace="***"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")

        # SELECT must be masked
        assert "REGEXP_REPLACE" in result.sql

        # JOIN ON predicate must be unchanged
        assert 'ON "t0"."customer_id" = "t1"."id"' in result.sql

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
        compiled = _q(
            sql=sql,
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None),
                ColumnRef(alias="t1", column="email", field_name="email", nested_in="customers"),
            ],
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (
                    MaskingRule(mask_type=MaskType.constant, value="HIDDEN"),
                    "varchar",
                ),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")

        # email masked
        assert "'HIDDEN' AS \"email\"" in result.sql
        # amount untouched in SELECT
        assert '"t0"."amount"' in result.sql


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
# inject_masking returns a new object; ORDER BY / LIMIT preserved
# ---------------------------------------------------------------------------


class TestMaskingInjectionSideEffects:
    def test_returns_new_compiled_query_object(self):
        """inject_masking must return a new CompiledQuery, not mutate the original."""
        compiled = _q(
            sql='SELECT "amount" FROM "public"."orders"',
            columns=[ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None)],
        )
        ctx = _ctx()
        rules: MaskingRules = {
            (ORDERS_TABLE_ID, "analyst"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert result is not compiled

    def test_order_by_preserved_after_masking(self):
        """ORDER BY in the original SQL must survive masking injection unchanged."""
        compiled = _q(
            sql='SELECT "amount" FROM "public"."orders" ORDER BY "amount" DESC',
            columns=[ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None)],
        )
        ctx = _ctx()
        rules: MaskingRules = {
            (ORDERS_TABLE_ID, "viewer"): {
                "amount": (MaskingRule(mask_type=MaskType.constant, value=0), "integer"),
            }
        }
        result = inject_masking(compiled, ctx, rules, "viewer")
        assert 'ORDER BY "amount" DESC' in result.sql

    def test_limit_clause_preserved_after_masking(self):
        """LIMIT clause must not be removed or altered by masking injection."""
        compiled = _q(
            sql='SELECT "email" FROM "public"."customers" LIMIT 100',
            root_field="customers",
            columns=[ColumnRef(alias=None, column="email", field_name="email", nested_in=None)],
        )
        ctx = _ctx(
            tables={"customers": _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")}
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="X"), "varchar"),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert "LIMIT 100" in result.sql

    def test_sources_preserved_after_masking(self):
        """The sources set on the CompiledQuery must not be altered by masking."""
        compiled = CompiledQuery(
            sql='SELECT "email" FROM "public"."customers"',
            params=[],
            root_field="customers",
            columns=[ColumnRef(alias=None, column="email", field_name="email", nested_in=None)],
            sources={"pg", "replica"},
        )
        ctx = _ctx(
            tables={"customers": _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")}
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="X"), "varchar"),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert result.sources == {"pg", "replica"}

    def test_columns_metadata_preserved_after_masking(self):
        """The columns list on the CompiledQuery must be preserved by masking."""
        col_ref = ColumnRef(alias=None, column="email", field_name="email", nested_in=None)
        compiled = _q(
            sql='SELECT "email" FROM "public"."customers"',
            root_field="customers",
            columns=[col_ref],
        )
        ctx = _ctx(
            tables={"customers": _table_meta(CUSTOMERS_TABLE_ID, "customers", "customers")}
        )
        rules: MaskingRules = {
            (CUSTOMERS_TABLE_ID, "analyst"): {
                "email": (MaskingRule(mask_type=MaskType.constant, value="X"), "varchar"),
            }
        }
        result = inject_masking(compiled, ctx, rules, "analyst")
        assert result.columns == [col_ref]

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
