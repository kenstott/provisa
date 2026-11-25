# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3c46f8-6e57-4bbd-827c-f884ab8a348c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``saphana`` dialect (REQ-2236): a governed statement written in SAP HANA's SQL where each
construct has a form there giving PostgreSQL's answer, and refused by name where one has none."""

# Requirements: REQ-2236, REQ-2167, REQ-1922

from __future__ import annotations

import math
from fractions import Fraction

import pytest
import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.dialect import Dialect

from provisa.core import sql_integral
from provisa.core.source_registry import engine_dialect
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile


def _hana(sql: str) -> str:
    return transpile(sql, "saphana", generated=False)


def test_saphana_is_a_registered_sqlglot_dialect() -> None:
    assert engine_dialect("saphana") == "saphana"
    assert engine_dialect("hana") == "saphana"  # sqlalchemy-hana's URL scheme
    assert "saphana" in Dialect.classes


# -- the forms a compiled statement takes ------------------------------------------------------


def test_filter_and_paging() -> None:
    sql = (
        'SELECT "o"."id", "o"."status" FROM "sales"."orders" AS "o" '
        'WHERE "o"."amount" > 10 AND "o"."status" <> \'void\' AND "o"."flag" = TRUE '
        'ORDER BY "o"."id" DESC LIMIT 20 OFFSET 40'
    )
    assert _hana(sql) == (
        'SELECT "o"."id", "o"."status" FROM "sales"."orders" AS "o" '
        'WHERE "o"."amount" > 10 AND "o"."status" <> \'void\' AND "o"."flag" = TRUE '
        'ORDER BY "o"."id" DESC NULLS FIRST LIMIT 20 OFFSET 40'
    )


def test_null_order_is_written_as_postgres_sorts() -> None:
    # HANA sorts NULL as the smallest value; PostgreSQL as the largest, so each order says where.
    assert _hana('SELECT "a" FROM "t" ORDER BY "a"') == (
        'SELECT "a" FROM "t" ORDER BY "a" NULLS LAST'
    )
    assert _hana('SELECT "a" FROM "t" ORDER BY "a" DESC') == (
        'SELECT "a" FROM "t" ORDER BY "a" DESC NULLS FIRST'
    )


def test_fetch_first_is_limit() -> None:
    assert _hana('SELECT "a" FROM "t" ORDER BY "a" DESC FETCH FIRST 3 ROWS ONLY') == (
        'SELECT "a" FROM "t" ORDER BY "a" DESC NULLS FIRST LIMIT 3'
    )


def test_joins() -> None:
    sql = (
        'SELECT "c"."name", "o"."id" FROM "s"."customers" AS "c" '
        'JOIN "s"."orders" AS "o" ON "o"."customer_id" = "c"."id" '
        'LEFT JOIN "s"."notes" AS "n" ON "n"."order_id" = "o"."id" '
        'WHERE "n"."id" IS NULL'
    )
    assert _hana(sql) == sql


def test_aggregates_group_by_having() -> None:
    sql = (
        'SELECT "region", COUNT(*), COUNT(DISTINCT "customer_id"), SUM("amount"), MIN("amount"), '
        'MAX("amount") FROM "s"."orders" GROUP BY "region" HAVING COUNT(*) > 2'
    )
    assert _hana(sql) == sql


def test_window_functions() -> None:
    assert _hana(
        'SELECT "a", ROW_NUMBER() OVER (PARTITION BY "b" ORDER BY "c" DESC), '
        'RANK() OVER (ORDER BY "c" DESC), SUM("a") OVER (ORDER BY "c" DESC '
        'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "t"'
    ) == (
        'SELECT "a", ROW_NUMBER() OVER (PARTITION BY "b" ORDER BY "c" DESC NULLS FIRST), '
        'RANK() OVER (ORDER BY "c" DESC NULLS FIRST), '
        'SUM("a") OVER (ORDER BY "c" DESC NULLS FIRST '
        'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "t"'
    )


def test_subqueries_ctes_and_set_operations() -> None:
    assert _hana(
        'WITH "c" AS (SELECT "a" FROM "t") SELECT "a" FROM "c" WHERE "a" IN (SELECT "b" FROM "u") '
        'UNION ALL SELECT "b" FROM "u" WHERE EXISTS (SELECT 1 FROM "v" WHERE "v"."x" = "u"."b") '
        'EXCEPT SELECT "b" FROM "w"'
    ) == (
        'WITH "c" AS (SELECT "a" FROM "t") SELECT "a" FROM "c" WHERE "a" IN (SELECT "b" FROM "u") '
        'UNION ALL SELECT "b" FROM "u" WHERE EXISTS(SELECT 1 FROM "v" WHERE "v"."x" = "u"."b") '
        'EXCEPT SELECT "b" FROM "w"'
    )


def test_dates() -> None:
    assert _hana(
        "SELECT DATE '2026-01-31', TIMESTAMP '2026-01-31 10:00:00', EXTRACT(YEAR FROM \"d\"), "
        'EXTRACT(MONTH FROM "d") FROM "t" WHERE "d" >= DATE \'2026-01-01\''
    ) == (
        "SELECT CAST('2026-01-31' AS DATE), CAST('2026-01-31 10:00:00' AS TIMESTAMP), "
        'EXTRACT(YEAR FROM "d"), EXTRACT(MONTH FROM "d") FROM "t" '
        "WHERE \"d\" >= CAST('2026-01-01' AS DATE)"
    )


def test_casts_and_type_names() -> None:
    assert _hana(
        'SELECT CAST("a" AS TEXT), CAST("b" AS VARCHAR(10)), CAST("b" AS VARCHAR), '
        'CAST("d" AS DOUBLE PRECISION), CAST("e" AS NUMERIC(10, 2)), CAST(COUNT(*) AS INTEGER), '
        'CAST(3 AS BIGINT), CAST("i" AS REAL), CAST(TRUE AS BOOLEAN) FROM "t"'
    ) == (
        'SELECT CAST("a" AS NVARCHAR(5000)), CAST("b" AS NVARCHAR(10)), '
        'CAST("b" AS NVARCHAR(5000)), CAST("d" AS DOUBLE), CAST("e" AS DECIMAL(10, 2)), '
        'CAST(COUNT(*) AS INTEGER), CAST(3 AS BIGINT), CAST("i" AS REAL), '
        'CAST(TRUE AS BOOLEAN) FROM "t"'
    )


def test_strings() -> None:
    assert _hana(
        'SELECT "a" || "b", LOWER("a"), UPPER("a"), TRIM("a"), REPLACE("a", \'x\', \'y\'), '
        'LENGTH("a"), STRPOS("a", \'x\'), '
        'COALESCE("a", "b"), NULLIF("a", "b") FROM "t"'
    ) == (
        'SELECT "a" || "b", LOWER("a"), UPPER("a"), TRIM(BOTH FROM "a"), '
        "REPLACE(\"a\", 'x', 'y'), LENGTH(\"a\"), LOCATE(\"a\", 'x'), "
        'COALESCE("a", "b"), NULLIF("a", "b") FROM "t"'
    )


def test_substring_of_literal_bounds_is_written() -> None:
    """HANA is a column of the polyfill grid (REQ-2236): its substr row keeps the engine's call,
    which the dialect writes where the start is a literal of at least 1 and the length a literal."""
    assert _hana('SELECT SUBSTRING("a" FROM 2 FOR 3) FROM "t"') == (
        'SELECT SUBSTRING("a", 2, 3) FROM "t"'
    )


def test_substr_of_constant_bounds_cast_to_integer_is_written() -> None:
    """substr with a start of at least 1 and a length of at least 0, each a constant cast to
    integer, is the engine's own SUBSTRING (REQ-2236); a start below 1 is refused by name."""
    assert _hana('SELECT pg_catalog."substr"("a", 2::integer, 3::integer) FROM "t"') == (
        'SELECT SUBSTRING("a", CAST(2 AS INTEGER), CAST(3 AS INTEGER)) AS "substr" FROM "t"'
    )
    with pytest.raises(
        FunctionRefused, match="substring with a start that is not a literal of at least"
    ):
        _hana('SELECT pg_catalog."substr"("a", 0::integer) FROM "t"')


def test_like_takes_postgres_escape_and_ilike_folds_case() -> None:
    assert _hana('SELECT "a" FROM "t" WHERE "a" LIKE \'a\\_%\' AND "b" ILIKE \'%x%\'') == (
        'SELECT "a" FROM "t" WHERE "a" LIKE \'a\\_%\' ESCAPE \'\\\' '
        "AND LOWER(\"b\") LIKE LOWER('%x%') ESCAPE '\\'"
    )


def test_modulo() -> None:
    assert _hana('SELECT "a" % 3 FROM "t"') == 'SELECT MOD("a", 3) FROM "t"'


def test_null_safe_comparison_is_never_unknown() -> None:
    assert _hana('SELECT "a" FROM "t" WHERE "a" IS NOT DISTINCT FROM "b"') == (
        'SELECT "a" FROM "t" WHERE (("a" = "b" AND NOT "a" IS NULL AND NOT "b" IS NULL) '
        'OR ("a" IS NULL AND "b" IS NULL))'
    )
    assert _hana('SELECT "a" FROM "t" WHERE "a" IS DISTINCT FROM "b"') == (
        'SELECT "a" FROM "t" WHERE NOT (("a" = "b" AND NOT "a" IS NULL AND NOT "b" IS NULL) '
        'OR ("a" IS NULL AND "b" IS NULL))'
    )


def test_predicate_as_a_value_is_a_case() -> None:
    # HANA takes a predicate only as a condition; unknown stays unknown, as in PostgreSQL.
    assert _hana('SELECT "a" > "b" AS "gt" FROM "t" WHERE "a" > 0') == (
        'SELECT CASE WHEN "a" > "b" THEN TRUE WHEN NOT ("a" > "b") THEN FALSE END AS "gt" '
        'FROM "t" WHERE "a" > 0'
    )


def test_case_condition_stays_a_predicate() -> None:
    assert _hana('SELECT CASE WHEN "a" > 1 AND "b" IS NULL THEN 1 ELSE 0 END FROM "t"') == (
        'SELECT CASE WHEN "a" > 1 AND "b" IS NULL THEN 1 ELSE 0 END FROM "t"'
    )


def test_select_without_from_reads_dummy() -> None:
    assert _hana("SELECT 1") == 'SELECT 1 FROM "DUMMY"'


def test_values_list_is_a_union_of_selects() -> None:
    assert _hana("SELECT * FROM (VALUES (1, 'a', TRUE), (2, 'b', FALSE)) AS v(x, y, z)") == (
        'SELECT * FROM (SELECT 1 AS "x", \'a\' AS "y", TRUE AS "z" FROM "DUMMY" '
        'UNION ALL SELECT 2, \'b\', FALSE FROM "DUMMY") AS "v"'
    )


def test_unnamed_values_columns_take_postgres_names() -> None:
    assert _hana("SELECT * FROM (VALUES (1), (2)) AS v") == (
        'SELECT * FROM (SELECT 1 AS "column1" FROM "DUMMY" UNION ALL SELECT 2 FROM "DUMMY") AS "v"'
    )


def test_every_identifier_is_quoted() -> None:
    # HANA folds an unquoted name to upper case; Provisa's tables are created quoted lower case.
    assert _hana("SELECT a FROM s.t WHERE b = 1") == 'SELECT "a" FROM "s"."t" WHERE "b" = 1'


def test_positional_parameters_are_markers() -> None:
    assert _hana('SELECT "a" FROM "t" WHERE "a" = $1 AND "b" = $2') == (
        'SELECT "a" FROM "t" WHERE "a" = ? AND "b" = ?'
    )


def test_writes() -> None:
    assert _hana('INSERT INTO "s"."t" ("a", "b") VALUES (1, \'x\')') == (
        'INSERT INTO "s"."t" ("a", "b") VALUES (1, \'x\')'
    )
    assert _hana('INSERT INTO "s"."t" ("a", "b") VALUES (1, \'x\'), (2, \'y\')') == (
        'INSERT INTO "s"."t" ("a", "b") SELECT 1, \'x\' FROM "DUMMY" '
        "UNION ALL SELECT 2, 'y' FROM \"DUMMY\""
    )
    assert _hana('UPDATE "s"."t" SET "a" = 2 WHERE "b" = \'x\'') == (
        'UPDATE "s"."t" SET "a" = 2 WHERE "b" = \'x\''
    )
    assert _hana('DELETE FROM "s"."t" WHERE "b" = \'x\'') == (
        'DELETE FROM "s"."t" WHERE "b" = \'x\''
    )


# -- integer division: a truncating form where the operands' integer types are known ----------


def _hana_value(node: exp.Expr) -> Fraction:
    """The value HANA gives the written form, by HANA's definitions: an integer quotient is the
    exact decimal quotient (a bigint's fits HANA's 34 digits), ROUND(x, 0, ROUND_DOWN) rounds
    toward zero, and a cast of an integral value to an integer type is that value."""
    if isinstance(node, exp.Literal):
        return Fraction(int(node.this))
    if isinstance(node, exp.Neg):
        return -_hana_value(node.this)
    if isinstance(node, exp.Paren):
        return _hana_value(node.this)
    if isinstance(node, exp.Div):
        return _hana_value(node.this) / _hana_value(node.expression)
    if isinstance(node, exp.Round):
        assert node.args["decimals"].this == "0" and node.args["truncate"].name == "ROUND_DOWN"
        value = _hana_value(node.this)
        return Fraction(math.trunc(value))
    assert isinstance(node, exp.Cast) and node.to.this in sql_integral.INTEGER_TYPES
    value = _hana_value(node.this)
    assert value.denominator == 1, "the cast is of an integral value"
    return value


@pytest.mark.parametrize(
    ("a", "b", "postgres"),
    [
        # PostgreSQL's integer division truncates toward zero (its docs: 5 / 2 is 2, -5 / 2 -2).
        ("7", "2", 3),
        ("-7", "2", -3),
        ("7", "-2", -3),
        ("-7", "-2", 3),
        ("-1", "2", 0),
        ("1", "-2", 0),
        ("-6", "3", -2),
        ("6", "3", 2),
        ("0", "-5", 0),
        ("9223372036854775807", "-2", -4611686018427387903),
        ("-9223372036854775807", "10", -922337203685477580),
    ],
)
def test_integer_division_gives_postgres_truncated_quotient(a: str, b: str, postgres: int) -> None:
    written = _hana(f"SELECT {a} / {b}")
    (value,) = sqlglot.parse_one(written).selects
    assert "ROUND_DOWN" in written
    assert _hana_value(value) == postgres


def test_integer_division_is_of_the_wider_operands_type() -> None:
    assert _hana(
        "SELECT CAST(7 AS SMALLINT) / CAST(2 AS SMALLINT), 7 / CAST(2 AS SMALLINT), "
        "COUNT(*) / 2, 9223372036854775807 / 2"
    ) == (
        "SELECT CAST(ROUND(CAST(7 AS SMALLINT) / CAST(2 AS SMALLINT), 0, ROUND_DOWN) AS SMALLINT), "
        "CAST(ROUND(7 / CAST(2 AS SMALLINT), 0, ROUND_DOWN) AS INTEGER), "
        "CAST(ROUND(COUNT(*) / 2, 0, ROUND_DOWN) AS BIGINT), "
        'CAST(ROUND(9223372036854775807 / 2, 0, ROUND_DOWN) AS BIGINT) FROM "DUMMY"'
    )


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT "a" / "b" FROM "t"',  # untyped columns
        "SELECT 7.5 / 2",  # a numeric operand
        'SELECT FLOOR("a") / 2 FROM "t"',  # integral, but PostgreSQL's floor is a double
        "SELECT 99999999999999999999 / 2",  # past bigint: PostgreSQL's numeric
    ],
)
def test_division_of_operands_not_known_to_be_integers_is_refused(sql: str) -> None:
    with pytest.raises(FunctionRefused, match="/ of operands not known to be integers"):
        _hana(sql)


# -- refused by name: no HANA form gives PostgreSQL's answer -----------------------------------


@pytest.mark.parametrize(
    ("sql", "named"),
    [
        # HANA's quotient of two integers is a decimal; PostgreSQL's truncates. Untyped columns.
        ('SELECT "a" / "b" FROM "t"', "/ of operands not known to be integers"),
        ('SELECT AVG("a") FROM "t"', "avg"),
        # HANA returns NULL for any NULL argument; PostgreSQL skips it.
        ('SELECT GREATEST("a", "b") FROM "t"', "greatest"),
        ('SELECT CONCAT("a", "b") FROM "t"', "concat"),
        ('SELECT ROUND("a", 2) FROM "t"', "round"),
        # HANA's substring of a start below 1 is not PostgreSQL's (the grid's substr row keeps it).
        ('SELECT SUBSTRING("a" FROM "s" FOR 3) FROM "t"', "substring with a start"),
        # No interval type; the server's clock zone, not the session's.
        ('SELECT "d" + INTERVAL \'1 day\' FROM "t"', "interval"),
        ('SELECT DATE_TRUNC(\'month\', "d") FROM "t"', "timestamp_trunc"),
        ("SELECT CURRENT_DATE", "current_date"),
        ("SELECT NOW()", "current_timestamp"),
        # HANA truncates a fraction; PostgreSQL rounds.
        ('SELECT CAST("a" AS INTEGER) FROM "t"', "integer type"),
        ('SELECT CAST("a" AS BOOLEAN) FROM "t"', "cast to boolean"),
        ('SELECT CAST("a" AS NUMERIC) FROM "t"', "numeric"),
        ('SELECT CAST("a" AS CHAR(3)) FROM "t"', "char"),
        ('SELECT CAST("a" AS VARCHAR(6000)) FROM "t"', "varchar longer than 5000"),
        ('SELECT CAST("a" AS TIME) FROM "t"', "time"),
        ('SELECT CAST("a" AS TIMESTAMPTZ) FROM "t"', "timestamptz"),
        ('SELECT CAST("a" AS UUID) FROM "t"', "uuid"),
        ('SELECT "a" FROM "t" WHERE "a" ~ \'x\'', "regexp_like"),
        ('SELECT DISTINCT ON ("a") "a" FROM "t"', "distinct on"),
        ('SELECT EXTRACT(SECOND FROM "d") FROM "t"', "extract of second"),
        ('SELECT EXTRACT(DOW FROM "d") FROM "t"', "extract of dow"),
        ('SELECT "a" FROM "t" WHERE "f" IS TRUE', "is true"),
        ('SELECT "a" FROM "t" INTERSECT ALL SELECT "a" FROM "u"', "intersect all"),
        ('SELECT "a" FROM "t" EXCEPT ALL SELECT "a" FROM "u"', "except all"),
        (
            'SELECT SUM("a") OVER (ORDER BY "b" RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) '
            'FROM "t"',
            "range frame",
        ),
        ('SELECT "a" FROM "t" ORDER BY "a" FETCH FIRST 3 ROWS WITH TIES', "with ties"),
        ('SELECT "a" FROM "t" OFFSET 3', "offset without limit"),
        ('UPDATE "t" SET "a" = 1 FROM "u" WHERE "t"."b" = "u"."b"', "update from"),
        ('DELETE FROM "t" USING "u" WHERE "t"."b" = "u"."b"', "delete using"),
        ('WITH RECURSIVE "r" AS (SELECT 1) SELECT * FROM "r"', "with recursive"),
        ('INSERT INTO "t" ("a") VALUES (1) ON CONFLICT DO NOTHING', "onconflict"),
        ('SELECT "a" FROM "t" WHERE "a" = $2 AND "b" = $1', "in order"),
        # The null-safe form names each operand twice, so a parameter in it is bound twice.
        ('SELECT "a" FROM "t" WHERE "a" IS NOT DISTINCT FROM $1', "in order"),
        ('SELECT "a" FROM "t" LIMIT $1', "LIMIT or OFFSET"),
    ],
)
def test_refused_by_name(sql: str, named: str) -> None:
    with pytest.raises(FunctionRefused, match=named) as refused:
        _hana(sql)
    assert refused.value.params["dialect"] == "saphana"


def test_refusal_reports_the_function_unsupported_code() -> None:
    with pytest.raises(FunctionRefused) as refused:
        _hana('SELECT GREATEST("a", "b") FROM "t"')
    assert refused.value.code == "query.function_unsupported"
    assert str(refused.value) == "greatest has no form on SAP HANA that gives PostgreSQL's answer"
