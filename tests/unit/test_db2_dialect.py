# Copyright (c) 2026 Kenneth Stott
# Canary: be6fad58-2040-4c5c-b899-a63f925a86db
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``db2`` dialect (REQ-2236): a governed statement written in Db2's SQL where each construct
has a form there giving PostgreSQL's answer, and refused by name where one has none."""

# Requirements: REQ-2236, REQ-2167, REQ-1922

from __future__ import annotations

import pytest
from sqlglot import parse_one
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.dialects.dialect import Dialect

from provisa.core.source_registry import engine_dialect
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile


def _db2(sql: str) -> str:
    return transpile(sql, "db2", generated=False)


def test_db2_is_a_registered_sqlglot_dialect() -> None:
    assert engine_dialect("db2") == "db2"
    assert engine_dialect("ibm_db_sa") == "db2"
    assert "db2" in Dialect.classes


# -- the forms a compiled statement takes ------------------------------------------------------


def test_filter_and_paging() -> None:
    sql = (
        'SELECT "o"."id", "o"."status" FROM "sales"."orders" AS "o" '
        'WHERE "o"."amount" > 10 AND "o"."status" <> \'void\' AND "o"."flag" = TRUE '
        'ORDER BY "o"."id" LIMIT 20 OFFSET 40'
    )
    assert _db2(sql) == (
        'SELECT "o"."id", "o"."status" FROM "sales"."orders" AS "o" '
        'WHERE "o"."amount" > 10 AND "o"."status" <> \'void\' AND "o"."flag" = TRUE '
        'ORDER BY "o"."id" OFFSET 40 ROWS FETCH FIRST 20 ROWS ONLY'
    )


def test_limit_alone_is_fetch_first() -> None:
    assert _db2('SELECT "a" FROM "t" ORDER BY "a" DESC NULLS LAST LIMIT 3') == (
        'SELECT "a" FROM "t" ORDER BY "a" DESC NULLS LAST FETCH FIRST 3 ROWS ONLY'
    )


def test_ascending_order_adds_no_null_clause() -> None:
    # Db2 sorts NULL after every value ascending, as PostgreSQL does.
    assert _db2('SELECT "a" FROM "t" ORDER BY "a"') == 'SELECT "a" FROM "t" ORDER BY "a"'


def test_joins() -> None:
    sql = (
        'SELECT "c"."name", "o"."id" FROM "s"."customers" AS "c" '
        'JOIN "s"."orders" AS "o" ON "o"."customer_id" = "c"."id" '
        'LEFT JOIN "s"."notes" AS "n" ON "n"."order_id" = "o"."id" '
        'WHERE "n"."id" IS NULL'
    )
    assert _db2(sql) == sql


def test_aggregates_group_by_having() -> None:
    sql = (
        'SELECT "region", COUNT(*), COUNT(DISTINCT "customer_id"), SUM("amount"), MIN("amount"), '
        'MAX("amount") FROM "s"."orders" GROUP BY "region" HAVING COUNT(*) > 2'
    )
    assert _db2(sql) == sql


def test_window_functions() -> None:
    assert _db2(
        'SELECT "a", ROW_NUMBER() OVER (PARTITION BY "b" ORDER BY "c" DESC), '
        'RANK() OVER (ORDER BY "c"), DENSE_RANK() OVER (ORDER BY "c") FROM "t"'
    ) == (
        'SELECT "a", ROW_NUMBER() OVER (PARTITION BY "b" ORDER BY "c" DESC), '
        'RANK() OVER (ORDER BY "c"), DENSE_RANK() OVER (ORDER BY "c") FROM "t"'
    )


def test_subqueries_ctes_and_set_operations() -> None:
    assert _db2(
        'WITH "c" AS (SELECT "a" FROM "t") SELECT "a" FROM "c" WHERE "a" IN (SELECT "b" FROM "u") '
        'UNION ALL SELECT "b" FROM "u" WHERE EXISTS (SELECT 1 FROM "v" WHERE "v"."x" = "u"."b")'
    ) == (
        'WITH "c" AS (SELECT "a" FROM "t") SELECT "a" FROM "c" WHERE "a" IN (SELECT "b" FROM "u") '
        'UNION ALL SELECT "b" FROM "u" WHERE EXISTS(SELECT 1 FROM "v" WHERE "v"."x" = "u"."b")'
    )


def test_dates() -> None:
    assert _db2(
        "SELECT DATE '2026-01-31', TIMESTAMP '2026-01-31 10:00:00', EXTRACT(YEAR FROM \"d\"), "
        'EXTRACT(MONTH FROM "d") FROM "t" WHERE "d" >= DATE \'2026-01-01\''
    ) == (
        "SELECT CAST('2026-01-31' AS DATE), CAST('2026-01-31 10:00:00' AS TIMESTAMP(6)), "
        'EXTRACT(YEAR FROM "d"), EXTRACT(MONTH FROM "d") FROM "t" '
        "WHERE \"d\" >= CAST('2026-01-01' AS DATE)"
    )


def test_casts_and_type_names() -> None:
    assert _db2(
        'SELECT CAST("a" AS TEXT), CAST("b" AS VARCHAR(10)), CAST("c" AS BOOLEAN), '
        'CAST("d" AS DOUBLE PRECISION), CAST("e" AS NUMERIC(10, 2)), CAST(1 AS BIGINT), '
        'CAST(2 AS INTEGER), CAST(3 AS SMALLINT), CAST("i" AS REAL), CAST("j" AS CHAR(3)) '
        'FROM "t"'
    ) == (
        'SELECT CAST("a" AS VARCHAR(8168 CODEUNITS32)), CAST("b" AS VARCHAR(10 CODEUNITS32)), '
        'CAST("c" AS BOOLEAN), CAST("d" AS DOUBLE), CAST("e" AS DECIMAL(10, 2)), '
        'CAST(1 AS BIGINT), CAST(2 AS INTEGER), CAST(3 AS SMALLINT), CAST("i" AS REAL), '
        'CAST("j" AS CHAR(3 CODEUNITS32)) FROM "t"'
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT CAST(2.5 AS INTEGER)",
        'SELECT CAST("a" AS SMALLINT) FROM "t"',
        'SELECT CAST("a" AS INTEGER) FROM "t"',
        'SELECT CAST("a" AS BIGINT) FROM "t"',
        'SELECT CAST("a" * 2 AS INTEGER) FROM "t"',
        "SELECT CAST(1.5 + 1 AS BIGINT)",
    ],
)
def test_integer_cast_of_a_value_that_may_have_a_fraction_is_refused(sql: str) -> None:
    # Db2 truncates (CAST(2.5 AS INTEGER) is 2); PostgreSQL rounds (3).
    with pytest.raises(FunctionRefused, match="integer type") as refused:
        _db2(sql)
    assert refused.value.code == "query.function_unsupported"
    assert refused.value.params["dialect"] == "db2"


def test_integer_cast_of_an_integral_value_is_written() -> None:
    assert _db2(
        "SELECT CAST(3 AS INTEGER), CAST(-3 AS SMALLINT), CAST(COUNT(*) AS INTEGER), "
        'CAST(COUNT("a") + 1 AS BIGINT), CAST((2 * 3) % 4 AS BIGINT), '
        'CAST(FLOOR("b") AS INTEGER) FROM "t" GROUP BY "a", "b"'
    ) == (
        "SELECT CAST(3 AS INTEGER), CAST(-3 AS SMALLINT), CAST(COUNT(*) AS INTEGER), "
        'CAST(COUNT("a") + 1 AS BIGINT), CAST(MOD((2 * 3), 4) AS BIGINT), '
        'CAST(FLOOR("b") AS INTEGER) FROM "t" GROUP BY "a", "b"'
    )


def test_integer_cast_of_an_integer_column_is_written() -> None:
    # An integer column is integral where the caller supplied the column types.
    tree = annotate_types(
        parse_one('SELECT CAST("t"."a" AS SMALLINT) FROM "t"', read="postgres"),
        schema={"t": {"a": "INT"}},
    )
    assert tree.sql(dialect="db2") == 'SELECT CAST("t"."a" AS SMALLINT) FROM "t"'


def test_strings_count_characters() -> None:
    assert _db2(
        'SELECT "a" || "b", LOWER("a"), UPPER("a"), TRIM("a"), REPLACE("a", \'x\', \'y\'), '
        'COALESCE("a", "b"), NULLIF("a", "b") FROM "t"'
    ) == (
        'SELECT "a" || "b", LOWER("a"), UPPER("a"), TRIM(BOTH FROM "a"), '
        'REPLACE("a", \'x\', \'y\'), COALESCE("a", "b"), NULLIF("a", "b") FROM "t"'
    )


def test_like_takes_postgres_escape_and_ilike_folds_case() -> None:
    assert _db2('SELECT "a" FROM "t" WHERE "a" LIKE \'a\\_%\' AND "b" ILIKE \'%x%\'') == (
        'SELECT "a" FROM "t" WHERE "a" LIKE \'a\\_%\' ESCAPE \'\\\' '
        "AND LOWER(\"b\") LIKE LOWER('%x%') ESCAPE '\\'"
    )


def test_modulo_and_null_safe_comparison() -> None:
    assert _db2('SELECT "a" % 3 FROM "t" WHERE "a" IS NOT DISTINCT FROM "b"') == (
        'SELECT MOD("a", 3) FROM "t" WHERE "a" IS NOT DISTINCT FROM "b"'
    )


def test_select_without_from_reads_the_dummy_table() -> None:
    assert _db2("SELECT 1") == 'SELECT 1 FROM "SYSIBM"."SYSDUMMY1"'


def test_values_and_booleans() -> None:
    assert _db2("SELECT * FROM (VALUES (1, 'a', TRUE), (2, 'b', FALSE)) AS v(x, y, z)") == (
        'SELECT * FROM (VALUES (1, \'a\', TRUE), (2, \'b\', FALSE)) AS "v"("x", "y", "z")'
    )


def test_every_identifier_is_quoted() -> None:
    # Db2 folds an unquoted name to upper case; Provisa's tables are created quoted lower case.
    assert _db2("SELECT a FROM s.t WHERE b = 1") == 'SELECT "a" FROM "s"."t" WHERE "b" = 1'


def test_positional_parameters_are_markers() -> None:
    assert _db2('SELECT "a" FROM "t" WHERE "a" = $1 AND "b" = $2') == (
        'SELECT "a" FROM "t" WHERE "a" = ? AND "b" = ?'
    )


def test_writes() -> None:
    assert _db2('INSERT INTO "s"."t" ("a", "b") VALUES (1, \'x\')') == (
        'INSERT INTO "s"."t" ("a", "b") VALUES (1, \'x\')'
    )
    assert _db2('UPDATE "s"."t" SET "a" = 2 WHERE "b" = \'x\'') == (
        'UPDATE "s"."t" SET "a" = 2 WHERE "b" = \'x\''
    )
    assert _db2('DELETE FROM "s"."t" WHERE "b" = \'x\'') == (
        'DELETE FROM "s"."t" WHERE "b" = \'x\''
    )


# -- refused by name: no Db2 form gives PostgreSQL's answer ------------------------------------


@pytest.mark.parametrize(
    ("sql", "named"),
    [
        # Db2's AVG of an integer column is an integer; PostgreSQL's is numeric.
        ('SELECT AVG("a") FROM "t"', "avg"),
        # Db2 returns NULL for any NULL argument; PostgreSQL skips it.
        ('SELECT GREATEST("a", "b") FROM "t"', "greatest"),
        ('SELECT CONCAT("a", "b") FROM "t"', "concat"),
        # No interval type; a labeled duration keeps a date a date.
        ('SELECT "d" + INTERVAL \'1 day\' FROM "t"', "interval"),
        ('SELECT DATE_TRUNC(\'month\', "d") FROM "t"', "timestamp_trunc"),
        # The server's clock zone, not the session's.
        ("SELECT CURRENT_DATE", "current_date"),
        ("SELECT NOW()", "current_timestamp"),
        # Db2's default DECIMAL is (5,0); its TIME has no fractions; it has no timestamptz.
        ('SELECT CAST("a" AS NUMERIC) FROM "t"', "numeric"),
        ('SELECT CAST("a" AS TIME) FROM "t"', "time"),
        ('SELECT CAST("a" AS TIMESTAMPTZ) FROM "t"', "timestamptz"),
        ('SELECT CAST("a" AS UUID) FROM "t"', "uuid"),
        # ICU's regular expressions, not PostgreSQL's.
        ('SELECT "a" FROM "t" WHERE "a" ~ \'x\'', "regexp_like"),
        ('SELECT DISTINCT ON ("a") "a" FROM "t"', "distinct on"),
        ('SELECT EXTRACT(DOW FROM "d") FROM "t"', "extract of dow"),
        ('SELECT "a" FROM "t" WHERE "a" = $2 AND "b" = $1', "in order"),
        ('SELECT "a" FROM "t" LIMIT $1', "LIMIT or OFFSET"),
    ],
)
def test_refused_by_name(sql: str, named: str) -> None:
    with pytest.raises(FunctionRefused, match=named) as refused:
        _db2(sql)
    assert refused.value.params["dialect"] == "db2"


def test_refusal_reports_the_function_unsupported_code() -> None:
    with pytest.raises(FunctionRefused) as refused:
        _db2('SELECT GREATEST("a", "b") FROM "t"')
    assert refused.value.code == "query.function_unsupported"
    assert str(refused.value) == "greatest has no form on Db2 that gives PostgreSQL's answer"
