# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1f7a52-9e04-4b6d-8a21-f5d0c7e94b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wrong answers on ClickHouse J1309 found and left (REQ-2236; REQ-2167, AN ANSWER WITHIN A
STATED BOUND COMES BEFORE A REFUSAL), each closed by PostgreSQL's answer, its error outside a
stated bound, or a refusal by name:

1. LIKE ... ESCAPE: ClickHouse's writer dropped the ESCAPE clause, and its backslash escapes only
   %, _ and itself;
2. a text cast to a timestamp or a timestamptz outside what DateTime64 holds, which ClickHouse
   holds at its type's end; a timestamp's offset, which ClickHouse's cast reads;
3. a range's bound a row computes outside what DateTime64 and Date32 hold;
4. a bytea LIKE whose value or pattern a row here did not write, read by ClickHouse's UTF-8 LIKE;
5. bytea's ILIKE, which PostgreSQL does not have (42883);
6. the native TCP backend's read of an engine's message holding bytes that are not UTF-8.

Each answer is PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

from typing import Any

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill import grid, instants, ranges
from provisa.transpiler.transpile import transpile


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _rows(runtime: ClickHouseFederationRuntime, sql: str) -> list[tuple]:
    return list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())


def _one(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    ((value,),) = _rows(runtime, sql)
    return value


def _raised(runtime: ClickHouseFederationRuntime, sql: str) -> PostgresError:
    statement = transpile(sql, "clickhouse")
    with pytest.raises(Exception) as raised:
        list(runtime.run_sync(statement).iter_rows())
    error = from_engine(raised.value)
    assert error is not None, f"{type(raised.value).__name__}: {str(raised.value)[:300]}"
    return error


def _refused(sql: str) -> FunctionRefused:
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    return refused.value


# --- 1. LIKE ... ESCAPE ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ("SELECT 'a_' LIKE 'a#_' ESCAPE '#' AS v", True),
        ("SELECT 'ab' LIKE 'a#_' ESCAPE '#' AS v", False),
        ("SELECT 'a_' NOT LIKE 'a#_' ESCAPE '#' AS v", False),
        ("SELECT 'A_' ILIKE 'a#_' ESCAPE '#' AS v", True),
        ("SELECT 'Ab' ILIKE 'a#_' ESCAPE '#' AS v", False),
        ("SELECT 'a%' LIKE 'a!%' ESCAPE '!' AS v", True),
        ("SELECT 'ab' LIKE 'a!%' ESCAPE '!' AS v", False),
        # The escape written twice is itself; a backslash is no escape beside another.
        ("SELECT 'a#' LIKE 'a##' ESCAPE '#' AS v", True),
        ("SELECT 'a\\' LIKE 'a\\' ESCAPE '#' AS v", True),
        # An empty escape: no character escapes.
        ("SELECT 'a\\b' LIKE 'a\\b' ESCAPE '' AS v", True),
        ("SELECT 'a_' LIKE 'a\\_' ESCAPE '' AS v", False),
        # A backslash escape: PostgreSQL's own.
        ("SELECT 'a_' LIKE 'a\\_' ESCAPE '\\' AS v", True),
        ("SELECT c LIKE 'a#_' ESCAPE '#' AS v FROM (VALUES ('a_'), ('ab')) AS t(c) ORDER BY c",
         True),
        # PostgreSQL's backslash makes any character itself; ClickHouse's escapes only %, _, \.
        ("SELECT 'ab' LIKE 'a\\b' AS v", True),
        ("SELECT 'a\\b' LIKE 'a\\\\b' AS v", True),
        # bytea: like_escape's bytes, matched byte by byte.
        ("SELECT 'a_'::bytea LIKE 'a#_'::bytea ESCAPE '#' AS v", True),
        ("SELECT 'ab'::bytea LIKE 'a#_' ESCAPE '#' AS v", False),
        ("SELECT '\\xc3a95f'::bytea LIKE '__#_' ESCAPE '#' AS v", True),
        ("SELECT 'a_'::bytea NOT LIKE 'a#_'::bytea ESCAPE '#' AS v", False),
    ],
)  # fmt: skip
def test_like_escape_is_postgresql_s(runtime, sql, answer):
    value = _rows(runtime, sql)[0][0]
    assert value is not None and bool(value) is answer


@pytest.mark.parametrize(
    "sql",
    ["SELECT 'a' LIKE 'a' ESCAPE NULL AS v", "SELECT 'a' LIKE NULL ESCAPE '#' AS v"],
)
def test_like_escape_of_a_null_is_null(runtime, sql):
    assert _one(runtime, sql) is None


def test_an_escape_of_more_than_one_character_is_postgresql_s_error():
    with pytest.raises(RowRefused) as refused:
        transpile("SELECT 'a' LIKE 'a' ESCAPE '##' AS v", "clickhouse")
    assert (refused.value.sqlstate, str(refused.value)) == ("22025", "invalid escape string")


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        # A computed pattern or escape: no form reads it.
        ("SELECT 'a' LIKE c ESCAPE '#' AS v FROM (VALUES ('a_')) AS t(c)", "like_escape"),
        # PostgreSQL raises 22025 only where a match reaches the escape at the end.
        ("SELECT 'a' LIKE 'a#' ESCAPE '#' AS v", "like"),
        ("SELECT 'x' SIMILAR TO 'a#_' ESCAPE '#' AS v", "similar_to_escape"),
    ],
)
def test_like_escape_with_no_exact_form_is_refused_by_name(sql, function):
    assert str(_refused(sql)) == f"{function}() has no form on clickhouse"


@pytest.mark.parametrize("dialect", [d for d in grid.DIALECTS if d != "clickhouse"])
def test_every_other_dialect_s_writer_keeps_escape(dialect):
    written = transpile("SELECT c LIKE 'a#_' ESCAPE '#' AS v FROM t", dialect)
    assert "ESCAPE '#'" in written


# --- 2. a text cast to a timestamp outside the bound -----------------------------------------

_OUTSIDE_INSTANT = instants.out_of_bound("timestamptz_in")
_OUTSIDE_STAMP = instants.out_of_bound("timestamp_in", instants.STAMP_BOUND)


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("SELECT '2300-01-01+00'::timestamptz AS v", _OUTSIDE_INSTANT),
        ("SELECT date_trunc('day', '2300-01-01+00'::timestamptz) AS v", _OUTSIDE_INSTANT),
        ("SELECT '2300-01-01'::timestamptz AS v", _OUTSIDE_INSTANT),
        ("SELECT timestamptz '2300-01-01 00:00:00+00' AS v", _OUTSIDE_INSTANT),
        ("SELECT '2299-12-31 23:00:00-05'::timestamptz AS v", _OUTSIDE_INSTANT),
        ("SELECT '1900-01-01 01:00:00+02'::timestamptz AS v", _OUTSIDE_INSTANT),
        ("SELECT 'infinity'::timestamptz AS v", _OUTSIDE_INSTANT),
        ("SELECT '2300-01-01'::timestamp AS v", _OUTSIDE_STAMP),
        ("SELECT '1800-01-01'::timestamp AS v", _OUTSIDE_STAMP),
        ("SELECT TIMESTAMP '2300-01-01' AS v", _OUTSIDE_STAMP),
        ("SELECT '2300-01-01'::timestamp(3) AS v", _OUTSIDE_STAMP),
        ("SELECT '10000-01-01'::timestamp AS v", _OUTSIDE_STAMP),
        ("SELECT '2000-01-01 BC'::timestamp AS v", _OUTSIDE_STAMP),
        ("SELECT '-infinity'::timestamp AS v", _OUTSIDE_STAMP),
        ("SELECT date_trunc('day', '2300-01-01'::timestamp) AS v", _OUTSIDE_STAMP),
        ("SELECT c FROM (VALUES ('2300-01-01'::timestamp)) AS t(c)", _OUTSIDE_STAMP),
    ],
)
def test_a_timestamp_s_text_outside_the_bound_raises(runtime, sql, message):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == ("0A000", message)


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ("SELECT '2299-12-31 23:59:59.999999'::timestamp AS v", "2299-12-31 23:59:59.999999"),
        ("SELECT '1900-01-01'::timestamptz AS v", "1900-01-01 00:00:00"),
        ("SELECT '2000-01-01+05'::timestamptz AS v", "1999-12-31 19:00:00"),
        # A timestamp's input drops its offset.
        ("SELECT '2299-12-31 23:00:00-05'::timestamp AS v", "2299-12-31 23:00:00"),
        ("SELECT '2000-01-01+05'::timestamp AS v", "2000-01-01 00:00:00"),
        ("SELECT '2000-01-01 10:00 UTC'::timestamp AS v", "2000-01-01 10:00:00"),
    ],
)
def test_a_timestamp_s_text_inside_the_bound_answers(runtime, sql, answer):
    assert _one(runtime, sql).strftime("%Y-%m-%d %H:%M:%S.%f").removesuffix(".000000") == answer


def test_isfinite_of_a_literal_outside_the_bound_is_still_read_as_there():
    # isfinite reads only that a value is there: J1305's row, unchanged.
    sql = "SELECT pg_catalog.\"isfinite\"('2300-01-01 00:00:00+00'::timestamp with time zone) AS v"
    assert "throwIf" not in transpile(sql, "clickhouse")


# --- 4. a bytea LIKE of a value no row wrote -------------------------------------------------

_C3A9 = "(VALUES ('\\xc3a9'::bytea)) AS t(c)"


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (f"SELECT substring(c, 1) LIKE '_'::bytea AS v FROM {_C3A9}", False),
        (f"SELECT (c || '\\x'::bytea) LIKE '_'::bytea AS v FROM {_C3A9}", False),
        (f"SELECT (c || '\\x'::bytea) LIKE '__'::bytea AS v FROM {_C3A9}", True),
        (f"SELECT (c || '\\x'::bytea) NOT LIKE '__'::bytea AS v FROM {_C3A9}", False),
        # An untyped pattern beside a bytea is a bytea's input.
        ("SELECT '\\xc3a9'::bytea LIKE '_' AS v", False),
    ],
)
def test_a_bytea_like_of_a_computed_value_matches_byte_by_byte(runtime, sql, answer):
    value = _one(runtime, sql)
    assert value is not None and value == answer


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT '\\xc3a9'::bytea LIKE substring('_'::bytea, 1) AS v",
        # The escape form, which ClickHouse holds as its text.
        "SELECT '\\x61'::bytea LIKE 'a\\\\'::bytea AS v",
        # A pattern that ends in its escape: PostgreSQL raises only where a match reaches it.
        "SELECT '\\x61'::bytea LIKE '\\x615c'::bytea AS v",
    ],
)
def test_a_bytea_like_with_no_exact_form_is_refused_by_name(sql):
    assert str(_refused(sql)) == "like() has no form on clickhouse"


def test_text_like_bytea_is_postgresql_s_error():
    with pytest.raises(RowRefused) as refused:
        transpile("SELECT 'abc'::text LIKE '_'::bytea AS v", "clickhouse")
    assert (refused.value.sqlstate, str(refused.value)) == (
        "42883",
        "operator does not exist: text ~~ bytea",
    )


# --- 5. bytea ILIKE --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("SELECT '\\x41'::bytea ILIKE 'a'::bytea AS v", "operator does not exist: bytea ~~* bytea"),
        ("SELECT '\\x41'::bytea ILIKE 'a' AS v", "operator does not exist: bytea ~~* unknown"),
        ("SELECT '\\x41'::bytea NOT ILIKE 'a' AS v", "operator does not exist: bytea !~~* unknown"),
        ("SELECT 'a' ~~* '\\x41'::bytea AS v", "operator does not exist: unknown ~~* bytea"),
        ("SELECT c ILIKE 'a' AS v FROM (VALUES ('\\x41'::bytea)) AS t(c)",
         "operator does not exist: bytea ~~* unknown"),
    ],
)  # fmt: skip
def test_bytea_ilike_is_postgresql_s_error(sql, message):
    with pytest.raises(RowRefused) as refused:
        transpile(sql, "clickhouse")
    assert (refused.value.sqlstate, str(refused.value)) == ("42883", message)


def test_ranges_bound_text_is_shared():
    assert ranges.HELD_ON_CLICKHOUSE["timestamp"][2] == instants.STAMP_BOUND
