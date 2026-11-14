# Copyright (c) 2026 Kenneth Stott
# Canary: 8e468137-643a-48e7-b920-6724aa8f32c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Four holes on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES BEFORE A
REFUSAL), each closed by PostgreSQL's answer, its error outside a stated bound, or a refusal by
name:

1. a raise in a statement holding bytes that are not UTF-8: ClickHouse's message echoes them;
2. a range's literal bound outside what DateTime64 and Date32 hold, which ClickHouse clamps;
3. to_char(timestamp, text), whose template sqlglot rewrites as strftime text (now written
   from PostgreSQL's tokens);
4. bytea's LIKE operator in a statement that names no function.

Each answer is PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

from typing import Any

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.polyfill import instants, ranges
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
    with pytest.raises(Exception) as raised:
        _rows(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None, f"{type(raised.value).__name__}: {str(raised.value)[:300]}"
    return error


# --- 1. a raise beside bytes that are not UTF-8 ------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "sqlstate", "message"),
    [
        ("SELECT pg_catalog.\"overlay\"('\\xff01'::bytea, '\\x03'::bytea, 0) AS v",
         "22011", "negative substring length not allowed"),
        ("SELECT pg_catalog.\"get_byte\"('\\xfffe'::bytea, 2) AS v",
         "2202E", "index out of valid range"),
        ("SELECT pg_catalog.\"substr\"('\\xc3'::bytea, 1, -1) AS v",
         "22011", "negative substring length not allowed"),
    ],
)  # fmt: skip
def test_a_raise_beside_bytes_that_are_not_utf8_is_postgresql_s_error(
    runtime, sql, sqlstate, message
):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == (sqlstate, message)


# --- 2. a range's literal bound outside what ClickHouse holds ----------------------------------

_STAMPS = "(VALUES ('2000-01-01 00:00:00'::timestamp)) AS t(c)"
_INSTANTS = "(VALUES ('2000-01-01 00:00:00+00'::timestamp with time zone)) AS t(c)"
_DAYS = "(VALUES ('2000-01-01'::date)) AS t(c)"


@pytest.mark.parametrize(
    ("sql", "constructor"),
    [
        (f"SELECT tstzrange(c, '2300-01-01 00:00:00+00') AS v FROM {_INSTANTS}", "tstzrange"),
        (f"SELECT tstzrange(c, '2299-12-31 23:00:00-05') AS v FROM {_INSTANTS}", "tstzrange"),
        (f"SELECT tsrange(c, '2300-01-01', '[]') AS v FROM {_STAMPS}", "tsrange"),
        (f"SELECT tsrange(c, '2300-01-01') AS v FROM {_STAMPS}", "tsrange"),
        (f"SELECT tsrange('1800-01-01', c, '[]') AS v FROM {_STAMPS}", "tsrange"),
        (f"SELECT tsrange(c, 'infinity') AS v FROM {_STAMPS}", "tsrange"),
        (f"SELECT daterange(c, '2300-01-01', '[]') AS v FROM {_DAYS}", "daterange"),
        (f"SELECT daterange(c, '2300-01-01') AS v FROM {_DAYS}", "daterange"),
        # The day after it, which an inclusive upper bound is made into, is past Date32's end.
        (f"SELECT daterange(c, '2299-12-31', '[]') AS v FROM {_DAYS}", "daterange"),
        # An element beside a range is read as a bound is.
        (f"SELECT tsrange(c, '2001-01-01') @> '2300-01-01 12:00:00'::timestamp AS v FROM {_STAMPS}",
         "tsrange"),
    ],
)  # fmt: skip
def test_a_literal_bound_past_what_clickhouse_holds_raises(runtime, sql, constructor):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == ("0A000", ranges.outside_held(constructor))


def test_tstzrange_s_bound_is_the_one_its_other_rows_raise_outside():
    assert ranges.outside_held("tstzrange") == instants.out_of_bound("tstzrange")


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (f"SELECT tsrange(c, '2299-12-31 23:59:59.999999') AS v FROM {_STAMPS}",
         '["2000-01-01 00:00:00","2299-12-31 23:59:59.999999")'),
        (f"SELECT tsrange('1900-01-01', c, '[]') AS v FROM {_STAMPS}",
         '["1900-01-01 00:00:00","2000-01-01 00:00:00"]'),
        (f"SELECT tstzrange(c, '2299-12-31 23:59:59.999999+00') AS v FROM {_INSTANTS}",
         '["2000-01-01 00:00:00+00","2299-12-31 23:59:59.999999+00")'),
        (f"SELECT daterange(c, '2299-12-30', '[]') AS v FROM {_DAYS}", "[2000-01-01,2299-12-31)"),
        (f"SELECT daterange('1900-01-01', c) AS v FROM {_DAYS}", "[1900-01-01,2000-01-01)"),
        # Every bound a literal: read here, where nothing clamps it.
        ("SELECT tsrange('2000-01-01', '2300-01-01', '[]') AS v",
         '["2000-01-01 00:00:00","2300-01-01 00:00:00"]'),
    ],
)  # fmt: skip
def test_a_literal_bound_inside_what_clickhouse_holds_answers(runtime, sql, answer):
    assert _one(runtime, sql) == answer


# --- 3. to_char(timestamp, text) ---------------------------------------------------------------

_STAMP = "'2024-06-15 12:30:45'::timestamp"


@pytest.mark.parametrize(
    ("template", "answer"),
    [
        ("YYYY-MM-DD", "2024-06-15"),
        ("yyyy/mm/dd", "2024/06/15"),
        ("YYYY-MM-DD HH24", "2024-06-15 12"),
    ],
)
def test_to_char_of_a_template_postgresql_reads_as_strftime_s_answers(runtime, template, answer):
    assert _one(runtime, f"SELECT to_char({_STAMP}, '{template}') AS v") == answer


@pytest.mark.parametrize(
    ("template", "answer"),
    [
        ("%Y", "%4"),  # PostgreSQL: '%' and Y (the year's last digit)
        ("%Y-%m-%d", "%4-%m-%7"),
        ("SSSS", "45045"),  # PostgreSQL: the seconds past midnight
        ("DDTHH24", "15THH24"),  # PostgreSQL: DD with its TH suffix, then 'H24'
        ("DDDD", "1677"),  # PostgreSQL: the day of the year and of the week
        ("FMDD", "15"),
        ('"YYYY"', "YYYY"),
    ],
)
def test_to_char_of_a_template_strftime_would_misread_is_written_from_its_tokens(
    runtime, template, answer
):
    # Each keyword is written as PostgreSQL's text (provisa.transpiler.to_char), on Trino
    # too (tests/unit/polyfill/test_polyfill_to_char.py), where these were refused by name.
    assert _one(runtime, f"SELECT to_char({_STAMP}, '{template}') AS v") == answer
    assert "DATE_FORMAT" not in transpile(f"SELECT to_char({_STAMP}, '{template}') AS v", "trino")


# --- 4. bytea's LIKE in a statement that names no function -------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        # One character of UTF-8 is two bytes: '_' matches one byte.
        ("SELECT '\\xc3a9'::bytea LIKE '_'::bytea AS v", False),
        ("SELECT '\\xc3a9'::bytea LIKE '__'::bytea AS v", True),
        ("SELECT '\\xe282ac'::bytea LIKE '_'::bytea AS v", False),
        # A byte no UTF-8 starts with is one byte all the same.
        ("SELECT '\\xff'::bytea LIKE '_'::bytea AS v", True),
        ("SELECT '\\xff41'::bytea LIKE '%A'::bytea AS v", True),
        ("SELECT '\\xc3a9'::bytea NOT LIKE '_'::bytea AS v", True),
        ("SELECT '\\xc3a9'::bytea ~~ '_'::bytea AS v", False),
        ("SELECT '\\xc3a9'::bytea !~~ '_'::bytea AS v", True),
    ],
)
def test_bytea_like_matches_byte_by_byte_where_no_function_is_named(runtime, sql, answer):
    value = _one(runtime, sql)
    assert value is not None and value == answer  # ClickHouse's own LIKE answers 0 or 1


def test_bytea_not_like_beside_a_function_is_notlike(runtime):
    # sqlglot reads NOT LIKE as LIKE marked negated: the like row had answered it as LIKE.
    sql = "SELECT '\\xc3a9'::bytea NOT LIKE '_'::bytea AS v, octet_length('\\x01'::bytea) AS w"
    assert _rows(runtime, sql) == [(True, 1)]
