# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b2d17-c3a6-4f91-9b05-7a2e6d1c8f33
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of array containment, array_remove and array_position from a start,
pg_size_pretty of a bigint, pg_encoding_max_length, pg_typeof, bit of an integer and max and min of
tids (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES BEFORE A REFUSAL): each answered on
the embedded engine (chdb), against PostgreSQL's answer (the embedded PostgreSQL the unit tests boot,
PostgreSQL 16.11), on the function support measure's own calls of each overload and its edges."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [  # (statement, PostgreSQL's text of its one row)
    ('SELECT pg_catalog."arraycontains"(NULL::integer[], NULL::integer[]) AS v', None),
    ("SELECT pg_catalog.\"arraycontains\"('{1,2,3}'::integer[], '{1,2,3}'::integer[]) AS v", "t"),
    ("SELECT pg_catalog.\"arraycontains\"('{}'::integer[], '{}'::integer[]) AS v", "t"),
    (
        "SELECT pg_catalog.\"arraycontains\"('{-7,NULL,42}'::integer[], '{-7,NULL,42}'::integer[]) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"arraycontains\"(NULL::integer[], '{1,2,3}'::integer[]) AS v", None),
    ("SELECT pg_catalog.\"arraycontains\"('{1,2,3}'::integer[], NULL::integer[]) AS v", None),
    (
        "SELECT pg_catalog.\"arraycontains\"('{2147483647,-2147483648}'::integer[], '{1,2,3}'::integer[]) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"arraycontains\"('{1,2,3}'::integer[], '{2147483647,-2147483648}'::integer[]) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"arraycontains\"('{NULL}'::integer[], '{1,2,3}'::integer[]) AS v", "f"),
    ("SELECT pg_catalog.\"arraycontains\"('{1,2,3}'::integer[], '{NULL}'::integer[]) AS v", "f"),
    (
        "SELECT pg_catalog.\"arraycontains\"('{{1,2},{3,4}}'::integer[], '{1,2,3}'::integer[]) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"arraycontains\"('{1,2,3}'::integer[], '{{1,2},{3,4}}'::integer[]) AS v",
        "f",
    ),
    ('SELECT pg_catalog."arraycontained"(NULL::integer[], NULL::integer[]) AS v', None),
    ("SELECT pg_catalog.\"arraycontained\"('{1,2,3}'::integer[], '{1,2,3}'::integer[]) AS v", "t"),
    ("SELECT pg_catalog.\"arraycontained\"('{}'::integer[], '{}'::integer[]) AS v", "t"),
    (
        "SELECT pg_catalog.\"arraycontained\"('{-7,NULL,42}'::integer[], '{-7,NULL,42}'::integer[]) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"arraycontained\"(NULL::integer[], '{1,2,3}'::integer[]) AS v", None),
    ("SELECT pg_catalog.\"arraycontained\"('{1,2,3}'::integer[], NULL::integer[]) AS v", None),
    (
        "SELECT pg_catalog.\"arraycontained\"('{2147483647,-2147483648}'::integer[], '{1,2,3}'::integer[]) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"arraycontained\"('{1,2,3}'::integer[], '{2147483647,-2147483648}'::integer[]) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"arraycontained\"('{NULL}'::integer[], '{1,2,3}'::integer[]) AS v", "f"),
    ("SELECT pg_catalog.\"arraycontained\"('{1,2,3}'::integer[], '{NULL}'::integer[]) AS v", "f"),
    (
        "SELECT pg_catalog.\"arraycontained\"('{{1,2},{3,4}}'::integer[], '{1,2,3}'::integer[]) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"arraycontained\"('{1,2,3}'::integer[], '{{1,2},{3,4}}'::integer[]) AS v",
        "t",
    ),
    ('SELECT pg_catalog."arrayoverlap"(NULL::integer[], NULL::integer[]) AS v', None),
    ("SELECT pg_catalog.\"arrayoverlap\"('{1,2,3}'::integer[], '{1,2,3}'::integer[]) AS v", "t"),
    ("SELECT pg_catalog.\"arrayoverlap\"('{}'::integer[], '{}'::integer[]) AS v", "f"),
    (
        "SELECT pg_catalog.\"arrayoverlap\"('{-7,NULL,42}'::integer[], '{-7,NULL,42}'::integer[]) AS v",
        "t",
    ),
    ("SELECT pg_catalog.\"arrayoverlap\"(NULL::integer[], '{1,2,3}'::integer[]) AS v", None),
    ("SELECT pg_catalog.\"arrayoverlap\"('{1,2,3}'::integer[], NULL::integer[]) AS v", None),
    (
        "SELECT pg_catalog.\"arrayoverlap\"('{2147483647,-2147483648}'::integer[], '{1,2,3}'::integer[]) AS v",
        "f",
    ),
    (
        "SELECT pg_catalog.\"arrayoverlap\"('{1,2,3}'::integer[], '{2147483647,-2147483648}'::integer[]) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"arrayoverlap\"('{NULL}'::integer[], '{1,2,3}'::integer[]) AS v", "f"),
    ("SELECT pg_catalog.\"arrayoverlap\"('{1,2,3}'::integer[], '{NULL}'::integer[]) AS v", "f"),
    (
        "SELECT pg_catalog.\"arrayoverlap\"('{{1,2},{3,4}}'::integer[], '{1,2,3}'::integer[]) AS v",
        "t",
    ),
    (
        "SELECT pg_catalog.\"arrayoverlap\"('{1,2,3}'::integer[], '{{1,2},{3,4}}'::integer[]) AS v",
        "t",
    ),
    ('SELECT pg_catalog."array_remove"(NULL::integer[], NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"array_remove\"('{1,2,3}'::integer[], 3::integer) AS v", "{1,2}"),
    ("SELECT pg_catalog.\"array_remove\"('{}'::integer[], (-7)::integer) AS v", "{}"),
    (
        "SELECT pg_catalog.\"array_remove\"('{-7,NULL,42}'::integer[], 42::integer) AS v",
        "{-7,NULL}",
    ),
    ('SELECT pg_catalog."array_remove"(NULL::integer[], 3::integer) AS v', None),
    ("SELECT pg_catalog.\"array_remove\"('{1,2,3}'::integer[], NULL::integer) AS v", "{1,2,3}"),
    (
        "SELECT pg_catalog.\"array_remove\"('{2147483647,-2147483648}'::integer[], 3::integer) AS v",
        "{2147483647,-2147483648}",
    ),
    (
        "SELECT pg_catalog.\"array_remove\"('{1,2,3}'::integer[], '-2147483648'::integer) AS v",
        "{1,2,3}",
    ),
    ("SELECT pg_catalog.\"array_remove\"('{NULL}'::integer[], 3::integer) AS v", "{NULL}"),
    (
        "SELECT pg_catalog.\"array_remove\"('{1,2,3}'::integer[], '2147483647'::integer) AS v",
        "{1,2,3}",
    ),
    ("SELECT pg_catalog.\"array_remove\"('{1,2,3}'::integer[], '0'::integer) AS v", "{1,2,3}"),
    (
        'SELECT pg_catalog."array_position"(NULL::integer[], NULL::integer, NULL::integer) AS v',
        None,
    ),
    (
        "SELECT pg_catalog.\"array_position\"('{1,2,3}'::integer[], 3::integer, 3::integer) AS v",
        "3",
    ),
    (
        "SELECT pg_catalog.\"array_position\"('{}'::integer[], (-7)::integer, (-7)::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"array_position\"('{-7,NULL,42}'::integer[], 42::integer, 42::integer) AS v",
        None,
    ),
    ('SELECT pg_catalog."array_position"(NULL::integer[], 3::integer, 3::integer) AS v', None),
    (
        "SELECT pg_catalog.\"array_position\"('{1,2,3}'::integer[], NULL::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"array_position\"('{2147483647,-2147483648}'::integer[], 3::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"array_position\"('{1,2,3}'::integer[], '-2147483648'::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"array_position\"('{1,2,3}'::integer[], 3::integer, '-2147483648'::integer) AS v",
        "3",
    ),
    (
        "SELECT pg_catalog.\"array_position\"('{NULL}'::integer[], 3::integer, 3::integer) AS v",
        None,
    ),
    (
        "SELECT pg_catalog.\"array_position\"('{1,2,3}'::integer[], '2147483647'::integer, 3::integer) AS v",
        None,
    ),
    ('SELECT pg_catalog."pg_size_pretty"(NULL::bigint) AS v', None),
    ('SELECT pg_catalog."pg_size_pretty"(3::bigint) AS v', "3 bytes"),
    ('SELECT pg_catalog."pg_size_pretty"((-7)::bigint) AS v', "-7 bytes"),
    ('SELECT pg_catalog."pg_size_pretty"(5000000000::bigint) AS v', "4768 MB"),
    ("SELECT pg_catalog.\"pg_size_pretty\"('-9223372036854775808'::bigint) AS v", "-8192 PB"),
    ("SELECT pg_catalog.\"pg_size_pretty\"('9223372036854775807'::bigint) AS v", "8192 PB"),
    ("SELECT pg_catalog.\"pg_size_pretty\"('0'::bigint) AS v", "0 bytes"),
    ('SELECT pg_catalog."pg_encoding_max_length"(NULL::integer) AS v', None),
    ('SELECT pg_catalog."pg_encoding_max_length"(3::integer) AS v', "3"),
    ('SELECT pg_catalog."pg_encoding_max_length"((-7)::integer) AS v', None),
    ('SELECT pg_catalog."pg_encoding_max_length"(42::integer) AS v', None),
    ("SELECT pg_catalog.\"pg_encoding_max_length\"('-2147483648'::integer) AS v", None),
    ("SELECT pg_catalog.\"pg_encoding_max_length\"('2147483647'::integer) AS v", None),
    ("SELECT pg_catalog.\"pg_encoding_max_length\"('0'::integer) AS v", "1"),
    ('SELECT pg_catalog."pg_typeof"(NULL::integer) AS v', "integer"),
    ('SELECT pg_catalog."pg_typeof"(3::integer) AS v', "integer"),
    ('SELECT pg_catalog."pg_typeof"((-7)::integer) AS v', "integer"),
    ('SELECT pg_catalog."pg_typeof"(42::integer) AS v', "integer"),
    ("SELECT pg_catalog.\"pg_typeof\"('-2147483648'::integer) AS v", "integer"),
    ("SELECT pg_catalog.\"pg_typeof\"('2147483647'::integer) AS v", "integer"),
    ("SELECT pg_catalog.\"pg_typeof\"('0'::integer) AS v", "integer"),
    ('SELECT pg_catalog."bit"(NULL::integer, NULL::integer) AS v', None),
    ('SELECT pg_catalog."bit"(3::integer, 3::integer) AS v', "011"),
    ('SELECT pg_catalog."bit"((-7)::integer, (-7)::integer) AS v', "1"),
    (
        'SELECT pg_catalog."bit"(42::integer, 42::integer) AS v',
        "000000000000000000000000000000000000101010",
    ),
    ('SELECT pg_catalog."bit"(NULL::integer, 3::integer) AS v', None),
    ('SELECT pg_catalog."bit"(3::integer, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"bit\"('-2147483648'::integer, 3::integer) AS v", "000"),
    ("SELECT pg_catalog.\"bit\"(3::integer, '-2147483648'::integer) AS v", "1"),
    ("SELECT pg_catalog.\"bit\"('2147483647'::integer, 3::integer) AS v", "111"),
    ("SELECT pg_catalog.\"bit\"(3::integer, '2147483647'::integer) AS v", "1"),
    ("SELECT pg_catalog.\"bit\"('0'::integer, 3::integer) AS v", "000"),
    ("SELECT pg_catalog.\"bit\"(3::integer, '0'::integer) AS v", "1"),
    ('SELECT pg_catalog."bit"(NULL::bigint, NULL::integer) AS v', None),
    ('SELECT pg_catalog."bit"(3::bigint, 3::integer) AS v', "011"),
    ('SELECT pg_catalog."bit"((-7)::bigint, (-7)::integer) AS v', "1"),
    (
        'SELECT pg_catalog."bit"(5000000000::bigint, 42::integer) AS v',
        "000000000100101010000001011111001000000000",
    ),
    ('SELECT pg_catalog."bit"(NULL::bigint, 3::integer) AS v', None),
    ('SELECT pg_catalog."bit"(3::bigint, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"bit\"('-9223372036854775808'::bigint, 3::integer) AS v", "000"),
    ("SELECT pg_catalog.\"bit\"(3::bigint, '-2147483648'::integer) AS v", "1"),
    ("SELECT pg_catalog.\"bit\"('9223372036854775807'::bigint, 3::integer) AS v", "111"),
    ("SELECT pg_catalog.\"bit\"(3::bigint, '2147483647'::integer) AS v", "1"),
    ("SELECT pg_catalog.\"bit\"('0'::bigint, 3::integer) AS v", "000"),
    ("SELECT pg_catalog.\"bit\"(3::bigint, '0'::integer) AS v", "1"),
    ('SELECT pg_catalog."max"(NULL::tid) AS v', None),
    (
        "SELECT pg_catalog.\"max\"(c1) AS v FROM (VALUES ('(0,1)'::tid), ('(5,3)'::tid), ('(4294967295,65535)'::tid)) AS t(c1)",
        "(4294967295,65535)",
    ),
    ('SELECT pg_catalog."max"(c1) AS v FROM (VALUES (NULL::tid), (NULL::tid)) AS t(c1)', None),
    ('SELECT pg_catalog."min"(NULL::tid) AS v', None),
    (
        "SELECT pg_catalog.\"min\"(c1) AS v FROM (VALUES ('(0,1)'::tid), ('(5,3)'::tid), ('(4294967295,65535)'::tid)) AS t(c1)",
        "(0,1)",
    ),
    ('SELECT pg_catalog."min"(c1) AS v FROM (VALUES (NULL::tid), (NULL::tid)) AS t(c1)', None),
    ("SELECT pg_catalog.pg_size_pretty((-9223372036854775807-1)::bigint) AS v", "-8192 PB"),
    ("SELECT pg_catalog.pg_size_pretty(9223372036854775807::bigint) AS v", "8192 PB"),
    ("SELECT pg_catalog.pg_size_pretty(10239::bigint) AS v", "10239 bytes"),
    ("SELECT pg_catalog.pg_size_pretty(10240::bigint) AS v", "10 kB"),
    ("SELECT pg_catalog.pg_size_pretty((-10240)::bigint) AS v", "-10 kB"),
    ("SELECT pg_catalog.pg_size_pretty(20971519::bigint) AS v", "20 MB"),
    ("SELECT pg_catalog.pg_size_pretty(20971520::bigint) AS v", "20 MB"),
    ("SELECT pg_catalog.pg_size_pretty(1099511627775::bigint) AS v", "1024 GB"),
    ("SELECT pg_catalog.pg_size_pretty(1125899906842623::bigint) AS v", "1024 TB"),
    ("SELECT pg_catalog.pg_size_pretty(1125899906842624::bigint) AS v", "1024 TB"),
    ("SELECT pg_catalog.pg_size_pretty((-1)::bigint) AS v", "-1 bytes"),
    ("SELECT pg_catalog.pg_size_pretty(1048575::bigint) AS v", "1024 kB"),
    ("SELECT pg_catalog.pg_size_pretty(10485759::bigint) AS v", "10 MB"),
    ("SELECT pg_catalog.pg_size_pretty((-20971519)::bigint) AS v", "-20 MB"),
    ("SELECT pg_catalog.pg_size_pretty(10485759999999::bigint) AS v", "9766 GB"),
    ("SELECT pg_catalog.pg_size_pretty(10485759999::bigint) AS v", "10000 MB"),
    ("SELECT pg_catalog.pg_size_pretty(21474836479::bigint) AS v", "20 GB"),
    ("SELECT pg_catalog.pg_size_pretty(21474836480::bigint) AS v", "20 GB"),
    ("SELECT pg_catalog.pg_size_pretty(5242879::bigint) AS v", "5120 kB"),
    ("SELECT pg_catalog.pg_size_pretty(5242880::bigint) AS v", "5120 kB"),
    ("SELECT pg_catalog.pg_encoding_max_length(41) AS v", "2"),
    ("SELECT pg_catalog.pg_encoding_max_length(34) AS v", "1"),
    ("SELECT pg_catalog.pg_encoding_max_length(35) AS v", "2"),
    ("SELECT pg_catalog.pg_encoding_max_length(42) AS v", None),
    ("SELECT pg_catalog.pg_encoding_max_length(-1) AS v", None),
    ("SELECT pg_catalog.pg_typeof(3000000000) AS v", "bigint"),
    ("SELECT pg_catalog.pg_typeof('a') AS v", "unknown"),
    ("SELECT pg_catalog.pg_typeof(1.5) AS v", "numeric"),
    ("SELECT pg_catalog.pg_typeof(NULL) AS v", "unknown"),
    ("SELECT pg_catalog.pg_typeof('1'::varchar(5)) AS v", "character varying"),
    ("SELECT pg_catalog.pg_typeof(true) AS v", "boolean"),
    ("SELECT pg_catalog.pg_typeof('1'::text) AS v", "text"),
    ('SELECT pg_catalog."bit"((-3)::integer, 40) AS v', "1111111111111111111111111111111111111101"),
    (
        'SELECT pg_catalog."bit"((-3)::bigint, 70) AS v',
        "1111111111111111111111111111111111111111111111111111111111111111111101",
    ),
    ('SELECT pg_catalog."bit"(5::integer, 0) AS v', "1"),
    ('SELECT pg_catalog."bit"(5::integer, 2) AS v', "01"),
    (
        "SELECT pg_catalog.max(c) AS v FROM (VALUES ('(1,9)'::tid), ('(2,1)'::tid), (NULL::tid)) AS t(c)",
        "(2,1)",
    ),
    (
        "SELECT pg_catalog.min(c) AS v FROM (VALUES ('(10,0)'::tid), ('(9,65535)'::tid)) AS t(c)",
        "(9,65535)",
    ),
    ("SELECT pg_catalog.arraycontains('{1,NULL}'::integer[], '{NULL}'::integer[]) AS v", "f"),
    ("SELECT pg_catalog.arrayoverlap('{1,NULL}'::integer[], '{NULL,3}'::integer[]) AS v", "f"),
    ("SELECT pg_catalog.array_remove('{1,NULL,2,NULL}'::integer[], NULL::integer) AS v", "{1,2}"),
    ("SELECT pg_catalog.array_position('{1,2,1}'::integer[], 1, 2) AS v", "3"),
    ("SELECT pg_catalog.array_position('{1,2,1}'::integer[], 1, 9) AS v", None),
]
RAISED = [
    ("SELECT pg_catalog.\"array_remove\"('{{1,2},{3,4}}'::integer[], 3::integer) AS v", "0A000"),
    (
        "SELECT pg_catalog.\"array_position\"('{1,2,3}'::integer[], 3::integer, NULL::integer) AS v",
        "22004",
    ),
    ("SELECT pg_catalog.\"pg_size_pretty\"('9223372036854775808'::bigint) AS v", "22003"),
    ("SELECT pg_catalog.\"pg_encoding_max_length\"('2147483648'::integer) AS v", "22003"),
    ("SELECT pg_catalog.\"pg_typeof\"('2147483648'::integer) AS v", "22003"),
]

#: Calls a row refuses by name, each within its stated bound.
REFUSED = [
    "SELECT pg_catalog.pg_typeof(c) AS v FROM conformance.function_columns",
]


def _element(value: object) -> str:
    if value is None:
        return "NULL"
    text = _text(value)
    if text == "" or text.upper() == "NULL" or any(ch in text for ch in ',{}"\\ '):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def _text(value: object) -> str:
    """The engine's value as PostgreSQL's text output writes it: a boolean t or f, an array in
    braces."""
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (list, tuple)):
        return "{" + ",".join(_element(v) for v in value) + "}"
    return str(value)


def _same(want: str | None, got: object) -> bool:
    if want is None or got is None:
        return want is got
    if isinstance(got, float):
        return float(want) == got
    return want == _text(got)


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    return value


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    got = _answer(runtime, sql)
    assert _same(want, got), (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    if isinstance(raised.value, RowRefused):
        assert raised.value.sqlstate == state
        return
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


@pytest.mark.parametrize("sql", REFUSED)
def test_a_call_past_the_row_s_bound_is_refused_by_name(sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse")
