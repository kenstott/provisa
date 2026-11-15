# Copyright (c) 2026 Kenneth Stott
# Canary: 72335ee8-120f-4a70-8eaf-fee49fa7608c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's forms carried over from DuckDB (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND
COMES BEFORE A REFUSAL): each overload of polyfill.ported.PORTED, answered on the embedded engine,
against PostgreSQL's answer. Each was refused by name on ClickHouse on the base.

The answers are PostgreSQL 17.11's (a throwaway postgres:17.11 container): the edge values of each
overload, with the errors PostgreSQL raises. The overloads the bulk port carried that differ from
PostgreSQL on such values (regexp_like's flags, a multibyte btrim, inet's mask, numeric's scale,
exp's overflow, the bit and oidvector orders, and the rest of the report's list) are not listed, and
stay refused by name: test_an_overload_not_carried_stays_refused_by_name."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime  # noqa: F401 -- the answers' reprs

import pytest
import sqlglot

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import ported
from provisa.transpiler.transpile import transpile

ANSWERED = [
    ("SELECT pg_catalog.replace('abcabc'::text, ''::text, 'x'::text) AS v", "abcabc"),
    ("SELECT pg_catalog.replace('abcabc'::text, 'b'::text, ''::text) AS v", "acac"),
    ("SELECT pg_catalog.replace('aaa'::text, 'aa'::text, 'b'::text) AS v", "ba"),
    ("SELECT pg_catalog.replace('a.b.c'::text, '.'::text, '\\'::text) AS v", "a\\b\\c"),
    ("SELECT pg_catalog.replace('ABC'::text, 'b'::text, 'x'::text) AS v", "ABC"),
    ("SELECT pg_catalog.dexp(709.0::double precision) AS v", 8.218407461554972e307),
    ("SELECT pg_catalog.dexp(1.0::double precision) AS v", 2.718281828459045),
    ("SELECT pg_catalog.dexp(0.123456789::double precision) AS v", 1.1314011145122336),
    ("SELECT pg_catalog.dexp(50.5::double precision) AS v", 8.548134287298057e21),
    ("SELECT pg_catalog.dexp('Infinity'::double precision) AS v", float("inf")),
    ("SELECT pg_catalog.dexp('NaN'::double precision) AS v", float("nan")),
    ("SELECT pg_catalog.dlog1(1e-300::double precision) AS v", -690.7755278982137),
    ("SELECT pg_catalog.dlog1(2.718281828459045::double precision) AS v", 1.0),
    ("SELECT pg_catalog.dlog1('Infinity'::double precision) AS v", float("inf")),
    ("SELECT pg_catalog.dlog1(123.456::double precision) AS v", 4.815884817283264),
    ("SELECT pg_catalog.text('<a>x &amp; y</a>'::xml) AS v", "<a>x &amp; y</a>"),
    (
        "SELECT pg_catalog.text('<?xml version=\"1.0\"?><r/>'::xml) AS v",
        '<?xml version="1.0"?><r/>',
    ),
    ("SELECT pg_catalog.text('plain'::xml) AS v", "plain"),
    ("SELECT pg_catalog.tideq('(2,1)'::tid, '(10,0)'::tid) AS v", False),
    ("SELECT pg_catalog.tideq('(2,10)'::tid, '(2,9)'::tid) AS v", False),
    ("SELECT pg_catalog.tideq('(3,3)'::tid, '(3,3)'::tid) AS v", True),
    ("SELECT pg_catalog.tidne('(2,1)'::tid, '(10,0)'::tid) AS v", True),
    ("SELECT pg_catalog.tidne('(2,10)'::tid, '(2,9)'::tid) AS v", True),
    ("SELECT pg_catalog.tidne('(3,3)'::tid, '(3,3)'::tid) AS v", False),
    ("SELECT pg_catalog.tidlt('(2,1)'::tid, '(10,0)'::tid) AS v", True),
    ("SELECT pg_catalog.tidlt('(2,10)'::tid, '(2,9)'::tid) AS v", False),
    ("SELECT pg_catalog.tidlt('(3,3)'::tid, '(3,3)'::tid) AS v", False),
    ("SELECT pg_catalog.tidle('(2,1)'::tid, '(10,0)'::tid) AS v", True),
    ("SELECT pg_catalog.tidle('(2,10)'::tid, '(2,9)'::tid) AS v", False),
    ("SELECT pg_catalog.tidle('(3,3)'::tid, '(3,3)'::tid) AS v", True),
    ("SELECT pg_catalog.tidgt('(2,1)'::tid, '(10,0)'::tid) AS v", False),
    ("SELECT pg_catalog.tidgt('(2,10)'::tid, '(2,9)'::tid) AS v", True),
    ("SELECT pg_catalog.tidgt('(3,3)'::tid, '(3,3)'::tid) AS v", False),
    ("SELECT pg_catalog.tidge('(2,1)'::tid, '(10,0)'::tid) AS v", False),
    ("SELECT pg_catalog.tidge('(2,10)'::tid, '(2,9)'::tid) AS v", True),
    ("SELECT pg_catalog.tidge('(3,3)'::tid, '(3,3)'::tid) AS v", True),
    ("SELECT pg_catalog.tidlarger('(2,1)'::tid, '(10,0)'::tid) AS v", "(10,0)"),
    ("SELECT pg_catalog.tidlarger('(2,10)'::tid, '(2,9)'::tid) AS v", "(2,10)"),
    ("SELECT pg_catalog.tidlarger('(3,3)'::tid, '(3,3)'::tid) AS v", "(3,3)"),
    ("SELECT pg_catalog.tidsmaller('(2,1)'::tid, '(10,0)'::tid) AS v", "(2,1)"),
    ("SELECT pg_catalog.tidsmaller('(2,10)'::tid, '(2,9)'::tid) AS v", "(2,9)"),
    ("SELECT pg_catalog.tidsmaller('(3,3)'::tid, '(3,3)'::tid) AS v", "(3,3)"),
    ("SELECT pg_catalog.replace('日本日'::text, '日'::text, 'x'::text) AS v", "x本x"),
    ("SELECT pg_catalog.replace('a^b$c'::text, '^'::text, '$'::text) AS v", "a$b$c"),
    ("SELECT pg_catalog.replace('a.*b'::text, '.*'::text, 'Z'::text) AS v", "aZb"),
    ("SELECT pg_catalog.replace(NULL::text, 'a'::text, 'b'::text) AS v", None),
    ("SELECT pg_catalog.replace('abc'::text, 'b'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.replace('aaaa'::text, 'a'::text, 'aa'::text) AS v", "aaaaaaaa"),
    ("SELECT pg_catalog.dexp(708.9::double precision) AS v", 7.436322587880697e307),
    ("SELECT pg_catalog.dexp(-708.5::double precision) AS v", 2.006132305331306e-308),
    ("SELECT pg_catalog.dexp(1e-20::double precision) AS v", 1.0),
    ("SELECT pg_catalog.dexp('-Infinity'::double precision) AS v", 0.0),
    ("SELECT pg_catalog.dexp(3.14159::double precision) AS v", 23.14063122695496),
    ("SELECT pg_catalog.dexp(-0.5::double precision) AS v", 0.6065306597126334),
    ("SELECT pg_catalog.dlog1('NaN'::double precision) AS v", float("nan")),
    ("SELECT pg_catalog.dlog1(1.0::double precision) AS v", 0.0),
    ("SELECT pg_catalog.dlog1(5e-324::double precision) AS v", -744.4400719213812),
    ("SELECT pg_catalog.dlog1(1.7976931348623157e308::double precision) AS v", 709.782712893384),
    ("SELECT pg_catalog.dlog1(0.999999::double precision) AS v", -1.000000500029089e-06),
    ("SELECT pg_catalog.text('<a b=\"1\"/>'::xml) AS v", '<a b="1"/>'),
    ("SELECT pg_catalog.text('<a>日本</a>'::xml) AS v", "<a>日本</a>"),
    ("SELECT pg_catalog.text(NULL::xml) AS v", None),
    ("SELECT pg_catalog.text('<a><![CDATA[x<y]]></a>'::xml) AS v", "<a><![CDATA[x<y]]></a>"),
    ("SELECT pg_catalog.tideq('(4294967295,65535)'::tid, '(0,0)'::tid) AS v", False),
    ("SELECT pg_catalog.tideq('(10,1)'::tid, '(9,2)'::tid) AS v", False),
    ("SELECT pg_catalog.tideq('(1,10)'::tid, '(1,9)'::tid) AS v", False),
    ("SELECT pg_catalog.tideq(NULL::tid, '(1,1)'::tid) AS v", None),
    ("SELECT pg_catalog.tideq('(100,1)'::tid, '(20,1)'::tid) AS v", False),
    ("SELECT pg_catalog.tidne('(4294967295,65535)'::tid, '(0,0)'::tid) AS v", True),
    ("SELECT pg_catalog.tidne('(10,1)'::tid, '(9,2)'::tid) AS v", True),
    ("SELECT pg_catalog.tidne('(1,10)'::tid, '(1,9)'::tid) AS v", True),
    ("SELECT pg_catalog.tidne(NULL::tid, '(1,1)'::tid) AS v", None),
    ("SELECT pg_catalog.tidne('(100,1)'::tid, '(20,1)'::tid) AS v", True),
    ("SELECT pg_catalog.tidlt('(4294967295,65535)'::tid, '(0,0)'::tid) AS v", False),
    ("SELECT pg_catalog.tidlt('(10,1)'::tid, '(9,2)'::tid) AS v", False),
    ("SELECT pg_catalog.tidlt('(1,10)'::tid, '(1,9)'::tid) AS v", False),
    ("SELECT pg_catalog.tidlt(NULL::tid, '(1,1)'::tid) AS v", None),
    ("SELECT pg_catalog.tidlt('(100,1)'::tid, '(20,1)'::tid) AS v", False),
    ("SELECT pg_catalog.tidle('(4294967295,65535)'::tid, '(0,0)'::tid) AS v", False),
    ("SELECT pg_catalog.tidle('(10,1)'::tid, '(9,2)'::tid) AS v", False),
    ("SELECT pg_catalog.tidle('(1,10)'::tid, '(1,9)'::tid) AS v", False),
    ("SELECT pg_catalog.tidle(NULL::tid, '(1,1)'::tid) AS v", None),
    ("SELECT pg_catalog.tidle('(100,1)'::tid, '(20,1)'::tid) AS v", False),
    ("SELECT pg_catalog.tidgt('(4294967295,65535)'::tid, '(0,0)'::tid) AS v", True),
    ("SELECT pg_catalog.tidgt('(10,1)'::tid, '(9,2)'::tid) AS v", True),
    ("SELECT pg_catalog.tidgt('(1,10)'::tid, '(1,9)'::tid) AS v", True),
    ("SELECT pg_catalog.tidgt(NULL::tid, '(1,1)'::tid) AS v", None),
    ("SELECT pg_catalog.tidgt('(100,1)'::tid, '(20,1)'::tid) AS v", True),
    ("SELECT pg_catalog.tidge('(4294967295,65535)'::tid, '(0,0)'::tid) AS v", True),
    ("SELECT pg_catalog.tidge('(10,1)'::tid, '(9,2)'::tid) AS v", True),
    ("SELECT pg_catalog.tidge('(1,10)'::tid, '(1,9)'::tid) AS v", True),
    ("SELECT pg_catalog.tidge(NULL::tid, '(1,1)'::tid) AS v", None),
    ("SELECT pg_catalog.tidge('(100,1)'::tid, '(20,1)'::tid) AS v", True),
    (
        "SELECT pg_catalog.tidlarger('(4294967295,65535)'::tid, '(0,0)'::tid) AS v",
        "(4294967295,65535)",
    ),
    ("SELECT pg_catalog.tidlarger('(10,1)'::tid, '(9,2)'::tid) AS v", "(10,1)"),
    ("SELECT pg_catalog.tidlarger('(1,10)'::tid, '(1,9)'::tid) AS v", "(1,10)"),
    ("SELECT pg_catalog.tidlarger(NULL::tid, '(1,1)'::tid) AS v", None),
    ("SELECT pg_catalog.tidlarger('(100,1)'::tid, '(20,1)'::tid) AS v", "(100,1)"),
    ("SELECT pg_catalog.tidsmaller('(4294967295,65535)'::tid, '(0,0)'::tid) AS v", "(0,0)"),
    ("SELECT pg_catalog.tidsmaller('(10,1)'::tid, '(9,2)'::tid) AS v", "(9,2)"),
    ("SELECT pg_catalog.tidsmaller('(1,10)'::tid, '(1,9)'::tid) AS v", "(1,9)"),
    ("SELECT pg_catalog.tidsmaller(NULL::tid, '(1,1)'::tid) AS v", None),
    ("SELECT pg_catalog.tidsmaller('(100,1)'::tid, '(20,1)'::tid) AS v", "(20,1)"),
]
RAISED = [
    ("SELECT pg_catalog.dexp(710.0::double precision) AS v", "22003"),
    ("SELECT pg_catalog.dexp(-746.0::double precision) AS v", "22003"),
    ("SELECT pg_catalog.dlog1(-1.0::double precision) AS v", "2201E"),
    ("SELECT pg_catalog.dlog1(0.0::double precision) AS v", "2201E"),
    ("SELECT pg_catalog.dexp(-1000.0::double precision) AS v", "22003"),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    return value


def _same(want: object, got: object) -> bool:
    """PostgreSQL's answer and the engine's: a boolean the engine holds as 0 or 1, a double within
    one part in 10^14 (an infinity or NaN itself), any other value as it is."""
    if want is None or got is None:
        return want is got
    if isinstance(want, bool):
        return got in (0, 1) and bool(got) is want
    if isinstance(want, float):
        got = float(got)  # type: ignore[arg-type]
        if want != want or abs(want) == float("inf"):
            return repr(want) == repr(got)
        return abs(want - got) <= 1e-14 * abs(want)
    return want == got


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    got = _answer(runtime, sql)
    assert _same(want, got), (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


def test_every_carried_overload_is_compared():
    compared = {sql.split("pg_catalog.")[1].split("(")[0] for sql, _ in ANSWERED}
    assert compared == {name for name, _ in ported.PORTED}


def test_a_column_s_value_is_carried(runtime):
    sql = (
        "SELECT pg_catalog.replace(c, 'b', 'xy') AS v "
        "FROM (VALUES ('abcb'::text), ('日本b'::text)) AS t(c) ORDER BY c"
    )
    rows = [row[0] for row in runtime.run_sync(transpile(sql, "clickhouse")).iter_rows()]
    assert rows == ["axycxy", "日本xy"]


def test_a_call_inside_another_is_carried(runtime):
    sql = "SELECT upper(pg_catalog.replace('abc'::text, 'b'::text, ''::text)) AS v"
    assert _answer(runtime, sql) == "AC"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.btrim('xxabcxx'::text, ''::text) AS v",
        "SELECT pg_catalog.regexp_like('Hello'::text, 'h.l'::text, 'i'::text) AS v",
        "SELECT pg_catalog.oidvectorlt('10'::oidvector, '9'::oidvector) AS v",
        'SELECT pg_catalog."exp"(710.0::double precision) AS v',
    ],
)
def test_an_overload_not_carried_stays_refused_by_name(sql):
    with pytest.raises(FunctionRefused, match="has no form on clickhouse"):
        transpile(sql, "clickhouse")


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "postgres"])
def test_every_other_dialect_keeps_what_it_had(dialect):
    sql = "SELECT pg_catalog.tidlt('(2,1)'::tid, '(10,0)'::tid) AS v"
    assert ported.held_out(sql, dialect)[0] == sql
    assert ported.carried(sqlglot.parse_one(sql, read="postgres").selects[0], dialect) is None
