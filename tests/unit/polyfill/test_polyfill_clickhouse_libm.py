# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6f2a8e-93b1-4c57-a8e4-6b1f0c2d7e95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""exp, ln and tanh of a double on ClickHouse, and their pg_proc names dexp and dlog1, give
PostgreSQL's exact double (REQ-2236): each is glibc's own algorithm in ClickHouse's arithmetic
(provisa.transpiler.clickhouse_libm), not ClickHouse's exp, log or tanh, which the 24.3 server
answers by a vectorized approximation (dexp(0.5) and dlog1(100) differed from the 12th digit in
run 45629262597).

GLIBC's answers are PostgreSQL 17's (a throwaway postgres:17 container, glibc, its products fused
as on every x86-64 with FMA and AArch64), at values where ClickHouse's own exp, log and sinh / cosh
answer another double. EMBEDDED's are compared live with the embedded PostgreSQL the unit tests
boot, at values where its C library and glibc agree (macOS's libm differs from glibc's at about one
value in a thousand). Each answer is compared bit for bit."""

# Requirements: REQ-2236

from __future__ import annotations

import math
import re
import struct

import psycopg
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.transpile import transpile

#: (function, argument, PostgreSQL's answer): ClickHouse's own function answers another double.
GLIBC = [
    ("exp", 158.40392178784384, 6.222274183282417e68),
    ("exp", -2.5350887309531625e-11, 0.9999999999746492),
    ("exp", -1.8339893561660593e-05, 0.9999816602746131),
    ("exp", -23.15260982890163, 8.809460440010027e-11),
    ("exp", 0.4207608713773232, 1.523120013266435),
    ("exp", -3.1848804198474667e-09, 0.9999999968151196),
    ("exp", -0.7862097644945829, 0.4555682380030483),
    ("exp", -2.3063934060245097e-10, 0.9999999997693607),
    ("exp", -1.94245664563357e-15, 0.9999999999999981),
    ("exp", 699.3404030788129, 5.24418530376637e303),
    ("exp", -0.001144032725843456, 0.9988566214301131),
    ("exp", -81.81476998065705, 2.9396583500400176e-36),
    ("ln", 0.9668976045530281, -0.033662678946290395),
    ("ln", 0.937398586770069, -0.06464670110073875),
    ("ln", 1.0795863461537905, 0.07657795494741902),
    ("ln", 1.0969683502874659, 0.09255032972599363),
    ("ln", 1.0870731226057795, 0.08348887598305312),
    ("ln", 1.0698814053735384, 0.06754780623154358),
    ("ln", 0.9103713024767296, -0.09390273799097998),
    ("ln", 1.0470566395804437, 0.045983027445689856),
    ("ln", 1.092283337909259, 0.08827031061685663),
    ("ln", 1.0171384023689176, 0.016993196665500337),
    ("ln", 675.6233372407002, 6.5156357273221),
    ("ln", 1.0683864780406074, 0.06614954590308644),
    ("tanh", -18.876524671541674, -0.9999999999999999),
    ("tanh", 16.29942535496651, 0.9999999999999861),
    ("tanh", -0.5755836741321666, -0.5194481465586407),
    ("tanh", -6.881938709622844, -0.9999978940234764),
    ("tanh", -5.698287107352094, -0.999977532444518),
    ("tanh", -15.54004236450671, -0.9999999999999365),
    ("tanh", 8.933066361251537, 0.9999999651769238),
    ("tanh", 0.7006279684265835, 0.60476622226695),
    ("tanh", -10.622194005810115, -0.9999999988122925),
    ("tanh", 0.42507213913192143, 0.40119481451320227),
    ("tanh", -0.08910258383431784, -0.08886752710751177),
    ("tanh", 0.5092787529504927, 0.4693830477635867),
]

#: dexp and dlog1, carried from DuckDB on the base (polyfill.ported): PostgreSQL 17.11's answers.
NAMED = [
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
]
RAISED = [
    ("SELECT pg_catalog.dexp(710.0::double precision) AS v", "22003"),
    ("SELECT pg_catalog.dexp(-746.0::double precision) AS v", "22003"),
    ("SELECT pg_catalog.dlog1(-1.0::double precision) AS v", "2201E"),
    ("SELECT pg_catalog.dlog1(0.0::double precision) AS v", "2201E"),
    ("SELECT pg_catalog.dexp(-1000.0::double precision) AS v", "22003"),
    ("SELECT pg_catalog.dlog1('-Infinity'::double precision) AS v", "2201E"),
    ("SELECT pg_catalog.dlog1('-0'::double precision) AS v", "2201E"),
    ("SELECT pg_catalog.dexp('1.7976931348623157e308'::double precision) AS v", "22003"),
]

#: Compared live with the embedded PostgreSQL: values where its libm and glibc agree, and the edges.
EMBEDDED = {
    "exp": (
        -8.184327646556973e-09, 88.16178632052834, 139.25793317757325, -1.7353953421701214e-12,
        -0.8757030878585645, -69.66885676748188, 0.11837495640425466, 265.6231388642142,
        2.2555067356848364e-19, -616.8966519177197, -467.952402591461, -164.74785363598482,
        32.75759872605727, -0.6187002061827671, 158.7466599261653, 0.7774463325212051,
        0.3523195288662371, 636.1094183313892, 0.1420708291911765, 258.4268765725044,
        -400.88598444127325, 280.47819512157594, 0.3994132875878802, 646.2478119620068,
        0.5, 1.0, -1.0, 100.0, -2.25, 709.78, -745.1, -708.5, 1e-300, 0.0, -0.0,
        float("nan"), float("inf"), float("-inf"),
    ),
    "ln": (
        2.2135145692083227e-101, 7.402604232377692e-12, 6.5249391094844705e65, 0.972845559748636,
        863.0520900865083, 147.75566054335775, 4.452448670330487e-227, 1.980851386177951e-45,
        5.218436643220315e-309, 9.405211300839046e-93, 0.9336810588039466, 3.886866986109605e242,
        2.185964508256949e65, 7.085583019963277e-131, 1.0523073271076455, 6.0629273814246786e-198,
        2.5696290605620804e-227, 1.2699011527173644e107, 4.5046789530971985e140,
        6.235284537239953e263, 1.024825666271469e-100, 785.1725446975186, 442.4907770822812,
        0.00020472736298502583, 0.5, 1.0, 2.0, 10.0, 100.0, 5e-324, 1.7976931348623157e308,
        float("nan"), float("inf"),
    ),
    "tanh": (
        -0.261206420583061, -0.0001048611352368751, -0.9305784603943863, 8.935243211728601e-12,
        1.0848790588873023, -9.716383791045379, -0.44645121964360907, 4.972716726681458,
        13.126538713162915, 13.473115228927504, 1.448627927986266e-09, -2.2533081581897356,
        -15.716260883369404, 16.728483602704934, -11.648598820035971, -2.0928979502206453e-10,
        -21.423677628686402, 13.277921996238518, 6.364969413632174, 0.5184283903875941,
        -17.25064501200359, 0.6173151992761583, -0.5860286381319555, -0.26530751002211256,
        0.0, -0.0, 1.0, -1.0, 0.5, 22.0, -22.5, 1e-300, 5e-324, float("nan"), float("inf"),
        float("-inf"),
    ),
}  # fmt: skip

#: The functions written here, and ClickHouse's own functions none of their forms may call.
FUNCTIONS = ("exp", "dexp", "ln", "dlog1", "tanh")
_APPROXIMATE = {"exp", "log", "ln", "tanh", "sinh", "cosh", "exp2", "pow", "power"}


def _bits(value: float) -> int:
    return struct.unpack("<Q", struct.pack("<d", value))[0]


def _same(want: float, got: object) -> bool:
    """The same double, bit for bit (a NaN any NaN)."""
    assert isinstance(got, float), got
    if math.isnan(want):
        return math.isnan(got)
    return _bits(want) == _bits(got)


def _call(function: str, value: float) -> str:
    return f"SELECT pg_catalog.\"{function}\"('{value!r}'::double precision) AS v"


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())
    return value


@pytest.mark.parametrize(("function", "value", "want"), GLIBC)
def test_clickhouse_answers_postgresql_s_double(runtime, function, value, want):
    got = _answer(runtime, _call(function, value))
    assert _same(want, got), (want, got)


@pytest.mark.parametrize(("sql", "want"), NAMED)
def test_dexp_and_dlog1_answer_postgresql_s_double(runtime, sql, want):
    got = _answer(runtime, sql)
    assert _same(want, got), (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


@pytest.fixture(scope="module")
def postgresql(embedded_postgres):
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as connection:
        yield connection


@pytest.mark.parametrize(
    ("function", "value"),
    [(function, value) for function, values in EMBEDDED.items() for value in values],
)
def test_clickhouse_answers_the_embedded_postgresql_s_double(runtime, postgresql, function, value):
    sql = _call(function, value)
    try:
        ((want,),) = postgresql.execute(sql).fetchall()
    except psycopg.Error as error:
        with pytest.raises(Exception) as raised:
            _answer(runtime, sql)
        engine = from_engine(raised.value)
        assert engine is not None and engine.sqlstate == error.sqlstate, str(raised.value)[:300]
        return
    got = _answer(runtime, sql)
    assert _same(want, got), (want, got)


@pytest.mark.parametrize("function", FUNCTIONS)
def test_no_form_calls_clickhouse_s_approximation(function):
    written = transpile(_call(function, 0.5), "clickhouse", generated=False)
    tree = sqlglot.parse_one(written, read="clickhouse")
    called = {
        (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()
        for node in tree.find_all(exp.Func)
    }
    assert not called & _APPROXIMATE, called & _APPROXIMATE


@pytest.mark.parametrize("function", FUNCTIONS)
def test_no_row_states_an_approximation(function):
    (feature,) = [f for f in GRID.features if f.name == function]
    stated = [limitation.text for limitation in feature.read.limitations.get("clickhouse", ())]
    assert not [t for t in stated if re.search(r"approximat|last place|significant digit", t)]


@pytest.mark.parametrize("function", FUNCTIONS)
def test_the_statement_is_planned_by_the_old_analyzer(function):
    written = transpile(_call(function, 0.5), "clickhouse", generated=False)
    assert written.endswith("SETTINGS allow_experimental_analyzer = 0")
