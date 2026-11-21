# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e5d20-9f14-4c86-a2d7-0e6c8b4f1a39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of text by characters and of a character(n) literal (REQ-2236; REQ-2171, the
function support measure's edge values): each answered on the embedded engine (chdb), against
PostgreSQL's answer. On the base ClickHouse read a text's bytes (left, right, substr, substring,
reverse, lpad, rpad and ascii cut a multi-byte character), padded with an empty fill, answered a
NULL position with the whole text, and held a character(n) literal unpadded.

The statements are the function support measure's own calls of each overload (its NULL call,
samples and edge values, function_support.calls and edge_calls) and the edges the rows read,
answered by the embedded PostgreSQL the unit tests boot (PostgreSQL 16.11, its text output)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import math
import struct

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [
    ('SELECT pg_catalog."ascii"(NULL::text) AS v', None),
    ("SELECT pg_catalog.\"ascii\"('abc'::text) AS v", "97"),
    ("SELECT pg_catalog.\"ascii\"('Hello World'::text) AS v", "72"),
    ("SELECT pg_catalog.\"ascii\"('12'::text) AS v", "49"),
    ("SELECT pg_catalog.\"ascii\"(''::text) AS v", "0"),
    ("SELECT pg_catalog.\"ascii\"('日本語テキスト😀'::text) AS v", "26085"),
    ("SELECT pg_catalog.\"ascii\"('été'::text) AS v", "101"),
    ("SELECT pg_catalog.\"ascii\"('  padded  '::text) AS v", "32"),
    ('SELECT pg_catalog."char_length"(NULL::character) AS v', None),
    ("SELECT pg_catalog.\"char_length\"('abc'::character) AS v", "1"),
    ("SELECT pg_catalog.\"char_length\"('Hello World'::character) AS v", "1"),
    ("SELECT pg_catalog.\"char_length\"('12'::character) AS v", "1"),
    ("SELECT pg_catalog.\"char_length\"(''::character) AS v", "0"),
    ("SELECT pg_catalog.\"char_length\"('日本語テキスト😀'::character) AS v", "1"),
    ("SELECT pg_catalog.\"char_length\"('été'::character) AS v", "1"),
    ("SELECT pg_catalog.\"char_length\"('  padded  '::character) AS v", "0"),
    ('SELECT pg_catalog."character_length"(NULL::character) AS v', None),
    ("SELECT pg_catalog.\"character_length\"('abc'::character) AS v", "1"),
    ("SELECT pg_catalog.\"character_length\"('Hello World'::character) AS v", "1"),
    ("SELECT pg_catalog.\"character_length\"('12'::character) AS v", "1"),
    ("SELECT pg_catalog.\"character_length\"(''::character) AS v", "0"),
    ("SELECT pg_catalog.\"character_length\"('日本語テキスト😀'::character) AS v", "1"),
    ("SELECT pg_catalog.\"character_length\"('été'::character) AS v", "1"),
    ("SELECT pg_catalog.\"character_length\"('  padded  '::character) AS v", "0"),
    ('SELECT pg_catalog."left"(NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"left\"('abc'::text, 3::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"left\"('Hello World'::text, (-7)::integer) AS v", "Hell"),
    ("SELECT pg_catalog.\"left\"('12'::text, 42::integer) AS v", "12"),
    ('SELECT pg_catalog."left"(NULL::text, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"left\"('abc'::text, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"left\"(''::text, 3::integer) AS v", ""),
    ("SELECT pg_catalog.\"left\"('abc'::text, '-2147483648'::integer) AS v", ""),
    ("SELECT pg_catalog.\"left\"('日本語テキスト😀'::text, 3::integer) AS v", "日本語"),
    ("SELECT pg_catalog.\"left\"('abc'::text, '2147483647'::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"left\"('été'::text, 3::integer) AS v", "ét"),
    ("SELECT pg_catalog.\"left\"('abc'::text, '0'::integer) AS v", ""),
    ('SELECT pg_catalog."length"(NULL::character) AS v', None),
    ("SELECT pg_catalog.\"length\"('abc'::character) AS v", "1"),
    ("SELECT pg_catalog.\"length\"('Hello World'::character) AS v", "1"),
    ("SELECT pg_catalog.\"length\"('12'::character) AS v", "1"),
    ("SELECT pg_catalog.\"length\"(''::character) AS v", "0"),
    ("SELECT pg_catalog.\"length\"('日本語テキスト😀'::character) AS v", "1"),
    ("SELECT pg_catalog.\"length\"('été'::character) AS v", "1"),
    ("SELECT pg_catalog.\"length\"('  padded  '::character) AS v", "0"),
    ('SELECT pg_catalog."lpad"(NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"lpad\"('abc'::text, 6::integer) AS v", "   abc"),
    ("SELECT pg_catalog.\"lpad\"('Hello World'::text, 2::integer) AS v", "He"),
    ("SELECT pg_catalog.\"lpad\"('12'::text, 5::integer) AS v", "   12"),
    ('SELECT pg_catalog."lpad"(NULL::text, 6::integer) AS v', None),
    ("SELECT pg_catalog.\"lpad\"(''::text, 6::integer) AS v", "      "),
    ("SELECT pg_catalog.\"lpad\"('日本語テキスト😀'::text, 6::integer) AS v", "日本語テキス"),
    ("SELECT pg_catalog.\"lpad\"('été'::text, 6::integer) AS v", " été"),
    ("SELECT pg_catalog.\"lpad\"('  padded  '::text, 6::integer) AS v", "  padd"),
    ('SELECT pg_catalog."lpad"(NULL::text, NULL::integer, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"lpad\"('abc'::text, 6::integer, 'abc'::text) AS v", "abcabc"),
    ("SELECT pg_catalog.\"lpad\"('Hello World'::text, 2::integer, 'Hello World'::text) AS v", "He"),
    ("SELECT pg_catalog.\"lpad\"('12'::text, 5::integer, '12'::text) AS v", "12112"),
    ("SELECT pg_catalog.\"lpad\"(NULL::text, 6::integer, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"lpad\"('abc'::text, 6::integer, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"lpad\"(''::text, 6::integer, 'abc'::text) AS v", "abcabc"),
    ("SELECT pg_catalog.\"lpad\"('abc'::text, 6::integer, ''::text) AS v", "abc"),
    (
        "SELECT pg_catalog.\"lpad\"('日本語テキスト😀'::text, 6::integer, 'abc'::text) AS v",
        "日本語テキス",
    ),
    (
        "SELECT pg_catalog.\"lpad\"('abc'::text, 6::integer, '日本語テキスト😀'::text) AS v",
        "日本語abc",
    ),
    ("SELECT pg_catalog.\"lpad\"('été'::text, 6::integer, 'abc'::text) AS v", "aété"),
    ("SELECT pg_catalog.\"lpad\"('abc'::text, 6::integer, 'été'::text) AS v", "étabc"),
    ('SELECT pg_catalog."name"(NULL::character) AS v', None),
    ("SELECT pg_catalog.\"name\"('abc'::character) AS v", "a"),
    ("SELECT pg_catalog.\"name\"('Hello World'::character) AS v", "H"),
    ("SELECT pg_catalog.\"name\"('12'::character) AS v", "1"),
    ("SELECT pg_catalog.\"name\"(''::character) AS v", ""),
    ("SELECT pg_catalog.\"name\"('日本語テキスト😀'::character) AS v", "日"),
    ("SELECT pg_catalog.\"name\"('été'::character) AS v", "e"),
    ("SELECT pg_catalog.\"name\"('  padded  '::character) AS v", ""),
    ('SELECT pg_catalog."octet_length"(NULL::character) AS v', None),
    ("SELECT pg_catalog.\"octet_length\"('abc'::character) AS v", "1"),
    ("SELECT pg_catalog.\"octet_length\"('Hello World'::character) AS v", "1"),
    ("SELECT pg_catalog.\"octet_length\"('12'::character) AS v", "1"),
    ("SELECT pg_catalog.\"octet_length\"(''::character) AS v", "1"),
    ("SELECT pg_catalog.\"octet_length\"('日本語テキスト😀'::character) AS v", "3"),
    ("SELECT pg_catalog.\"octet_length\"('été'::character) AS v", "1"),
    ("SELECT pg_catalog.\"octet_length\"('  padded  '::character) AS v", "1"),
    ('SELECT pg_catalog."reverse"(NULL::text) AS v', None),
    ("SELECT pg_catalog.\"reverse\"('abc'::text) AS v", "cba"),
    ("SELECT pg_catalog.\"reverse\"('Hello World'::text) AS v", "dlroW olleH"),
    ("SELECT pg_catalog.\"reverse\"('12'::text) AS v", "21"),
    ("SELECT pg_catalog.\"reverse\"(''::text) AS v", ""),
    ("SELECT pg_catalog.\"reverse\"('日本語テキスト😀'::text) AS v", "😀トスキテ語本日"),
    ("SELECT pg_catalog.\"reverse\"('été'::text) AS v", "́et́e"),
    ("SELECT pg_catalog.\"reverse\"('  padded  '::text) AS v", "  deddap  "),
    ('SELECT pg_catalog."right"(NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"right\"('abc'::text, 3::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"right\"('Hello World'::text, (-7)::integer) AS v", "orld"),
    ("SELECT pg_catalog.\"right\"('12'::text, 42::integer) AS v", "12"),
    ('SELECT pg_catalog."right"(NULL::text, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"right\"('abc'::text, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"right\"(''::text, 3::integer) AS v", ""),
    ("SELECT pg_catalog.\"right\"('abc'::text, '-2147483648'::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"right\"('日本語テキスト😀'::text, 3::integer) AS v", "スト😀"),
    ("SELECT pg_catalog.\"right\"('abc'::text, '2147483647'::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"right\"('été'::text, 3::integer) AS v", "té"),
    ("SELECT pg_catalog.\"right\"('abc'::text, '0'::integer) AS v", ""),
    ('SELECT pg_catalog."rpad"(NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"rpad\"('abc'::text, 6::integer) AS v", "abc   "),
    ("SELECT pg_catalog.\"rpad\"('Hello World'::text, 2::integer) AS v", "He"),
    ("SELECT pg_catalog.\"rpad\"('12'::text, 5::integer) AS v", "12   "),
    ('SELECT pg_catalog."rpad"(NULL::text, 6::integer) AS v', None),
    ("SELECT pg_catalog.\"rpad\"(''::text, 6::integer) AS v", "      "),
    ("SELECT pg_catalog.\"rpad\"('日本語テキスト😀'::text, 6::integer) AS v", "日本語テキス"),
    ("SELECT pg_catalog.\"rpad\"('été'::text, 6::integer) AS v", "été "),
    ("SELECT pg_catalog.\"rpad\"('  padded  '::text, 6::integer) AS v", "  padd"),
    ('SELECT pg_catalog."rpad"(NULL::text, NULL::integer, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"rpad\"('abc'::text, 6::integer, 'abc'::text) AS v", "abcabc"),
    ("SELECT pg_catalog.\"rpad\"('Hello World'::text, 2::integer, 'Hello World'::text) AS v", "He"),
    ("SELECT pg_catalog.\"rpad\"('12'::text, 5::integer, '12'::text) AS v", "12121"),
    ("SELECT pg_catalog.\"rpad\"(NULL::text, 6::integer, 'abc'::text) AS v", None),
    ("SELECT pg_catalog.\"rpad\"('abc'::text, 6::integer, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"rpad\"(''::text, 6::integer, 'abc'::text) AS v", "abcabc"),
    ("SELECT pg_catalog.\"rpad\"('abc'::text, 6::integer, ''::text) AS v", "abc"),
    (
        "SELECT pg_catalog.\"rpad\"('日本語テキスト😀'::text, 6::integer, 'abc'::text) AS v",
        "日本語テキス",
    ),
    (
        "SELECT pg_catalog.\"rpad\"('abc'::text, 6::integer, '日本語テキスト😀'::text) AS v",
        "abc日本語",
    ),
    ("SELECT pg_catalog.\"rpad\"('été'::text, 6::integer, 'abc'::text) AS v", "étéa"),
    ("SELECT pg_catalog.\"rpad\"('abc'::text, 6::integer, 'été'::text) AS v", "abcét"),
    ('SELECT pg_catalog."substr"(NULL::bytea, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, 3::integer) AS v", "\\xbeef"),
    ("SELECT pg_catalog.\"substr\"('abc'::bytea, (-7)::integer) AS v", "\\x616263"),
    ("SELECT pg_catalog.\"substr\"('\\x00ff'::bytea, 42::integer) AS v", "\\x"),
    ('SELECT pg_catalog."substr"(NULL::bytea, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"substr\"('\\x'::bytea, 3::integer) AS v", "\\x"),
    (
        "SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, '-2147483648'::integer) AS v",
        "\\xdeadbeef",
    ),
    ("SELECT pg_catalog.\"substr\"('\\x00ff00'::bytea, 3::integer) AS v", "\\x00"),
    ("SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, '2147483647'::integer) AS v", "\\x"),
    ("SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, '0'::integer) AS v", "\\xdeadbeef"),
    ('SELECT pg_catalog."substr"(NULL::bytea, NULL::integer, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, 3::integer, 3::integer) AS v", "\\xbeef"),
    ("SELECT pg_catalog.\"substr\"('\\x00ff'::bytea, 42::integer, 42::integer) AS v", "\\x"),
    ('SELECT pg_catalog."substr"(NULL::bytea, 3::integer, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, NULL::integer, 3::integer) AS v", None),
    ("SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, 3::integer, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"substr\"('\\x'::bytea, 3::integer, 3::integer) AS v", "\\x"),
    (
        "SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, '-2147483648'::integer, 3::integer) AS v",
        "\\x",
    ),
    ("SELECT pg_catalog.\"substr\"('\\x00ff00'::bytea, 3::integer, 3::integer) AS v", "\\x00"),
    (
        "SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, '2147483647'::integer, 3::integer) AS v",
        "\\x",
    ),
    ('SELECT pg_catalog."substr"(NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"substr\"('abc'::text, 3::integer) AS v", "c"),
    ("SELECT pg_catalog.\"substr\"('Hello World'::text, (-7)::integer) AS v", "Hello World"),
    ("SELECT pg_catalog.\"substr\"('12'::text, 42::integer) AS v", ""),
    ('SELECT pg_catalog."substr"(NULL::text, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"substr\"('abc'::text, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"substr\"(''::text, 3::integer) AS v", ""),
    ("SELECT pg_catalog.\"substr\"('abc'::text, '-2147483648'::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"substr\"('日本語テキスト😀'::text, 3::integer) AS v", "語テキスト😀"),
    ("SELECT pg_catalog.\"substr\"('abc'::text, '2147483647'::integer) AS v", ""),
    ("SELECT pg_catalog.\"substr\"('été'::text, 3::integer) AS v", "té"),
    ("SELECT pg_catalog.\"substr\"('abc'::text, '0'::integer) AS v", "abc"),
    ('SELECT pg_catalog."substr"(NULL::text, NULL::integer, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"substr\"('abc'::text, 3::integer, 3::integer) AS v", "c"),
    ("SELECT pg_catalog.\"substr\"('12'::text, 42::integer, 42::integer) AS v", ""),
    ('SELECT pg_catalog."substr"(NULL::text, 3::integer, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"substr\"('abc'::text, NULL::integer, 3::integer) AS v", None),
    ("SELECT pg_catalog.\"substr\"('abc'::text, 3::integer, NULL::integer) AS v", None),
    ("SELECT pg_catalog.\"substr\"(''::text, 3::integer, 3::integer) AS v", ""),
    ("SELECT pg_catalog.\"substr\"('abc'::text, '-2147483648'::integer, 3::integer) AS v", ""),
    (
        "SELECT pg_catalog.\"substr\"('日本語テキスト😀'::text, 3::integer, 3::integer) AS v",
        "語テキ",
    ),
    ("SELECT pg_catalog.\"substr\"('abc'::text, '2147483647'::integer, 3::integer) AS v", ""),
    ('SELECT pg_catalog."substring"(NULL::text, NULL::integer, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"substring\"('abc'::text, 1::integer, 2::integer) AS v", "ab"),
    ("SELECT pg_catalog.\"substring\"('Hello World'::text, 2::integer, 3::integer) AS v", "ell"),
    ("SELECT pg_catalog.\"substring\"('12'::text, 1::integer, 1::integer) AS v", "1"),
    ('SELECT pg_catalog."substring"(NULL::text, 1::integer, 2::integer) AS v', None),
    ("SELECT pg_catalog.\"substring\"(''::text, 1::integer, 2::integer) AS v", ""),
    (
        "SELECT pg_catalog.\"substring\"('日本語テキスト😀'::text, 1::integer, 2::integer) AS v",
        "日本",
    ),
    ("SELECT pg_catalog.\"substring\"('été'::text, 1::integer, 2::integer) AS v", "é"),
    ("SELECT pg_catalog.\"substring\"('  padded  '::text, 1::integer, 2::integer) AS v", "  "),
    ('SELECT pg_catalog."text"(NULL::character) AS v', None),
    ("SELECT pg_catalog.\"text\"('abc'::character) AS v", "a"),
    ("SELECT pg_catalog.\"text\"('Hello World'::character) AS v", "H"),
    ("SELECT pg_catalog.\"text\"('12'::character) AS v", "1"),
    ("SELECT pg_catalog.\"text\"(''::character) AS v", ""),
    ("SELECT pg_catalog.\"text\"('日本語テキスト😀'::character) AS v", "日"),
    ("SELECT pg_catalog.\"text\"('été'::character) AS v", "e"),
    ("SELECT pg_catalog.\"text\"('  padded  '::character) AS v", ""),
    ('SELECT pg_catalog."substring"(NULL::bytea, NULL::integer, NULL::integer) AS v', None),
    (
        "SELECT pg_catalog.\"substring\"('\\xdeadbeef'::bytea, 1::integer, 2::integer) AS v",
        "\\xdead",
    ),
    ("SELECT pg_catalog.\"substring\"('abc'::bytea, 2::integer, 3::integer) AS v", "\\x6263"),
    ("SELECT pg_catalog.\"substring\"('\\x00ff'::bytea, 1::integer, 1::integer) AS v", "\\x00"),
    ('SELECT pg_catalog."substring"(NULL::bytea, 1::integer, 2::integer) AS v', None),
    ("SELECT pg_catalog.\"substring\"('\\x'::bytea, 1::integer, 2::integer) AS v", "\\x"),
    ("SELECT pg_catalog.\"substring\"('\\x00ff00'::bytea, 1::integer, 2::integer) AS v", "\\x00ff"),
    ('SELECT pg_catalog."substring"(NULL::text, NULL::integer) AS v', None),
    ("SELECT pg_catalog.\"substring\"('abc'::text, 1::integer) AS v", "abc"),
    ("SELECT pg_catalog.\"substring\"('Hello World'::text, 2::integer) AS v", "ello World"),
    ("SELECT pg_catalog.\"substring\"('12'::text, 1::integer) AS v", "12"),
    ('SELECT pg_catalog."substring"(NULL::text, 1::integer) AS v', None),
    ("SELECT pg_catalog.\"substring\"(''::text, 1::integer) AS v", ""),
    (
        "SELECT pg_catalog.\"substring\"('日本語テキスト😀'::text, 1::integer) AS v",
        "日本語テキスト😀",
    ),
    ("SELECT pg_catalog.\"substring\"('été'::text, 1::integer) AS v", "été"),
    ("SELECT pg_catalog.\"substring\"('  padded  '::text, 1::integer) AS v", "  padded  "),
    ("SELECT pg_catalog.rpad('abc'::text, (-3)::integer, 'x'::text)", ""),
    ("SELECT pg_catalog.lpad('日本'::text, 5::integer, 'ab'::text)", "aba日本"),
    ("SELECT pg_catalog.left('abcdef'::text, (-2)::integer)", "abcd"),
    ("SELECT pg_catalog.right('abcdef'::text, (-2)::integer)", "cdef"),
    ("SELECT pg_catalog.ascii('é'::text)", "233"),
    ("SELECT pg_catalog.ascii(''::text)", "0"),
    ("SELECT pg_catalog.substr('日本語'::text, 0::integer, 2::integer)", "日"),
    ("SELECT pg_catalog.char_length('ab  '::character(6))", "2"),
    ("SELECT pg_catalog.octet_length('é'::character(3))", "4"),
    ("SELECT pg_catalog.text('ab'::character(4))", "ab"),
]
RAISED = [
    ("SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, '2147483648'::integer) AS v", "22003"),
    (
        "SELECT pg_catalog.\"substr\"('\\xdeadbeef'::bytea, 3::integer, '-2147483648'::integer) AS v",
        "22011",
    ),
    (
        "SELECT pg_catalog.\"substr\"('Hello World'::text, (-7)::integer, (-7)::integer) AS v",
        "22011",
    ),
    ("SELECT pg_catalog.\"substr\"('abc'::text, 3::integer, '-2147483648'::integer) AS v", "22011"),
    ("SELECT pg_catalog.lpad('abc'::text, 536870912::integer)", "54000"),
]

#: scripts/conformance/function_support.py's FLOAT_TOLERANCE.
_TOLERANCE = 1e-14


def _real(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _same(want, got) -> bool:
    """PostgreSQL's text of a value against the engine's: a boolean t or f (ClickHouse's 1 or 0),
    a float by its value (NaN equal to NaN), a real by its value as a real, a double within 2
    units in the last place (libm's trigonometry and pow differ by one)."""
    if want is None or got is None:
        return want is got
    if isinstance(got, bytes):
        return want == "\\x" + got.hex()
    if want.startswith("\\x") and isinstance(got, str):
        return want == "\\x" + got.encode().hex()  # a bytea chdb reads as UTF-8 text
    if want in ("t", "f") and isinstance(got, (bool, int)):
        return (want == "t") == bool(got)
    if isinstance(got, float):
        expected = float(want)
        if math.isnan(expected) or math.isnan(got):
            return math.isnan(expected) and math.isnan(got)
        if expected == got or _real(expected) == got:
            return True
        return math.isfinite(expected) and abs(expected - got) <= _TOLERANCE * max(
            abs(expected), abs(got)
        )
    if isinstance(got, int):
        return int(want) == got
    return want == str(got)


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


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT left(t, 3) FROM x",
        "SELECT substr(t, 2, 3) FROM x",
        "SELECT lpad(t, 6, 'ab') FROM x",
        "SELECT reverse(t) FROM x",
    ],
)
def test_trino_keeps_its_form(sql):
    assert "UTF8" not in transpile(sql, "trino")
