# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8d2b61-7a3e-4c95-b0d7-2e6a9c1f5b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pattern that can match the empty text, on the embedded engine (chdb) (REQ-2236). Each answer
below is PostgreSQL 16's.

PostgreSQL finds an empty match where ClickHouse's countMatches, extractAll and replaceRegexpAll go
past it, so regexp_count, regexp_instr, regexp_substr and regexp_replace with the g flag and a
replacement holding a backslash raised 0A000 by name on such a pattern. Its matches are now found
one by one as PostgreSQL finds them, on text of LONGEST_BOUND characters at most (0A000 by name
beyond); a pattern that cannot match the empty text keeps RE2's form, on text of any length. A
replacement naming one of the pattern's groups still raises: PostgreSQL chooses a group's part of
an empty match by the group's own preference."""

# Requirements: REQ-2236

from __future__ import annotations

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import regex
from provisa.transpiler.transpile import transpile

#: (statement, PostgreSQL's answer)
ANSWERED = [
    # regexp_count: each empty match counted, then the search goes one character on.
    ("SELECT regexp_count('abc', 'b*') AS v", 4),
    ("SELECT regexp_count('abc', 'b*', 2) AS v", 3),
    ("SELECT regexp_count('', 'x*') AS v", 1),
    ("SELECT regexp_count('a b', '\\y') AS v", 4),
    ("SELECT regexp_count('abc', '$') AS v", 1),
    ("SELECT regexp_count(E'a\\nb', '^', 1, 'n') AS v", 2),
    ("SELECT regexp_count('aaa', 'a*?') AS v", 4),
    # regexp_substr: the Nth match, empty or not.
    ("SELECT regexp_substr('abc', 'b*', 1, 2) AS v", "b"),
    ("SELECT regexp_substr('xbbx', 'b*', 2) AS v", "bb"),
    ("SELECT regexp_substr('日本', 'x*', 1, 3) AS v", ""),
    ("SELECT regexp_substr('aaa', 'a*?', 1, 2) AS v", ""),
    # regexp_instr: the Nth match's start, or its end.
    ("SELECT regexp_instr('abc', 'b*', 1, 2) AS v", 2),
    ("SELECT regexp_instr('abc', 'b*', 1, 2, 1) AS v", 3),
    ("SELECT regexp_instr('abc', 'b*', 1, 4) AS v", 4),
    ("SELECT regexp_instr('abc', 'b*', 1, 5) AS v", 0),
    ("SELECT regexp_instr('abc', 'c?$') AS v", 3),
    # regexp_replace with g, a replacement holding a backslash: \& the match, \\ a backslash, a
    # backslash before anything else itself, \N nothing past the pattern's groups.
    ("SELECT regexp_replace('abc', 'b*', '[\\&]', 'g') AS v", "[]a[b][]c[]"),
    ("SELECT regexp_replace('abc', 'x*', '\\\\', 'g') AS v", "\\a\\b\\c\\"),
    ("SELECT regexp_replace('abc', 'x*', '\\x', 'g') AS v", "\\xa\\xb\\xc\\x"),
    ("SELECT regexp_replace('abc', 'x*', '<\\1>', 'g') AS v", "<>a<>b<>c<>"),
    ("SELECT regexp_replace('abc', '(b)*', '<\\2>', 'g') AS v", "<>a<><>c<>"),
    ("SELECT regexp_replace('aBc', 'b*', '[\\&]', 'gi') AS v", "[]a[B][]c[]"),
    ("SELECT regexp_replace('', 'x*', '[\\&]', 'g') AS v", "[]"),
    # A pattern that cannot match the empty text keeps RE2's form.
    ("SELECT regexp_replace('ab', 'b', '[\\&]', 'g') AS v", "a[b]"),
    # A start of a type that holds NULL.
    ("SELECT regexp_count('abc', 'b*', 2::integer) AS v", 3),
    ("SELECT regexp_substr('abcd', 'a|ab', 1::integer) AS v", "ab"),
    # A column text.
    ("SELECT regexp_count(a, 'b*') AS v FROM (VALUES ('abc')) AS t(a)", 4),
    (
        "SELECT regexp_replace(a, 'b*', '[\\&]', 'g') AS v FROM (VALUES ('abc')) AS t(a)",
        "[]a[b][]c[]",
    ),
]

NULLS = [
    "SELECT regexp_substr('abc', 'b*', 1, 5) AS v",
    "SELECT regexp_count(NULL::text, 'b*') AS v",
    "SELECT regexp_count('abc', 'b*', b) AS v FROM (VALUES (NULL::integer)) AS t(b)",
    "SELECT regexp_instr('abc', 'b*', 1, n) AS v FROM (VALUES (NULL::integer)) AS t(n)",
    "SELECT regexp_replace(NULL::text, 'b*', '[\\&]', 'g') AS v",
]

_LONG = "a" * (regex.LONGEST_BOUND + 1)

#: (statement, SQLSTATE)
RAISED = [
    (f"SELECT regexp_count('{_LONG}', 'b*') AS v", "0A000"),
    (f"SELECT regexp_substr('{_LONG}', 'b*') AS v", "0A000"),
    (f"SELECT regexp_instr('{_LONG}', 'b*') AS v", "0A000"),
    (f"SELECT regexp_replace('{_LONG}', 'b*', '[\\&]', 'g') AS v", "0A000"),
    # A replacement naming one of the pattern's groups.
    ("SELECT regexp_replace('abc', '(x*)', '<\\1>', 'g') AS v", "0A000"),
    # The arguments' own errors.
    ("SELECT regexp_count('abc', 'b*', 0) AS v", "22023"),
    ("SELECT regexp_instr('abc', 'b*', 1, 0) AS v", "22023"),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())
    return value


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_finds_each_empty_match_postgresql_finds(runtime, sql, want):
    assert _answer(runtime, sql) == want


@pytest.mark.parametrize("sql", NULLS)
def test_clickhouse_answers_null_where_postgresql_does(runtime, sql):
    assert _answer(runtime, sql) is None


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error_or_its_stated_bound(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    if isinstance(raised.value, FunctionRefused):
        assert raised.value.sqlstate == state, raised.value
        return
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


def test_the_bound_is_named(runtime):
    with pytest.raises(Exception) as raised:
        _answer(runtime, f"SELECT regexp_count('{_LONG}', 'x*') AS v")
    assert regex.LONGEST_BEYOND.message in str(raised.value)


@pytest.mark.parametrize(
    "call",
    [
        "regexp_count(a, 'b')",
        "regexp_substr(a, 'b', 1, 2)",
        "regexp_instr(a, 'b', 1, 2)",
        "regexp_replace(a, 'b', '[\\&]', 'g')",
    ],
)
def test_a_pattern_that_cannot_match_the_empty_text_is_not_bounded(runtime, call):
    text = "ab" * regex.LONGEST_BOUND
    got = _answer(runtime, f"SELECT {call} AS v FROM (VALUES ('{text}')) AS t(a)")
    assert got is not None
