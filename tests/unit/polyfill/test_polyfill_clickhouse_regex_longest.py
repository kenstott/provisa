# Copyright (c) 2026 Kenneth Stott
# Canary: 9c2e5a71-3d8b-4f06-a1e4-6b7d0f2c8e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's longest (or shortest) match where RE2 chooses another at the same start
(REQ-2236), and regexp_replace of six arguments (REQ-2167, amended 2026-11-25), on the embedded
engine (chdb). Each answer below is PostgreSQL 16's.

A pattern of alternatives, of quantifiers of mixed greediness or of a group repeated a varying
number of times was refused by name where the function answers the match: RE2 matches
leftmost-first, PostgreSQL the longest at the leftmost start (the shortest where the whole pattern
is non-greedy). Each match is now found as PostgreSQL finds it, on text of LONGEST_BOUND characters
at most; beyond it the call raises 0A000 by name. regexp_replace(text, pattern, replacement,
start, n, flags) was refused by name on every engine; ClickHouse reads its flags' option letters
(g beside n changes nothing, as in PostgreSQL)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler import are_re2
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import regex
from provisa.transpiler.transpile import transpile

#: (statement, PostgreSQL's answer)
ANSWERED = [
    # The first match: the longest at the leftmost start.
    ("SELECT regexp_substr('abcd', '(a|ab)(c|bcd)') AS v", "abcd"),
    ("SELECT regexp_substr('abcd', '(ab)?(abcd)?') AS v", "abcd"),
    ("SELECT regexp_substr('xxabcd', '(ab)?(abcd)?', 3) AS v", "abcd"),
    ("SELECT regexp_substr('ab', 'a|ab|^b') AS v", "ab"),
    ("SELECT substring('abcd' from '(?:ab)?(?:abcd)?') AS v", "abcd"),
    # ... the shortest where the whole pattern's first quantifier is non-greedy.
    ("SELECT regexp_substr('aabbb', 'a+?b+') AS v", "aab"),
    ("SELECT substring('xaabbb' from 'a+?b+') AS v", "aab"),
    ("SELECT regexp_instr('aabbb', 'a*b+?') AS v", 1),
    # Every match after it, searched from the end of the one before.
    ("SELECT regexp_instr('xabcdab', '(ab)*(abcd)?', 1, 2, 1) AS v", 6),
    ("SELECT regexp_count('ab', '(a)|(ab)') AS v", 1),
    ("SELECT regexp_count('abcabcab', '(?:abc|ab)+?c?') AS v", 3),
    ("SELECT regexp_count('ab ab', '\\yab?|b') AS v", 2),
    # regexp_replace: the first match, every match, the Nth from a start.
    ("SELECT regexp_replace('ab', 'a|b*', 'X') AS v", "Xb"),
    ("SELECT regexp_replace('aabbb', 'a+?b+', 'X') AS v", "Xbb"),
    ("SELECT regexp_replace('abcd abcd', '(?:a|ab)(?:c|bcd)', '[\\&]', 'g') AS v", "[abcd] [abcd]"),
    ("SELECT regexp_replace('abc', 'b*|x', 'X', 'g') AS v", "XaXXcX"),
    ("SELECT regexp_replace('abab ab', 'a|ab*?', '-', 1, 0) AS v", "-- -"),
    ("SELECT regexp_replace('abab ab', 'a|ab*?', '-', 3, 2) AS v", "abab -"),
    # Six arguments: the flags' option letters read; g beside n changes nothing.
    ("SELECT regexp_replace('abcb', 'B', 'X', 1, 2, 'i') AS v", "abcX"),
    ("SELECT regexp_replace('abcb', 'b', 'X', 1, 2, 'g') AS v", "abcX"),
    ("SELECT regexp_replace('aBAb', 'a|ab*', '-', 1, 2, 'gi') AS v", "aB-"),
    ("SELECT regexp_replace(E'a\\nb', 'a.b', 'X', 1, 1, 'n') AS v", "a\nb"),
    # A constant pattern that is no literal: RE2's reading, where its run-time checks pass.
    ("SELECT regexp_replace('abcb', lower('B'), 'X', 1, 2, 'i') AS v", "abcX"),
    ("SELECT regexp_replace('abc', lower(''), 'abc', 3, 3) AS v", "abc"),
]

NULLS = [
    "SELECT regexp_replace('abcb', 'b', 'X', 1, 2, NULL) AS v",
    "SELECT regexp_substr(NULL::text, 'a|b*') AS v",
]

_LONG = "a" * (regex.LONGEST_BOUND + 1)

#: (statement, SQLSTATE)
RAISED = [
    (f"SELECT regexp_substr('{_LONG}', 'a|b*') AS v", "0A000"),
    (f"SELECT regexp_replace('{_LONG}', 'a+?b*', 'X', 'g') AS v", "0A000"),
    # The Nth match's form of a pattern RE2 matches as PostgreSQL does: its stated bound.
    ("SELECT regexp_replace('abc', 'b', 'X', 0, 1, 'i') AS v", "0A000"),
    ("SELECT regexp_replace('abc', 'a|b*', 'X', 0, 1) AS v", "22023"),
    ("SELECT regexp_replace('abc', 'b', 'X', 1, 1, 'z') AS v", "22023"),
    ("SELECT regexp_replace('abcb', lower('\\y'), 'X', 1, 2) AS v", "0A000"),
    ("SELECT regexp_replace('abab', lower('A|AB'), 'X', 1, 1) AS v", "0A000"),
]

#: (statement, the reason the refusal names): a group's part of the match, which PostgreSQL
#: chooses by the group's own preference.
REFUSED = [
    ("SELECT substring('abcd' from '(ab)?(abcd)?') AS v", "of a group repeated"),
    ("SELECT regexp_replace('ab', '(a|ab)(c|b)?', '\\1', 'g') AS v", "of alternatives"),
    (
        "SELECT regexp_replace(a, 'b', 'X', 1, 1, f) AS v FROM (VALUES ('ab', 'i')) AS t(a, f)",
        "regexp_replace",
    ),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())
    return value


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_finds_the_match_postgresql_finds(runtime, sql, want):
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
        _answer(runtime, f"SELECT regexp_count('{_LONG}', 'a|b*') AS v")
    assert regex.LONGEST_BEYOND.message in str(raised.value)


def test_text_at_the_bound_is_answered(runtime):
    text = "ab" * (regex.LONGEST_BOUND // 2)
    assert _answer(runtime, f"SELECT regexp_count('{text}', 'a|ab*?') AS v") == (
        regex.LONGEST_BOUND // 2
    )


@pytest.mark.parametrize(("sql", "why"), REFUSED)
def test_a_group_s_part_stays_refused_by_name(sql, why):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse", generated=False)
    assert refused.value.sqlstate == "0A000"
    assert why in str(refused.value), str(refused.value)


@pytest.mark.parametrize(
    ("pattern", "shortest"),
    [
        ("a|ab", False),
        ("a+?b+", True),
        ("a*b+?", False),
        ("(a+?)", True),
        ("x(a+?|b)", False),  # alternatives prefer the longest
        ("a+?|b", False),
        ("a{2}?b*?", True),  # a count alone keeps its atom's preference: none
        ("a{2}b+?", True),
        ("a{2,2}?b*", True),  # {m,m}? is non-greedy
        ("((a*?)b)", True),
        ("abc", False),
    ],
)
def test_the_whole_pattern_s_preference_is_postgresql_s(pattern, shortest):
    assert are_re2.translated(pattern).shortest is shortest


@pytest.mark.parametrize(
    ("call", "name"),
    [
        ("regexp_replace(a, 'b', 'X', 1, 1)", "regexp_replace"),
        ("regexp_count(a, 'a|b*')", "regexp_count"),
        ("regexp_substr(a, 'b')", "regexp_substr"),
    ],
)
def test_the_column_is_named_for_the_function(call, name):
    """As PostgreSQL names it, not for the grid row that writes it ("regexp_replace from a
    start", "regular expressions on ClickHouse")."""
    written = transpile(f"SELECT {call} FROM t", "clickhouse", generated=False)
    assert written.endswith(f" AS {name} FROM t"), written[-80:]
