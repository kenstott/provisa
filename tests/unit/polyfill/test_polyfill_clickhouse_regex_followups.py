# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e1d92-6c3a-4f58-b0e7-2d9a5c81f604
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What ClickHouse's RE2 answered otherwise than PostgreSQL 17 (REQ-2236), each answered on the
embedded engine (chdb) as PostgreSQL answers it, or refused by name with its reason:

- substring(text FROM pattern) was written as a substring from a position (ClickHouse's error of
  max2 of a text), and SIMILAR TO went to ClickHouse as written (its syntax error);
- regexp_replace of a start and an Nth match read its pattern as RE2 does at run time (\\b a word
  boundary, not PostgreSQL's backspace);
- a pattern that is no literal went to RE2 as written: its escapes and its . read as RE2 reads them;
- RE2 matches leftmost-first where PostgreSQL matches the longest (or, of a pattern whose quantifiers
  are all non-greedy, the shortest): regexp_substr('ab', 'a|ab') was 'a', PostgreSQL's 'ab'."""

# Requirements: REQ-2236

from __future__ import annotations

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler import are_re2
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

#: (statement, PostgreSQL's text of its one row)
ANSWERED = [
    # 1. substring(text FROM pattern): the first group if the pattern has one, else the match.
    ("SELECT substring('abc' from 'b.') AS v", "bc"),
    ("SELECT substring('abc', 'b.') AS v", "bc"),
    ("SELECT substring('abc' from 'a(b)c') AS v", "b"),
    ("SELECT substring('abcabc' from 'c(a)') AS v", "a"),
    ("SELECT substring('abc' from '(?:b)c') AS v", "bc"),
    ("SELECT substring(E'a\\bc' from '\\b.') AS v", "\bc"),
    ("SELECT substring('foob' from 'o(b)?$') AS v", "b"),
    ("SELECT substring(E'a\\nb' from 'a.b') AS v", "a\nb"),
    # SIMILAR TO: % any text, _ any character, the rest a regular expression anchored at both ends.
    ("SELECT 'ABC' SIMILAR TO '%B%' AS v", "t"),
    ("SELECT 'abc' SIMILAR TO 'a_c' AS v", "t"),
    ("SELECT 'abc' SIMILAR TO 'a.c' AS v", "f"),
    ("SELECT 'a.c' SIMILAR TO 'a.c' AS v", "t"),
    ("SELECT 'abc' SIMILAR TO '(a|x)bc' AS v", "t"),
    ("SELECT 'abc' SIMILAR TO 'b' AS v", "f"),
    ("SELECT 'abc' NOT SIMILAR TO 'b' AS v", "t"),
    ("SELECT 'a%' SIMILAR TO 'a\\%' AS v", "t"),
    ("SELECT 'ab' SIMILAR TO 'a\\%' AS v", "f"),
    ("SELECT 'a%' SIMILAR TO 'a#%' ESCAPE '#' AS v", "t"),
    ("SELECT 'x' SIMILAR TO 'a#_' ESCAPE '#' AS v", "f"),
    ("SELECT 'a_' SIMILAR TO 'a#_' ESCAPE '#' AS v", "t"),
    ("SELECT 'ab' NOT SIMILAR TO 'a#_' ESCAPE '#' AS v", "t"),
    ("SELECT 'a\\b' SIMILAR TO 'a\\b' ESCAPE '' AS v", "t"),
    ("SELECT 'aaa' SIMILAR TO 'a{3}' AS v", "t"),
    ("SELECT 'abab' SIMILAR TO '(ab)+' AS v", "t"),
    ("SELECT 'a^b' SIMILAR TO 'a^b' AS v", "t"),
    ("SELECT 'ab' SIMILAR TO '[ab]%' AS v", "t"),
    ("SELECT E'a\\nb' SIMILAR TO 'a_b' AS v", "t"),
    ("SELECT 'abc' SIMILAR TO 'a#\"b#\"c' ESCAPE '#' AS v", "t"),
    # substring(text FROM pattern FOR escape): the text where all of it matches.
    ("SELECT substring('abc' from 'a%' for '#') AS v", "abc"),
    # 2. regexp_replace from a start, its Nth match: the pattern read as PostgreSQL reads it.
    ("SELECT regexp_replace(E'a\\bc', '\\b', 'X', 1, 1) AS v", "aXc"),
    ("SELECT regexp_replace(E'a\\bc\\b', '\\b', 'X', 1, 2) AS v", "a\bcX"),
    ("SELECT regexp_replace('abcb', 'b', 'X', 1, 2) AS v", "abcX"),
    ("SELECT regexp_replace('abcb', 'b', 'X', 1, 0) AS v", "aXcX"),
    ("SELECT regexp_replace('abcb', 'b', 'X', 3) AS v", "abcX"),
    ("SELECT regexp_replace(E'a\\x0bb', '\\s', '-', 1, 1) AS v", "a-b"),
    ("SELECT regexp_replace('a=b', '\\x3D', '-', 1, 1) AS v", "a-b"),
    ("SELECT regexp_replace(E'a\\nb', 'a.b', 'X', 1, 1) AS v", "X"),
    # 3. A pattern that is no literal: PostgreSQL's reading where RE2's is the same.
    ("SELECT a ~ b AS v FROM (VALUES ('abc', 'b')) AS t(a, b)", "t"),
    ("SELECT a ~ b AS v FROM (VALUES (E'a\\nc', 'a.c')) AS t(a, b)", "t"),
    ("SELECT a ~ b AS v FROM (VALUES ('a.c', 'a\\.c')) AS t(a, b)", "t"),
    ("SELECT a ~ b AS v FROM (VALUES ('abc', '\\d')) AS t(a, b)", "f"),
    ("SELECT a !~ b AS v FROM (VALUES ('abc', 'x')) AS t(a, b)", "t"),
    ("SELECT a ~* b AS v FROM (VALUES ('ABC', 'b')) AS t(a, b)", "t"),
    ("SELECT regexp_like(a, b) AS v FROM (VALUES (E'a\\nc', 'a.c')) AS t(a, b)", "t"),
    ("SELECT a ~ b AS v FROM (VALUES ('ab', 'a|ab')) AS t(a, b)", "t"),
    ("SELECT a ~ b AS v FROM (VALUES ('abc', '[[:alpha:]]+')) AS t(a, b)", "t"),
    ("SELECT regexp_replace(a, b, 'X', 'g') AS v FROM (VALUES ('abab', 'b')) AS t(a, b)", "aXaX"),
    ("SELECT regexp_replace(a, b, 'X', 'g') AS v FROM (VALUES (E'a\\nb', 'a.b')) AS t(a, b)", "X"),
    # 4. PostgreSQL's longest match among alternatives, where an order of them is exact.
    ("SELECT regexp_substr('ab', 'a|ab') AS v", "ab"),
    ("SELECT regexp_replace('ab', 'a|ab', 'X') AS v", "X"),
    ("SELECT regexp_replace('abab', 'a|ab', 'X', 'g') AS v", "XX"),
    ("SELECT regexp_count('abab', 'a|ab') AS v", "2"),
    ("SELECT regexp_instr('xab', 'a|ab', 1, 1, 1) AS v", "4"),
    ("SELECT substring('ab' from 'a|ab') AS v", "ab"),
    ("SELECT substring('xab' from 'x(a|ab)') AS v", "ab"),
    ("SELECT regexp_split_to_array('1ab2', 'a|ab') AS v", "{1,2}"),
    ("SELECT regexp_like('ab', 'a|ab') AS v", "t"),
    ("SELECT regexp_like('aabbb', 'a+?b+') AS v", "t"),
    ("SELECT regexp_substr('aaa', 'a+?') AS v", "a"),
    ("SELECT regexp_substr('aaab', 'a*b') AS v", "aaab"),
]

#: Statements PostgreSQL answers NULL.
NULLS = [
    "SELECT substring('abc' from 'x') AS v",
    "SELECT substring(NULL::text from 'b') AS v",
    "SELECT substring('abc' from NULL::text) AS v",
    "SELECT substring('abc' from 'a' for '#') AS v",
    "SELECT NULL::text SIMILAR TO 'a' AS v",
]

#: (statement, SQLSTATE): PostgreSQL's error, or a form's 0A000 within its stated bound.
RAISED = [
    ("SELECT 'a' SIMILAR TO 'a' ESCAPE 'ab' AS v", "22025"),
    # A word boundary in the Nth match's form, which reads the match apart from its text.
    ("SELECT regexp_replace('ab b', '\\yb', 'X', 1, 1) AS v", "0A000"),
    ("SELECT regexp_replace('été', '\\w', 'X', 1, 1) AS v", "0A000"),
    # A first group that may match nothing, where the match leaves it empty: no group or ''.
    ("SELECT substring('foobar' from 'o(b)?') AS v", "0A000"),
    # 3. A pattern that is no literal holding what RE2 reads otherwise.
    ("SELECT a ~ b AS v FROM (VALUES (E'a\\bc', '\\b')) AS t(a, b)", "0A000"),
    ("SELECT a ~ b AS v FROM (VALUES ('abc', '\\y')) AS t(a, b)", "0A000"),
    ("SELECT a ~ b AS v FROM (VALUES ('abc', '(?=b)')) AS t(a, b)", "0A000"),
    ("SELECT a ~ b AS v FROM (VALUES ('aa', '(a)\\1')) AS t(a, b)", "0A000"),
    ("SELECT a ~ b AS v FROM (VALUES ('été', '\\w')) AS t(a, b)", "0A000"),
    ("SELECT regexp_like(a, b) AS v FROM (VALUES ('ab', '[[.a.]]')) AS t(a, b)", "0A000"),
    ("SELECT regexp_replace(a, b, 'X', 'g') AS v FROM (VALUES ('abc', '\\y')) AS t(a, b)", "0A000"),
    # 4. ... and alternatives or quantifiers of RE2's leftmost-first choice.
    ("SELECT regexp_replace(a, b, 'X', 'g') AS v FROM (VALUES ('ab', 'a|ab')) AS t(a, b)", "0A000"),
    (
        "SELECT regexp_replace(a, b, 'X', 'g') AS v FROM (VALUES ('aab', 'a+?b+')) AS t(a, b)",
        "0A000",
    ),
    (
        "SELECT regexp_replace(a, b, 'X', 'g') AS v FROM (VALUES ('abab', '(ab)*')) AS t(a, b)",
        "0A000",
    ),
]

#: (statement, the reason the refusal names)
REFUSED = [
    ("SELECT regexp_substr('abcd', '(a|ab)(c|bcd)') AS v", "of alternatives"),
    ("SELECT regexp_replace('ab', 'a|b*', 'X') AS v", "of alternatives"),
    ("SELECT regexp_count('ab', '(a)|(ab)') AS v", "of alternatives"),
    ("SELECT regexp_substr('aabbb', 'a+?b+') AS v", "of quantifiers of mixed greediness"),
    ("SELECT regexp_instr('aabbb', 'a*b+?') AS v", "of quantifiers of mixed greediness"),
    ("SELECT regexp_substr('abcd', '(ab)?(abcd)?') AS v", "of a group repeated"),
    ("SELECT substring('abcd' from '(ab)?(abcd)?') AS v", "of a group repeated"),
    ("SELECT regexp_replace('aa', '(a)\\1', 'X', 1, 1) AS v", "of a back reference"),
    ("SELECT substring('abc' from '%#\"b#\"%' for '#') AS v", "of escape-double-quote separators"),
    (
        "SELECT a SIMILAR TO b AS v FROM (VALUES ('a', 'a')) AS t(a, b)",
        "of a pattern that is no literal",
    ),
    (
        "SELECT substring(a from b::text) AS v FROM (VALUES ('a', 'a')) AS t(a, b)",
        "of a pattern that is no literal",
    ),
    (
        "SELECT regexp_count(a, b) AS v FROM (VALUES ('a', 'a')) AS t(a, b)",
        "of a pattern that is no literal",
    ),
]


def _text(value: object) -> str:
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (list, tuple)):
        return "{" + ",".join(str(v) for v in value) + "}"
    return str(value)


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())
    return value


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_as_postgresql_does(runtime, sql, want):
    got = _answer(runtime, sql)
    assert got is not None and _text(got) == want, (want, got)


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


@pytest.mark.parametrize(("sql", "why"), REFUSED)
def test_what_has_no_exact_form_is_refused_with_its_reason(sql, why):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse", generated=False)
    assert refused.value.sqlstate == "0A000"
    assert why in str(refused.value), str(refused.value)


# --- the translation itself ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("pattern", "written"),
    [
        ("a|ab", "(?s:ab|a)"),
        ("(a|ab)c", "(?s:(ab|a)c)"),
        ("x(?:b|abc|ab)", "(?s:x(?:abc|ab|b))"),
        ("ab|a", "(?s:ab|a)"),
    ],
)
def test_alternatives_of_fixed_widths_are_written_longest_first(pattern, written):
    re2 = are_re2.translated(pattern)
    assert re2.text == written and re2.leftmost is None


@pytest.mark.parametrize(
    ("pattern", "why"),
    [
        ("a|b*", are_re2.ALTERNATIVES),
        ("(a|ab)(c|bcd)", are_re2.ALTERNATIVES),
        ("(a)|(ab)", are_re2.ALTERNATIVES),
        ("a+?b+", are_re2.MIXED_GREEDINESS),
        ("a{2}?b*", are_re2.MIXED_GREEDINESS),
        ("(ab)*c", are_re2.REPEATED_GROUP),
        ("(ab)?c", are_re2.REPEATED_GROUP),
    ],
)
def test_a_match_rE2_chooses_otherwise_is_named(pattern, why):
    assert are_re2.translated(pattern).leftmost == why


@pytest.mark.parametrize("pattern", ["a*b", "a+?b*?", "[ab]{1,3}c?", "(ab){2}c", "(a)(b)", "abc"])
def test_a_match_rE2_chooses_as_postgresql_does_is_not_named(pattern):
    assert are_re2.translated(pattern).leftmost is None


@pytest.mark.parametrize(
    ("pattern", "optional"),
    [("a(b)c", False), ("a(b)?c", True), ("(a)|b", True), ("((a)b)*", True), ("(a|ab)", False)],
)
def test_whether_the_first_group_may_match_nothing(pattern, optional):
    assert are_re2.translated(pattern).first_group_optional is optional


@pytest.mark.parametrize(
    ("pattern", "escape", "written"),
    [
        ("%B%", "\\", "^(?:.*B.*)$"),
        ("a_c", "\\", "^(?:a.c)$"),
        ("a.c", "\\", "^(?:a\\.c)$"),
        ("(a|b)", "\\", "^(?:(?:a|b))$"),
        ("a\\%", "\\", "^(?:a\\%)$"),
        ("a#%", "#", "^(?:a\\%)$"),
        ("a\\b", "", "^(?:a\\\\b)$"),
        ("[%_]", "\\", "^(?:[%_])$"),
        ('a#"b#"c', "#", "^(?:a){1,1}?(b){1,1}(?:c)$"),
    ],
)
def test_similar_to_as_postgresql_writes_its_regular_expression(pattern, escape, written):
    assert are_re2.similar_to(pattern, escape) == written


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT regexp_replace('abcb', 'B', 'X', 1, 2, 'i') AS v",
        "SELECT regexp_replace('abcb', 'b', 'X', 1, 2, 'g') AS v",
        "SELECT regexp_replace(a, b, 'X', 1, 1) AS v FROM (VALUES ('ab', 'b')) AS t(a, b)",
    ],
)
def test_six_arguments_and_a_varying_nth_pattern_stay_refused_by_name(sql):
    """REQ-2167 (amended 2026-11-02): regexp_replace of six arguments is refused by name, and so is
    the Nth match of a pattern that varies on ClickHouse, before the grid reads the call."""
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse", generated=False)
    assert refused.value.sqlstate == "0A000"
    assert "regexp_replace" in str(refused.value)
