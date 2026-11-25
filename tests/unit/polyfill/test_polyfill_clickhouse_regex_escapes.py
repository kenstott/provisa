# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2b4c71-3d5a-4f90-a6c2-91d7e0b5f348
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's regular-expression escapes on ClickHouse (REQ-2236): each answered on the embedded
engine (chdb) as PostgreSQL 17 answers it (functions-matching, 9.7.3.3 to 9.7.3.5).

ClickHouse matches by RE2, which read \\b as a word boundary where PostgreSQL's advanced
expressions read a backspace, and rejected \\y \\m \\M \\Z; the pattern went to RE2 as written. Each
call here is a pattern read as PostgreSQL reads it (provisa.transpiler.are_re2): an escape RE2
cannot express is refused by name, a pattern PostgreSQL rejects raises its error, and a locale's
class on text beyond ASCII raises 0A000."""

# Requirements: REQ-2236

from __future__ import annotations

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler import are_re2
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [  # (statement, PostgreSQL's text of its one row)
    # \b is a backspace, \B a backslash: never a word boundary.
    ("SELECT regexp_replace(E'a\\bc', '\\b', 'X', 'g') AS v", "aXc"),
    ("SELECT regexp_replace(E'a\\bc', '\\b', 'X') AS v", "aXc"),
    ("SELECT regexp_replace('abc', '\\b', 'X', 'g') AS v", "abc"),
    ("SELECT regexp_replace('abc', '\\b', 'X') AS v", "abc"),
    ("SELECT regexp_replace('a\\b', '\\B', 'X', 'g') AS v", "aXb"),
    ("SELECT regexp_like(E'a\\bc', '\\b') AS v", "t"),
    ("SELECT regexp_like('abc', '\\b') AS v", "f"),
    ("SELECT 'abc' ~ '\\b' AS v", "f"),
    ("SELECT 'abc' !~ '\\b' AS v", "t"),
    ("SELECT E'a\\bc' ~ '\\b' AS v", "t"),
    ("SELECT regexp_count(E'a\\bc\\b', '\\b') AS v", "2"),
    ("SELECT regexp_instr(E'a\\bc', '\\b') AS v", "2"),
    ("SELECT regexp_substr(E'a\\bc', '.\\b') AS v", "a\b"),
    ("SELECT regexp_split_to_array(E'a\\bc', '\\b') AS v", "{a,c}"),
    # The other character-entry escapes.
    ("SELECT regexp_replace(E'a\\x07b', '\\a', '-') AS v", "a-b"),
    ("SELECT regexp_replace(E'a\\x1bb', '\\e', '-') AS v", "a-b"),
    ("SELECT regexp_replace(E'a\\x01b', '\\cA', '-') AS v", "a-b"),
    ("SELECT regexp_replace(E'a\\x0bb', '\\v', '-') AS v", "a-b"),
    ("SELECT regexp_replace('a=b', '\\x3D', '-') AS v", "a-b"),
    ("SELECT regexp_replace('a=b', '\\u003D', '-') AS v", "a-b"),
    ("SELECT regexp_replace('a=b', '\\U0000003D', '-') AS v", "a-b"),
    ("SELECT regexp_replace('a=b', '\\075', '-') AS v", "a-b"),
    ("SELECT regexp_replace(E'a\\nb', '\\12', '-') AS v", "a-b"),
    ("SELECT regexp_replace('a[b', '\\x5B', '-') AS v", "a-b"),
    ("SELECT regexp_replace('a{b', '{b', '-') AS v", "a-"),
    # Constraint escapes: \A, \Z, \y, \Y, \m, \M, [[:<:]], [[:>:]].
    ("SELECT regexp_replace('abc', 'c\\Z', 'X') AS v", "abX"),
    ("SELECT regexp_replace('abc', '\\Ab', 'X') AS v", "abc"),
    ("SELECT regexp_like('ab', 'a\\Z') AS v", "f"),
    ("SELECT regexp_like('ba', 'a\\Z') AS v", "t"),
    ("SELECT regexp_replace('a b', '\\y', 'X', 'g') AS v", "XaX XbX"),
    ("SELECT regexp_replace('ab b', '\\yb', 'X', 'g') AS v", "ab X"),
    ("SELECT regexp_replace('ab b', '\\Yb', 'X', 'g') AS v", "aX b"),
    ("SELECT 'a b' ~ '\\yb' AS v", "t"),
    ("SELECT 'ab' ~ '\\yb' AS v", "f"),
    ("SELECT regexp_replace('ab cd', '\\mc', 'X', 'g') AS v", "ab Xd"),
    ("SELECT regexp_replace('ab cb', '\\m[a-z]+b\\M', 'X', 'g') AS v", "X X"),
    ("SELECT regexp_replace('ab cd', 'b\\M', 'X', 'g') AS v", "aX cd"),
    ("SELECT regexp_replace('abc abc', '[[:<:]]a', 'X', 'g') AS v", "Xbc Xbc"),
    ("SELECT regexp_replace('cab cab', 'b[[:>:]]', 'X', 'g') AS v", "caX caX"),
    ("SELECT regexp_count('ab ab', '\\mab\\M') AS v", "2"),
    # Class shorthands, in and out of a bracket expression.
    ("SELECT regexp_replace(E'a\\x0bb', '\\s', '-') AS v", "a-b"),
    ("SELECT regexp_like(E'a\\x0bb', 'a\\sb') AS v", "t"),
    ("SELECT regexp_replace(E'a\\x0bb', '\\S+', '-', 'g') AS v", "-\x0b-"),
    ("SELECT regexp_replace('a_b-c', '\\w+', 'X', 'g') AS v", "X-X"),
    ("SELECT regexp_replace('a_b-c', '\\W', 'X', 'g') AS v", "a_bXc"),
    ("SELECT regexp_replace('a1b', '\\d', 'X') AS v", "aXb"),
    ("SELECT regexp_replace('a1b', '\\D', 'X', 'g') AS v", "X1X"),
    ("SELECT regexp_replace('ab12', '[\\d]', 'X', 'g') AS v", "abXX"),
    ("SELECT regexp_replace(E'a\\x0bb', '[\\s]', '-') AS v", "a-b"),
    ("SELECT regexp_replace('a b', '[a\\S]', '-', 'g') AS v", "- -"),
    ("SELECT regexp_replace('a-b', '[\\w]', 'X', 'g') AS v", "X-X"),
    ("SELECT regexp_substr('ab', '\\w') AS v", "a"),
    ("SELECT regexp_substr('a1b', '[[:alpha:]]\\d') AS v", "a1"),
    ("SELECT regexp_substr('a1', '[[:digit:]]') AS v", "1"),
    ("SELECT regexp_replace('a-b', '[[:punct:]]', 'X') AS v", "aXb"),
    ("SELECT regexp_split_to_array(E'a\\x0bb', '\\s') AS v", "{a,b}"),
    ("SELECT regexp_replace('abc', '[[.b.]]', 'X') AS v", "aXc"),
    ("SELECT regexp_replace('abc', '[[=b=]]', 'X') AS v", "aXc"),
    # Directors and embedded options, which override the flags.
    ("SELECT regexp_replace('a.b', '***=.', 'X') AS v", "aXb"),
    ("SELECT regexp_replace('a.b', '***:b', 'X') AS v", "a.X"),
    ("SELECT regexp_replace('ABC', '(?i)b', 'X') AS v", "AXC"),
    ("SELECT regexp_replace('ABC', '(?c)b', 'X', 'i') AS v", "ABC"),
    ("SELECT 'ABC' ~* 'b' AS v", "t"),
    ("SELECT 'ABC' ~* '(?c)b' AS v", "f"),
    ("SELECT regexp_replace('ab', '(?#note)b', 'X') AS v", "aX"),
    ("SELECT regexp_replace('a b', '(?x) a \\  b # comment', 'X') AS v", "X"),
    ("SELECT regexp_like('ab', 'a b', 'x') AS v", "t"),
    ("SELECT regexp_like('a.b', 'a.b', 'q') AS v", "t"),
    ("SELECT regexp_like('axb', 'a.b', 'q') AS v", "f"),
    # How a newline is read: s (the default), n, p and w.
    ("SELECT regexp_replace(E'a\\nb', 'a.b', 'X') AS v", "X"),
    ("SELECT regexp_replace(E'a\\nb', 'a.b', 'X', 'n') AS v", "a\nb"),
    ("SELECT regexp_replace(E'a\\nb', '^b', 'X') AS v", "a\nb"),
    ("SELECT regexp_replace(E'a\\nb', '^b', 'X', 'n') AS v", "a\nX"),
    ("SELECT regexp_replace(E'a\\nb', 'a[^x]b', 'X', 'p') AS v", "a\nb"),
    ("SELECT regexp_replace(E'a\\nb', 'a$', 'X', 'w') AS v", "X\nb"),
    ("SELECT regexp_replace(E'a\\nb', '^', '-', 'gn') AS v", "-a\n-b"),
    # The replacement: \& the whole match, \N a group (nothing past the pattern's groups), \\ a
    # backslash, a backslash before anything else itself.
    ("SELECT regexp_replace('abc', 'b', '[\\&]') AS v", "a[b]c"),
    ("SELECT regexp_replace('abc', '(b)', '[\\1\\2]', 'g') AS v", "a[b]c"),
    ("SELECT regexp_replace('abc', 'b', '[\\x]') AS v", "a[\\x]c"),
    ("SELECT regexp_replace('abc', 'b', '[\\\\]') AS v", "a[\\]c"),
    ("SELECT regexp_replace('abc', 'b', '[\\0]') AS v", "a[\\0]c"),
]

#: Calls PostgreSQL raises on, or a form raises on within its stated bound: (statement, SQLSTATE).
RAISED = [
    # PostgreSQL's own errors of the pattern.
    ("SELECT regexp_replace('abc', '\\q', 'X') AS v", "2201B"),
    ("SELECT regexp_like('abc', '\\1') AS v", "2201B"),
    ("SELECT regexp_like('abc', '[[:foo:]]') AS v", "2201B"),
    ("SELECT regexp_like('abc', '\\u12') AS v", "2201B"),
    ("SELECT regexp_like('abc', '(?z)a') AS v", "2201B"),
    ("SELECT regexp_replace('abc', 'b', 'X', 'z') AS v", "22023"),
    ("SELECT regexp_like('abc', 'b', 'z') AS v", "22023"),
    # A locale's class or a word boundary on text beyond ASCII: PostgreSQL's locale decides.
    ("SELECT 'été b' ~ '\\yb' AS v", "0A000"),
    ("SELECT regexp_replace('été', '\\w', 'X') AS v", "0A000"),
    ("SELECT regexp_replace('a\u3000b', '\\s', '-') AS v", "0A000"),
    ("SELECT regexp_count('été', '[[:alpha:]]') AS v", "0A000"),
    # A pattern that matches the empty text where the function counts it (a stated bound).
    ("SELECT regexp_count('a b', '\\y') AS v", "0A000"),
]

#: What RE2 cannot express: refused by name (0A000), with the reason.
REFUSED = [
    ("SELECT regexp_replace('aa', '(a)\\1', 'X') AS v", "of a back reference"),
    ("SELECT regexp_like('aa', '(a)\\1') AS v", "of a back reference"),
    ("SELECT 'aa' ~ '(a)\\1' AS v", "of a back reference"),
    ("SELECT regexp_count('ab', 'a(?=b)') AS v", "of a lookahead or lookbehind constraint"),
    ("SELECT regexp_like('ab', '(?<=a)b') AS v", "of a lookahead or lookbehind constraint"),
    ("SELECT regexp_like('ab', '(?b)a') AS v", "of a basic or extended regular expression"),
    ("SELECT regexp_like('ab', 'a', 'e') AS v", "of a basic or extended regular expression"),
    ("SELECT regexp_like('a b', '\\m.') AS v", "of \\m beside what may not be a word character"),
    ("SELECT regexp_like('a b', '.\\M') AS v", "of \\M beside what may not be a word character"),
    ("SELECT regexp_like('a b', '\\ma*') AS v", "of \\m beside what may not be a word character"),
    ("SELECT regexp_like('a', '[[.space.]]') AS v", "of a named collating element"),
    ("SELECT regexp_like('a', '[^a\\S]') AS v", "of \\S in a complemented bracket expression"),
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
def test_clickhouse_reads_the_escape_as_postgresql_does(runtime, sql, want):
    got = _answer(runtime, sql)
    assert got is not None and _text(got) == want, (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    if isinstance(raised.value, FunctionRefused):
        assert raised.value.sqlstate == state, raised.value
        return
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


@pytest.mark.parametrize(("sql", "why"), REFUSED)
def test_what_re2_cannot_express_is_refused_with_its_reason(sql, why):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse", generated=False)
    assert refused.value.sqlstate == "0A000"
    assert why in str(refused.value), str(refused.value)


def test_a_two_argument_match_of_a_column_pattern_keeps_clickhouse_s_match():
    sql = "SELECT a ~ b AS v FROM (VALUES ('x', 'y')) AS t(a, b)"
    assert "match(a, b)" in transpile(sql, "clickhouse", generated=False)


# --- the translation itself ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("pattern", "flags", "written"),
    [
        ("\\b", "", "(?s:\\x{8})"),
        ("\\B", "", "(?s:\\x{5C})"),
        ("\\y", "", "(?s:\\b)"),
        ("\\Y", "", "(?s:\\B)"),
        ("\\mab\\M", "", "(?s:\\bab\\b)"),
        ("\\Z", "", "(?s:\\z)"),
        ("\\s", "", "(?s:[\\t\\n\\x{0B}\\f\\r ])"),
        ("a.b", "n", "(?sm:a[^\\n]b)"),
        ("(?i)a", "", "(?si:a)"),
        ("***=a.b", "", "(?s:a\\x{2E}b)"),
    ],
)
def test_the_re2_pattern_of_each_escape(pattern, flags, written):
    assert are_re2.translated(pattern, flags).text == written


def test_a_word_boundary_matches_everywhere_in_the_loose_pattern():
    """Whether a pattern can match the empty text anywhere is read with its boundaries matching."""
    re2 = are_re2.translated("\\y")
    assert re2.word_boundary and re2.loose == "(?s:(?:))"


def test_capturing_groups_are_counted_as_postgresql_numbers_them():
    assert are_re2.translated("(a)(?:b)(c)").groups == 2


def test_a_multi_digit_escape_past_the_groups_closed_is_octal():
    assert are_re2.translated("(a)\\12").text == "(?s:(a)\\x{A})"
