# Copyright (c) 2026 Kenneth Stott
# Canary: 1cef0f3d-7e17-49e0-ba27-0eba339c4145
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""regexp_replace of a pattern that matches the empty text, searched as PostgreSQL searches it
(REQ-2236): an empty match is replaced and the search goes on one character past it, and an empty
match at the end of a non-empty match is one, which RE2's search goes past. The edge 'a|' of the
functions conformance run (scripts/conformance/function_edges.py) found it: PostgreSQL 17.11
answers regexp_replace('abc', 'a|', 'abc', 'g') with 'abcabcbabccabc', DuckDB and CockroachDB
answered 'abcbabccabc'. DuckDB's answers run on the embedded engine; Trino's and openGauss's forms
are read as written (their engines run in the Conformance (functions) legs)."""

# Requirements: REQ-2236

from __future__ import annotations

import duckdb
import pytest

from provisa.core.pg_error import from_engine
from provisa.transpiler.transpile import transpile, transpile_to_duckdb


def _call(*args: str) -> str:
    return f'SELECT pg_catalog."regexp_replace"({", ".join(args)}) AS v'


def _on_duckdb(sql: str):
    return duckdb.connect().execute(transpile_to_duckdb(sql, generated=False)).fetchone()[0]


#: (arguments, PostgreSQL 17.11's answer). The first four are the conformance run's recorded
#: answers; the rest follow PostgreSQL's search (each empty match replaced, then the character
#: after it copied; an empty match where a non-empty one ended is one).
EVERY_MATCH = [
    (("'abc'::text", "'a|'::text", "'abc'::text", "'g'::text"), "abcabcbabccabc"),
    (("'abc'::text", "'x*'::text", "'abc'::text", "'g'::text"), "abcaabcbabccabc"),
    (("'abc'::text", "''::text", "'abc'::text", "'g'::text"), "abcaabcbabccabc"),
    (("'abc'::text", "lower('')::text", "'abc'::text", "'g'::text"), "abcaabcbabccabc"),
    (("'xa'::text", "'x*'::text", "'R'::text", "'g'::text"), "RRaR"),
    (("'aa'::text", "'a|'::text", "'R'::text", "'g'::text"), "RRR"),
    (("'abc'::text", "'a|^'::text", "'R'::text", "'g'::text"), "Rbc"),
    (("'aa'::text", "'(a)|'::text", "'<\\1>'::text", "'g'::text"), "<a><a><>"),
    (("'ABA'::text", "'a|'::text", "'-'::text", "'gi'::text"), "--B--"),
    (("'ABA'::text", "'a|'::text", "'-'::text", "'gic'::text"), "-A-B-A-"),
    (("'abc'::text", "'a|'::text", "NULL::text", "'g'::text"), None),
    (("NULL::text", "'a|'::text", "'R'::text", "'g'::text"), None),
    (("''::text", "'a|'::text", "'R'::text", "'g'::text"), "R"),
]


@pytest.mark.parametrize(("args", "want"), EVERY_MATCH)
def test_duckdb_searches_every_match_as_postgresql(args: tuple[str, ...], want: str | None):
    assert _on_duckdb(_call(*args)) == want


def test_a_replacement_that_is_empty_on_an_empty_match_keeps_duckdbs_own():
    """RE2 differs only in an empty match where a non-empty one ended: replacing it with nothing
    (or with groups, each empty there) changes nothing, so DuckDB's own call is sent."""
    for replacement in ("''", "'\\1'", "'\\&\\2'"):
        sql = f"SELECT regexp_replace(s, 'a|', {replacement}, 'g') AS v FROM t"
        written = transpile_to_duckdb(sql, generated=False)
        assert written == f"SELECT REGEXP_REPLACE(s, 'a|', {replacement}, 'g') AS v FROM t"


def test_a_pattern_that_cannot_match_the_empty_text_is_duckdbs_own_search():
    sql = _call("'abcb'::text", "'b'::text", "'X'::text", "'g'::text")
    assert _on_duckdb(sql) == "aXcX"


def test_backslash_zero_in_the_walk_is_refused_by_name_on_duckdb():
    """RE2's \\0 is the match where PostgreSQL copies \\0 as written; the walk reads it otherwise
    again, so it is refused by name, as the grid row states."""
    sql = _call("'abc'::text", "'a|'::text", "'<\\0>'::text", "'g'::text")
    with pytest.raises(duckdb.Error) as raised:
        _on_duckdb(sql)
    error = from_engine(raised.value)
    assert error is not None
    assert (error.sqlstate, str(error)) == ("0A000", "regexp_replace does not read this value")


#: (start, n, PostgreSQL's answer) of regexp_replace('abc', pattern, 'abc', start, n) for a pattern
#: that matches the empty text: from 3 the matches are the empty ones before 'c' and at the end.
FROM_A_START = [
    ("''", 3, 3, "abc"),
    ("'x*'", 3, 3, "abc"),
    ("'a|'", 3, 3, "abc"),
    ("lower('')", 3, 3, "abc"),
    ("'x*'", 3, 1, "ababcc"),
    ("'a|'", 3, 1, "ababcc"),
    ("'a|'", 1, 0, "abcabcbabccabc"),
    ("'a|'", 1, 2, "aabcbc"),
    ("'b*'", 1, 1, "abcabc"),
]


@pytest.mark.parametrize(("pattern", "start", "n", "want"), FROM_A_START)
def test_duckdb_searches_from_a_start_as_postgresql(pattern: str, start: int, n: int, want: str):
    sql = _call(
        "'abc'::text", f"{pattern}::text", "'abc'::text", f"{start}::integer", f"{n}::integer"
    )
    assert _on_duckdb(sql) == want


def test_trino_searches_from_a_start_instead_of_refusing():
    """Trino's five-argument form walks the text where the pattern matches the empty text, and
    refuses by name only a text longer than its sequence of places holds."""
    sql = "SELECT regexp_replace(s, 'a|', 'X', 1, 0) AS v FROM t"
    written = transpile(sql, "trino", generated=False)
    assert "REDUCE(TRANSFORM(SEQUENCE(1, LENGTH(s) + 1)" in written
    assert "WHEN REGEXP_LIKE('', 'a|') AND (LENGTH(s) > 9998) THEN CAST(FAIL(" in written
    assert "WHEN REGEXP_LIKE('', 'a|') THEN SUBSTR(s, 1, 1 - 1) || REDUCE(" in written
    # Each place's match is read behind one character of the text, so ^ holds only at its start.
    assert "REGEXP_LIKE(SUBSTR(s, _i - 1), '\\A(?s:.)(?:' || 'a|' || ')')" in written


def test_opengauss_replaces_the_first_match_from_a_start():
    """openGauss's four-argument call replaces every match from the position (its occurrence
    defaults to 0): N 1 is PostgreSQL's first match."""
    sql = _call("'abc'::text", "'x*'::text", "'abc'::text", "3::integer")
    written = transpile(sql, "postgres", generated=False, engine_kind="opengauss")
    assert written == (
        "SELECT REGEXP_REPLACE(CAST('abc' AS TEXT), CAST('x*' AS TEXT), CAST('abc' AS TEXT), "
        "CAST(3 AS INT), 1) AS v"
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT regexp_replace(s, 'x*', 'abc', 'g') FROM t",  # flags
        "SELECT regexp_replace(s, 'x*', 'abc', 3, 2) FROM t",  # N given
        "SELECT regexp_replace(s, 'x*', 'abc', f) FROM t",  # a fourth argument of no evident type
    ],
)
def test_opengauss_leaves_every_other_call_as_written(sql: str):
    assert transpile(sql, "postgres", generated=False, engine_kind="opengauss") == transpile(
        sql, "postgres", generated=False
    )


def test_another_postgresql_engine_keeps_the_four_argument_call():
    sql = "SELECT regexp_replace(s, 'x*', 'abc', 3) FROM t"
    written = transpile(sql, "postgres", generated=False, engine_kind="cockroachdb")
    assert written == "SELECT REGEXP_REPLACE(s, 'x*', 'abc', 3) FROM t"
