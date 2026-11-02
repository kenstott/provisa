# Copyright (c) 2026 Kenneth Stott
# Canary: b92c7d16-3ea0-4f58-8d41-67f0a2e9c3d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The text search functions on DuckDB against what postgres:17.11 answered (REQ-2167, REQ-2171).
Trino and ClickHouse replay the same statements in tests/integration/test_text_search_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import random

import duckdb
import pytest

from provisa.core import pg_tsearch
from provisa.core import pg_tsearch_parse as parse
from provisa.core.pg_error import PostgresError
from provisa.core.pg_tsearch_english import lexize
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.pg17_text_search_answers import RECORDED
from tests.unit.pgwire.text_search_corpus import (
    LIMIT_TEXTS,
    OVERLOADS,
    TEXTS,
    WORDS,
    null_call,
    statements,
)
from tests.unit.pgwire.text_search_replay import BY_FUNCTION, outcome, problems


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


def test_every_overload_is_in_the_corpus():
    assert len(OVERLOADS) == 83
    assert all(null_call(name, types) in RECORDED for name, types in OVERLOADS)


@pytest.mark.parametrize("function", sorted(BY_FUNCTION))
def test_duckdb_answers_what_postgresql_17_answered(function: str):
    connection = duckdb.connect()
    wrong, _ = problems(function, "duckdb", lambda sql: connection.execute(sql).fetchall())
    assert not wrong, f"{len(wrong)} differ: " + "\n".join(wrong[:20])


def test_the_calls_the_conformance_run_makes_are_answered_or_are_postgresqls_errors():
    connection = duckdb.connect()
    run = lambda sql: connection.execute(sql).fetchall()  # noqa: E731
    named = [s for n, t in OVERLOADS for s in (null_call(n, t),)]
    assert [s for s in named if outcome(s, "duckdb", run) != "ok"] == []


def test_a_form_reads_a_column_row_by_row():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('''a'':1 ''b'':2'), (NULL)) v(s)")
    written = transpile("SELECT ts_delete(CAST(s AS tsvector), 'a') AS v FROM t", "duckdb")
    assert connection.execute(written).fetchall() == [("'b':2",), (None,)]


def test_a_text_of_no_form_of_postgresql_is_refused_by_name():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT 'not a vector' AS s")
    written = transpile("SELECT ts_delete(CAST(s AS tsvector), 'a') AS v FROM t", "duckdb")
    with pytest.raises(duckdb.Error, match="ts_delete does not read this value"):
        connection.execute(written).fetchall()


def test_weights_that_are_not_constants_are_refused_by_name():
    with pytest.raises(FunctionRefused, match="ts_filter"):
        transpile("SELECT ts_filter(CAST(s AS tsvector), w) AS v FROM t", "duckdb")


def test_a_query_that_is_not_a_constant_is_refused_by_name():
    with pytest.raises(FunctionRefused, match="ts_match_vq"):
        transpile("SELECT ts_match_vq(CAST(s AS tsvector), q) AS v FROM t", "duckdb")


def test_a_document_that_is_not_a_constant_is_refused_by_name():
    with pytest.raises(FunctionRefused, match="to_tsvector"):
        transpile("SELECT to_tsvector(s) AS v FROM t", "duckdb")


def test_a_syntax_error_is_postgresqls_own():
    with pytest.raises(PostgresError) as raised:
        pg_tsearch.tsvector_in("a:")
    assert raised.value.sqlstate == "42601"
    assert str(raised.value) == 'syntax error in tsvector: "a:"'


def test_the_stemmer_leaves_a_stop_word_nothing():
    assert lexize("The") == []
    assert lexize("universities") == ["univers"]


#: Texts no corpus holds: the characters the parser's shapes and the stemmer's steps turn on.
_ALPHABET = [
    *"abcyeiouYsl-./@<>:'_ 12é²,;§Eh",
    "http://",
    "ing",
    "ed",
    "ies",
    "sses",
    "ation",
    "x.com",
]


def _texts() -> list[str]:
    chosen = random.Random(7)
    fuzz = (
        "".join(chosen.choice(_ALPHABET) for _ in range(chosen.randint(1, 12))) for _ in range(300)
    )
    return [*TEXTS, *LIMIT_TEXTS, *fuzz]


def _reading(config: str, text: str) -> str | None:
    try:
        return pg_tsearch.tsvector_text(parse.to_tsvector(config, text))
    except parse.Unread:
        return None


@pytest.mark.parametrize("config", ["english", "simple"])
def test_a_text_column_is_read_as_the_parser_reads_a_constant(config: str):
    read = {t: v for t in _texts() if (v := _reading(config, t)) is not None}
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT unnest(?) AS s", [list(read)])
    written = transpile(f"SELECT s, to_tsvector('{config}', s::text) AS v FROM t", "duckdb")
    got = dict(connection.execute(written).fetchall())
    assert {t: v for t, v in got.items() if v != read[t]} == {}


def test_a_text_the_parser_does_not_read_is_refused_row_by_row():
    unread = [t for t in _texts() if _reading("english", t) is None]
    assert len(unread) > 50
    connection = duckdb.connect()
    written = transpile("SELECT to_tsvector(c1) AS v FROM (VALUES ('x'::text)) AS t(c1)", "duckdb")
    for text in unread[:20]:
        with pytest.raises(duckdb.Error, match="to_tsvector does not read this value"):
            connection.execute(written.replace("'x'", "?"), [text]).fetchall()


def test_a_word_column_is_stemmed_as_ts_lexize_stems_a_constant():
    words = [w for t in TEXTS for w in t.split()] + list(WORDS)
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT unnest(?) AS s", [words])
    written = transpile("SELECT s, ts_lexize('english_stem', s::text) AS v FROM t", "duckdb")
    got = {s: v for s, v in connection.execute(written).fetchall()}
    assert {w: got[w] for w in words if got[w] != parse.lexize("english_stem", w)} == {}


def test_a_column_the_statement_does_not_say_is_text_is_refused_by_name():
    with pytest.raises(FunctionRefused, match="to_tsvector"):
        transpile("SELECT to_tsvector('english', body) AS v FROM t", "duckdb")
    with pytest.raises(FunctionRefused, match="@@"):
        transpile("SELECT body @@ to_tsquery('english', 'a') AS v FROM t", "duckdb")


def test_a_configuration_that_is_not_a_constant_is_refused_by_name():
    with pytest.raises(FunctionRefused, match="to_tsvector"):
        transpile("SELECT to_tsvector(c, body::text) AS v FROM t", "duckdb")


def test_at_at_of_a_column_vector_and_a_constant_query_call():
    connection = duckdb.connect()
    rows = "('The Fat Rats'::text), (NULL::text), ('cats running'::text)"
    sql = (
        "SELECT to_tsvector('english', c1) @@ to_tsquery('english', 'rats & fat') AS v "
        f"FROM (VALUES {rows}) AS t(c1)"
    )
    assert connection.execute(transpile(sql, "duckdb")).fetchall() == [(True,), (None,), (False,)]
