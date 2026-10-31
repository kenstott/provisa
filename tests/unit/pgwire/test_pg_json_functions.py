# Copyright (c) 2026 Kenneth Stott
# Canary: c6a4e1d2-7f03-4b58-92ae-1d8b3f6c0e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's json and jsonb functions and operators (REQ-2167; REQ-2171, AN ANSWER THAT DIFFERS
IS REFUSED): each statement run as a client runs it, over pgwire on the default engine, against
PostgreSQL 17.11's answer. tests/integration/test_json_on_engines.py replays them on Trino and
ClickHouse. What does not answer as PostgreSQL did is in pg17_json_matrix, by name of its outcome."""

# Requirements: REQ-2167, REQ-2171
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from psycopg import pq

from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_forms_json import REFUSED, jsonb_text
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.pgwire.pg17_json_answers import RECORDED
from tests.unit.pgwire.pg17_json_matrix import ERROR, MATRIX, REFUSED as NAMED, SERVED, outcome
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_ENGINES = ("duckdb", "trino", "clickhouse")
#: What json_agg's answer is on the engine: one-to-many GraphQL compiles to it, so it stays.
JSON_AGG = {
    "SELECT json_agg(x) FROM (VALUES ('a'), ('b\"c')) t(x)": '["a","b\\"c"]',
    "SELECT json_agg(x) FROM (VALUES (1), (NULL), (2)) t(x)": "[1,null,2]",
    "SELECT json_agg(x) FROM (VALUES (1.50)) t(x)": "[1.5]",
}


def _write(sql: str, dialect: str) -> str:
    return transpile_to_duckdb(sql) if dialect == "duckdb" else transpile(sql, dialect)


def _rows(res: Any) -> list[tuple]:
    cells = (range(res.ntuples), range(res.nfields))
    return [
        tuple(None if (v := res.get_value(i, j)) is None else bytes(v).decode() for j in cells[1])
        for i in cells[0]
    ]


def _outcome(conn: Any, sql: str) -> str:
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    recorded = RECORDED[sql]
    if res.status == pq.ExecStatus.TUPLES_OK:
        if recorded[0] != "ERROR" and recorded[0] != [res.ftype(j) for j in range(res.nfields)]:
            return "type"
        return outcome(recorded, _rows(res), False)
    message = res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode()
    return outcome(recorded, None, " has no form on " in message)


def test_every_statement_answers_as_the_matrix_says_on_the_default_engine(conn: Any):
    found = {sql: said for sql in RECORDED if (said := _outcome(conn, sql)) != SERVED}
    assert found == MATRIX["duckdb"]


def test_what_postgresql_refused_is_refused_or_named_not_answered(conn: Any):
    answered = {sql for sql, said in MATRIX["duckdb"].items() if RECORDED[sql][0] == "ERROR"}
    assert all(MATRIX["duckdb"][sql] in (ERROR, NAMED) for sql in answered)


def test_the_engines_json_agg_stays_the_engines_and_differs_only_as_pinned(conn: Any):
    answers = {
        sql: _rows(conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0))
        for sql in JSON_AGG
    }
    assert answers == {sql: [(want,)] for sql, want in JSON_AGG.items()}
    assert all(RECORDED[sql][1] != [(want,)] for sql, want in JSON_AGG.items())


@pytest.mark.parametrize("dialect", _ENGINES)
def test_every_statement_reads_back_or_is_refused_by_name(dialect: str):
    for sql in RECORDED:
        if MATRIX[dialect].get(sql) == ERROR:
            continue  # an error is the engine's, or sqlglot's reading of a form PostgreSQL has
        try:
            sqlglot.parse_one(_write(sql, dialect), read=dialect)
        except FunctionRefused:
            continue


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT (a -> 'b') FROM t",
        "SELECT a ->> 'b' FROM t",
        "SELECT a -> 0, a ->> 1 FROM t",
    ],
)
@pytest.mark.parametrize("dialect", ["duckdb", "trino"])
def test_a_read_stays_the_engines_own_operator_with_nothing_per_row(sql: str, dialect: str):
    written = _write(sql, dialect).upper()
    assert not any(
        word in written for word in ("REGEXP", "REPLACE(", "CASE", "COALESCE", "LATERAL")
    )


@pytest.mark.parametrize("dialect", _ENGINES)
def test_a_read_of_a_column_is_not_typed_so_nothing_is_written_per_row_at_the_wire(dialect: str):
    assert result_casts("SELECT a -> 'b', a ->> 'c' FROM t") == (None, None)


@pytest.mark.parametrize("dialect", _ENGINES)
def test_a_column_compared_to_a_jsonb_literal_runs_as_it_did(dialect: str):
    written = _write("""SELECT * FROM t WHERE a = '{"x":1}'::jsonb""", dialect)
    assert '{"x":1}' in written and " = " in written


@pytest.mark.parametrize("dialect", _ENGINES)
@pytest.mark.parametrize(
    "sql",
    [
        """SELECT ('{"b":1,"a":2}'::jsonb)::text""",
        """SELECT jsonb_build_object('b', 1)::text""",
        """SELECT CAST(a::jsonb -> 'b' AS TEXT) FROM t""",
        """SELECT to_jsonb(a)::varchar FROM t""",
    ],
)
def test_the_text_of_a_computed_jsonb_is_refused(sql: str, dialect: str):
    with pytest.raises(FunctionRefused, match="jsonb_out"):
        _write(sql, dialect)


@pytest.mark.parametrize("dialect", _ENGINES)
def test_the_text_of_a_column_is_not_refused(dialect: str):
    assert "t" in _write("SELECT a::text FROM t", dialect)


@pytest.mark.parametrize("dialect", _ENGINES)
def test_containment_of_ranges_and_arrays_is_not_a_json_operator(dialect: str):
    if dialect != "clickhouse":
        _write("SELECT a @> b FROM t", dialect)


def test_a_refused_name_is_refused_on_every_engine():
    assert "jsonb_pretty" in REFUSED
    for dialect in _ENGINES:
        with pytest.raises(FunctionRefused, match="jsonb_pretty"):
            _write("SELECT jsonb_pretty(a) FROM t", dialect)


def test_jsonpath_stays_refused():
    for call in ("jsonb_path_exists(a, '$')", "jsonb_path_query_first(a, '$')", "a @? '$.b'"):
        with pytest.raises(FunctionRefused):
            _write(f"SELECT {call} FROM t", "duckdb")


@pytest.mark.parametrize(
    ("text", "want"),
    [
        ('{"b": 1, "a": 2, "b": 3}', '{"a": 2, "b": 3}'),
        ('{"bb":1,"a":2,"é":3}', '{"a": 2, "bb": 1, "é": 3}'),
        ("[1e2, 1.0E+1, 1.50, -0, 0.10]", "[100, 10, 1.50, 0, 0.10]"),
        ('"\\u00e9\\n"', '"é\\n"'),
        ('{"a":{"z":null,"b":[true,false]}}', '{"a": {"b": [true, false], "z": null}}'),
    ],
)
def test_a_jsonb_is_written_as_jsonb_out_writes_it(text: str, want: str):
    assert jsonb_text(text) == want


def test_the_matrix_lists_only_recorded_statements():
    assert all(set(MATRIX[engine]) <= set(RECORDED) for engine in _ENGINES)
