# Copyright (c) 2026 Kenneth Stott
# Canary: c6f27b08-94e1-4a3d-b1d6-5a08e3c7d412
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg_forms_system: each statement of system_corpus, written for DuckDB, against what postgres:17.11
answered (REQ-2171). Trino and ClickHouse replay the same statements in
tests/integration/test_system_forms_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from typing import Any

import chdb
import duckdb
import pytest
import sqlglot
from sqlglot import exp

from provisa.core.pg_error import MARK, PostgresError, from_engine
from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.function_macros import MACROS, FunctionRefused, outcome
from provisa.transpiler.metadata_facts import CatalogFacts
from provisa.transpiler.metadata_functions import expand
from provisa.transpiler.pg_forms_system import ROWS
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.pgwire.pg17_system_answers import RECORDED
from tests.unit.pgwire.system_corpus import statements

_ENGINES = ("duckdb", "trino", "clickhouse")


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


@pytest.fixture(scope="module")
def connection() -> Any:
    return duckdb.connect()


def _on_the_engine(sql: str) -> str:
    """A statement with no table, as pgwire hands it to the engine: past the catalog's forms."""
    return expand(sqlglot.parse_one(sql, read="postgres"), CatalogFacts("analyst")).sql("postgres")


@pytest.mark.parametrize("sql", list(RECORDED))
def test_duckdb_answers_as_postgresql_17_answered(connection: Any, sql: str):
    want = RECORDED[sql]
    try:
        rows = connection.execute(transpile(_on_the_engine(sql), "duckdb")).fetchall()
    except PostgresError as raised:  # settled by the statement's shape, before any engine
        assert ("err", raised.sqlstate, str(raised)) == want
        return
    except duckdb.Error as raised:
        error = from_engine(raised)
        assert error is not None, raised
        assert ("err", error.sqlstate, str(error)) == want
        return
    assert want[0] == "ok", rows
    assert [row[0] for row in rows] == want[2]


@pytest.mark.parametrize("sql", [s for s, want in RECORDED.items() if want[0] == "ok"])
def test_a_column_says_the_recorded_type(sql: str):
    said = (result_casts(sql) or (None,))[0]
    assert said is None or said.oid == RECORDED[sql][1]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_collation_for(s) AS v FROM t",
        "SELECT pg_column_compression(s) AS v FROM t",
        "SELECT pg_column_toast_chunk_id(s) AS v FROM t",
    ],
)
@pytest.mark.parametrize("dialect", _ENGINES)
def test_a_column_is_refused_by_name_for_its_storage_is_its_sources(sql: str, dialect: str):
    with pytest.raises(FunctionRefused, match=f"has no form on {dialect}"):
        transpile(sql, dialect)


@pytest.mark.parametrize(
    ("pattern", "replacement"),
    [("^a", "x"), (r"\bb", "x"), ("b*", "x"), ("b", r"\&"), ("b", r"\\")],
)
def test_what_a_search_keeps_of_its_place_is_refused_by_name(pattern: str, replacement: str):
    sql = f"SELECT regexp_replace('abc', '{pattern}', '{replacement}', 1, 1) AS v"
    with pytest.raises(duckdb.Error, match=rf"{MARK} 0A000: regexp_replace does not read"):
        duckdb.connect().execute(transpile(sql, "duckdb")).fetchall()


def test_an_anchor_duckdb_cannot_read_is_its_own_error_never_an_answer():
    sql = r"SELECT regexp_replace('abc', '\yb', 'x', 1, 1) AS v"
    with pytest.raises(duckdb.Error, match="invalid escape sequence"):
        duckdb.connect().execute(transpile(sql, "duckdb")).fetchall()


@pytest.mark.parametrize("dialect", _ENGINES)
def test_each_row_has_a_form_that_parses_on_each_engine(dialect: str):
    for name, params, _ in ROWS:
        assert outcome(name, len(params), dialect) == "emulated", name
        _, form = MACROS[(name, len(params))].resolved(dialect)
        assert isinstance(sqlglot.parse_one(form, read=dialect), exp.Expr), name


@pytest.mark.parametrize("dialect", ["spark", "snowflake", "bigquery", "mysql", "sqlite", "tsql"])
def test_an_engine_with_no_form_refuses_by_name(dialect: str):
    assert {outcome(name, len(params), dialect) for name, params, _ in ROWS} == {"refused"}


@pytest.mark.parametrize(
    ("value", "count", "want"),
    [
        ("'ab'", "3", "ababab"),
        ("'雪'", "2", "雪雪"),
        ("'x'", "20000", "x" * 20000),  # past the 10000 elements Trino's own repeat builds
        ("''", "3", ""),
        ("'x'", "0", ""),
        ("'x'", "-1", ""),
        ("NULL::text", "3", None),
        ("NULL::text", "0", None),
        ("NULL::text", "-1", None),
        ("NULL", "0", None),
        ("'x'", "NULL::integer", None),
    ],
)
def test_repeat_on_trino_is_a_text_as_postgresql_answers(value: str, count: str, want: str | None):
    """Trino's own repeat builds an array. No Trino runs here: DuckDB's rpad pads the same way but
    for its size's type, and tests/integration/test_system_forms_on_engines.py replays on Trino."""
    sql = transpile(f"SELECT repeat({value}, {count}) AS v", "trino")
    engine = duckdb.connect()
    engine.execute("CREATE MACRO trino_rpad(s, n, p) AS rpad(s, CAST(n AS INTEGER), p)")
    assert engine.execute(sql.replace("RPAD(", "trino_rpad(")).fetchall() == [(want,)]


def test_a_form_that_means_trinos_array_repeat_keeps_it():
    assert "REPEAT(1, " in transpile("SELECT array_fill(1, ARRAY[2])", "trino")


@pytest.mark.parametrize("dialect", ["duckdb", "clickhouse", "spark", "snowflake"])
def test_repeat_stays_the_engines_own_elsewhere(dialect: str):
    assert "REPEAT('x', 3)" in transpile("SELECT repeat('x', 3) AS v", dialect)


_STRICT_REPEAT = [
    ("NULL::text", "3", None),
    ("NULL::text", "0", None),
    ("NULL::text", "-1", None),
    ("NULL", "3", None),
    ("NULL", "0", None),
    ("NULL", "-1", None),
    ("'x'", "NULL::integer", None),
    ("'x'", "-1", ""),
    ("'x'", "0", ""),
    ("'ab'", "2", "abab"),
]


@pytest.mark.parametrize(("value", "count", "want"), _STRICT_REPEAT)
def test_repeat_is_strict_on_duckdb(value: str, count: str, want: str | None):
    written = transpile(f"SELECT repeat({value}, {count}) AS v", "duckdb")
    assert duckdb.connect().execute(written).fetchall() == [(want,)]


@pytest.mark.parametrize(("value", "count", "want"), _STRICT_REPEAT)
def test_repeat_is_strict_on_clickhouse(value: str, count: str, want: str | None):
    written = transpile(f"SELECT repeat({value}, {count}) AS v", "clickhouse")
    answer = chdb.query(written, "CSV").bytes().decode().strip()
    assert answer == ("\\N" if want is None else f'"{want}"')


@pytest.mark.parametrize("dialect", ["spark", "snowflake", "bigquery", "mysql", "tsql"])
def test_repeat_of_nothing_is_null_for_any_count_elsewhere(dialect: str):
    written = transpile("SELECT repeat(a, b) AS v FROM t", dialect)
    assert re.search(r"\ba\W* IS NOT NULL", written), written


@pytest.mark.parametrize(
    ("n", "want"),
    [
        (2, "aXb-c"),  # postgres:16.11 answered each; regexp.c deduces N only when not given
        (1, "a-bXc"),
        (0, "a-b-c"),
        (3, "aXbXc"),
    ],
)
def test_the_g_flag_is_ignored_where_n_is_given(n: int, want: str):
    sql = f"SELECT regexp_replace('aXbXc', 'X', '-', 1, {n}, 'g') AS v"
    assert duckdb.connect().execute(transpile_to_duckdb(sql)).fetchall() == [(want,)]
    assert chdb.query(transpile(sql, "clickhouse"), "CSV").bytes().decode().strip() == f'"{want}"'
    assert "'g'" not in transpile(sql, "trino")


@pytest.mark.parametrize("flags", ["'gi'", "'i'", "f"])
@pytest.mark.parametrize("dialect", _ENGINES)
def test_a_flag_beside_n_is_refused_by_name(flags: str, dialect: str):
    sql = f"SELECT regexp_replace(a, 'X', '-', 1, 2, {flags}) AS v FROM t"
    with pytest.raises(FunctionRefused, match=f"regexp_replace.*has no form on {dialect}"):
        transpile(sql, dialect)


@pytest.mark.parametrize(
    "call",
    [
        "'abc'::text, 'b*'::text, 'x'::text, NULL::integer, 1::integer",
        "'abc'::text, 'b*'::text, NULL::text, 1::integer, 1::integer",
        "'abc'::text, NULL::text, 'x'::text, 1::integer, 1::integer",
        "'abc'::text, 'b'::text, '\\\\&'::text, 1::integer, NULL::integer",
    ],
)
def test_clickhouse_answers_null_before_it_refuses_a_value(call: str):
    written = transpile(f"SELECT regexp_replace({call}) AS v", "clickhouse")
    assert chdb.query(written, "CSV").bytes().decode().strip() == "\\N"


def test_clickhouse_builds_no_array_from_a_nullable():
    """Its 24.3 refuses Nullable(Array) when it reads the statement, whatever the values, which
    chdb's own version allows. So past the checks each operand is taken out of Nullable."""
    written = transpile("SELECT regexp_replace(a, 'p', c, d, e) FROM t", "clickhouse")
    answer = written[written.index(" THEN CONCAT(") :].removesuffix(" FROM t")
    assert re.findall(r"\b[acde]\b", re.sub(r"assumeNotNull\([acde]\)", "", answer)) == []
