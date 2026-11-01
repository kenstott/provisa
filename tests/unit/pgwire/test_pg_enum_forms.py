# Copyright (c) 2026 Kenneth Stott
# Canary: e4a19d76-0c3b-4f52-8e27-5b6d13c8f9a0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The enum functions over a source's labels (REQ-2167, REQ-2171), replayed on DuckDB and ClickHouse
against postgres:17.11's recorded answers, on a literal and on a column. Neither engine holds the
type here: the labels are all it is told. Trino's replay is
tests/integration/test_enum_forms_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from typing import Any

import chdb
import duckdb
import pytest
from chdb.session import Session

from provisa.core.pg_enum import SourceEnum, array_text
from provisa.core.pg_error import PostgresError
from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_forms_enum import with_source_enums
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.pgwire.everyday_function_statements import PROBES, STATEMENTS
from tests.unit.pgwire.pg17_everyday_function_answers import RECORDED

_NAME = "conformance.probe_enum"
_PROBE = SourceEnum("conformance", "probe_enum", 16412, 16411, ("a", "b", "c"))
ENUMS = {("conformance", "probe_enum"): _PROBE}
ENUM_STATEMENTS = [
    sql
    for sql in (*STATEMENTS["Enum"], *PROBES["Enum"], *PROBES["Aggregate and window"])
    if re.search(r"enum_(first|last|range|larger|smaller)|\bmax\(|\bmin\(", sql) and _NAME in sql
]


def _duckdb(sql: str) -> list[str | None]:
    written = transpile_to_duckdb(with_source_enums(sql, ENUMS))
    return [_shown(row[-1]) for row in duckdb.connect().execute(written).fetchall()]


def _clickhouse(sql: str) -> list[str | None]:
    written = transpile(with_source_enums(sql, ENUMS), "clickhouse")
    held = chdb.query(written, "TabSeparated").bytes().decode()
    return [None if line == "\\N" else _array(line) for line in held.splitlines()]


def _array(cell: str) -> str:
    return "{" + cell[1:-1].replace("'", "") + "}" if cell.startswith("[") else cell


def _shown(value: Any) -> str | None:
    return array_text(value) if isinstance(value, list) else value


ENGINES = {"duckdb": _duckdb, "clickhouse": _clickhouse}


@pytest.mark.parametrize(
    "name", ["enum_first", "enum_last", "enum_larger", "enum_smaller", "enum_range", "max", "min"]
)
def test_each_function_of_the_slice_is_replayed(name: str):
    assert any(name in sql for sql in ENUM_STATEMENTS)


def test_each_overload_of_enum_range_is_replayed():
    assert {sql.count(_NAME) for sql in ENUM_STATEMENTS if "enum_range" in sql} == {1, 2}


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("sql", ENUM_STATEMENTS)
def test_a_literal_answers_as_postgresql_recorded(engine: str, sql: str):
    kind, _, want = RECORDED[sql]
    assert kind == "ok"
    assert ENGINES[engine](sql) == want


def _native(_table: Any, name: str) -> str | None:
    return "probe_enum" if name.startswith("c") else None


def _on_a_column(sql: str) -> tuple[str, list[str]]:
    """``sql`` with its arguments read from a table's columns, and what its row holds."""
    held: list[str] = []

    def column(match: re.Match[str]) -> str:
        held.append(match[1])
        return f"c{len(held) - 1}"

    body = re.sub(rf"('\w'|NULL)::{_NAME}", column, sql.removesuffix(" AS v"))
    return f"{body} AS v FROM t", held


def _duckdb_table(sql: str, held: list[str]) -> list[str | None]:
    engine = duckdb.connect()
    columns = ", ".join(f"c{i} VARCHAR" for i in range(len(held)))
    engine.execute(f"CREATE TABLE t ({columns})")
    engine.execute(f"INSERT INTO t VALUES ({', '.join(held)})")
    written = transpile_to_duckdb(with_source_enums(sql, ENUMS, _native))
    return [_shown(row[-1]) for row in engine.execute(written).fetchall()]


def _clickhouse_table(sql: str, held: list[str]) -> list[str | None]:
    columns = ", ".join(f"c{i} Nullable(String)" for i in range(len(held)))
    with Session() as engine:
        engine.query("CREATE DATABASE IF NOT EXISTS d ENGINE = Memory")
        engine.query(f"CREATE TABLE d.t ({columns}) ENGINE = Memory")
        engine.query(f"INSERT INTO d.t VALUES ({', '.join(held)})")
        written = transpile(with_source_enums(sql, ENUMS, _native), "clickhouse")
        cells = engine.query(written.replace(" FROM t", " FROM d.t"), "TabSeparated").bytes()
    return [None if line == "\\N" else _array(line) for line in cells.decode().splitlines()]


COLUMN_ENGINES = {"duckdb": _duckdb_table, "clickhouse": _clickhouse_table}


@pytest.mark.parametrize("engine", COLUMN_ENGINES)
@pytest.mark.parametrize("sql", ENUM_STATEMENTS)
def test_a_column_answers_as_its_literal_did(engine: str, sql: str):
    written, held = _on_a_column(sql)
    if not held:
        pytest.fail(f"no argument to read from a column: {sql}")
    assert COLUMN_ENGINES[engine](written, held) == RECORDED[sql][2]


def test_many_rows_are_ordered_by_the_labels_not_the_alphabet():
    enums = {("s", "e"): SourceEnum("s", "e", 1, 2, ("z", "y", "x"))}
    engine = duckdb.connect()
    engine.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('z'), ('x'), (NULL)) v(c0)")
    written = with_source_enums(
        "SELECT max(c0) AS high, min(c0) AS low FROM t", enums, lambda _t, _n: "e"
    )
    assert engine.execute(transpile_to_duckdb(written)).fetchall() == [("x", "z")]


def test_no_row_has_no_extreme():
    written = with_source_enums("SELECT max(c0) AS v FROM t WHERE c0 = 'none'", ENUMS, _native)
    engine = duckdb.connect()
    engine.execute("CREATE TABLE t (c0 VARCHAR)")
    assert engine.execute(transpile_to_duckdb(written)).fetchall() == [(None,)]


def test_a_registered_column_is_the_enum_its_source_declares():
    def native(_table: Any, name: str) -> str | None:
        return "probe_enum" if name == "status" else None

    written = with_source_enums("SELECT enum_last(status) AS v FROM orders", ENUMS, native)
    assert "'c'" in written


def test_a_label_the_type_does_not_hold_is_postgresql_error():
    with pytest.raises(PostgresError, match=f'invalid input value for enum {_NAME}: "z"') as held:
        with_source_enums(f"SELECT enum_first('z'::{_NAME}) AS v", ENUMS)
    assert held.value.sqlstate == "22P02"


def test_a_result_is_told_the_type_the_source_declares():
    value = with_source_enums(f"SELECT enum_first(NULL::{_NAME}) AS v", ENUMS)
    array = with_source_enums(f"SELECT enum_range(NULL::{_NAME}) AS v", ENUMS)
    aggregate = with_source_enums(f"SELECT max('b'::{_NAME}) AS v", ENUMS)
    assert [result_casts(sql)[0].oid for sql in (value, array, aggregate)] == [16412, 16411, 16412]


@pytest.mark.parametrize(
    ("labels", "text"),
    [
        (["a", "b"], "{a,b}"),
        (["in progress", "NULL", 'q"t', "", "x,y"], '{"in progress","NULL","q\\"t","","x,y"}'),
        ([], "{}"),
    ],
)
def test_an_array_of_labels_is_written_as_array_out_writes_it(labels: list[str], text: str):
    assert array_text(labels) == text


def test_a_window_over_an_enum_stays_the_engines():
    sql = f"SELECT max('b'::{_NAME}) OVER () AS v"
    assert with_source_enums(sql, ENUMS) == sql


def test_a_statement_with_no_enum_is_left_as_it_is():
    sql = "SELECT max(n) AS v FROM (SELECT 1 AS n) t"
    assert with_source_enums(sql, ENUMS) == sql


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "clickhouse"])
def test_lastval_is_postgresql_error_in_a_session_that_advanced_nothing(dialect: str):
    assert "lastval is not yet defined in this session" in transpile(
        'SELECT pg_catalog."lastval"() AS v', dialect
    )


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "clickhouse"])
def test_lastval_beside_nextval_is_refused_by_name(dialect: str):
    sql = (
        'SELECT pg_catalog."lastval"() AS v '
        "FROM (SELECT nextval('conformance.probe_sequence') OFFSET 0) s"
    )
    with pytest.raises(FunctionRefused, match="lastval"):
        transpile(sql, dialect)


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "clickhouse"])
@pytest.mark.parametrize(
    "call", ["VARIADIC NULL::text[]", "VARIADIC '{}'::text[]", "VARIADIC ARRAY[]::text[]"]
)
def test_no_publication_is_no_row(dialect: str, call: str):
    written = transpile(f'SELECT pg_catalog."pg_get_publication_tables"({call}) AS v', dialect)
    assert "WHERE FALSE" in written.upper()
    assert "VARIADIC" not in written.upper()


def test_no_row_runs_on_duckdb_and_clickhouse():
    sql = 'SELECT pg_catalog."pg_get_publication_tables"(VARIADIC NULL::text[]) AS v'
    assert duckdb.connect().execute(transpile_to_duckdb(sql)).fetchall() == []
    assert chdb.query(transpile(sql, "clickhouse"), "CSV").bytes() == b""


@pytest.mark.parametrize(
    "call", ["'p'", "VARIADIC ARRAY['p']", "VARIADIC '{p}'::text[]", "'p', 'q'"]
)
def test_a_publication_named_does_not_exist(call: str):
    with pytest.raises(PostgresError, match='publication "p" does not exist') as held:
        transpile(f"SELECT pg_get_publication_tables({call}) AS v", "trino")
    assert held.value.sqlstate == "42704"


def test_publications_a_column_names_are_refused_by_name():
    with pytest.raises(FunctionRefused, match="pg_get_publication_tables"):
        transpile("SELECT pg_get_publication_tables(c) AS v FROM t", "trino")


@pytest.mark.parametrize(
    "call",
    [
        "regexp_replace(c, p, 'x', 1, 1)",
        "regexp_replace(c, p || 'b', 'x', 1, 1)",
        "pg_catalog.\"regexp_replace\"(c, p, 'x', 1, 1)",
    ],
)
def test_a_pattern_a_column_holds_is_refused_by_name_on_clickhouse(call: str):
    with pytest.raises(FunctionRefused, match=r"regexp_replace\(\) has no form on clickhouse"):
        transpile(f"SELECT {call} AS v FROM t", "clickhouse")


@pytest.mark.parametrize("dialect", ["duckdb", "trino"])
def test_a_pattern_a_column_holds_is_read_where_the_engine_can(dialect: str):
    assert transpile("SELECT regexp_replace(c, p, 'x', 1, 1) AS v FROM t", dialect)


def test_a_constant_pattern_on_a_column_answers_on_clickhouse():
    sql = "SELECT regexp_replace(c, 'b', 'x', 1, 2) AS v FROM (SELECT 'abcb'::text AS c) t"
    assert chdb.query(transpile(sql, "clickhouse"), "TabSeparated").bytes() == b"abcx\n"


@pytest.mark.parametrize(
    ("call", "want"),
    [
        ("NULL::text, NULL::integer", b"\\N\n"),
        ("'ab'::text, 2::integer", b"abab\n"),
        ("'ab'::text, 0::integer", b"\n"),
    ],
)
def test_repeat_under_pg_catalog_answers_on_clickhouse(call: str, want: bytes):
    written = transpile(f'SELECT pg_catalog."repeat"({call}) AS v', "clickhouse")
    assert "pg_catalog" not in written
    assert chdb.query(written, "TabSeparated").bytes() == want
