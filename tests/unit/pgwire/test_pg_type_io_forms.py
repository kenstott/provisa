# Copyright (c) 2026 Kenneth Stott
# Canary: e6d40b73-1c28-4f95-a07e-3b94c1d7f026
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The type operators family answers as PostgreSQL 17 does (REQ-2171): each statement of
type_io_corpus, written for DuckDB, against what postgres:17.11 answered. Trino and ClickHouse replay
the same statements in tests/integration/test_type_io_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from provisa.core.pg_error import from_engine
from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.function_macros import MACROS, outcome
from provisa.transpiler.pg_forms_type_io import ROWS, readable
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.pgwire.pg17_type_io_answers import RECORDED
from tests.unit.pgwire.type_io_corpus import (
    REFUSALS,
    REFUSED,
    by_function,
    statements,
    verdict,
)

_FUNCTIONS = by_function(RECORDED)
_ENGINES = ("duckdb", "trino", "clickhouse")


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


@pytest.fixture(scope="module")
def connection() -> Any:
    connected = duckdb.connect()
    connected.execute("SET TimeZone='UTC'")
    return connected


def _difference(connection: Any, sql: str) -> str | None:
    try:
        got = connection.execute(transpile_to_duckdb(sql)).fetchone()[0]
    except duckdb.Error as raised:
        refusal = from_engine(raised)
        if refusal is None:
            return f"not raised as PostgreSQL's: {raised}"
        return verdict(sql, RECORDED[sql], None, f"{refusal.sqlstate}: {refusal}")
    return verdict(sql, RECORDED[sql], got, None)


@pytest.mark.parametrize("function", sorted(_FUNCTIONS))
def test_duckdb_answers_as_postgresql_17_answered(connection: Any, function: str):
    found = {sql: _difference(connection, sql) for sql in _FUNCTIONS[function]}
    assert {s: why for s, why in found.items() if why not in (None, REFUSED)} == {}
    assert sum(why == REFUSED for why in found.values()) <= REFUSALS.get(function, 0)


@pytest.mark.parametrize("name, arity", [(row[0], len(row[1])) for row in ROWS])
@pytest.mark.parametrize("dialect", _ENGINES)
def test_every_overload_has_a_form_on_the_engine(name: str, arity: int, dialect: str):
    assert outcome(name, arity, dialect) != "refused"
    assert MACROS[(name, arity)].forms


def test_a_function_is_read_row_by_row():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (-7), (NULL)) v(i)")
    rows = connection.execute(transpile_to_duckdb("SELECT int4send(i) AS v FROM t")).fetchall()
    assert [row[0] for row in rows] == [b"\x00\x00\x00\x01", b"\xff\xff\xff\xf9", None]


def test_a_statement_that_calls_none_of_them_is_not_parsed():
    assert readable("SELECT ¿ FROM t") == "SELECT ¿ FROM t"


def test_a_call_keeps_the_name_of_its_column():
    written = transpile_to_duckdb("SELECT pg_catalog.int4out(NULL::integer)")
    assert duckdb.connect().execute(written).description[0][0] == "int4out"


@pytest.mark.parametrize(
    "sql", [sql for sql, (oid, _) in RECORDED.items() if oid is not None and "ROW(" not in sql]
)
def test_the_wire_says_the_type_postgresql_says(sql: str):
    casts = result_casts(sql)
    assert casts is not None and casts[0] is not None and casts[0].oid == RECORDED[sql][0]
