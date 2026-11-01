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

from typing import Any

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from provisa.core.pg_error import MARK, from_engine
from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.function_macros import MACROS, FunctionRefused, outcome
from provisa.transpiler.metadata_facts import CatalogFacts
from provisa.transpiler.metadata_functions import expand
from provisa.transpiler.pg_forms_system import ROWS
from provisa.transpiler.transpile import transpile
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
    [("^a", "x"), (r"\yb", "x"), ("b*", "x"), ("b", r"\&"), ("b", r"\\")],
)
def test_what_a_search_keeps_of_its_place_is_refused_by_name(pattern: str, replacement: str):
    sql = f"SELECT regexp_replace('abc', '{pattern}', '{replacement}', 1, 1) AS v"
    with pytest.raises(duckdb.Error, match=rf"{MARK} 0A000: regexp_replace does not read"):
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
