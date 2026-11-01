# Copyright (c) 2026 Kenneth Stott
# Canary: 8f26d1a4-e05b-47c3-9a18-c47b30e6d952
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's aggregate state functions and index support functions as forms (REQ-2171): each
statement of aggregate_corpus, written for DuckDB, against what PostgreSQL 17.11 answered. Trino and
ClickHouse replay them in tests/integration/test_hashes_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from typing import Any

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from provisa.core.pg_error import MARK, from_engine
from provisa.transpiler.function_macros import MACROS, Pg
from provisa.transpiler.pg_forms_aggregate import STATE_FUNCTIONS
from provisa.transpiler.pg_forms_index import SUPPORT_FUNCTIONS
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.aggregate_corpus import index_statements, same_answer, statements
from tests.unit.pgwire.pg17_aggregate_answers import RECORDED


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == [*statements(), *index_statements()]


@pytest.fixture(scope="module")
def connection():
    return duckdb.connect()


@pytest.mark.parametrize("sql", list(RECORDED))
def test_duckdb_answers_as_postgresql_17_answered(connection: Any, sql: str):
    try:
        got: Any = connection.execute(transpile(sql, "duckdb")).fetchone()[0]
    except duckdb.Error as raised:
        got = raised
    assert same_answer(sql, got, RECORDED[sql])


@pytest.mark.parametrize("form", (*STATE_FUNCTIONS, *SUPPORT_FUNCTIONS), ids=lambda f: f.name)
def test_a_form_is_postgresql_syntax(form: Any):
    macro = MACROS[(form.name, len(form.params.split(", ")))]
    _, written = macro.resolved("duckdb")
    assert isinstance(written, Pg)
    tree = sqlglot.parse_one(written, read="postgres")
    assert not list(tree.find_all(exp.Lambda))


def test_a_refusal_is_clickhouses_throw_where_its_cast_reads_null():
    written = transpile("SELECT int4pl(2147483647::integer, 1::integer) AS v", "clickhouse")
    assert "throwIf(1" in written
    assert MARK in written


def test_an_engine_error_reads_back_as_postgresqls():
    raised = RuntimeError(f"Code: 395. DB::Exception: {MARK} 22003: integer out of range")
    error = from_engine(raised)
    assert error is not None and error.sqlstate == "22003"
