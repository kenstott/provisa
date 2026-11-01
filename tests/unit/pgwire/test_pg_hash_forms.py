# Copyright (c) 2026 Kenneth Stott
# Canary: 92f3c0b6-7e41-4a1d-b850-16d4ea7f3c29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's hash functions as forms (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED): each
statement of hash_corpus, written for DuckDB, against what PostgreSQL 17.11 answered. Trino and
ClickHouse replay the same statements in tests/integration/test_hashes_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from typing import Any

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from provisa.core.pg_error import MARK
from provisa.transpiler.function_macros import MACROS, FunctionRefused, outcome
from provisa.transpiler.pg_forms_hash import ENGINES, forms
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.hash_corpus import VALUES, statements
from tests.unit.pgwire.pg17_hash_answers import RECORDED

_ON_ENGINES = tuple(name for name, _ in ENGINES)


def _text(value: Any) -> str | None:
    return None if value is None else str(value)


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


@pytest.fixture(scope="module")
def connection():
    return duckdb.connect()


@pytest.mark.parametrize("sql", list(RECORDED))
def test_duckdb_answers_as_postgresql_17_answered(connection: Any, sql: str):
    got = connection.execute(transpile(sql, "duckdb")).fetchone()[0]
    assert _text(got) == RECORDED[sql][1]


def test_a_hash_is_read_as_its_column_is_named():
    written = transpile('SELECT pg_catalog."hashint4"(1::integer)', "duckdb")
    described = duckdb.connect().execute(written).description
    assert described[0][0] == "hashint4"


def test_a_hash_reads_a_column_row_by_row():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('abc'), ('雪'), (NULL)) v(s)")
    rows = connection.execute(transpile("SELECT hashtext(s) AS v FROM t", "duckdb")).fetchall()
    assert [row[0] for row in rows] == [
        RECORDED["SELECT hashtext('abc'::text) AS v"][1] and -785388649,
        int(RECORDED["SELECT hashtext('雪'::text) AS v"][1]),
        None,
    ]


def test_a_hash_of_a_hash_reads_the_inner_call():
    written = transpile("SELECT hashint4(hashint4(1::integer)) AS v", "duckdb")
    inner = int(RECORDED["SELECT hashint4((1)::integer) AS v"][1])
    alone = transpile(f"SELECT hashint4(({inner})::integer) AS v", "duckdb")
    connection = duckdb.connect()
    assert connection.execute(written).fetchone() == connection.execute(alone).fetchone()


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        ("SELECT hashchar('\\xe9'::\"char\") AS v", "hashchar"),
        ("SELECT hash_numeric('1e3'::numeric) AS v", "hash_numeric"),
        (f"SELECT hashname('{'é' * 40}'::name) AS v", "hashname"),
        ("SELECT hash_record('(1,2)'::record) AS v", "hash_record"),
        ("SELECT hash_aclitem('a=r/b'::aclitem) AS v", "hash_aclitem"),
        ("SELECT hashenum(1::oid) AS v", "hashenum"),
    ],
)
def test_a_value_no_form_can_answer_is_refused_by_name(sql: str, function: str):
    with pytest.raises(duckdb.Error, match=rf"{MARK} 0A000: {function} does not read"):
        duckdb.connect().execute(transpile(sql, "duckdb")).fetchall()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT hash_array(c) AS v FROM t",
        "SELECT hash_range(c) AS v FROM t",
        "SELECT hash_multirange(c) AS v FROM t",
        "SELECT hash_array('{1}'::uuid[]) AS v",
    ],
)
def test_a_composite_of_no_said_type_is_refused_by_name(sql: str):
    with pytest.raises(FunctionRefused, match=r"has no form on duckdb"):
        transpile(sql, "duckdb")


def _named(plain: str) -> list[tuple[str, int]]:
    return [key for key in MACROS if key[0].startswith(plain)]


@pytest.mark.parametrize("plain", [pair[0] for pair in VALUES])
@pytest.mark.parametrize(
    "dialect", ["spark", "snowflake", "bigquery", "mysql", "sqlite", "tsql", "oracle", "presto"]
)
def test_an_engine_with_no_fold_here_refuses_by_name(plain: str, dialect: str):
    assert {outcome(name, arity, dialect) for name, arity in _named(plain)} == {"refused"}


@pytest.mark.parametrize("dialect", _ON_ENGINES)
def test_each_form_parses_as_its_engine_writes_it(dialect: str):
    engine = dict(ENGINES)[dialect]
    for (name, _), sql in forms(engine).items():
        tree = sqlglot.parse_one(sql, read=dialect)
        assert isinstance(tree, exp.Expr), name
        assert "provisa_" not in sql, name
