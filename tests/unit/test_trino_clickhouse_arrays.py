# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e1c42-9d5a-4f61-8a2e-6c0f4d9b1e73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrays on Trino and ClickHouse as PostgreSQL reads them (REQ-2236, REQ-1965): an array's
text form is written as its elements, a call ClickHouse would make of a NULL array is the NULL
PostgreSQL answers, and Trino's array result is sent as PostgreSQL's array type."""

# Requirements: REQ-2236, REQ-1965

from __future__ import annotations

import importlib

import chdb
import pytest
import sqlglot
import sqlglot.expressions as exp
from buenavista.core import BVType
from buenavista.postgres import BVTYPE_TO_OID

from provisa.pgwire.server import _describe_casts, _sql_type_to_bvtype
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile, transpile_to_trino


def _array_text_casts(sql: str, dialect: str) -> list[exp.Cast]:
    """Each cast of a string literal to an array type in ``sql``."""
    tree = sqlglot.parse_one(sql, read=dialect)
    return [
        cast
        for cast in tree.find_all(exp.Cast)
        if cast.to.is_type(exp.DataType.Type.ARRAY)
        and isinstance(cast.this, exp.Literal)
        and cast.this.is_string
    ]


def _first_column(sql: str, dialect: str) -> exp.Expr:
    tree = sqlglot.parse_one(sql, read=dialect)
    assert isinstance(tree, exp.Select)
    return tree.selects[0]


def _null_array_casts(sql: str, dialect: str) -> list[exp.Cast]:
    tree = sqlglot.parse_one(sql, read=dialect)
    return [
        cast
        for cast in tree.find_all(exp.Cast)
        if cast.to.is_type(exp.DataType.Type.ARRAY) and isinstance(cast.this, exp.Null)
    ]


@pytest.mark.parametrize(
    "pg_sql",
    [
        "SELECT pg_catalog.\"array_dims\"('{1,2,3}'::integer[]) AS v",
        "SELECT pg_catalog.\"array_ndims\"('{1,2,3}'::integer[]) AS v",
        "SELECT pg_catalog.\"array_length\"('{1,2,3}'::integer[], 1) AS v",
        "SELECT pg_catalog.\"array_lower\"('{1,2,3}'::integer[], 1) AS v",
        "SELECT pg_catalog.\"array_upper\"('{1,2,3}'::integer[], 1) AS v",
        "SELECT pg_catalog.\"array_fill\"(NULL::integer, '{1}'::integer[]) AS v",
        "SELECT pg_catalog.\"array_positions\"('{-7,NULL,42}'::integer[], 42::integer) AS v",
        "SELECT '{{1,2},{3,4}}'::integer[] AS v",
        "SELECT '{}'::integer[] AS v",
    ],
)
@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
def test_an_array_text_form_is_written_as_its_elements(pg_sql, dialect):
    written = transpile_to_trino(pg_sql) if dialect == "trino" else transpile(pg_sql, dialect)
    assert _array_text_casts(written, dialect) == []
    assert sqlglot.parse_one(written, read=dialect).find(exp.Array) is not None


def test_trino_reads_the_elements_cast_to_the_array_type():
    written = transpile_to_trino("SELECT '{1,NULL,3}'::integer[] AS v")
    assert written == "SELECT CAST(ARRAY['1', NULL, '3'] AS ARRAY(INTEGER)) AS v"


def test_a_nested_text_form_is_an_array_of_its_dimensions_on_trino():
    written = transpile_to_trino("SELECT '{{1,2},{3,4}}'::integer[] AS v")
    cast = sqlglot.parse_one(written, read="trino").find(exp.Cast)
    assert cast is not None
    assert cast.to.sql(dialect="trino") == "ARRAY(ARRAY(INTEGER))"


@pytest.mark.parametrize(
    "pg_sql",
    [
        'SELECT pg_catalog."array_dims"(NULL::integer[]) AS v',
        'SELECT pg_catalog."array_ndims"(NULL::integer[]) AS v',
        'SELECT pg_catalog."array_length"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."array_lower"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."array_upper"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."array_positions"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."array_replace"(NULL::integer[], NULL::integer, NULL::integer) AS v',
        'SELECT pg_catalog."array_shuffle"(NULL::integer[]) AS v',
    ],
)
def test_a_call_of_a_null_array_is_null_on_clickhouse(pg_sql):
    written = transpile(pg_sql, "clickhouse")
    assert _null_array_casts(written, "clickhouse") == []
    column = _first_column(written, "clickhouse")
    assert isinstance(column.unalias(), exp.Null)
    assert column.alias == "v"


def test_a_null_array_appended_to_is_not_answered_null_on_clickhouse():
    # array_append(NULL, x) is {x} in PostgreSQL: not STRICT, and it does not answer NULL.
    written = transpile('SELECT pg_catalog."array_append"(NULL::integer[], 1) AS v', "clickhouse")
    column = _first_column(written, "clickhouse")
    assert not isinstance(column.unalias(), exp.Null)


# PostgreSQL 17 (array_userfuncs.c): array_append and array_prepend read a NULL array as an empty
# one, and array_cat answers the other argument, NULL only where both are NULL. ClickHouse has no
# NULL of an array: each is written as that answer, by the polyfill grid's rows (REQ-2236).
@pytest.mark.parametrize(
    ("pg_sql", "written"),
    [
        ('SELECT pg_catalog."array_append"(NULL::integer[], 1) AS v', "SELECT [1] AS v"),
        ("SELECT array_append(NULL::integer[], 1) AS v", "SELECT [1] AS v"),
        ('SELECT pg_catalog."array_prepend"(1, NULL::integer[]) AS v', "SELECT [1] AS v"),
        ("SELECT array_prepend(1, NULL::integer[]) AS v", "SELECT [1] AS v"),
        ("SELECT array_cat(NULL::integer[], ARRAY[1, 2]) AS v", "SELECT [1, 2] AS v"),
        ("SELECT array_cat(ARRAY[1, 2], NULL::integer[]) AS v", "SELECT [1, 2] AS v"),
        ('SELECT pg_catalog."array_cat"(NULL::integer[], ARRAY[1]) AS v', "SELECT [1] AS v"),
        ('SELECT pg_catalog."array_cat"(ARRAY[1], NULL::integer[]) AS v', "SELECT [1] AS v"),
        ("SELECT array_cat(NULL::integer[], NULL::integer[]) AS v", "SELECT NULL AS v"),
        (
            'SELECT pg_catalog."array_cat"(NULL::integer[], NULL::integer[]) AS v',
            "SELECT NULL AS v",
        ),
    ],
)
def test_a_null_array_is_written_as_postgresqls_answer_on_clickhouse(pg_sql, written):
    assert transpile(pg_sql, "clickhouse") == written


def _on_clickhouse(sql: str) -> list[str | None]:
    held = chdb.query(transpile(sql, "clickhouse"), "TabSeparated").bytes().decode()
    return [None if line == "\\N" else line for line in held.splitlines()]


@pytest.mark.parametrize(
    ("pg_sql", "answer"),
    [
        ('SELECT pg_catalog."array_append"(NULL::integer[], 1) AS v', "[1]"),
        ('SELECT pg_catalog."array_append"(NULL::integer[], NULL::integer) AS v', "[NULL]"),
        ('SELECT pg_catalog."array_prepend"(1, NULL::integer[]) AS v', "[1]"),
        ('SELECT pg_catalog."array_prepend"(NULL::integer, NULL::integer[]) AS v', "[NULL]"),
        ("SELECT array_cat(NULL::integer[], '{1,2}'::integer[]) AS v", "[1,2]"),
        ("SELECT array_cat('{1,2}'::integer[], NULL::integer[]) AS v", "[1,2]"),
        ('SELECT pg_catalog."array_cat"(NULL::integer[], NULL::integer[]) AS v', None),
    ],
)
def test_clickhouse_answers_a_null_array_as_postgresql_does(pg_sql, answer):
    assert _on_clickhouse(pg_sql) == [answer]


@pytest.mark.parametrize(
    ("pg_sql", "written"),
    [
        # ClickHouse's own names: it has no ARRAY_APPEND or ARRAY_PREPEND (an unknown function).
        ("SELECT array_append(ARRAY[1], 2) AS v", "SELECT arrayPushBack([1], 2) AS v"),
        ("SELECT array_prepend(1, ARRAY[2]) AS v", "SELECT arrayPushFront([2], 1) AS v"),
        ("SELECT array_append(a, 2) AS v FROM t", "SELECT arrayPushBack(a, 2) AS v FROM t"),
        ("SELECT array_prepend(2, a) AS v FROM t", "SELECT arrayPushFront(a, 2) AS v FROM t"),
    ],
)
def test_an_array_that_is_not_a_null_is_written_as_before_on_clickhouse(pg_sql, written):
    """No NULL array's form: the call, in ClickHouse's spelling of it."""
    assert transpile(pg_sql, "clickhouse") == written


@pytest.mark.parametrize(
    ("pg_sql", "answer"),
    [
        ("SELECT array_append(ARRAY[1], 2) AS v", "[1,2]"),
        ("SELECT array_prepend(1, ARRAY[2]) AS v", "[1,2]"),
    ],
)
def test_an_array_that_is_not_a_null_is_answered_on_clickhouse(pg_sql, answer):
    assert _on_clickhouse(pg_sql) == [answer]


def test_array_cat_of_two_arrays_is_still_refused_on_clickhouse():
    with pytest.raises(FunctionRefused, match=r"array_cat\(\) has no form on clickhouse"):
        transpile("SELECT array_cat(a, b) AS v FROM t", "clickhouse")


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
def test_every_statement_is_read_for_an_array_text_form_on_its_tree(monkeypatch, dialect):
    """REQ-913: whether a statement holds an array's text form is read from its tree, never from
    a substring of its text; a statement with none is written as before."""
    module = importlib.import_module("provisa.transpiler.transpile")
    read: list[exp.Expr] = []
    own = module.array_inputs

    def spied(tree: exp.Expr) -> exp.Expr:
        read.append(tree)
        return own(tree)

    monkeypatch.setattr(module, "array_inputs", spied)
    sql = "SELECT a + 1 AS v FROM t WHERE b = 'x'"
    assert transpile(sql, dialect) == sql
    assert len(read) == 1
    assert transpile("SELECT '{1,2}'::integer[] AS v", dialect) != "SELECT '{1,2}'::integer[] AS v"
    assert len(read) == 2


@pytest.mark.parametrize(
    ("served", "oid"),
    [
        ("array(integer)", 1007),
        ("array(array(integer))", 1007),
        ("array(bigint)", 1016),
        ("array(varchar)", 1009),
        ("array(varchar(3))", 1009),
        ("array(double)", 1022),
        ("array(boolean)", 1000),
    ],
)
def test_a_trino_array_type_is_sent_as_postgresqls(served, oid):
    assert BVTYPE_TO_OID[_sql_type_to_bvtype(served)] == oid


def _described(sql: str, served: str) -> tuple[BVType, int | None]:
    types = [_sql_type_to_bvtype(served)]
    oids: list[int | None] = [None]
    _describe_casts(sql, types, oids, [-1], [], [served])
    return types[0], oids[0]


@pytest.mark.parametrize(
    "pg_sql",
    [
        'SELECT pg_catalog."array_positions"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."array_shuffle"(NULL::integer[]) AS v',
        'SELECT pg_catalog."trim_array"(NULL::integer[], NULL::integer) AS v',
    ],
)
def test_an_int_array_result_on_trino_is_described_as_int4_array(pg_sql):
    assert _described(pg_sql, "array(integer)") == (BVType.INTEGERARRAY, 1007)


def test_an_int_array_result_clickhouse_could_not_type_is_described_as_int4_array():
    # ClickHouse's NULL (its Nothing) reaches Arrow as a binary: the call's type describes it.
    sql = 'SELECT pg_catalog."array_positions"(NULL::integer[], NULL::integer) AS v'
    assert _described(sql, "BLOB") == (BVType.INTEGERARRAY, 1007)
