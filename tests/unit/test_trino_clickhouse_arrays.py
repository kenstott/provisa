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

import pytest
import sqlglot
import sqlglot.expressions as exp
from buenavista.core import BVType
from buenavista.postgres import BVTYPE_TO_OID

from provisa.pgwire.server import _describe_casts, _sql_type_to_bvtype
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
