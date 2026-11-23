# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7b1c94-0d2a-4f6e-9b58-71c4a2e6d0f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB's array functions of an array of more than one dimension (REQ-2236, amended 31776071a3).
Each statement is the one the function support measure sent (scripts/conformance/expected/
functions.json, run 45101362913), as Provisa writes it for DuckDB (transpile_to_duckdb) and runs
it in-process. PostgreSQL's answers are array.c's, json.c's and tsvector_op.c's: the functions that
read an array element by element read it flattened, array_cmp compares elements in storage order,
then the element count, then the dimensions, and the calls PostgreSQL refuses of such an array raise
its SQLSTATE."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import duckdb
import pytest

from provisa.federation.duckdb_pg_functions import register
from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.transpile import transpile_to_duckdb

_SQUARE = "'{{1,2},{3,4}}'::integer[]"
_ROW = "'{1,2,3}'::integer[]"
_TEXTS = "'{{a,b},{c,d}}'::text[]"
_JSONB = '\'{"a": 1, "b": [1, 2]}\'::jsonb'


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    register(c)
    return c


def _rows(con: duckdb.DuckDBPyConnection, call: str) -> list[tuple]:
    return con.execute(
        transpile_to_duckdb(f"SELECT {call} AS v LIMIT 1001", generated=False)
    ).fetchall()


def _one(con: duckdb.DuckDBPyConnection, call: str) -> object:
    rows = _rows(con, call)
    assert len(rows) == 1, rows
    return rows[0][0]


@pytest.mark.parametrize(
    ("call", "want"),
    [
        (f'pg_catalog."cardinality"({_SQUARE})', 4),
        (f'pg_catalog."arraycontains"({_SQUARE}, {_ROW})', True),
        (f'pg_catalog."arraycontained"({_SQUARE}, {_ROW})', False),
        (f'pg_catalog."arrayoverlap"({_SQUARE}, {_ROW})', True),
        (f"{_SQUARE} @> {_ROW}", True),
        (f'pg_catalog."array_to_tsvector"({_TEXTS})', "'a' 'b' 'c' 'd'"),
        (
            f"pg_catalog.\"setweight\"('cat:1 fat:2'::tsvector, 'a'::\"char\", {_TEXTS})",
            "'cat':1 'fat':2",
        ),
        (
            "pg_catalog.\"setweight\"('cat:1 fat:2'::tsvector, 'a'::\"char\", "
            "'{{cat,b},{c,d}}'::text[])",
            "'cat':1A 'fat':2",
        ),
        (f"pg_catalog.\"ts_delete\"('cat:1 fat:2'::tsvector, {_TEXTS})", "'cat':1 'fat':2"),
        ("pg_catalog.\"ts_delete\"('cat:1 fat:2'::tsvector, '{{cat,b},{c,d}}'::text[])", "'fat':2"),
        (f'pg_catalog."jsonb_exists_any"({_JSONB}, {_TEXTS})', True),
        (f'pg_catalog."jsonb_exists_all"({_JSONB}, {_TEXTS})', False),
    ],
)
def test_an_array_of_dimensions_is_read_element_by_element(con, call, want):
    assert _one(con, call) == want


def test_unnest_answers_each_element_of_the_elements_type(con):
    sql = transpile_to_duckdb(
        f'SELECT pg_catalog."unnest"({_SQUARE}) AS v LIMIT 1001', generated=False
    )
    result = con.execute(sql)
    assert result.fetchall() == [(1,), (2,), (3,), (4,)]
    assert str(result.description[0][1]) == "INTEGER"  # int4, as PostgreSQL's unnest of int4[]


@pytest.mark.parametrize("name", ["hash_array", "hash_array_extended"])
def test_hash_array_hashes_the_elements_alone(con, name):
    # hash_array reads each element in storage order and not the dimensions (array.c)
    seed = ", 3::bigint" if name == "hash_array_extended" else ""
    square = _one(con, f'pg_catalog."{name}"({_SQUARE}{seed})')
    flat = _one(con, f"pg_catalog.\"{name}\"('{{1,2,3,4}}'::integer[]{seed})")
    assert square == flat


@pytest.mark.parametrize("name", ["json_object", "jsonb_object"])
def test_json_object_reads_two_columns_as_pairs(con, name):
    pairs = _one(con, f'pg_catalog."{name}"({_TEXTS})')
    flat = _one(con, f"pg_catalog.\"{name}\"('{{a,b,c,d}}'::text[])")
    assert pairs == flat
    assert str(pairs).replace(" ", "") == '{"a":"b","c":"d"}'


#: array_cmp of {{1,2},{3,4}} and {1,2,3}: the first three elements agree, the first has more.
@pytest.mark.parametrize(
    ("call", "want"),
    [
        (f'pg_catalog."array_eq"({_SQUARE}, {_ROW})', False),
        (f'pg_catalog."array_ne"({_SQUARE}, {_ROW})', True),
        (f'pg_catalog."array_lt"({_SQUARE}, {_ROW})', False),
        (f'pg_catalog."array_le"({_SQUARE}, {_ROW})', False),
        (f'pg_catalog."array_gt"({_SQUARE}, {_ROW})', True),
        (f'pg_catalog."array_ge"({_SQUARE}, {_ROW})', True),
        (f'pg_catalog."array_lt"({_ROW}, {_SQUARE})', True),
        (f'pg_catalog."btarraycmp"({_SQUARE}, {_ROW})', 1),
        (f'pg_catalog."btarraycmp"({_ROW}, {_SQUARE})', -1),
        (f'pg_catalog."array_larger"({_SQUARE}, {_ROW})', [[1, 2], [3, 4]]),
        (f'pg_catalog."array_smaller"({_SQUARE}, {_ROW})', [1, 2, 3]),
        # as many elements, all equal: more dimensions is larger
        (f"pg_catalog.\"btarraycmp\"({_SQUARE}, '{{1,2,3,4}}'::integer[])", 1),
        # as many dimensions: the longer first dimension is larger
        (f'pg_catalog."btarraycmp"({_SQUARE}, ' + "'{{1,2,3,4}}'::integer[])", 1),
        # an element decides before the dimensions
        ("pg_catalog.\"btarraycmp\"('{{1,2},{3,4}}'::integer[], '{1,2,3,5}'::integer[])", -1),
        (f'pg_catalog."array_eq"({_SQUARE}, {_SQUARE})', True),
        (
            f'pg_catalog."array_larger"({_SQUARE}, ' + "'{{1,2},{3,5}}'::integer[])",
            [[1, 2], [3, 5]],
        ),
    ],
)
def test_arrays_of_dimensions_are_ordered_as_array_cmp(con, call, want):
    assert _one(con, call) == want


@pytest.mark.parametrize(
    ("call", "sqlstate", "message"),
    [
        (
            f'pg_catalog."array_append"({_SQUARE}, 3::integer)',
            "22000",
            "argument must be empty or one-dimensional array",
        ),
        (
            f'pg_catalog."array_prepend"(3::integer, {_SQUARE})',
            "22000",
            "argument must be empty or one-dimensional array",
        ),
        (
            f'pg_catalog."array_positions"({_SQUARE}, 3::integer)',
            "0A000",
            "searching for elements in multidimensional arrays is not supported",
        ),
        (
            f'pg_catalog."array_remove"({_SQUARE}, 3::integer)',
            "0A000",
            "removing elements from multidimensional arrays is not supported",
        ),
        (
            f'pg_catalog."width_bucket"(3::integer, {_SQUARE})',
            "2202E",
            "thresholds must be one-dimensional array",
        ),
        (
            f'pg_catalog."array_cat"({_SQUARE}, {_ROW})',
            "2202E",
            "cannot concatenate incompatible arrays",
        ),
        (
            "pg_catalog.\"array_larger\"('{{a,b},{c,d}}'::text[], '{a,b,c}'::text[])",
            "0A000",
            "array_larger() and array_smaller() of arrays of different dimensions have no form "
            "on duckdb unless both are integer arrays of constants",
        ),
        (
            "pg_catalog.\"json_object\"('{{a,b,c},{d,e,f}}'::text[])",
            "2202E",
            "array must have two columns",
        ),
    ],
)
def test_a_call_postgresql_refuses_raises_its_error(call, sqlstate, message):
    with pytest.raises(RowRefused) as raised:
        transpile_to_duckdb(f"SELECT {call} AS v LIMIT 1001", generated=False)
    assert (raised.value.sqlstate, str(raised.value)) == (sqlstate, message)


@pytest.mark.parametrize(
    ("call", "want"),
    [
        # one dimension fewer, of the other's inner dimensions: one more element of it
        (f"pg_catalog.\"array_cat\"({_SQUARE}, '{{5,6}}'::integer[])", [[1, 2], [3, 4], [5, 6]]),
        (f"pg_catalog.\"array_cat\"('{{5,6}}'::integer[], {_SQUARE})", [[5, 6], [1, 2], [3, 4]]),
        (f"pg_catalog.\"array_cat\"('{{}}'::integer[], {_SQUARE})", [[1, 2], [3, 4]]),
        (f'pg_catalog."array_cat"({_SQUARE}, {_SQUARE})', [[1, 2], [3, 4], [1, 2], [3, 4]]),
    ],
)
def test_array_cat_joins_arrays_as_array_c(con, call, want):
    assert _one(con, call) == want


@pytest.mark.parametrize(
    ("call", "want"),
    [
        (f'pg_catalog."cardinality"({_ROW})', 3),
        (f"pg_catalog.\"array_lt\"({_ROW}, '{{1,2,4}}'::integer[])", True),
        (f'pg_catalog."array_append"({_ROW}, 4::integer)', [1, 2, 3, 4]),
    ],
)
def test_an_array_of_one_dimension_keeps_its_form(con, call, want):
    assert _one(con, call) == want
