# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1e8b37-2f94-4c5a-b0e7-9a3c4d2f8e61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's wrong answers of run 43361055823 that are its forms' (REQ-2236, REQ-2171): grid rows.

array_agg(NULL::integer) answered '{}' where PostgreSQL answers '{NULL}': ClickHouse's groupArray
skips a NULL, and answers an empty array of no rows, where PostgreSQL's is NULL. current_schemas
answered ClickHouse's own currentSchemas, its database's name, where PostgreSQL answers its search
path (NULL of a NULL): refused by name, as on Trino."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import pytest
import sqlglot

from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.polyfill.clickhouse import ARRAY_AGG, CURRENT_SCHEMAS
from provisa.transpiler.transpile import transpile


def _clickhouse(sql: str) -> str:
    return transpile(sql, "clickhouse")


def test_each_row_is_in_the_grid():
    # By name: the grid holds each row with what polyfill.measured states of it.
    in_the_grid = {f.name for f in GRID.features}
    assert {ARRAY_AGG.name, CURRENT_SCHEMAS.name} <= in_the_grid


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."array_agg"(NULL::integer) AS v',
        "SELECT array_agg(x) AS v FROM t",
    ],
)
def test_array_agg_keeps_its_nulls_and_is_null_of_no_rows(sql):
    written = _clickhouse(sql)
    assert "groupArray(tuple(" in written
    assert "count() = 0" in written
    assert "ARRAY_AGG" not in written.upper().replace("GROUPARRAY", "")
    sqlglot.parse_one(written, read="clickhouse")  # ClickHouse's own SQL


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT array_agg(DISTINCT x) AS v FROM t",
        "SELECT array_agg(x) FILTER (WHERE x > 1) AS v FROM t",
        "SELECT array_agg(x) OVER () AS v FROM t",
    ],
)
def test_array_agg_of_distinct_a_filter_or_a_window_keeps_its_form(sql):
    """Only the plain call is this row's: the others keep what they had (an ordered one is
    refused by name: test_polyfill_clickhouse_remaining)."""
    assert "groupArray(tuple(" not in _clickhouse(sql)


@pytest.mark.parametrize("dialect", ["duckdb", "trino"])
def test_array_agg_on_another_engine_is_unchanged(dialect):
    written = transpile('SELECT pg_catalog."array_agg"(NULL::integer) AS v', dialect)
    assert "groupArray" not in written


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."current_schemas"(NULL::boolean) AS v',
        "SELECT current_schemas(true) AS v",
    ],
)
def test_current_schemas_is_refused_by_name_on_clickhouse(sql):
    with pytest.raises(RowRefused) as refused:
        _clickhouse(sql)
    assert "current_schemas() has no form on clickhouse" in str(refused.value)


def test_current_schemas_is_still_refused_on_trino():
    with pytest.raises(RowRefused, match="current_schemas"):
        transpile("SELECT current_schemas(true) AS v", "trino")


def test_current_schemas_on_duckdb_is_unchanged():
    assert "current_schemas" in transpile("SELECT current_schemas(true) AS v", "duckdb").lower()


def test_array_agg_of_an_order_is_refused_by_name():
    with pytest.raises(RowRefused, match="array_agg"):
        _clickhouse("SELECT array_agg(x ORDER BY y) AS v FROM t")


_OF_ARRAYS = [
    "SELECT array_agg('{1}'::integer[]) AS v",
    "SELECT array_agg(ARRAY[1]) AS v",
    "SELECT array_agg(a) AS v FROM (VALUES (ARRAY[1,2]), (ARRAY[3,4])) t(a)",
]


@pytest.mark.parametrize("sql", _OF_ARRAYS)
def test_array_agg_of_arrays_writes_no_null_of_an_array(sql):
    """ClickHouse 24.3 holds no NULL of an array: if(count() = 0, NULL, <array of arrays>) is its
    error 'some of them are Array and some of them are not' (run 43725815055), not an answer."""
    written = _clickhouse(sql)
    assert "groupArray(" in written
    assert "THEN NULL" not in written.upper() and ", NULL," not in written.upper(), written
    sqlglot.parse_one(written, read="clickhouse")


def _on_chdb(sql: str) -> str:
    import chdb

    return chdb.query(_clickhouse(sql), "TabSeparated").bytes().decode().strip()


def _raised_on_chdb(sql: str) -> tuple[str, str]:
    from provisa.core.pg_error import from_engine

    with pytest.raises(Exception) as raised:  # noqa: PT011 -- chdb raises its own
        _on_chdb(sql)
    said = from_engine(raised.value)
    assert said is not None, str(raised.value)[:300]
    return said.sqlstate, str(said)


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ("SELECT array_agg(ARRAY[1]) AS v", "[[1]]"),
        ("SELECT array_agg(a) AS v FROM (VALUES (ARRAY[1,2]), (ARRAY[3,4])) t(a)", "[[1,2],[3,4]]"),
        (
            "SELECT array_agg(a) AS v FROM (VALUES (ARRAY['a,b', 'c']), (ARRAY['d', 'e'])) t(a)",
            "[['a,b','c'],['d','e']]",
        ),
    ],
)
def test_array_agg_of_arrays_answers_their_array(sql, answer):
    assert _on_chdb(sql) == answer


@pytest.mark.parametrize(
    ("sql", "said"),
    [
        # PostgreSQL's NULL of no rows, which ClickHouse has no value of: the edge raises.
        (
            "SELECT array_agg(a) AS v FROM (VALUES (ARRAY[1])) t(a) WHERE false",
            ("0A000", "array_agg of no rows does not read this value"),
        ),
        # PostgreSQL 17's accumArrayResultArr.
        (
            "SELECT array_agg(a) AS v FROM (VALUES (ARRAY[1]), (CAST(ARRAY[] AS integer[]))) t(a)",
            ("2202E", "cannot accumulate empty arrays"),
        ),
        (
            "SELECT array_agg(a) AS v FROM (VALUES (ARRAY[1]), (ARRAY[1, 2])) t(a)",
            ("2202E", "cannot accumulate arrays of different dimensionality"),
        ),
        (
            "SELECT array_agg(a) AS v FROM (VALUES (ARRAY[[1, 2], [3, 4]]), (ARRAY[[1, 2, 3, 4]])) t(a)",
            ("2202E", "cannot accumulate arrays of different dimensionality"),
        ),
    ],
)
def test_array_agg_of_arrays_raises_what_postgresql_raises(sql, said):
    assert _raised_on_chdb(sql) == said


def test_array_agg_opens_the_walk_only_where_its_row_has_a_form():
    """The row has a form on ClickHouse alone: on Trino array_agg is the engine's own, and its name
    does not walk a statement through every row (run 43725815055: a range form's own substrings,
    each guarded as a call's, passed Trino's compiler limit)."""
    from provisa.transpiler.function_macros import mentioned

    assert mentioned("SELECT array_agg(x) AS v FROM t", "clickhouse")
    assert not mentioned("SELECT array_agg(x) AS v FROM t", "trino")


def test_a_rewrite_opens_the_walk_only_on_its_own_column():
    """A ClickHouse rewrite mentions ::text; on Trino that cast is the engine's own, and it does not
    walk the statement (main after b4d5f79d7b: the range form's substrings were guarded again)."""
    from provisa.transpiler.function_macros import mentioned

    assert mentioned("SELECT v::text AS r FROM t", "clickhouse")
    assert not mentioned("SELECT v::text AS r FROM t", "trino")
    assert mentioned("SELECT interval '1 day' AS r", "trino")


def test_a_multirange_of_an_aggregated_array_on_trino_keeps_its_forms_own_substrings():
    from provisa.compiler.native_subtype import mark_subtype_operations

    statement = "SELECT tstzmultirange(VARIADIC array_agg(v ORDER BY i))::text AS r FROM s.t"
    marked = mark_subtype_operations(statement, {("s", "t"): {"v": "tstzrange"}})
    assert "GREATEST(" not in transpile(marked, "trino").upper()
