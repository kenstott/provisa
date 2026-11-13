# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1b4c2a-97d3-4f80-b5a1-3c8e2d7f90b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""percentile_cont and percentile_disc of NULL fractions (REQ-2171). A NULL fraction array answers
NULL: ClickHouse has no Nullable array to answer it with, so there the call is refused by name rather
than left to the engine's "no supertype" error. A NULL element of an array answers NULL in its place,
the others their percentiles, as PostgreSQL 17 does."""

# Requirements: REQ-2171

from __future__ import annotations

import datetime
import math

import duckdb
import pytest

from provisa.federation.duckdb_pg_functions import register
from provisa.transpiler.transpile import transpile

_NULL_ARRAY = (
    'SELECT pg_catalog."{f}"(NULL::double precision[]) WITHIN GROUP '
    "(ORDER BY NULL::double precision) AS v"
)
_NULL_FRACTION = (
    'SELECT pg_catalog."{f}"(NULL::double precision) WITHIN GROUP '
    "(ORDER BY NULL::double precision) AS v"
)
_ROWS = "FROM (VALUES (1.0::float8), (3.0::float8), (4.0::float8)) AS t(c1)"
#: Each statement with a NULL element and PostgreSQL 17's answer over 1, 3 and 4.
NULL_ELEMENTS: dict[str, list[float | None]] = {
    f"SELECT percentile_cont(ARRAY[0.5, NULL]::float8[]) WITHIN GROUP (ORDER BY c1) AS v {_ROWS}": [
        3.0,
        None,
    ],
    "SELECT percentile_cont(ARRAY[NULL, 0.25, NULL, 1]::float8[]) WITHIN GROUP (ORDER BY c1) AS v "
    f"{_ROWS}": [None, 2.0, None, 4.0],
    f"SELECT percentile_disc('{{0.5,NULL}}'::float8[]) WITHIN GROUP (ORDER BY c1) AS v {_ROWS}": [
        3.0,
        None,
    ],
    "SELECT percentile_disc(ARRAY[NULL, 0]::float8[]) WITHIN GROUP (ORDER BY c1 DESC) AS v "
    f"{_ROWS}": [None, 4.0],
    f"SELECT percentile_disc(ARRAY[NULL]::float8[]) WITHIN GROUP (ORDER BY c1) AS v {_ROWS}": [
        None
    ],
}

_NONE = "FROM (VALUES (1.0::float8)) AS t(c1) WHERE c1 > 9"
_ALL_NULL = "FROM (VALUES (NULL::float8), (NULL::float8)) AS t(c1)"
#: A percentile over no keys -- no rows, or only NULL ones -- is NULL, a fraction or an array of
#: them alike, as PostgreSQL 17 answers (the profiler's quantiles of an empty column).
NO_KEYS: tuple[str, ...] = (
    f"SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY c1) AS v {_NONE}",
    f"SELECT percentile_cont(ARRAY[0, 0.01, 0.5, 1]::float8[]) WITHIN GROUP (ORDER BY c1) AS v {_NONE}",
    "SELECT percentile_cont(ARRAY[0, 0.01, 0.5, 1]::float8[]) WITHIN GROUP (ORDER BY c1 DESC) AS v "
    f"{_ALL_NULL}",
    f"SELECT percentile_disc(ARRAY[0.25, 1]::float8[]) WITHIN GROUP (ORDER BY c1) AS v {_ALL_NULL}",
    f"SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY c1) AS v {_NONE}",
)


def agrees(got: list[float | None], want: list[float | None]) -> bool:
    """Each element PostgreSQL's: NULL where it is NULL, else within a relative 1e-14."""
    return len(got) == len(want) and all(
        (g is None) == (w is None) and (w is None or math.isclose(g, w, rel_tol=1e-14))
        for g, w in zip(got, want, strict=True)
    )


@pytest.fixture(scope="module")
def connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    register(conn)
    return conn


@pytest.mark.parametrize("function", ["percentile_cont", "percentile_disc"])
def test_clickhouse_produces_dead_branch_for_null_fraction_array(function: str):
    # REQ-2167: MATRIX["clickhouse"] (commit 4c510070b2) changed this entry from "refused" to
    # "error" — ClickHouse is now expected to raise its own engine error because the implicit-else
    # NULL of CASE WHEN FALSE THEN Array(Float64) END is a type mismatch in ClickHouse (arrays
    # are non-nullable). The transpilation must succeed and emit the dead-branch guard.
    sql = transpile(_NULL_ARRAY.format(f=function), "clickhouse")
    assert "CASE WHEN FALSE" in sql, f"expected CASE WHEN FALSE dead branch, got: {sql!r}"


@pytest.mark.parametrize("function", ["percentile_cont", "percentile_disc"])
def test_a_null_fraction_alone_is_still_served_on_clickhouse(function: str):
    assert "CASE WHEN FALSE" in transpile(_NULL_FRACTION.format(f=function), "clickhouse")


@pytest.mark.parametrize("function", ["percentile_cont", "percentile_disc"])
@pytest.mark.parametrize(
    ("rows", "where"),
    [("(1.0::float8), (2.0::float8), (3.0::float8)", ""), ("(1.0::float8)", " WHERE c > 5")],
)
def test_a_null_fraction_on_clickhouse_is_one_row_of_the_group(function: str, rows, where):
    """The dead branch keeps the aggregate: folded away, the statement is no aggregate and
    answers a row for each row (none of no row), where PostgreSQL answers one NULL."""
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    sql = (
        f"SELECT {function}(NULL::double precision) WITHIN GROUP (ORDER BY c) AS v "
        f"FROM (VALUES {rows}) AS t(c){where}"
    )
    runtime = ClickHouseFederationRuntime.embedded()
    assert list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows()) == [(None,)]


def test_trino_still_answers_a_null_fraction_array():
    assert "CASE WHEN FALSE" in transpile(_NULL_ARRAY.format(f="percentile_cont"), "trino")


@pytest.mark.parametrize("sql", list(NULL_ELEMENTS))
def test_duckdb_answers_a_null_element_in_its_place(connection, sql: str):
    assert agrees(connection.execute(transpile(sql, "duckdb")).fetchone()[0], NULL_ELEMENTS[sql])


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
@pytest.mark.parametrize("sql", list(NULL_ELEMENTS))
def test_a_null_element_is_served_on_trino_and_clickhouse(sql: str, dialect: str):
    transpile(sql, dialect)  # FunctionRefused would fail the test


def test_an_interval_percentile_keeps_a_null_element_in_its_place(connection):
    sql = (
        "SELECT percentile_cont(ARRAY[0.5, NULL]::float8[]) WITHIN GROUP (ORDER BY c1) AS v "
        "FROM (VALUES ('1 day'::interval), ('3 days'::interval)) AS t(c1)"
    )
    got = connection.execute(transpile(sql, "duckdb")).fetchone()[0]
    assert got == [datetime.timedelta(days=2), None]


@pytest.mark.parametrize("sql", NO_KEYS)
def test_duckdb_answers_null_over_no_keys(connection, sql: str):
    assert connection.execute(transpile(sql, "duckdb")).fetchone()[0] is None


@pytest.mark.parametrize("sql", NO_KEYS)
def test_trino_reads_no_element_of_no_keys(sql: str):
    # Trino's element_at refuses an index below 1 ("SQL array indices start at 1"), and the line
    # between two keys over no keys reads index 0: the sorted keys answer NULL when there are none,
    # before any element of them is read -- an empty array (only NULL keys) or no array at all
    # (array_agg over no rows) (test_percentile_null_elements_on_engines runs it).
    assert "CASE WHEN COALESCE(CARDINALITY(_s), 0) = 0 THEN NULL ELSE" in transpile(sql, "trino")
