# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e9a73-2b84-4d6f-a0e3-9f7d18c2b465
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The last engine errors of run 43450123314, each now PostgreSQL's answer or a refusal by name
(REQ-2167, REQ-2236; polyfill grid).

1. bool and numeric of a jsonb value on ClickHouse: pg_forms_json holds a json value as its text
   there, and marks it with the type the statement said, so the grid's row reads it as jsonb and
   refuses it by name, as on Trino (ClickHouse sent bool(...) and numeric(...), no function of its).
2. spg_poly_quad_compress on ClickHouse: the grid's row. ClickHouse evaluates a lambda's CASE
   whole, so a refusal in a branch raised for every polygon; and its NULL was a CAST to a String,
   which holds none.
3. range_intersect_agg on Trino: the group's texts are aggregated once, and each bound of the
   fold's state and item is read once, each bound to a lambda's variable; written out at each
   naming, the form was more than Trino compiles (QUERY_EXCEEDED_COMPILER_LIMIT)."""

# Requirements: REQ-2167, REQ-2236

from __future__ import annotations

from typing import Any

import duckdb
import pytest
import sqlglot

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.transpile import transpile


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _rows(runtime: ClickHouseFederationRuntime, sql: str) -> list[tuple[Any, ...]]:
    return list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())


def _one(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    (row,) = _rows(runtime, sql)
    return row[0]


def _raised(runtime: ClickHouseFederationRuntime, sql: str) -> PostgresError:
    with pytest.raises(Exception) as raised:
        _rows(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None, str(raised.value)[:300]
    return error


# --- 1. bool and numeric of a jsonb value on ClickHouse -----------------------------------------


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        ('SELECT pg_catalog."bool"(NULL::jsonb) AS v', "bool"),
        ('SELECT pg_catalog."numeric"(NULL::jsonb) AS v', "numeric"),
        ("SELECT pg_catalog.bool('true'::jsonb) AS v", "bool"),
        ("SELECT pg_catalog.numeric('1.5'::jsonb) AS v", "numeric"),
        ("SELECT pg_catalog.numeric('1.5'::json) AS v", "numeric"),
        ("SELECT pg_catalog.bool(c::jsonb) AS v FROM (VALUES ('true')) AS t(c)", "bool"),
    ],
)
def test_bool_and_numeric_of_a_json_value_are_refused_by_name_on_clickhouse(sql, function):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    assert str(refused.value) == f"{function}() has no form on clickhouse"


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ("SELECT pg_catalog.bool(1) AS v", True),
        ("SELECT pg_catalog.numeric(5) AS v", 5),
        ("SELECT CAST('{\"a\": 1}'::jsonb ->> 'a' AS INTEGER) AS v", 1),
    ],
)
def test_what_is_no_json_value_answers_as_before_on_clickhouse(runtime, sql, answer):
    assert _one(runtime, sql) == answer


def test_trino_refuses_bool_of_a_jsonb_value_as_clickhouse_does():
    with pytest.raises(FunctionRefused):
        transpile('SELECT pg_catalog."bool"(NULL::jsonb) AS v', "trino")


# --- 2. spg_poly_quad_compress on ClickHouse ----------------------------------------------------


@pytest.mark.parametrize(
    ("polygon", "box"),
    [
        ("((0,0),(1,0),(1,1))", "(1,1),(0,0)"),
        ("((1.5,-2),(3,4.25),(-7,0))", "(3,4.25),(-7,-2)"),
        ("((2,3))", "(2,3),(2,3)"),
    ],
)
def test_spg_poly_quad_compress_is_the_bounding_box_on_clickhouse(runtime, polygon, box):
    assert _one(
        runtime, f"SELECT pg_catalog.spg_poly_quad_compress('{polygon}'::polygon) AS v"
    ) == (box)


def test_spg_poly_quad_compress_of_null_is_null_on_clickhouse(runtime):
    assert _one(runtime, "SELECT pg_catalog.spg_poly_quad_compress(NULL::polygon) AS v") is None


def test_spg_poly_quad_compress_of_a_number_postgresql_writes_otherwise_is_refused(runtime):
    error = _raised(
        runtime, "SELECT pg_catalog.spg_poly_quad_compress('((1e20,0),(1,1))'::polygon) AS v"
    )
    assert (error.sqlstate, str(error)) == (
        "0A000",
        "spg_poly_quad_compress does not read this value",
    )


def test_spg_poly_quad_compress_is_a_grid_row_on_clickhouse():
    feature = next(f for f in GRID.features if f.name == "spg_poly_quad_compress")
    assert "clickhouse" in feature.read.rules[0].forms


# --- 3. range_intersect_agg on Trino ------------------------------------------------------------

_RANGES = "FROM (VALUES {rows}) AS t(c1)"


def _intersection(rows: list[str | None], range_type: str, order: str) -> str:
    values = ", ".join(
        f"(NULL::{range_type})" if r is None else f"('{r}'::{range_type})" for r in rows
    )
    return f'SELECT pg_catalog."range_intersect_agg"(c1{order}) AS v ' + _RANGES.format(rows=values)


def test_range_intersect_agg_on_trino_is_within_what_trino_compiles():
    written = transpile(
        _intersection(["[1,5)", "empty", "(,3]"], "int4range", " ORDER BY c1"), "trino"
    )
    # 32311 characters before, which Trino refused as too complex; range_agg's 19431 compiles.
    assert len(written) < 15000
    assert written.upper().count("ARRAY_AGG(") == 1  # the group's texts, aggregated once
    assert "COUNT(" not in written.upper()


@pytest.fixture(scope="module")
def trino_on_duckdb() -> Any:
    """DuckDB reading Trino's form as sqlglot writes it for DuckDB: a check of the form's logic.
    The two functions it names only in a timestamptz bound's text (a branch no integer or
    numeric bound takes) are stood in for."""
    con = duckdb.connect()
    con.execute("CREATE MACRO at_timezone(a, b) AS a")
    con.execute("CREATE MACRO format_datetime(a, b) AS CAST(a AS VARCHAR)")
    return con


@pytest.mark.parametrize("order", ["", " ORDER BY c1"])
@pytest.mark.parametrize(
    ("range_type", "rows", "answer"),
    [
        ("int4range", ["[1,5)", "empty", "(,3]"], "empty"),
        ("int4range", ["[1,5)", "(,3]"], "[1,4)"),
        ("int4range", ["[1,5)", "[2,9]"], "[2,5)"),
        ("int4range", ["[1,5)", "[7,9]"], "empty"),
        ("int4range", ["(,)", "[3,)"], "[3,)"),
        ("int4range", [None, None], None),
        ("int4range", ["[1,5)", None, "[2,3]"], "[2,4)"),
        ("numrange", ["[1.5,2)", "(1.7,3]"], "(1.7,2)"),
        ("numrange", ["[1.5,2]", "[2,3]"], "[2,2]"),
    ],
)
def test_range_intersect_agg_on_trino_answers_as_postgresql(
    trino_on_duckdb, range_type, rows, answer, order
):
    written = transpile(_intersection(rows, range_type, order), "trino")
    (on_duckdb,) = sqlglot.transpile(written, read="trino", write="duckdb")
    assert trino_on_duckdb.execute(on_duckdb).fetchall() == [(answer,)]
