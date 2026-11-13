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
   there, and marks it with the type the statement said, so the grid's row reads it as jsonb
   (ClickHouse was sent bool(...) and numeric(...), no function of its). bool answers a JSON
   boolean, numeric a JSON number's own digits; any other kind is PostgreSQL's error, and what has
   no exact answer (a number with an exponent, text that is no JSON) raises: the row's limitation.
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

_BIG = "1234567890123456789012345678901234567890.123456789012345678901234567890"


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ('SELECT pg_catalog."bool"(NULL::jsonb) AS v', None),
        ("SELECT pg_catalog.bool('true'::jsonb) AS v", True),
        ("SELECT pg_catalog.bool('false'::jsonb) AS v", False),
        ("SELECT pg_catalog.bool(' true '::jsonb) AS v", True),
        ("SELECT pg_catalog.bool(c::jsonb) AS v FROM (VALUES ('false')) AS t(c)", False),
        ("SELECT pg_catalog.bool(c::jsonb) AS v FROM (VALUES (NULL::text)) AS t(c)", None),
    ],
)
def test_bool_of_a_json_boolean_is_its_value_on_clickhouse(runtime, sql, answer):
    assert _one(runtime, sql) is answer


@pytest.mark.parametrize(
    ("sql", "digits"),
    [
        ('SELECT pg_catalog."numeric"(NULL::jsonb) AS v', None),
        ("SELECT pg_catalog.numeric('1.50'::jsonb) AS v", "1.50"),
        ("SELECT pg_catalog.numeric('-7'::jsonb) AS v", "-7"),
        ("SELECT pg_catalog.numeric('0.000001'::jsonb) AS v", "0.000001"),
        ("SELECT pg_catalog.numeric('-0'::jsonb) AS v", "0"),
        ("SELECT pg_catalog.numeric('-0.00'::jsonb) AS v", "0.00"),
        (f"SELECT pg_catalog.numeric('{_BIG}'::jsonb) AS v", _BIG),
        (f"SELECT pg_catalog.numeric('-{_BIG}'::jsonb) AS v", f"-{_BIG}"),
        ("SELECT pg_catalog.numeric(c::jsonb) AS v FROM (VALUES (' 2.5 ')) AS t(c)", "2.5"),
    ],
)
def test_numeric_of_a_json_number_is_its_digits_never_rounded_on_clickhouse(runtime, sql, digits):
    """The number's own digits, which PostgreSQL's numeric holds exactly: no Decimal's precision
    or scale bounds them, and none is rounded."""
    assert _one(runtime, sql) == digits


@pytest.mark.parametrize(
    ("sql", "oid"),
    [
        ("SELECT pg_catalog.numeric('1.50'::jsonb) AS v", 1700),
        ('SELECT pg_catalog."numeric"(NULL::jsonb) AS v', 1700),
        ("SELECT pg_catalog.bool('true'::jsonb) AS v", 16),
        ('SELECT pg_catalog."bool"(NULL::jsonb) AS v', 16),
    ],
)
def test_a_jsonb_cast_is_described_as_its_type_on_clickhouse(runtime, sql, oid):
    """The type ClickHouse serves the answer as (a NULL too), described as PostgreSQL's."""
    from provisa.pgwire.server import _describe_casts, _sql_type_to_bvtype, _type_oid

    ((served,),) = runtime.run_sync(
        f"SELECT toTypeName(v) FROM ({transpile(sql, 'clickhouse')})"
    ).iter_rows()
    arrow = {"Nullable(String)": "string", "Nullable(Bool)": "bool"}[served]
    types: list[Any] = [_sql_type_to_bvtype(arrow)]
    oids: list[int | None] = [_type_oid(arrow)]
    _describe_casts(sql, types, oids, [-1], [], [arrow])
    assert oids == [oid]


@pytest.mark.parametrize(
    ("value", "function", "sqlstate", "message"),
    [
        ('"t"', "bool", "22023", "cannot cast jsonb string to type boolean"),
        ("1", "bool", "22023", "cannot cast jsonb numeric to type boolean"),
        (_BIG, "bool", "22023", "cannot cast jsonb numeric to type boolean"),
        ("null", "bool", "22023", "cannot cast jsonb null to type boolean"),
        ("[true]", "bool", "22023", "cannot cast jsonb array to type boolean"),
        ('{"a": true}', "bool", "22023", "cannot cast jsonb object to type boolean"),
        ('"1"', "numeric", "22023", "cannot cast jsonb string to type numeric"),
        ("true", "numeric", "22023", "cannot cast jsonb boolean to type numeric"),
        ("null", "numeric", "22023", "cannot cast jsonb null to type numeric"),
        ("[1]", "numeric", "22023", "cannot cast jsonb array to type numeric"),
        ("{}", "numeric", "22023", "cannot cast jsonb object to type numeric"),
        # PostgreSQL's numeric of an exponent is its digits shifted: not written here.
        ("1e3", "numeric", "0A000", "numeric does not read this value"),
        ("1.5E-2", "numeric", "0A000", "numeric does not read this value"),
        # Text a jsonb column holds unchecked: PostgreSQL would have refused it when stored.
        ("tru", "bool", "22P02", "invalid input syntax for type json"),
        ("01", "numeric", "22P02", "invalid input syntax for type json"),
        ("1 2", "numeric", "22P02", "invalid input syntax for type json"),
    ],
)
def test_what_is_no_exact_answer_raises_on_clickhouse(runtime, value, function, sqlstate, message):
    error = _raised(
        runtime,
        f"SELECT pg_catalog.{function}(c::jsonb) AS v FROM (VALUES ('{value}')) AS t(c)",
    )
    assert (error.sqlstate, str(error)) == (sqlstate, message)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.numeric('1.5'::json) AS v",  # PostgreSQL casts no json to numeric
        "SELECT pg_catalog.bool('true'::json) AS v",
    ],
)
def test_bool_and_numeric_of_a_json_value_are_refused_by_name_on_clickhouse(sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse")


def test_the_clickhouse_limitation_states_what_differs():
    feature = next(f for f in GRID.features if f.name == "numeric")
    (stated,) = feature.read.limitations["clickhouse"]
    assert "exponent" in stated.text and "invalid" in stated.text


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


@pytest.mark.parametrize("function", ["bool", "numeric"])
def test_trino_refuses_bool_and_numeric_of_a_jsonb_value(function):
    with pytest.raises(FunctionRefused):
        transpile(f'SELECT pg_catalog."{function}"(NULL::jsonb) AS v', "trino")


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
