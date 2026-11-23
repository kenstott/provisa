# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4c7e12-5b3d-4f81-a6e0-2d8b1f9c3e57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB's float functions at their edge values (REQ-2236, amended 31776071a3: a recognisable edge
with a fixed PostgreSQL answer raises PostgreSQL's SQLSTATE). Each statement is the one the function
support measure sent (scripts/conformance/expected/functions.json, run 45101362913), as Provisa
now writes it for DuckDB and runs it in-process; each value inside the domain still answers as
PostgreSQL does."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import math

import duckdb
import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.duckdb_pg_scalars import register
from provisa.transpiler.transpile import transpile

_MAX = "'1.7976931348623157e+308'::double precision"
_TWO_MAX = f"FROM (VALUES ({_MAX}), ({_MAX})) AS t(c1)"


@pytest.fixture(scope="module")
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    register(c)
    return c


def _run(con: duckdb.DuckDBPyConnection, select: str) -> list[tuple]:
    return con.execute(transpile(f"SELECT {select} LIMIT 1001", "duckdb")).fetchall()


def _raised(con: duckdb.DuckDBPyConnection, select: str) -> tuple[str, str]:
    with pytest.raises(duckdb.Error) as caught:
        _run(con, select)
    error = from_engine(caught.value)
    assert error is not None, caught.value
    return error.sqlstate, str(error)


_OUT_OF_RANGE = ("22003", "input is out of range")
_LOG_NEGATIVE = ("2201E", "cannot take logarithm of a negative number")
_COMPLEX = ("2201F", "a negative number raised to a non-integer power yields a complex result")
_OVERFLOW = ("22003", "value out of range: overflow")

_EDGES = [
    *(
        (f"pg_catalog.\"{name}\"('-Infinity'::double precision) AS v", _OUT_OF_RANGE)
        for name in ("acos", "acosh", "asin", "atanh", "cos", "cot", "sin", "tan")
    ),
    *(
        (f"pg_catalog.\"{name}\"('-Infinity'::double precision) AS v", _LOG_NEGATIVE)
        for name in ("ln", "log", "log10")
    ),
    (
        "pg_catalog.\"sqrt\"('-Infinity'::double precision) AS v",
        ("2201F", "cannot take square root of a negative number"),
    ),
    *(
        (
            f"pg_catalog.\"{name}\"('-Infinity'::double precision, 0.5::double precision) AS v",
            _COMPLEX,
        )
        for name in ("pow", "power")
    ),
    *((f'pg_catalog."{name}"({_MAX}) AS v', _OVERFLOW) for name in ("degrees", "exp")),
    *(
        (f'pg_catalog."{name}"(c1) AS v {_TWO_MAX}', _OVERFLOW)
        for name in (
            "avg",
            "stddev",
            "stddev_pop",
            "stddev_samp",
            "sum",
            "var_pop",
            "var_samp",
            "variance",
        )
    ),
    *(
        (
            f'pg_catalog."in_range"(0.5::{kind}, 0.5::{kind}, '
            "'NaN'::double precision, true::boolean, true::boolean) AS v",
            ("22013", "invalid preceding or following size in window function"),
        )
        for kind in ("double precision", "real")
    ),
    (
        "pg_catalog.\"width_bucket\"('NaN'::double precision, 0::double precision, "
        "10::double precision, 5::integer) AS v",
        ("2201G", "operand, lower bound, and upper bound cannot be NaN"),
    ),
    (
        "pg_catalog.\"interval_div\"('1 day 02:03:04'::interval, 'NaN'::double precision) AS v",
        ("22008", "interval out of range"),
    ),
]


@pytest.mark.parametrize(("select", "error"), _EDGES, ids=[s.split("(")[0] for s, _ in _EDGES])
def test_an_edge_value_raises_postgresqls_error(con, select, error):
    sqlstate, message = _raised(con, select)
    assert (sqlstate, message) == error


@pytest.mark.parametrize(
    ("select", "want"),
    [
        ("acos(0.5::double precision)", math.acos(0.5)),
        ("asin(1::double precision)", math.asin(1)),
        ("atanh(0.5::double precision)", math.atanh(0.5)),
        ("acosh(2::double precision)", math.acosh(2)),
        ("sin(1::double precision)", math.sin(1)),
        ("cot(1::double precision)", 1 / math.tan(1)),
        ("sqrt(4::double precision)", 2.0),
        ("ln(1::double precision)", 0.0),
        ("log10(100::double precision)", 2.0),
        ("power(-8::double precision, 3::double precision)", -512.0),
        ("exp(1::double precision)", math.e),
        ("degrees(1::double precision)", math.degrees(1)),
        ("exp('-Infinity'::double precision)", 0.0),
        ("sqrt('Infinity'::double precision)", math.inf),
    ],
)
def test_a_value_in_the_domain_answers(con, select, want):
    assert _run(con, f"{select} AS v") == [(pytest.approx(want),)]


@pytest.mark.parametrize(
    "select",
    [
        "acos('NaN'::double precision)",
        "atanh('NaN'::double precision)",
        "sin('NaN'::double precision)",
        "sqrt('NaN'::double precision)",
        "ln('NaN'::double precision)",
        "exp('NaN'::double precision)",
        "power('NaN'::double precision, 2::double precision)",
    ],
)
def test_nan_answers_nan(con, select):
    [(value,)] = _run(con, f"{select} AS v")
    assert math.isnan(value)


def test_power_of_nan_to_zero_is_one(con):
    assert _run(con, "power('NaN'::double precision, 0::double precision) AS v") == [(1.0,)]


@pytest.mark.parametrize(
    ("function", "want"),
    [("sum", 6.0), ("avg", 2.0), ("var_pop", 2 / 3), ("stddev_samp", 1.0)],
)
def test_an_aggregate_of_finite_values_answers(con, function, want):
    rows = _run(
        con,
        f"{function}(c1) AS v FROM (VALUES (1::double precision), (2::double precision), (3::double precision)) AS t(c1)",
    )
    assert rows == [(pytest.approx(want),)]


def test_a_sum_holding_an_infinity_answers_infinity(con):
    rows = _run(
        con,
        "sum(c1) AS v FROM (VALUES ('Infinity'::double precision), (1::double precision)) AS t(c1)",
    )
    assert rows == [(math.inf,)]


@pytest.mark.parametrize(
    ("args", "want"),
    [
        ("1::double precision, 0.5::double precision, 1::double precision, false, true", True),
        ("2::double precision, 0.5::double precision, 1::double precision, false, true", False),
        ("'NaN'::double precision, 0.5::double precision, 1::double precision, false, true", False),
    ],
)
def test_in_range_of_floats_answers(con, args, want):
    assert _run(con, f'pg_catalog."in_range"({args}) AS v') == [(want,)]


def test_a_negative_in_range_offset_raises(con):
    sqlstate, _ = _raised(
        con,
        'pg_catalog."in_range"(0.5::double precision, 0.5::double precision, '
        "-1::double precision, true, true) AS v",
    )
    assert sqlstate == "22013"


def test_width_bucket_of_numbers_answers(con):
    select = (
        "width_bucket(5.35::double precision, 0.024::double precision, "
        "10.06::double precision, 5) AS v"
    )
    assert _run(con, select) == [(3,)]


def test_width_bucket_checks_the_count_before_nan(con):
    sqlstate, message = _raised(
        con,
        "width_bucket('NaN'::double precision, 0::double precision, 10::double precision, 0) AS v",
    )
    assert (sqlstate, message) == ("2201G", "count must be greater than zero")


def test_interval_div_of_a_number_answers(con):
    rows = _run(con, "pg_catalog.\"interval_div\"('1 day'::interval, 2::double precision) AS v")
    assert [str(v) for (v,) in rows] == ["12:00:00"]
