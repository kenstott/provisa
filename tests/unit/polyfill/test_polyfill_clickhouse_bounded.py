# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b1c62-9d3a-4f70-b5e2-a1c7d09f3b86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse refusals by name that answer within a stated and enforced bound (REQ-2236; REQ-2167,
AN ANSWER WITHIN A STATED BOUND COMES BEFORE A REFUSAL; function support run 43469834291).

avg, variance and stddev of an integer: PostgreSQL's numeric digits from the group's integer sums,
in ClickHouse's Int256. Each answer here is PostgreSQL 17's (the run's statements, and values its
numeric_div and sqrt_var round); the bound (a group of at most 2^40 values) raises beyond it, read
here at a bound lowered so a test reaches it."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import math
from typing import Any

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import GRID, aggregates
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


def _of(function: str, values: list[int], sql_type: str = "integer") -> str:
    rows = ", ".join(f"(({v})::{sql_type})" for v in values)
    return f'SELECT pg_catalog."{function}"(c1) AS v FROM (VALUES {rows}) AS t(c1)'


# --- PostgreSQL's numeric, as a model of numeric.c to check many groups against ---------------


def _weight_and_first(value: int) -> tuple[int, int]:
    digits = str(abs(value))
    if value == 0:
        return 0, 0
    weight = (len(digits) - 1) // 4
    return weight, int(digits[: len(digits) - 4 * weight])


def _div_scale(dividend: int, divisor: int) -> int:
    (w1, f1), (w2, f2) = _weight_and_first(dividend), _weight_and_first(divisor)
    return max(16 - 4 * (w1 - w2 - (1 if f1 <= f2 else 0)), 0)


def _written(digits: int, scale: int, negative: bool = False) -> str:
    text = str(digits).rjust(scale + 1, "0")
    text = text if scale == 0 else f"{text[:-scale]}.{text[-scale:]}"
    return ("-" if negative and digits else "") + text


def _divided(dividend: int, divisor: int) -> tuple[str, int, int]:
    scale = _div_scale(dividend, divisor)
    digits = (2 * abs(dividend) * 10**scale + divisor) // (2 * divisor)
    return _written(digits, scale, dividend < 0), digits, scale


def _pg(function: str, values: list[int]) -> str | None:
    n = len(values)
    if function == "avg":
        return None if n == 0 else _divided(sum(values), n)[0]
    sample = function in ("variance", "var_samp", "stddev", "stddev_samp")
    if n == 0 or (sample and n == 1):
        return None
    numerator = n * sum(v * v for v in values) - sum(values) ** 2
    if numerator <= 0:
        return "0"
    text, digits, scale = _divided(numerator, n * (n - 1) if sample else n * n)
    if not function.startswith("stddev"):
        return text
    root = math.isqrt(digits * 10**scale * 100)
    return _written((root + 5) // 10, scale)


# --- the exact case ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        # The run's own statements, and PostgreSQL 17's answers to them.
        ('SELECT pg_catalog."avg"(NULL::integer) AS v', None),
        (_of("avg", [3, -7, 42]), "12.6666666666666667"),
        (_of("avg", [3, -7, 5000000000], "bigint"), "1666666665.33333333"),
        (_of("var_samp", [3, -7, 5000000000], "bigint"), "8333333340000000026"),
        (_of("stddev", [3, -7, 42], "smallint"), "25.8907963055085143"),
        (_of("stddev_pop", [3, -7, 42], "smallint"), "21.1397466609439028"),
        (_of("var_pop", [3, -7, 42]), "446.8888888888888889"),
        # PostgreSQL's documented digits of small groups.
        (_of("avg", [1, 2]), "1.5000000000000000"),
        (_of("variance", [1, 2]), "0.50000000000000000000"),
        (_of("stddev_samp", [1, 2]), "0.70710678118654752440"),
        (_of("stddev", [1, 2, 3, 4]), "1.2909944487358056"),
        (_of("var_samp", [5]), None),  # a sample of one has no spread
        (_of("var_pop", [5]), "0"),
        (_of("stddev_pop", [7, 7, 7]), "0"),
        (_of("avg", [-1, 1]), "0.00000000000000000000"),
        (_of("avg", [-1, -2]), "-1.5000000000000000"),
    ],
)
def test_avg_variance_and_stddev_of_integers_answer_postgresql_digits_on_clickhouse(
    runtime, sql, answer
):
    assert _one(runtime, sql) == answer


_GROUPS = [
    [9223372036854775807] * 3,
    [-9223372036854775808, 9223372036854775807],
    [10**18, 1],
    [1, 10000],
    [2, 3],
    [123456789, -987654321, 5, 0, 77],
]


@pytest.mark.parametrize("values", _GROUPS)
@pytest.mark.parametrize(
    "function", ["avg", "var_samp", "var_pop", "stddev_samp", "stddev_pop", "variance", "stddev"]
)
def test_every_digit_is_numeric_divs_at_the_edges_of_bigint(runtime, function, values):
    assert _one(runtime, _of(function, values, "bigint")) == _pg(function, values)


@pytest.mark.parametrize(
    ("function", "values"),
    [("avg", [1, 2, 2, 5]), ("var_samp", [3, -7, 3, 42]), ("stddev_pop", [1, 1, 2, 4, 4]),
     ("var_pop", [-3, 3, -3]), ("stddev_samp", [9, 9])],
)  # fmt: skip
def test_distinct_values_are_each_counted_once(runtime, function, values):
    rows = ", ".join(f"(({v})::integer)" for v in values)
    sql = f"SELECT {function}(DISTINCT c1) AS v FROM (VALUES {rows}, (NULL)) AS t(c1)"
    assert _one(runtime, sql) == _pg(function, sorted(set(values)))


def test_a_null_value_is_skipped_as_postgresql_skips_it(runtime):
    sql = 'SELECT pg_catalog."avg"(c1) AS v FROM (VALUES (1::integer), (NULL), (2)) AS t(c1)'
    assert _one(runtime, sql) == "1.5000000000000000"


def test_the_answer_is_described_as_numeric(runtime):
    from provisa.pgwire.server import _describe_casts, _sql_type_to_bvtype, _type_oid

    sql = _of("avg", [1, 2])
    ((served,),) = runtime.run_sync(
        f"SELECT toTypeName(v) FROM ({transpile(sql, 'clickhouse')})"
    ).iter_rows()
    assert served == "Nullable(String)"
    types: list[Any] = [_sql_type_to_bvtype("string")]
    oids: list[int | None] = [_type_oid("string")]
    _describe_casts(sql, types, oids, [-1], [], ["string"])
    assert oids == [1700]


# --- the bound: at it, the answer; past it, the raise -------------------------------------------


@pytest.mark.parametrize("function", ["avg", "var_pop", "stddev_samp"])
def test_a_group_at_the_bound_answers_and_one_past_it_raises(runtime, monkeypatch, function):
    monkeypatch.setattr(aggregates, "MOST_VALUES", 3)
    assert _one(runtime, _of(function, [1, 2, 3])) == _pg(function, [1, 2, 3])
    error = _raised(runtime, _of(function, [1, 2, 3, 4]))
    assert (error.sqlstate, str(error)) == (
        "0A000",
        f"{function} of more than 3 values has no exact form on clickhouse",
    )


def test_the_stated_bound_is_the_one_enforced():
    feature = next(f for f in GRID.features if f.name == "avg")
    (stated,) = feature.read.limitations["clickhouse"]
    assert str(aggregates.MOST_VALUES) in stated.text
    assert f"count($v) > {aggregates.MOST_VALUES}" in aggregates.avg_form()


# --- what keeps what it had ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT avg(x::int) OVER () FROM (VALUES (1)) AS t(x)",  # the form's aggregates are two
        "SELECT avg(x::int) FILTER (WHERE x > 0) FROM (VALUES (1)) AS t(x)",
    ],
)
def test_a_window_or_a_filter_stays_refused_on_clickhouse(sql):
    with pytest.raises(FunctionRefused):
        transpile(sql, "clickhouse")


def test_duckdb_keeps_its_own_exact_form():
    sql = _of("avg", [3])
    assert "PROVISA_NUMERIC_QUOTIENT" in transpile(sql, "duckdb").upper()


def test_a_column_no_statement_types_keeps_the_engines_avg():
    assert "toInt256" not in transpile("SELECT avg(x) FROM t", "clickhouse")
