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


# --- xid8, snapshots and an integer's digits -----------------------------------------------------

_MAX_XID8 = "18446744073709551615"


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ('SELECT pg_catalog."xid8eq"(NULL::xid8, NULL::xid8) AS v', None),
        (f"SELECT pg_catalog.\"xid8eq\"('{_MAX_XID8}'::xid8, '{_MAX_XID8}'::xid8) AS v", True),
        (f"SELECT pg_catalog.\"xid8lt\"('3'::xid8, '{_MAX_XID8}'::xid8) AS v", True),
        (f"SELECT pg_catalog.\"xid8gt\"('3'::xid8, '{_MAX_XID8}'::xid8) AS v", False),
        ("SELECT pg_catalog.\"xid8ge\"('100'::xid8, '100'::xid8) AS v", True),
        ("SELECT pg_catalog.\"xid8le\"('101'::xid8, '100'::xid8) AS v", False),
        ("SELECT pg_catalog.\"xid8ne\"('3'::xid8, '3'::xid8) AS v", False),
        ('SELECT pg_catalog."xid8send"(NULL::xid8) AS v', None),
        ("SELECT pg_catalog.\"xid8send\"('100'::xid8) AS v", b"\x00\x00\x00\x00\x00\x00\x00d"),
        (f"SELECT pg_catalog.\"xid8send\"('{_MAX_XID8}'::xid8) AS v", b"\xff" * 8),
    ],
)
def test_xid8_orders_and_bytes_are_postgresqls_on_clickhouse(runtime, sql, answer):
    got = _one(runtime, sql)
    assert (got.encode("latin-1") if isinstance(got, str) else got) == answer


def _snapshot(function: str, text: str, kind: str = "pg_snapshot") -> str:
    return f"SELECT pg_catalog.\"{function}\"(c::{kind}) AS v FROM (VALUES ('{text}')) AS t(c)"


@pytest.mark.parametrize(
    ("sql", "rows"),
    [
        ('SELECT pg_catalog."pg_snapshot_xmin"(NULL::pg_snapshot) AS v', [(None,)]),
        (_snapshot("pg_snapshot_xmin", "10:20:10,14,15"), [(10,)]),
        (_snapshot("pg_snapshot_xmax", "10:20:10,14,15"), [(20,)]),
        (_snapshot("pg_snapshot_xmax", f"5:{_MAX_XID8}:"), [(int(_MAX_XID8),)]),
        (_snapshot("txid_snapshot_xmax", f"5:{_MAX_XID8}:", "txid_snapshot"), [(-1,)]),
        (_snapshot("pg_snapshot_xip", "10:20:10,14,15"), [(10,), (14,), (15,)]),
        (_snapshot("pg_snapshot_xip", "5:10:7,7,8"), [(7,), (8,)]),  # a duplicate is read once
        (_snapshot("pg_snapshot_xip", "1:10:0,0,3"), [(3,)]),  # as is a leading 0: not at all
        (_snapshot("txid_snapshot_xip", "1:1:", "txid_snapshot"), []),
        ('SELECT pg_catalog."pg_snapshot_xip"(NULL::pg_snapshot) AS v', []),
        (
            "SELECT pg_catalog.\"pg_visible_in_snapshot\"('3'::xid8, '10:20:10,14,15'::pg_snapshot)"
            " AS v",
            [(True,)],
        ),
        (
            "SELECT pg_catalog.\"pg_visible_in_snapshot\"('14'::xid8, '10:20:10,14,15'::pg_snapshot)"
            " AS v",
            [(False,)],
        ),
        (
            "SELECT pg_catalog.\"pg_visible_in_snapshot\"('13'::xid8, '10:20:10,14,15'::pg_snapshot)"
            " AS v",
            [(True,)],
        ),
        (
            "SELECT pg_catalog.\"txid_visible_in_snapshot\"((-7)::bigint, '1:1:'::txid_snapshot)"
            " AS v",
            [(False,)],
        ),
        (
            'SELECT pg_catalog."pg_visible_in_snapshot"(NULL::xid8, NULL::pg_snapshot) AS v',
            [(None,)],
        ),
    ],
)
def test_a_snapshot_is_read_as_postgresqls_input_reads_it_on_clickhouse(runtime, sql, rows):
    assert _rows(runtime, sql) == rows


def test_a_snapshots_bytes_are_pg_snapshot_send(runtime):
    got = _one(runtime, _snapshot("pg_snapshot_send", "10:20:10,14,14,15"))
    words = [(4, 3), (8, 10), (8, 20), (8, 10), (8, 14), (8, 15)]
    assert got.encode("latin-1") == b"".join(v.to_bytes(n, "big") for n, v in words)


@pytest.mark.parametrize(
    "text", ["0:5:", "6:5:", "5:10:8,7", "5:10", "5:10:x", "5:99999999999999999999:"]
)
@pytest.mark.parametrize("function", ["pg_snapshot_xmin", "pg_snapshot_xip", "pg_snapshot_send"])
def test_text_postgresqls_input_refuses_raises_its_error(runtime, function, text):
    error = _raised(runtime, _snapshot(function, text))
    assert (error.sqlstate, str(error)) == ("22P02", "invalid input syntax for type pg_snapshot")


@pytest.mark.parametrize(
    ("call", "digits"),
    [
        ("to_hex(NULL::integer)", None),
        ("to_hex(42::integer)", "2a"),
        ("to_hex((-7)::integer)", "fffffff9"),
        ("to_hex((-7)::bigint)", "fffffffffffffff9"),
        ("to_hex((-2147483648)::integer)", "80000000"),
        ("to_hex((-9223372036854775808)::bigint)", "8000000000000000"),
        ("to_hex(9223372036854775807::bigint)", "7fffffffffffffff"),
        ("to_hex(5000000000::bigint)", "12a05f200"),
        ("to_oct(0::integer)", "0"),
        ("to_oct((-7)::bigint)", "1777777777777777777771"),
        ("to_oct((-7)::integer)", "37777777771"),
        ("to_oct(42::integer)", "52"),
        ("to_bin(3::bigint)", "11"),
        ("to_bin((-1)::smallint)", "1" * 32),  # promoted to an integer
        ("to_bin((-7)::integer)", "11111111111111111111111111111001"),
    ],
)
def test_an_integers_digits_are_convert_to_bases_on_clickhouse(runtime, call, digits):
    name, rest = call.split("(", 1)
    assert _one(runtime, f'SELECT pg_catalog."{name}"({rest} AS v') == digits


@pytest.mark.parametrize(
    ("sql", "duckdb_form"),
    [
        ('SELECT pg_catalog."to_hex"(42::integer) AS v', "FORMAT('{:X}'"),
        ("SELECT pg_catalog.\"pg_snapshot_xmin\"('5:10:7'::pg_snapshot) AS v", "SPLIT_PART("),
        ('SELECT pg_catalog."avg"(c1) AS v FROM (VALUES (1::integer)) AS t(c1)', "QUOTIENT("),
    ],
)
def test_duckdb_keeps_its_own_forms(sql, duckdb_form):
    assert duckdb_form in transpile(sql, "duckdb").upper()
