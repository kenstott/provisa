# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A statement's bound values reach ClickHouse as parameters, never as SQL text.

The ClickHouse engine runtime dropped them (``del params``), so a parameterized statement
reached ClickHouse with its ``@N`` placeholders unbound and failed. The redirect path wrote the
values in as hand-quoted literals, which ClickHouse's backslash escapes can break out of. Both
now bind through ClickHouse's typed server-side parameters (``{pN:Type}``): a value is data
whatever it holds. Proven here on the embedded engine (chdb), in this process."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime, bind_parameters

HOSTILE = "a'); DROP TABLE x; --"
BACKSLASHED = "a\\'); DROP TABLE x; --"


@pytest.fixture(scope="module")
def runtime():
    rt = ClickHouseFederationRuntime.embedded()
    yield rt
    rt.close()


def test_a_parameterized_statement_binds_its_values(runtime):
    rows = runtime.run_sync(
        "SELECT number AS id FROM numbers(10) WHERE number >= @1 ORDER BY 1", [7]
    ).rows
    assert [r[0] for r in rows] == [7, 8, 9]


@pytest.mark.parametrize("value", [HOSTILE, BACKSLASHED, "it's", "", "東京"])
def test_a_string_value_round_trips_as_data(runtime, value):
    rows = runtime.run_sync("SELECT @1 AS v, length(@1) AS n", [value]).rows
    assert rows == [(value, len(value.encode("utf-8")))]


def test_a_repeated_placeholder_binds_its_one_value_each_time(runtime):
    assert runtime.run_sync("SELECT @1 + @2 + @1 AS s", [1, 10]).rows == [(12,)]


def test_values_of_each_type_bind_as_their_type(runtime):
    values = [True, 3, 2.5, None, Decimal("1.25"), dt.date(2026, 10, 3), "x"]
    placeholders = ", ".join(f"toTypeName(@{i})" for i in range(1, len(values) + 1))
    (row,) = runtime.run_sync(f"SELECT {placeholders}", values).rows
    assert row == (
        "Bool", "Int64", "Float64", "Nullable(String)", "Decimal(38, 2)", "Date", "String",
    )  # fmt: skip


def test_the_arrow_terminals_bind_too(runtime):
    table = runtime.run_arrow("SELECT @1 AS v", [HOSTILE])
    assert table.column("v").to_pylist() == [HOSTILE]
    schema, batches = runtime.run_arrow_stream("SELECT @1 AS v", [HOSTILE])
    assert [b.to_pylist() for b in batches] == [[{"v": HOSTILE}]]


def test_the_binding_names_each_value_and_keeps_text_out_of_the_sql():
    sql, params = bind_parameters("SELECT @1, @2, @10", [HOSTILE, 2, *range(3, 10), "ten"])
    assert sql == "SELECT {p1:String}, {p2:Int64}, {p10:String}"
    assert params["p1"] == HOSTILE and params["p10"] == "ten"
    assert HOSTILE not in sql
    assert bind_parameters("SELECT 1", None) == ("SELECT 1", {})


def test_a_value_of_a_type_clickhouse_cannot_take_is_refused_by_name():
    with pytest.raises(TypeError, match="cannot bind a value of type 'object'"):
        bind_parameters("SELECT @1", [object()])
