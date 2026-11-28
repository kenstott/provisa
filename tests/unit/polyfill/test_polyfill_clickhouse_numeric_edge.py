# Copyright (c) 2026 Kenneth Stott
# Canary: 77f7dc67-1be8-4d8a-b4dc-b529697f6e62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The scale and the remainder of a numeric on ClickHouse at the literal's bound (REQ-2236;
REQ-2167, NEVER A WRONG ANSWER): a numeric literal of no precision is a Decimal256 of up to 38
digits on either side of its point (polyfill.clickhouse.numeric_read).

1. scale read the digits of the value's text, which ClickHouse writes without trailing zeros:
   scale('...567890'::numeric) answered 29, PostgreSQL 30 (Conformance (functions) run
   46104630477); the scale is the Decimal type's, which holds it.
2. mod of a numeric (polyfill.floats._ch_mod, a rule of no pattern) wrote its arguments as the
   statement had them: a literal past the old table's 38 digits stayed a plain Decimal, which
   ClickHouse reads as Decimal(10, 0) and answered NULL.

Each answer is PostgreSQL 16's, answered here on the embedded engine (chdb). round, trunc and div
of the same literal answer exactly or are refused by name; a literal past the bound raises 0A000."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

from decimal import Decimal

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.transpile import transpile

_WHOLE = "123456789012345678901234567890"
_WIDE = f"'{_WHOLE}.{_WHOLE}'::numeric"
#: 39 digits before the point, and 39 after it: past the literal's bound.
_PAST_WHOLE = "'1234567890123456789012345678901234567890.5'::numeric"
_PAST_SCALE = "'0.123456789012345678901234567890123456789'::numeric"


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())
    return value


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        (f'SELECT pg_catalog."scale"({_WIDE}) AS v', 30),
        (f"SELECT scale({_WIDE}) AS v", 30),
        ("SELECT pg_catalog.\"scale\"('1.10'::numeric) AS v", 2),
        ("SELECT scale(1.10::numeric) AS v", 2),
        ("SELECT scale('0.00'::numeric) AS v", 2),
        ("SELECT scale(2.5::numeric) AS v", 1),
        ("SELECT scale(1000::numeric) AS v", 0),
        ("SELECT scale(5::integer) AS v", 0),
        ("SELECT scale(CAST(1.5 AS numeric(10, 4))) AS v", 4),
        ("SELECT scale(2.5::numeric * 1.10::numeric) AS v", 3),
        ("SELECT scale(NULL::numeric) AS v", None),
        (f'SELECT pg_catalog."min_scale"({_WIDE}) AS v', 29),
    ],
)
def test_scale_is_postgresql_s(runtime, sql, want):
    assert _answer(runtime, sql) == want


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        (f'SELECT pg_catalog."mod"({_WIDE}, 2.5::numeric) AS v', f"0.{_WHOLE}"),
        (f"SELECT mod({_WIDE}, 2.5::numeric) AS v", f"0.{_WHOLE}"),
        (f"SELECT {_WIDE} % 2.5::numeric AS v", f"0.{_WHOLE}"),
        (f'SELECT pg_catalog."numeric_mod"({_WIDE}, 2.5::numeric) AS v', f"0.{_WHOLE}"),
        ("SELECT mod('7.50'::numeric, 2::numeric) AS v", "1.50"),
        (
            f'SELECT pg_catalog."trim_scale"({_WIDE}) AS v',
            f"{_WHOLE}.12345678901234567890123456789",
        ),
    ],
)
def test_the_remainder_and_text_are_postgresql_s(runtime, sql, want):
    assert _answer(runtime, sql) == want


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        (f'SELECT pg_catalog."round"({_WIDE}) AS v', Decimal(_WHOLE)),
        (f'SELECT pg_catalog."round"({_WIDE}, 3) AS v', Decimal(f"{_WHOLE}.123")),
        (f'SELECT pg_catalog."trunc"({_WIDE}) AS v', Decimal(_WHOLE)),
        (f'SELECT pg_catalog."trunc"({_WIDE}, 3) AS v', Decimal(f"{_WHOLE}.123")),
        (f"SELECT {_WIDE} AS v", Decimal(f"{_WHOLE}.{_WHOLE}")),
    ],
)
def test_round_and_trunc_of_the_wide_literal_are_exact(runtime, sql, want):
    got = _answer(runtime, sql)
    assert isinstance(got, Decimal)
    assert got == want and got.as_tuple().exponent == want.as_tuple().exponent, (want, got)


def test_div_of_the_wide_literal_is_refused_by_name():
    with pytest.raises(FunctionRefused):
        transpile(
            f'SELECT pg_catalog."div"({_WIDE}, 2.5::numeric) AS v', "clickhouse", generated=False
        )


def _sqlstate(runtime: ClickHouseFederationRuntime, sql: str) -> str | None:
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    if isinstance(raised.value, (RowRefused, PostgresError)):
        return raised.value.sqlstate
    error = from_engine(raised.value)
    return None if error is None else error.sqlstate


@pytest.mark.parametrize("value", [_PAST_WHOLE, _PAST_SCALE])
@pytest.mark.parametrize(
    "form",
    [
        'SELECT pg_catalog."scale"({}) AS v',
        'SELECT pg_catalog."mod"({}, 2.5::numeric) AS v',
        "SELECT {} % 2.5::numeric AS v",
        'SELECT pg_catalog."round"({}, 3) AS v',
        'SELECT pg_catalog."trunc"({}, 3) AS v',
    ],
)
def test_a_literal_past_the_bound_raises_0a000(runtime, form, value):
    assert _sqlstate(runtime, form.format(value)) == "0A000"


def test_the_scale_row_states_its_limitation_on_clickhouse():
    feature = next(f for f in GRID.features if f.name == "scale")
    (limitation,) = feature.read.limitations["clickhouse"]
    assert "type" in limitation.text and "trailing zeros" in limitation.text
    assert "scale" in {f.name for f in GRID.on("clickhouse")}
