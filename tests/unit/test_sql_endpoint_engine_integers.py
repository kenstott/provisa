# Copyright (c) 2026 Kenneth Stott
# Canary: 3fa1c6d8-52b7-4e0a-9c41-7de80b6a2f95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/sql writes a column the engine types an integer as its number (REQ-1867).

DuckDB's SUM of a BIGINT is a HUGEINT, which its Arrow batches carry as a decimal128(38,0): the
reader's Decimal. Written by the representation it was the exact string "2400"; by the engine's
type, as every other surface writes it (serialize.result_records), it is the number 2400. A
DECIMAL stays the exact string it is."""

# Requirements: REQ-1867, REQ-1961

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from provisa.api.data.endpoint_dev import _column_types, _typed_rows


def _result(types: list[str | None], row: list) -> SimpleNamespace:
    return SimpleNamespace(
        column_names=[f"c{i}" for i in range(len(row))],
        column_types=types,
        rows=[row],
        result_shape=None,
    )


def test_a_hugeint_is_written_as_its_number():
    rows = asyncio.run(_typed_rows(_result(["HUGEINT"], [Decimal("2400")])))
    assert rows == [[2400]]
    assert isinstance(rows[0][0], int)


def test_a_decimal_stays_exact():
    rows = asyncio.run(_typed_rows(_result(["DECIMAL(38,2)"], [Decimal("2400.50")])))
    assert rows == [[Decimal("2400.50")]]
    assert isinstance(rows[0][0], Decimal)


def test_a_decimal_with_no_fraction_is_not_an_integer():
    rows = asyncio.run(_typed_rows(_result(["DECIMAL(38,0)"], [Decimal("2400")])))
    assert isinstance(rows[0][0], Decimal)


def test_an_untyped_column_is_left_as_read():
    rows = asyncio.run(_typed_rows(_result([None], [Decimal("2400")])))
    assert isinstance(rows[0][0], Decimal)


def test_a_result_with_no_types_is_left_as_read():
    result = _result([None], [Decimal("2400")])
    result.column_types = None
    assert asyncio.run(_typed_rows(result)) == [[Decimal("2400")]]


def test_the_listed_type_of_an_engine_integer_is_the_engines():
    types = asyncio.run(_column_types(_result(["HUGEINT"], [Decimal("2400")])))
    assert types == ["HUGEINT"]
