# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2b9e41-3c5a-4f60-8b17-e0a4d6c92f35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every value a VALUES CTE holds keeps its column's IR type (REQ-232, REQ-913)."""

# Requirements: REQ-232, REQ-913

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
import sqlglot

from provisa.cache.values_cte import InlineRows, _cte_literal, build_values_cte_sql


def _sql(value, ir_type: str) -> str:
    return _cte_literal(value, ir_type).sql(dialect="postgres")


@pytest.mark.parametrize(
    ("value", "ir_type", "expected"),
    [
        ({"a": 1}, "json", "CAST('{\"a\": 1}' AS JSONB)"),
        ([1, 2], "json", "CAST('[1, 2]' AS JSONB)"),
        ('{"a": 1}', "json", "CAST('{\"a\": 1}' AS JSONB)"),
        (7, "bigint", "CAST(7 AS BIGINT)"),
        (Decimal("1.50"), "decimal(10,2)", "CAST(1.50 AS DECIMAL(10, 2))"),
        (dt.date(2026, 1, 2), "date", "CAST('2026-01-02' AS DATE)"),
        (
            dt.datetime(2026, 1, 2, 3, 4, 5),
            "timestamp",
            "CAST('2026-01-02T03:04:05' AS TIMESTAMP)",
        ),
        (True, "boolean", "CAST(TRUE AS BOOLEAN)"),
        ("abc", "text", "CAST('abc' AS TEXT)"),
    ],
)
def test_a_value_is_cast_to_its_column_type(value, ir_type, expected):
    assert _sql(value, ir_type) == expected


def test_an_array_value_is_an_array_of_cast_elements():
    assert _sql([1, 2], "integer[]") == "CAST(ARRAY[CAST(1 AS INT), CAST(2 AS INT)] AS INT[])"


def test_a_nested_array_value_is_nested_arrays():
    sql = _sql([[1], [2]], "integer[][]")
    assert sql.count("ARRAY[") == 3
    assert sql.endswith("AS INT[][])")


def test_a_float_without_a_declared_type_stays_double_precision():
    assert _cte_literal(2.0).sql(dialect="postgres") == "CAST(2.0 AS DOUBLE PRECISION)"


def test_jsonb_containment_over_a_cte_column_parses_as_jsonb():
    entry = InlineRows(
        rows=[{"id": 1, "prefs": {"a": 1}}],
        column_names=["id", "prefs"],
        column_types={"id": "integer", "prefs": "json"},
    )
    sql = build_values_cte_sql(
        'SELECT "id" FROM "t" WHERE "prefs" @> \'{"a": 1}\'::jsonb', "t", entry
    )
    assert "CAST('{\"a\": 1}' AS JSONB)" in sql
    sqlglot.parse_one(sql, dialect="postgres")
