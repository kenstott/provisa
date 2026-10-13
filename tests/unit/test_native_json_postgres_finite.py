# Copyright (c) 2026 Kenneth Stott
# Canary: 5c8d3547-22e9-49f8-b0d4-548c8b7753f1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1965: a float element read as JSON on PostgreSQL, executed by PostgreSQL (the session's
embedded server). An array of finite numbers is handed over; a NaN or an infinity is refused,
naming the rule. The integration lane failed every finite array on main 15bd4bf1b: the refusal was
a cast of a constant, which PostgreSQL evaluates when it plans the statement."""

from __future__ import annotations

import json

import psycopg
import pytest
import sqlglot.expressions as exp

from provisa.transpiler.native_json import NON_FINITE, NestedColumn, native_json_marker
from provisa.transpiler.transpile import transpile


def _read(dsn: str, array: str) -> object:
    marker = native_json_marker(
        exp.column("v", "r", quoted=True), NestedColumn("double precision[]", 1, "double")
    )
    sql = transpile(
        f"SELECT {marker.sql(dialect='postgres')} FROM (SELECT {array} AS v) AS r", "postgres"
    )
    with psycopg.connect(dsn) as con:
        (value,) = con.execute(sql).fetchone()  # type: ignore[misc]
    return value if not isinstance(value, str) else json.loads(value)


def test_an_array_of_finite_numbers_is_handed_over(embedded_postgres):
    assert _read(embedded_postgres.dsn(), "ARRAY[1.5, NULL]::float8[]") == [1.5, None]


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_a_float_element_that_is_not_a_finite_number_is_refused(embedded_postgres, value):
    with pytest.raises(psycopg.Error, match=NON_FINITE):
        _read(embedded_postgres.dsn(), f"ARRAY[1.5, '{value}'::float8]")
