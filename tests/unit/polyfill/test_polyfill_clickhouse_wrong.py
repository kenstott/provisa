# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1e8b37-2f94-4c5a-b0e7-9a3c4d2f8e61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's wrong answers of run 43361055823 that are its forms' (REQ-2236, REQ-2171): grid rows.

array_agg(NULL::integer) answered '{}' where PostgreSQL answers '{NULL}': ClickHouse's groupArray
skips a NULL, and answers an empty array of no rows, where PostgreSQL's is NULL. current_schemas
answered ClickHouse's own currentSchemas, its database's name, where PostgreSQL answers its search
path (NULL of a NULL): refused by name, as on Trino."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import pytest
import sqlglot

from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.polyfill.clickhouse import ARRAY_AGG, CURRENT_SCHEMAS
from provisa.transpiler.transpile import transpile


def _clickhouse(sql: str) -> str:
    return transpile(sql, "clickhouse")


def test_each_row_is_in_the_grid():
    in_the_grid = {id(f) for f in GRID.features}
    assert {id(ARRAY_AGG), id(CURRENT_SCHEMAS)} <= in_the_grid


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."array_agg"(NULL::integer) AS v',
        "SELECT array_agg(x) AS v FROM t",
        "SELECT array_agg('{1}'::integer[]) AS v",
    ],
)
def test_array_agg_keeps_its_nulls_and_is_null_of_no_rows(sql):
    written = _clickhouse(sql)
    assert "groupArray(tuple(" in written
    assert "count() = 0" in written
    assert "ARRAY_AGG" not in written.upper().replace("GROUPARRAY", "")
    sqlglot.parse_one(written, read="clickhouse")  # ClickHouse's own SQL


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT array_agg(DISTINCT x) AS v FROM t",
        "SELECT array_agg(x) FILTER (WHERE x > 1) AS v FROM t",
        "SELECT array_agg(x) OVER () AS v FROM t",
    ],
)
def test_array_agg_of_distinct_a_filter_or_a_window_keeps_its_form(sql):
    """Only the plain call is this row's: the others keep what they had (an ordered one is
    refused by name: test_polyfill_clickhouse_remaining)."""
    assert "groupArray(tuple(" not in _clickhouse(sql)


@pytest.mark.parametrize("dialect", ["duckdb", "trino"])
def test_array_agg_on_another_engine_is_unchanged(dialect):
    written = transpile('SELECT pg_catalog."array_agg"(NULL::integer) AS v', dialect)
    assert "groupArray" not in written


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."current_schemas"(NULL::boolean) AS v',
        "SELECT current_schemas(true) AS v",
    ],
)
def test_current_schemas_is_refused_by_name_on_clickhouse(sql):
    with pytest.raises(RowRefused) as refused:
        _clickhouse(sql)
    assert "current_schemas() has no form on clickhouse" in str(refused.value)


def test_current_schemas_is_still_refused_on_trino():
    with pytest.raises(RowRefused, match="current_schemas"):
        transpile("SELECT current_schemas(true) AS v", "trino")


def test_current_schemas_on_duckdb_is_unchanged():
    assert "current_schemas" in transpile("SELECT current_schemas(true) AS v", "duckdb").lower()


def test_array_agg_of_an_order_is_refused_by_name():
    with pytest.raises(RowRefused, match="array_agg"):
        _clickhouse("SELECT array_agg(x ORDER BY y) AS v FROM t")
