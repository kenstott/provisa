# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8f1a62-9b4d-4c7e-a051-6d2f8b9c4e17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Rows held in the hot tier keep their registered column types in SQL (REQ-232, REQ-913): a
hot-held table's jsonb column takes ``@>``, and its numeric and date columns compare as numbers
and dates, not text."""

# Requirements: REQ-232, REQ-913

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import sqlglot

from provisa.api.data.materialization import _registered_types, _StatementHot
from provisa.cache.hot_tables import HotTableEntry
from provisa.cache.values_cte import InlineRows, build_values_cte_sql

_TABLE_ID = 7

# A registered API table: rows as the API path holds them -- values normalized to strings, JSON
# serialized -- under the columns the table registers.
_REGISTERED = {
    "id": _TABLE_ID,
    "table_name": "orders",
    "columns": [
        {"column_name": "id", "data_type": "integer"},
        {"column_name": "amount", "data_type": "numeric(10,2)"},
        {"column_name": "placedOn", "data_type": "date"},
        {"column_name": "lines", "data_type": "jsonb"},
    ],
}
_ROWS = [
    {"id": 1, "amount": "10.50", "placed_on": "2026-01-05", "lines": '[{"sku": "a"}]'},
    {"id": 2, "amount": "9.25", "placed_on": "2026-02-05", "lines": '[{"sku": "b"}]'},
]
_NAMES = ["id", "amount", "placed_on", "lines"]


def _held() -> HotTableEntry:
    """The entry a statement holds for ``orders`` after fetching all of its rows."""
    held: dict = {}
    manager = SimpleNamespace(
        hold=lambda e: held.__setitem__(e.table_id, e), entries_for=lambda _ids: {}
    )
    hot = _StatementHot(manager, SimpleNamespace(tables=[_REGISTERED]), [_TABLE_ID])
    entry = hot.hold(
        "orders",
        catalog="c",
        schema="s",
        pk_column="id",
        rows=_ROWS,
        column_names=_NAMES,
        column_types=_registered_types(_REGISTERED),
        whole=True,
    )
    assert held == {_TABLE_ID: entry}
    return entry


def _answer(sql: str) -> list[tuple]:
    """``sql`` (PostgreSQL) answered by DuckDB, as the engine is given it."""
    return duckdb.sql(sqlglot.transpile(sql, read="postgres", write="duckdb")[0]).fetchall()


def test_a_registered_column_is_typed_under_its_sql_name():
    types = _registered_types(_REGISTERED)
    assert types["placed_on"] == "date" and types["placedOn"] == "date"
    assert types["lines"] == "jsonb"


def test_a_held_entry_carries_the_registered_types():
    entry = _held()
    assert isinstance(entry, HotTableEntry)
    assert entry.column_types["lines"] == "jsonb"
    assert entry.column_types["amount"] == "numeric(10,2)"


def test_rows_answered_for_this_statement_alone_carry_them_too():
    manager = SimpleNamespace(hold=lambda _e: None, entries_for=lambda _ids: {})
    hot = _StatementHot(manager, SimpleNamespace(tables=[_REGISTERED]), [_TABLE_ID])
    entry = hot.hold(
        "orders",
        catalog="c",
        schema="s",
        pk_column="id",
        rows=_ROWS,
        column_names=_NAMES,
        column_types=_registered_types(_REGISTERED),
        whole=False,
    )
    assert isinstance(entry, InlineRows)
    assert entry.column_types["placed_on"] == "date"


def test_jsonb_containment_over_a_hot_held_table():
    sql = build_values_cte_sql(
        """SELECT "id" FROM "orders" WHERE "lines" @> '[{"sku": "a"}]'::jsonb""",
        "orders",
        _held(),
    )
    assert """CAST('[{"sku": "a"}]' AS JSONB)""" in sql
    sqlglot.parse_one(sql, dialect="postgres")


def test_a_numeric_comparison_over_hot_rows_compares_numbers():
    sql = build_values_cte_sql('SELECT "id" FROM "orders" WHERE "amount" > 10', "orders", _held())
    assert _answer(sql) == [(1,)]


def test_a_date_comparison_over_hot_rows_compares_dates():
    sql = build_values_cte_sql(
        """SELECT "id" FROM "orders" WHERE "placed_on" < DATE '2026-02-01'""", "orders", _held()
    )
    assert _answer(sql) == [(1,)]


def test_numbers_order_as_numbers_not_text():
    """As text, '10.50' sorts before '9.25'."""
    sql = build_values_cte_sql(
        'SELECT "id" FROM "orders" ORDER BY "amount" DESC', "orders", _held()
    )
    assert _answer(sql) == [(1,), (2,)]
