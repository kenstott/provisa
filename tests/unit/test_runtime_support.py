# Copyright (c) 2026 Kenneth Stott
# Canary: a8a85d81-d964-417a-b643-28d67124eff6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit: the shared federation-runtime helpers (runtime_support) are behavior-identical to the
inline logic they replaced in the pg / sqlalchemy / duckdb / clickhouse runtimes.

These are pure functions over the DBAPI-result contract (``.description`` / ``.fetchall()``) — no
service; the driver result object is the unit under test, supplied as a stand-in."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from decimal import Decimal

import pyarrow as pa

from provisa.executor.result import QueryResult
from provisa.federation.runtime_support import (
    arrow_batches_from_rows,
    columns_from_describe,
    result_from_dbapi,
    run_async,
)


class _FakeCursor:
    """A DBAPI cursor/result stand-in: description drives cols, fetchall drives rows."""

    def __init__(self, description, rows):
        self.description = description
        self._rows = rows

    def fetchall(self):
        return self._rows


def _inline_select(obj) -> QueryResult:
    """The exact expression the pg/sqlalchemy runtimes used before extraction."""
    cols = [d[0] for d in obj.description] if obj.description else []
    rows = obj.fetchall() if obj.description else []
    return QueryResult(rows=rows, column_names=cols)


def test_result_from_dbapi_select_matches_inline():
    desc = [("id",), ("amount",)]
    rows = [(1, 10), (2, 20)]
    cur = _FakeCursor(desc, rows)
    got = result_from_dbapi(cur)
    assert got.column_names == ["id", "amount"]
    assert got.rows == [(1, 10), (2, 20)]
    # identical to the pre-refactor inline logic
    ref = _inline_select(_FakeCursor(desc, rows))
    assert got.column_names == ref.column_names and got.rows == ref.rows


def test_result_from_dbapi_non_select_yields_empty():
    # description None (DDL/non-SELECT) → no columns, no rows; fetchall is NOT called.
    cur = _FakeCursor(None, None)
    got = result_from_dbapi(cur)
    assert got.column_names == [] and got.rows == []


def test_result_from_dbapi_duckdb_equivalence_on_ddl():
    # duckdb previously called res.fetchall() unconditionally; with description None the helper
    # returns [] rows. Equivalent to DuckDB's own [] for non-SELECT — assert the helper's guard.
    cur = _FakeCursor(None, [])  # a driver that would return [] anyway
    assert result_from_dbapi(cur).rows == []


def test_columns_from_describe_matches_inline():
    rows = [("id", "INTEGER", "YES"), ("name", "VARCHAR", "NO")]
    got = columns_from_describe(rows)
    assert got == {"id": "integer", "name": "varchar"}
    ref = {row[0]: str(row[1]).lower() for row in rows}
    assert got == ref


def test_run_async_delegates_to_run_sync():
    calls = []

    def run_sync(sql, params):
        calls.append((sql, params))
        return QueryResult(rows=[(1,)], column_names=["x"])

    res = asyncio.run(run_async(run_sync, "SELECT 1", [7]))
    assert calls == [("SELECT 1", [7])]
    assert res.rows == [(1,)] and res.column_names == ["x"]


def test_run_async_default_params_none():
    seen = SimpleNamespace(params="unset")

    def run_sync(sql, params):
        seen.params = params
        return QueryResult(rows=[], column_names=[])

    asyncio.run(run_async(run_sync, "SELECT 1"))
    assert seen.params is None


# ---- arrow_batches_from_rows: generic row→Arrow adapter (REQ-1219) -----------


def _rows_result(cols, rows):
    """A ResultStream: a StreamingQueryResult over batched rows (adapter reads column_names + iter_rows)."""
    from provisa.executor.result import StreamingQueryResult

    return StreamingQueryResult(iter([rows]) if rows else iter(()), column_names=cols)


def test_arrow_batches_locks_schema_from_first_batch():
    res = _rows_result(["id", "name"], [(1, "a"), (2, "b")])
    schema, batches = arrow_batches_from_rows(res)
    assert schema.names == ["id", "name"]
    assert schema.field("id").type == pa.int64()
    assert schema.field("name").type == pa.string()
    tbl = pa.Table.from_batches(list(batches), schema=schema)
    assert tbl.column("id").to_pylist() == [1, 2]
    assert tbl.column("name").to_pylist() == ["a", "b"]


def test_arrow_batches_converts_decimal_to_float():
    res = _rows_result(["amount"], [(Decimal("1.5"),), (Decimal("2.5"),)])
    schema, batches = arrow_batches_from_rows(res)
    assert schema.field("amount").type == pa.float64()
    tbl = pa.Table.from_batches(list(batches), schema=schema)
    assert tbl.column("amount").to_pylist() == [1.5, 2.5]


def test_arrow_batches_empty_result_yields_null_typed_schema():
    res = _rows_result(["id", "name"], [])
    schema, batches = arrow_batches_from_rows(res)
    assert schema.names == ["id", "name"]
    assert list(batches) == []


def test_arrow_batches_multi_batch_casts_to_locked_schema():
    rows = [(i, f"n{i}") for i in range(5)]
    res = _rows_result(["id", "name"], rows)
    schema, batches = arrow_batches_from_rows(res, batch_rows=2)
    out = list(batches)
    assert len(out) == 3  # 2 + 2 + 1
    tbl = pa.Table.from_batches(out, schema=schema)
    assert tbl.column("id").to_pylist() == [0, 1, 2, 3, 4]
