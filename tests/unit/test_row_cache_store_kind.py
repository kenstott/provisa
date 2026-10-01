# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2a6c41-7b3d-4e95-a0c7-5d1e9b3f6a82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The row cache is written where the replica STORE is, not where the engine is (REQ-1865).

A DuckDB engine may keep its replicas in an embedded DuckDB file (reached through the store broker)
or in Postgres (reached through the store write face). The row-cache helpers chose the broker by
the ENGINE's dialect, so a DuckDB engine on a Postgres store sent every keyed read of a row-level
table to a broker that does not exist: ``'NoneType' object has no attribute
'ensure_and_read_row_cache'``."""

# Requirements: REQ-1865, REQ-1915

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.federation import query_residency as qr


def _backend(store_is_duckdb: bool, broker=None):
    runtime = SimpleNamespace(
        _store_is_duckdb=lambda: store_is_duckdb,
        _store_broker=broker,
        ensure_materialize_attached=lambda: "mat_store",
    )
    return SimpleNamespace(dialect="duckdb", _runtime_for=lambda state: runtime)


@pytest.mark.asyncio
async def test_a_duckdb_engine_on_a_postgres_store_uses_the_store_write_face(monkeypatch):
    ensured = AsyncMock(return_value="cache-table")
    read = AsyncMock(return_value={(1,): "expires"})
    monkeypatch.setattr(qr, "_ensure_row_cache_table", ensured)
    monkeypatch.setattr(qr, "_read_row_cache", read)
    table, cached = await qr._ensure_and_read_row_cache(
        object(), _backend(False), object(), "s", "t", [("id", "integer")], ["id"], [(1,)]
    )
    assert (table, cached) == ("cache-table", {(1,): "expires"})


@pytest.mark.asyncio
async def test_a_duckdb_engine_on_a_duckdb_store_uses_the_broker():
    calls = []
    broker = SimpleNamespace(
        ensure_and_read_row_cache=lambda *a: calls.append(a) or {(1,): "expires"}
    )
    table, cached = await qr._ensure_and_read_row_cache(
        object(), _backend(True, broker), object(), "s", "t", [("id", "integer")], ["id"], [(1,)]
    )
    assert table is None and cached == {(1,): "expires"} and len(calls) == 1


def test_another_engine_never_has_a_duckdb_store():
    assert qr._is_duckdb_store(SimpleNamespace(dialect="postgres"), object()) is False
