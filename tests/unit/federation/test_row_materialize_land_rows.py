# Copyright (c) 2026 Kenneth Stott
# Canary: 43ec6380-ebae-4faa-adc6-c23d1db5331f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: land_rows by-key-not-append semantics + build_row_cache_table's bookkeeping columns."""

from __future__ import annotations

import contextlib
import datetime as dt
from typing import Any

import pytest
from sqlalchemy.sql.dml import Delete

from provisa.core.database import Capabilities
from provisa.federation.materialize_exec import build_row_cache_table, land_rows

COLUMNS = [("id", "bigint"), ("status", "text")]


class _Result:
    def fetchone(self):
        return None


class _FakeConn:
    def __init__(self, dialect: str = "postgresql"):
        self.capabilities = Capabilities.for_dialect(dialect)
        self.stmts: list[Any] = []
        self.landed: list[list[dict]] = []

    async def execute_core(self, stmt):
        self.stmts.append(stmt)
        return _Result()

    async def bulk_copy(self, table, rows):
        del table
        self.landed.append([dict(row) for row in rows])
        return len(rows)

    async def upsert(self, table, values, *, index_elements):
        raise AssertionError("row-level landing never upserts row by row")

    async def advisory_xact_lock(self, key):
        del key

    @contextlib.asynccontextmanager
    async def transaction(self):
        yield


def test_build_row_cache_table_adds_bookkeeping_columns():
    table = build_row_cache_table("mat", "orders", COLUMNS, ["id"])
    assert "_row_cached_at" in table.c
    assert "_row_expires_at" in table.c
    assert table.c["_row_cached_at"].nullable is False
    assert table.c["_row_expires_at"].nullable is False
    assert table.c["id"].primary_key is True


@pytest.mark.asyncio
async def test_land_rows_requires_pk_columns():
    conn = _FakeConn()
    table = build_row_cache_table("mat", "orders", COLUMNS, ["id"])
    with pytest.raises(ValueError, match="requires primary key columns"):
        await land_rows(conn, table, [], [{"id": 1, "status": "new"}], resolved_cache_ttl=60)


@pytest.mark.asyncio
async def test_land_rows_stamps_and_lands_by_pk():
    conn = _FakeConn()
    table = build_row_cache_table("mat", "orders", COLUMNS, ["id"])
    now = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    await land_rows(
        conn,
        table,
        ["id"],
        [{"id": 1, "status": "new"}],
        resolved_cache_ttl=60,
        now=now,
    )
    assert len(conn.landed) == 1 and len(conn.landed[0]) == 1
    values = conn.landed[0][0]
    assert sum(isinstance(s, Delete) for s in conn.stmts) == 1  # the key goes before its row lands
    assert values["_row_cached_at"] == now
    assert values["_row_expires_at"] == now + dt.timedelta(seconds=60)
    assert values["status"] == "new"


@pytest.mark.asyncio
async def test_land_rows_second_fetch_of_same_key_replaces_not_appends():
    """A re-fetch of an already-cached key overwrites in place — never a second row (REQ-1865)."""
    conn = _FakeConn()
    table = build_row_cache_table("mat", "orders", COLUMNS, ["id"])
    t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    t1 = t0 + dt.timedelta(seconds=120)

    await land_rows(
        conn, table, ["id"], [{"id": 1, "status": "new"}], resolved_cache_ttl=60, now=t0
    )
    await land_rows(
        conn, table, ["id"], [{"id": 1, "status": "shipped"}], resolved_cache_ttl=60, now=t1
    )

    # Each call deletes the key before it lands the row: that converges the two onto one row in
    # a real store (test_row_landing_sets); assert the call shape it depends on.
    deletes = [s for s in conn.stmts if isinstance(s, Delete)]
    assert len(deletes) == 2 and len(conn.landed) == 2
    for delete in deletes:
        assert '"id" IN' in str(delete)
    assert conn.landed[1][0]["status"] == "shipped"
    assert conn.landed[1][0]["_row_cached_at"] == t1
