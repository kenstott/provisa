# Copyright (c) 2026 Kenneth Stott
# Canary: 43ec6380-ebae-4faa-adc6-c23d1db5331f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: land_rows upsert-not-append semantics + build_row_cache_table's bookkeeping columns."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

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
        self.upserts: list[tuple[Any, dict, list[str]]] = []

    async def execute_core(self, stmt):
        self.stmts.append(stmt)
        return _Result()

    async def upsert(self, table, values, *, index_elements, update_columns=None, set_extra=None):
        self.upserts.append((table, dict(values), list(index_elements)))


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
async def test_land_rows_stamps_and_upserts_by_pk():
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
    assert len(conn.upserts) == 1
    _, values, index_elements = conn.upserts[0]
    assert index_elements == ["id"]
    assert values["_row_cached_at"] == now
    assert values["_row_expires_at"] == now + dt.timedelta(seconds=60)
    assert values["status"] == "new"


@pytest.mark.asyncio
async def test_land_rows_second_fetch_of_same_key_upserts_not_appends():
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

    # Both calls upsert (never a bulk insert / append path) on the SAME index_elements — the
    # store's upsert primitive is what converges the two calls onto one row, not a row count
    # this fake enforces itself; assert the call shape a real UPDATE-by-PK-else-INSERT depends on.
    assert len(conn.upserts) == 2
    for _, _, index_elements in conn.upserts:
        assert index_elements == ["id"]
    assert conn.upserts[1][1]["status"] == "shipped"
    assert conn.upserts[1][1]["_row_cached_at"] == t1
