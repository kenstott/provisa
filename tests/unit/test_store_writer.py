# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-848/932: the store write face — ensure_table (eager DDL) and land (DML), end-to-end.

Exercised against a real SQLite store (schemaless) so the SQLAlchemy write face is driven for
real — the engine is never involved; store_writer opens the store's own connection."""

from __future__ import annotations

import pytest

from provisa.federation import store_writer

_COLS = [("id", "bigint"), ("status", "text")]


def _dsn(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


@pytest.mark.asyncio
async def test_ensure_table_creates_empty_and_is_idempotent(tmp_path):
    dsn = _dsn(tmp_path)
    loc = await store_writer.ensure_table(
        dsn, schema="", table="pets", columns=_COLS, pk_columns=["id"]
    )
    assert loc == "pets"
    # second call is a no-op (CREATE TABLE IF NOT EXISTS) — no error, table still empty
    await store_writer.ensure_table(dsn, schema="", table="pets", columns=_COLS, pk_columns=["id"])
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT COUNT(*) FROM pets")
    assert rows[0][0] == 0


@pytest.mark.asyncio
async def test_land_replace_writes_rows_through_write_face(tmp_path):
    dsn = _dsn(tmp_path)
    await store_writer.ensure_table(dsn, schema="", table="pets", columns=_COLS)
    await store_writer.land(
        dsn,
        schema="",
        table="pets",
        columns=_COLS,
        rows=[{"id": 1, "status": "new"}, {"id": 2, "status": "sold"}],
    )  # change_signal defaults ttl → REPLACE
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT id, status FROM pets ORDER BY id")
    assert [(r[0], r[1]) for r in rows] == [(1, "new"), (2, "sold")]


@pytest.mark.asyncio
async def test_land_replace_is_full_refresh(tmp_path):
    dsn = _dsn(tmp_path)
    await store_writer.land(
        dsn, schema="", table="pets", columns=_COLS, rows=[{"id": 1, "status": "a"}]
    )
    await store_writer.land(
        dsn, schema="", table="pets", columns=_COLS, rows=[{"id": 9, "status": "z"}]
    )
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT id FROM pets")
    assert [r[0] for r in rows] == [9]  # replace dropped the prior row
