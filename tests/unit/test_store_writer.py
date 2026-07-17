# Copyright (c) 2026 Kenneth Stott
# Canary: 6aff961c-b76b-4fe1-b95f-7f785aa50c28
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
async def test_reconcile_creates_keeps_recreates(tmp_path):
    dsn = _dsn(tmp_path)
    # absent → created
    assert (
        await store_writer.reconcile_table(dsn, schema="", table="pets", columns=_COLS) == "created"
    )
    await store_writer.land(
        dsn, schema="", table="pets", columns=_COLS, rows=[{"id": 1, "status": "a"}]
    )
    # unchanged schema → kept, landed data survives (the restart / re-register-no-change case)
    assert await store_writer.reconcile_table(dsn, schema="", table="pets", columns=_COLS) == "kept"
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT id FROM pets")
    assert [r[0] for r in rows] == [1]
    # drifted schema (a table config change) → recreated, data dropped (re-landed on next refresh)
    drifted = _COLS + [("added", "text")]
    assert (
        await store_writer.reconcile_table(dsn, schema="", table="pets", columns=drifted)
        == "recreated"
    )
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT count(*) FROM pets")
    assert rows[0][0] == 0


def test_check_source_drift_ratios_and_floor():
    from provisa.federation.store_writer import check_source_drift

    # full match → ratio 1.0
    assert check_source_drift(_COLS, [{"id": 1, "status": "a"}]) == 1.0
    # partial match → ratio, extra keys dropped, missing land NULL
    assert check_source_drift(_COLS, [{"id": 1, "extra": 9}]) == 0.5
    # empty rows → no drift to judge
    assert check_source_drift(_COLS, []) == 1.0
    # 0% overlap at the default floor → refuse (mangled source → error)
    with pytest.raises(ValueError, match="source drift"):
        check_source_drift(_COLS, [{"nope": 1, "other": 2}])
    # a raised floor rejects partial drift too
    with pytest.raises(ValueError, match="source drift"):
        check_source_drift(_COLS, [{"id": 1, "extra": 9}], match_floor=0.5)


@pytest.mark.asyncio
async def test_land_refuses_fully_drifted_source(tmp_path):
    dsn = _dsn(tmp_path)
    await store_writer.ensure_table(dsn, schema="", table="pets", columns=_COLS)
    with pytest.raises(ValueError, match="source drift"):
        await store_writer.land(
            dsn, schema="", table="pets", columns=_COLS, rows=[{"x": 1, "y": 2}]
        )


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
