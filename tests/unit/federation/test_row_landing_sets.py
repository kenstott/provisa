# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Row-level landing is set-based (REQ-1865): one statement per batch, not one per row, against a
real store connection. Counted in statements, never timed: at one upsert per row a 40,000-order
join outran the 300 s request budget."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest
from sqlalchemy import event, select

from provisa.core.database import Database, create_engine_from_url
from provisa.federation.materialize_exec import (
    LAND_BATCH_ROWS,
    build_row_cache_table,
    land_rows,
)

COLUMNS = [("id", "bigint"), ("tenant", "text"), ("status", "text")]
T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


@pytest.fixture
def store(tmp_path: Path):
    engine = create_engine_from_url(f"sqlite:///{tmp_path / 'store.db'}", pool_size=1)
    statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def _count(_conn, _cursor, statement, _params, _context, _executemany):
        statements.append(statement)

    yield Database(engine, name="materialize"), statements
    engine.dispose()


def _rows(n: int, status: str = "new") -> list[dict]:
    return [{"id": i, "tenant": "t", "status": status} for i in range(n)]


async def _held(db: Database, table) -> list[tuple]:
    async with db.acquire() as conn:
        result = await conn.execute_core(select(table.c.id, table.c.status).order_by(table.c.id))
        return [tuple(row) for row in result.fetchall()]


@pytest.mark.asyncio
async def test_landing_issues_statements_by_the_batch_not_by_the_row(store):
    db, statements = store
    table = build_row_cache_table("", "orders", COLUMNS, ["id"])
    rows = _rows(LAND_BATCH_ROWS * 4 + 1)
    async with db.acquire() as conn:
        await land_rows(conn, table, ["id"], rows, resolved_cache_ttl=60)
    batches = 5
    writes = [s for s in statements if s.split()[0] in ("INSERT", "UPDATE", "DELETE")]
    assert len(writes) == batches * 2, "one DELETE and one bulk INSERT per batch"
    assert sum(s.startswith("UPDATE") for s in writes) == 0
    assert len(await _held(db, table)) == len(rows)


@pytest.mark.asyncio
async def test_a_second_land_replaces_each_key_and_leaves_the_others(store):
    db, _ = store
    table = build_row_cache_table("", "orders", COLUMNS, ())  # a row cache table holds no key
    async with db.acquire() as conn:
        await land_rows(conn, table, ["id"], _rows(3), resolved_cache_ttl=60, now=T0)
        await land_rows(
            conn,
            table,
            ["id"],
            [{"id": 1, "tenant": "t", "status": "shipped"}],
            resolved_cache_ttl=60,
            now=T0 + dt.timedelta(seconds=120),
        )
    assert await _held(db, table) == [(0, "new"), (1, "shipped"), (2, "new")]


@pytest.mark.asyncio
async def test_a_key_repeated_in_one_land_keeps_its_last_row(store):
    """As one upsert after another left it, and a keyed table takes the land at all."""
    db, _ = store
    table = build_row_cache_table("", "orders", COLUMNS, ["id"])
    rows = [
        {"id": 1, "tenant": "t", "status": "first"},
        {"id": 1, "tenant": "t", "status": "last"},
    ]
    async with db.acquire() as conn:
        await land_rows(conn, table, ["id"], rows, resolved_cache_ttl=60)
    assert await _held(db, table) == [(1, "last")]


@pytest.mark.asyncio
async def test_a_land_that_fails_leaves_what_the_store_held(store, monkeypatch):
    """The delete and the insert are one transaction: a reader never finds a key gone."""
    db, _ = store
    table = build_row_cache_table("", "orders", COLUMNS, ["id"])
    async with db.acquire() as conn:
        await land_rows(conn, table, ["id"], _rows(2), resolved_cache_ttl=60)

        async def _refused(_table, _rows):
            raise RuntimeError("the store refused the batch")

        monkeypatch.setattr(conn, "bulk_copy", _refused)
        with pytest.raises(RuntimeError, match="refused the batch"):
            await land_rows(conn, table, ["id"], _rows(2, "next"), resolved_cache_ttl=60)
    assert await _held(db, table) == [(0, "new"), (1, "new")]


@pytest.mark.asyncio
async def test_a_composite_key_lands_by_the_whole_key(store):
    db, _ = store
    table = build_row_cache_table("", "orders", COLUMNS, ["id", "tenant"])
    async with db.acquire() as conn:
        await land_rows(
            conn,
            table,
            ["id", "tenant"],
            [{"id": 1, "tenant": "a", "status": "x"}, {"id": 1, "tenant": "b", "status": "y"}],
            resolved_cache_ttl=60,
        )
        await land_rows(
            conn,
            table,
            ["id", "tenant"],
            [{"id": 1, "tenant": "a", "status": "z"}],
            resolved_cache_ttl=60,
        )
    assert await _held(db, table) == [(1, "y"), (1, "z")]


@pytest.mark.asyncio
async def test_landing_takes_the_table_lock_inside_its_transaction(store, monkeypatch):
    """Landers of one table run one at a time, or two could delete the same key and both insert:
    the row cache has no unique key to stop them."""
    db, statements = store
    table = build_row_cache_table("", "orders", COLUMNS, ["id"])
    keys: list[int] = []
    async with db.acquire() as conn:
        taken = conn.advisory_xact_lock

        async def _lock(key: int) -> None:
            assert conn._tx_depth > 0, "a transaction-scoped lock ends with its transaction"
            keys.append(key)
            await taken(key)

        monkeypatch.setattr(conn, "advisory_xact_lock", _lock)
        await land_rows(conn, table, ["id"], _rows(1), resolved_cache_ttl=60)
        await land_rows(conn, table, ["id"], _rows(1), resolved_cache_ttl=60)
    assert len(keys) == 2 and keys[0] == keys[1]
    assert statements
