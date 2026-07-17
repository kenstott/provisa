# Copyright (c) 2026 Kenneth Stott
# Canary: 34eff240-c3a8-43b7-91ae-b962aa7a5394
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Contract tests for the control-plane Database abstraction (core/database.py).

Runs against SQLite in-memory (no docker) to verify asyncpg-parity semantics:
$1 placeholder translation, autocommit-by-default, transaction commit/rollback,
nested savepoints, status strings, and the Row adapter.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Capabilities, Database, Row


@pytest.fixture
async def db():
    # A single shared in-memory connection so all acquires see the same schema.
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=__import__("sqlalchemy").pool.StaticPool,
    )
    database = Database(engine, name="test")
    async with database.acquire() as c:
        await c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, n INTEGER)")
    yield database
    await database.close()


async def test_placeholder_translation_and_fetch(db):
    async with db.acquire() as c:
        await c.execute("INSERT INTO t (id, name, n) VALUES ($1, $2, $3)", 1, "a", 10)
        await c.execute("INSERT INTO t (id, name, n) VALUES ($1, $2, $3)", 2, "b", 20)
        rows = await c.fetch("SELECT * FROM t ORDER BY id")
        assert [dict(r) for r in rows] == [
            {"id": 1, "name": "a", "n": 10},
            {"id": 2, "name": "b", "n": 20},
        ]


async def test_row_adapter_access_modes(db):
    async with db.acquire() as c:
        await c.execute("INSERT INTO t (id, name, n) VALUES ($1, $2, $3)", 1, "a", 10)
        row = await c.fetchrow("SELECT id, name, n FROM t WHERE id = $1", 1)
        assert isinstance(row, Row)
        assert row["name"] == "a"  # str key
        assert row[0] == 1  # positional
        assert row.get("missing") is None  # get with default
        assert "name" in row  # contains
        assert set(row.keys()) == {"id", "name", "n"}
        assert list(row) == [1, "a", 10]  # value iteration (asyncpg parity)


async def test_fetchrow_none_and_fetchval(db):
    async with db.acquire() as c:
        assert await c.fetchrow("SELECT * FROM t WHERE id = $1", 999) is None
        await c.execute("INSERT INTO t (id, name, n) VALUES ($1, $2, $3)", 1, "a", 10)
        assert await c.fetchval("SELECT COUNT(*) FROM t") == 1
        assert await c.fetchval("SELECT name FROM t WHERE id = $1", 1) == "a"


async def test_status_strings(db):
    async with db.acquire() as c:
        assert (
            await c.execute("INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", 1, "a", 1)
        ) == "INSERT 0 1"
        assert (await c.execute("UPDATE t SET n = $1 WHERE id = $2", 5, 1)) == "UPDATE 1"
        assert (await c.execute("DELETE FROM t WHERE id = $1", 1)) == "DELETE 1"
        # the delete-repo idiom
        await c.execute("INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", 2, "b", 2)
        assert (await c.execute("DELETE FROM t WHERE id = $1", 2)) == "DELETE 1"
        assert (await c.execute("DELETE FROM t WHERE id = $1", 999)) == "DELETE 0"


async def test_autocommit_default(db):
    async with db.acquire() as c:
        await c.execute("INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", 1, "a", 1)
    # visible on a fresh acquire => committed without an explicit transaction
    async with db.acquire() as c:
        assert await c.fetchval("SELECT COUNT(*) FROM t") == 1


async def test_transaction_rollback(db):
    async with db.acquire() as c:
        with pytest.raises(RuntimeError):
            async with c.transaction():
                await c.execute("INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", 1, "a", 1)
                raise RuntimeError("boom")
        assert await c.fetchval("SELECT COUNT(*) FROM t") == 0


async def test_transaction_commit(db):
    async with db.acquire() as c:
        async with c.transaction():
            await c.execute("INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", 1, "a", 1)
            await c.execute("INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", 2, "b", 2)
        assert await c.fetchval("SELECT COUNT(*) FROM t") == 2


async def test_nested_savepoint(db):
    async with db.acquire() as c:
        async with c.transaction():
            await c.execute("INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", 1, "a", 1)
            with pytest.raises(RuntimeError):
                async with c.transaction():
                    await c.execute("INSERT INTO t (id, name, n) VALUES ($1,$2,$3)", 2, "b", 2)
                    raise RuntimeError("inner")
        # outer row kept, inner rolled back via savepoint
        ids = {r["id"] for r in await c.fetch("SELECT id FROM t")}
        assert ids == {1}


async def test_executemany(db):
    async with db.acquire() as c:
        await c.executemany(
            "INSERT INTO t (id, name, n) VALUES ($1,$2,$3)",
            [(1, "a", 1), (2, "b", 2), (3, "c", 3)],
        )
        assert await c.fetchval("SELECT COUNT(*) FROM t") == 3


async def test_upsert_insert_then_update(db):
    from sqlalchemy import Column, Integer, MetaData, Table, Text

    md = MetaData()
    t = Table("up", md, Column("id", Text, primary_key=True), Column("n", Integer))
    async with db.acquire() as c:
        await c.execute("CREATE TABLE up (id TEXT PRIMARY KEY, n INTEGER)")
        await c.upsert(t, {"id": "a", "n": 1}, index_elements=["id"])
        assert await c.fetchval("SELECT n FROM up WHERE id = $1", "a") == 1
        # conflict on id -> update n
        await c.upsert(t, {"id": "a", "n": 99}, index_elements=["id"])
        assert await c.fetchval("SELECT n FROM up WHERE id = $1", "a") == 99
        assert await c.fetchval("SELECT COUNT(*) FROM up") == 1


async def test_insert_returning(db):
    from sqlalchemy import Column, Integer, MetaData, Table, Text

    md = MetaData()
    t = Table(
        "ins",
        md,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", Text),
    )
    async with db.acquire() as c:
        await c.execute("CREATE TABLE ins (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        new_id = await c.insert_returning(t, {"name": "x"}, returning="id")
        assert new_id == 1
        assert await c.fetchval("SELECT name FROM ins WHERE id = $1", new_id) == "x"


def test_capabilities_by_dialect():
    pg = Capabilities.for_dialect("postgresql")
    assert pg.listen_notify and pg.advisory_lock and pg.arrays and pg.rules and pg.returning
    sqlite = Capabilities.for_dialect("sqlite")
    assert sqlite.returning and not sqlite.arrays and not sqlite.listen_notify
    mysql = Capabilities.for_dialect("mysql")
    assert not mysql.returning and not mysql.arrays and mysql.advisory_lock


async def test_control_plane_sqlite_uses_wal(tmp_path):
    # REQ-1098: a file-based control-plane SQLite MUST open in WAL mode so the native DuckDB
    # engine can ATTACH and read it READ_ONLY while aiosqlite writes config changes. Rollback-
    # journal mode (the SQLite default) would transiently lock out the reader on a write commit.
    from sqlalchemy import text

    from provisa.core.database import create_engine_from_url

    engine = create_engine_from_url(f"sqlite+aiosqlite:///{tmp_path / 'cp.db'}")
    try:
        async with engine.begin() as c:
            mode = (await c.execute(text("PRAGMA journal_mode"))).scalar()
        assert mode == "wal"
    finally:
        await engine.dispose()
