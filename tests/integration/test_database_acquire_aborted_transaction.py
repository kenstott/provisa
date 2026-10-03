# Copyright (c) 2026 Kenneth Stott
# Canary: 1f7b3d92-6e4a-4c85-9b0d-7a2e5c81f4d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement that fails inside ``Database.acquire`` leaves a PostgreSQL transaction aborted. The
release-time ``RESET search_path`` then failed with "current transaction is aborted", replacing the
caller's own error -- which is how every coordinated MV refresh died on its idempotent seed INSERT
(``ensure_mv_row`` catches the duplicate key as its success case). Real PostgreSQL: the aborted
state is the server's, not something a fake reproduces."""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from provisa.core.database import Database, create_engine_from_url

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_SCHEMA = "org_acquire_abort"


@pytest.fixture
async def db():
    engine = create_engine_from_url(_ASYNC_URL)
    database = Database(engine, name="tenant", search_path=_SCHEMA)
    async with database.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA {_SCHEMA}")
        await conn.execute(f"CREATE TABLE {_SCHEMA}.seed (id text PRIMARY KEY)")
        await conn.execute(f"INSERT INTO {_SCHEMA}.seed VALUES ('x')")
    yield database
    async with database.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
    engine.dispose()


async def test_a_caught_duplicate_key_inside_acquire_releases_cleanly(db):
    with pytest.raises(IntegrityError):
        async with db.acquire() as conn:
            await conn.execute_core(text(f"INSERT INTO {_SCHEMA}.seed VALUES ('x')"))
    # The connection returns to the pool usable, on the role's default search_path.
    async with db.acquire() as conn:
        rows = (await conn.execute_core(text(f"SELECT count(*) FROM {_SCHEMA}.seed"))).fetchall()
    assert rows[0][0] == 1


async def test_a_caught_failure_leaves_the_same_connection_usable(db):
    """REQ-1882 (sync control plane): outside an explicit transaction each statement is its own
    unit, as under asyncpg — a caught failure must not poison the next statement on the SAME
    connection (the airport create_table -> schema rebuild path died on exactly this)."""
    async with db.acquire() as conn:
        with pytest.raises(IntegrityError):
            await conn.execute_core(text(f"INSERT INTO {_SCHEMA}.seed VALUES ('x')"))
        assert await conn.fetchval(f"SELECT count(*) FROM {_SCHEMA}.seed") == 1
        await conn.execute(f"INSERT INTO {_SCHEMA}.seed VALUES ('y')")
    async with db.acquire() as conn:
        assert await conn.fetchval(f"SELECT count(*) FROM {_SCHEMA}.seed") == 2


async def test_a_failed_multi_statement_script_leaves_the_connection_usable(db):
    """A multi-statement script runs on the raw psycopg cursor (simple query protocol), outside
    SQLAlchemy's transaction tracking. A caught failure must still be rolled back (the notify-
    trigger install in ensure_pg_notify_triggers catches its own failures) and a successful one
    committed, not lost on pool checkin."""
    from psycopg import errors as pg_errors

    async with db.acquire() as conn:
        with pytest.raises(pg_errors.UndefinedTable):
            await conn.execute(f"SELECT 1; SELECT * FROM {_SCHEMA}.no_such_table;")
        assert await conn.fetchval(f"SELECT count(*) FROM {_SCHEMA}.seed") == 1
        await conn.execute(
            f"CREATE TABLE {_SCHEMA}.scripted (id int); INSERT INTO {_SCHEMA}.scripted VALUES (1);"
        )
    async with db.acquire() as conn:
        assert await conn.fetchval(f"SELECT count(*) FROM {_SCHEMA}.scripted") == 1


async def test_a_failure_inside_an_explicit_transaction_still_rolls_the_whole_block_back(db):
    with pytest.raises(IntegrityError):
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f"INSERT INTO {_SCHEMA}.seed VALUES ('z')")
                await conn.execute_core(text(f"INSERT INTO {_SCHEMA}.seed VALUES ('x')"))
    async with db.acquire() as conn:
        assert await conn.fetchval(f"SELECT count(*) FROM {_SCHEMA}.seed WHERE id = 'z'") == 0


async def test_ensure_mv_row_is_idempotent_on_postgresql(db):
    from types import SimpleNamespace

    from provisa.core.db import init_schema
    from provisa.mv.coordination import ensure_mv_row

    schema_sql = os.path.join(
        os.path.dirname(__file__), "..", "..", "provisa", "core", "schema.sql"
    )
    with open(schema_sql, encoding="utf-8") as fh:
        await init_schema(db, fh.read(), org_id="acquire_abort")
    mv = SimpleNamespace(
        id="view-dim_pet",
        # A view bound by join pattern carries its inputs; a SQL-defined one carries none, and its
        # record names source_tables (provisa.mv.coordination.ensure_mv_row).
        inputs=(),
        source_tables=["pets"],
        target_catalog="_landing",
        target_schema="org_default_mv_cache",
        target_table="mv_dim_pet",
        refresh_interval=3600,
        enabled=True,
        sql="SELECT 1",
    )
    await ensure_mv_row(db, mv)
    await ensure_mv_row(db, mv)  # the duplicate is the success case, and must not poison release
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                text("SELECT count(*) FROM materialized_views WHERE id = 'view-dim_pet'")
            )
        ).fetchall()
    assert rows[0][0] == 1
