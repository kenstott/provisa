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
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
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
    await engine.dispose()


async def test_a_caught_duplicate_key_inside_acquire_releases_cleanly(db):
    with pytest.raises(IntegrityError):
        async with db.acquire() as conn:
            await conn.execute_core(text(f"INSERT INTO {_SCHEMA}.seed VALUES ('x')"))
    # The connection returns to the pool usable, on the role's default search_path.
    async with db.acquire() as conn:
        rows = (await conn.execute_core(text(f"SELECT count(*) FROM {_SCHEMA}.seed"))).fetchall()
    assert rows[0][0] == 1


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
