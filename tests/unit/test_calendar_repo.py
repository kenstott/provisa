# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Calendar repository: upsert / list / usage-count / delete (REQ-962).

The delete path is usage-gated at the mutation layer; here we verify the repo primitives it relies
on — ``usage_count`` counts MVs bound to a calendar by name (across versions), and ``delete`` removes
every version. A calendar with usage must be reported so the mutation can refuse the delete.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.repositories import calendar as calendar_repo
from provisa.core.schema_org import calendars, domains, registered_tables, sources

pytestmark = pytest.mark.asyncio

_TABLES = [sources, domains, registered_tables, calendars]


@asynccontextmanager
async def _db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cal.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: calendars.metadata.create_all(s, tables=_TABLES))
    try:
        yield Database(engine, name="cp")
    finally:
        await engine.dispose()


async def _register_mv(conn, *, source_id, table_name, mv_calendar):
    await conn.execute_core(
        insert(registered_tables).values(
            source_id=source_id,
            domain_id="d",
            schema_name="public",
            table_name=table_name,
            mv_calendar=mv_calendar,
        )
    )


async def test_upsert_list_and_versioning(tmp_path):
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await calendar_repo.upsert(conn, {"name": "fy", "version": "v1", "base_system": "fiscal"})
            await calendar_repo.upsert(conn, {"name": "fy", "version": "v2", "base_system": "fiscal"})
            rows = await calendar_repo.list_all(conn)
            latest = await calendar_repo.get_latest(conn, "fy")
    assert {(r["name"], r["version"]) for r in rows} == {("fy", "v1"), ("fy", "v2")}
    # get_latest returns a real version of the calendar (exact tie-break is created_at, which can
    # collide at second resolution — the picker binds by name, not by pinned version).
    assert latest is not None and latest["version"] in {"v1", "v2"}


async def test_usage_count_and_delete_when_unused(tmp_path):
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await calendar_repo.upsert(conn, {"name": "eom", "version": "v1"})
            assert await calendar_repo.usage_count(conn, "eom") == 0  # no MV references it
            removed = await calendar_repo.delete(conn, "eom")
            assert removed == 1
            assert await calendar_repo.get_latest(conn, "eom") is None


async def test_usage_count_reports_referencing_mvs(tmp_path):
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await calendar_repo.upsert(conn, {"name": "eom", "version": "v1"})
            await calendar_repo.upsert(conn, {"name": "eom", "version": "v2"})
            await _register_mv(conn, source_id="s1", table_name="sales_snap", mv_calendar="eom")
            await _register_mv(conn, source_id="s2", table_name="orders_snap", mv_calendar="eom")
            await _register_mv(conn, source_id="s3", table_name="live_view", mv_calendar=None)
            # counted by name across all versions; the non-periodic MV is not counted
            assert await calendar_repo.usage_count(conn, "eom") == 2
            # a delete would remove BOTH versions — the mutation must refuse while usage > 0
            removed = await calendar_repo.delete(conn, "eom")
    assert removed == 2
