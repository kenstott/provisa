# Copyright (c) 2026 Kenneth Stott
# Canary: 2aed85ea-bbf6-4a02-8b7c-fcaef825030a
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
from sqlalchemy.dialects.postgresql import insert as pg_insert

from provisa.core.repositories import calendar as calendar_repo
from provisa.core.schema_org import domains, registered_tables, sources

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _db(control_plane):
    yield control_plane.org()


async def _register_mv(conn, *, source_id, table_name, mv_calendar):
    # the table's source and domain exist: the control plane's foreign keys hold
    await conn.execute_core(insert(sources).values(id=source_id, type="postgresql"))
    await conn.execute_core(
        pg_insert(domains).values(id="d").on_conflict_do_nothing(index_elements=["id"])
    )
    await conn.execute_core(
        insert(registered_tables).values(
            source_id=source_id,
            domain_id="d",
            schema_name="public",
            table_name=table_name,
            mv_calendar=mv_calendar,
        )
    )


async def test_upsert_list_and_versioning(control_plane):
    async with _db(control_plane) as db:
        async with db.acquire() as conn:
            await calendar_repo.upsert(
                conn, {"name": "fy", "version": "v1", "base_system": "fiscal"}
            )
            await calendar_repo.upsert(
                conn, {"name": "fy", "version": "v2", "base_system": "fiscal"}
            )
            rows = await calendar_repo.list_all(conn)
            latest = await calendar_repo.get_latest(conn, "fy")
    assert {(r["name"], r["version"]) for r in rows} == {("fy", "v1"), ("fy", "v2")}
    # get_latest returns a real version of the calendar (exact tie-break is created_at, which can
    # collide at second resolution — the picker binds by name, not by pinned version).
    assert latest is not None and latest["version"] in {"v1", "v2"}


async def test_a_calendar_keeps_the_time_its_day_closes_at(control_plane):
    """REQ-2111: the close of the day is kept with its version; none means midnight."""
    async with _db(control_plane) as db:
        async with db.acquire() as conn:
            await calendar_repo.upsert(
                conn,
                {"name": "ldn", "version": "v1", "tz": "Europe/London", "day_close": "18:00"},
            )
            await calendar_repo.upsert(conn, {"name": "mid", "version": "v1"})
            rows = {r["name"]: r for r in await calendar_repo.list_all(conn)}
    assert rows["ldn"]["day_close"] == "18:00"
    assert rows["mid"]["day_close"] is None


async def test_usage_count_and_delete_when_unused(control_plane):
    async with _db(control_plane) as db:
        async with db.acquire() as conn:
            await calendar_repo.upsert(conn, {"name": "eom", "version": "v1"})
            assert await calendar_repo.usage_count(conn, "eom") == 0  # no MV references it
            removed = await calendar_repo.delete(conn, "eom")
            assert removed == 1
            assert await calendar_repo.get_latest(conn, "eom") is None


async def test_usage_count_reports_referencing_mvs(control_plane):
    async with _db(control_plane) as db:
        async with db.acquire() as conn:
            await calendar_repo.upsert(conn, {"name": "eom", "version": "v1"})
            await calendar_repo.upsert(conn, {"name": "eom", "version": "v2"})
            await _register_mv(conn, source_id="s1", table_name="sales_snap", mv_calendar="eom")
            await _register_mv(conn, source_id="s2", table_name="orders_snap", mv_calendar="eom")
            await _register_mv(conn, source_id="s3", table_name="live_view", mv_calendar=None)
            # counted by name across all versions; the non-periodic MV is not counted
            assert await calendar_repo.usage_count(conn, "eom") == 2
            # REQ-1918: a calendar in use is not deleted — the two views that take their
            # snapshot schedule from it are named, and neither version is removed.
            with pytest.raises(calendar_repo.CalendarDeleteRefused) as refused:
                await calendar_repo.delete(conn, "eom")
            assert [d.via for d in refused.value.dependents] == [
                ("registered_tables.mv_calendar",),
                ("registered_tables.mv_calendar",),
            ]
            assert len(await calendar_repo.list_all(conn)) == 2
            # Once nothing uses it, the delete removes BOTH versions.
            await conn.execute_core(registered_tables.update().values(mv_calendar=None))
            removed = await calendar_repo.delete(conn, "eom")
    assert removed == 2
