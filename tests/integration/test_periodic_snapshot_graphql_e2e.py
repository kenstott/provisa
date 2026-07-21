# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2a9e71-8b34-4d16-9f08-3e7d1c6b4a29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the snapshot-calendar admin surface through the real GraphQL API (REQ-962/1166).

Drives the ACTUAL ``admin_schema`` (parse → validate → resolve → repository → real Postgres) for the
user-facing surface: ``createCalendar`` persists a versioned calendar, ``calendars`` reads it back,
and the schema exposes the MV snapshot-binding fields. Source (a GraphQL request) to consumption (the
query response) over a real DB — no resolver shortcuts.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.api.admin.schema import admin_schema
from provisa.core.database import Database
from provisa.core.schema_org import calendars

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def _async_dsn(pg_dsn: str) -> str:
    return pg_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)


@pytest_asyncio.fixture
async def pool(pg_dsn):
    """A real PG-backed control-plane Database in a throwaway schema, wired in as the admin pool."""
    schema = f"cale2e_{uuid.uuid4().hex[:12]}"
    engine = create_async_engine(_async_dsn(pg_dsn))
    async with engine.begin() as c:
        await c.execute(text(f'CREATE SCHEMA "{schema}"'))
        await c.execute(text(f'SET search_path TO "{schema}"'))
        await c.run_sync(lambda s: calendars.metadata.create_all(s, tables=[calendars]))
    db = Database(engine, name="cale2e", search_path=schema)

    @asynccontextmanager
    async def _acquire():
        async with db.acquire() as conn:
            yield conn

    fake_pool = type("P", (), {"acquire": staticmethod(_acquire)})()
    with (
        patch("provisa.api.admin.schema_mutation._get_pool", return_value=fake_pool),
        patch("provisa.api.admin.schema_query._get_pool", return_value=fake_pool),
    ):
        yield
    async with engine.begin() as c:
        await c.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


_CREATE = """
mutation ($i: CalendarInput!) { createCalendar(input: $i) { success message } }
"""
_LIST = "{ calendars { name version baseSystem tz weekStart holidays weekend } }"


async def test_create_calendar_then_read_back(pool):
    res = await admin_schema.execute(
        _CREATE,
        variable_values={
            "i": {
                "name": "fiscal-us",
                "version": "v1",
                "baseSystem": "fiscal",
                "tz": "America/New_York",
                "fiscalAnchorMonth": 10,
                "fiscalAnchorDay": 1,
                "weekStart": 0,
                "holidays": ["2026-07-03"],
                "weekend": [5, 6],
            }
        },
    )
    assert res.errors is None, res.errors
    assert res.data["createCalendar"]["success"] is True

    listed = await admin_schema.execute(_LIST)
    assert listed.errors is None, listed.errors
    cals = listed.data["calendars"]
    assert len(cals) == 1
    got = cals[0]
    assert got["name"] == "fiscal-us" and got["version"] == "v1"
    assert got["baseSystem"] == "fiscal" and got["tz"] == "America/New_York"
    assert got["holidays"] == ["2026-07-03"] and got["weekend"] == [5, 6]


async def test_invalid_calendar_rejected(pool):
    res = await admin_schema.execute(
        _CREATE,
        variable_values={"i": {"name": "bad", "version": "v1", "baseSystem": "not_a_system"}},
    )
    assert res.errors is None, res.errors
    assert res.data["createCalendar"]["success"] is False
    assert "invalid calendar" in res.data["createCalendar"]["message"]
    # nothing persisted on a rejected calendar
    listed = await admin_schema.execute(_LIST)
    assert listed.data["calendars"] == []


async def test_schema_exposes_mv_snapshot_binding_fields():
    """The MV config input carries the snapshot-schedule binding (calendar/grain/lateness/preflight)."""
    sdl = admin_schema.as_str()
    for field in ("mvCalendar", "mvGrain", "mvAllowedLateness", "mvExpectedEvents", "mvBusinessDayGrain"):
        assert field in sdl, f"{field} missing from the admin schema"
