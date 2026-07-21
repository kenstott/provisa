# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The DB-backed per-input freshness reader (REQ-961/859).

Three cases, deliberately distinct:
- an ALWAYS-CURRENT (live/query-time) input is fresh-through any boundary — its missing stamp is
  expected, not an outage;
- a SCHEDULED input with a persisted stamp maps to that stamp;
- a SCHEDULED input with NO stamp is an outage (never a silent assume-fresh).
"""

from __future__ import annotations

import math
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.freshness_reader import make_db_freshness_of

pytestmark = pytest.mark.asyncio
UTC = timezone.utc
WIN_END = datetime(2026, 7, 10, tzinfo=UTC)


@asynccontextmanager
async def _db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fr.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    try:
        yield Database(engine, name="fr")
    finally:
        await engine.dispose()


async def test_always_current_input_is_fresh_through_any_boundary(tmp_path):
    async with _db(tmp_path) as db:
        reader = make_db_freshness_of(db, always_current={"live.orders"})
        subj = await reader("live.orders")  # never landed, no row — but live → fresh
    assert subj.last_refresh_ok() is True
    assert subj.last_refresh_at() == math.inf  # >= any window.end


async def test_scheduled_input_with_stamp_maps_to_it(tmp_path):
    async with _db(tmp_path) as db:
        async with db.acquire() as conn:
            await queue.record_refresh(conn, "mat.orders", at=WIN_END, ok=True)
        reader = make_db_freshness_of(db)
        subj = await reader("mat.orders")
    assert subj.last_refresh_ok() is True
    assert subj.last_refresh_at() == WIN_END.timestamp()


async def test_scheduled_input_without_stamp_is_an_outage(tmp_path):
    async with _db(tmp_path) as db:
        reader = make_db_freshness_of(db, always_current={"live.other"})
        subj = await reader("mat.never_landed")  # scheduled, no stamp, not in always_current
    assert subj.last_refresh_ok() is False  # fail loud, never assume fresh
    assert subj.last_refresh_at() is None
