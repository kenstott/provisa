# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-963: the MV debounce config (quiet / max_delay) persists through the table repository, so an
operator's NRT setting survives a save/reload round-trip and reaches the event loop at boot."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import registered_tables, table_columns


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'r.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: registered_tables.metadata.create_all(
                s, tables=[registered_tables, table_columns]
            )
        )
    try:
        async with Database(engine, name="r").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mv_debounce_round_trips_through_repo(tmp_path):
    t = Table(
        source_id="__provisa__",
        domain_id="d",
        schema_name="views",
        table_name="daily",
        columns=[Column(name="id", visible_to=[])],
        view_sql="SELECT 1 AS id",
        materialize=True,
        mv_debounce_quiet=3.5,
        mv_debounce_max_delay=12.0,
    )
    async with _conn(tmp_path) as conn:
        tid = await table_repo.upsert(conn, t)
        assert tid is not None
        got = await table_repo.get(conn, tid)

    assert got is not None
    assert got["mv_debounce_quiet"] == 3.5
    assert got["mv_debounce_max_delay"] == 12.0
    assert got["materialize"] is True or got["materialize"] == 1


@pytest.mark.asyncio
async def test_mv_debounce_defaults_when_unset(tmp_path):
    t = Table(
        source_id="__provisa__",
        domain_id="d",
        schema_name="views",
        table_name="live",
        columns=[Column(name="id", visible_to=[])],
        view_sql="SELECT 1 AS id",
        materialize=True,
    )
    async with _conn(tmp_path) as conn:
        tid = await table_repo.upsert(conn, t)
        assert tid is not None
        got = await table_repo.get(conn, tid)

    assert got is not None
    assert got["mv_debounce_quiet"] == 0.0  # real-time default
    assert got["mv_debounce_max_delay"] == 5.0
