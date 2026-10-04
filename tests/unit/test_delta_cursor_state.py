# Copyright (c) 2026 Kenneth Stott
# Canary: a65868ce-5bc7-4a84-9651-01e46b8bf9c7

"""The replica record stores the delta cursor and why a build was a whole rebuild (REQ-874)."""

from __future__ import annotations

import json

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import metadata
from provisa.core.schema_org import replica_state as rst
from provisa.federation import replica_state

pytestmark = pytest.mark.unit

KEY = ("pg", "public", "orders")


@pytest.fixture
async def db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[rst])
    return Database(engine, "test")


async def test_a_delta_applied_stores_the_cursor_and_clears_skipped(db):
    async with db.acquire() as conn:
        await replica_state.request_build(conn, KEY, replica_state.REASON_MODEL)
        await replica_state.record_whole_rebuild(conn, KEY, skipped="first_build", cursor=100)
        after_rebuild = await replica_state.read(conn, KEY)
        await replica_state.record_delta_applied(conn, KEY, cursor=250)
        after_delta = await replica_state.read(conn, KEY)
    assert after_rebuild is not None and after_delta is not None
    assert after_rebuild.delta_skipped == "first_build"
    assert json.loads(after_rebuild.delta_cursor) == 100
    assert after_delta.delta_skipped is None  # a delta cleared it
    assert json.loads(after_delta.delta_cursor) == 250


async def test_a_whole_rebuild_without_a_cursor_leaves_the_stored_one(db):
    async with db.acquire() as conn:
        await replica_state.request_build(conn, KEY, replica_state.REASON_MODEL)
        await replica_state.record_delta_applied(conn, KEY, cursor=7)
        await replica_state.record_whole_rebuild(
            conn, KEY, skipped="operator_requested", cursor=None
        )
        rec = await replica_state.read(conn, KEY)
    assert rec is not None
    assert rec.delta_skipped == "operator_requested"
    assert json.loads(rec.delta_cursor) == 7  # unchanged
