# Copyright (c) 2026 Kenneth Stott
# Canary: 565a0f4c-3fee-44a2-8dab-8af629ee5b43
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-992 — full-replace poll delivers only on content-hash change."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest
from pytest_bdd import given, scenarios, then, when

import provisa.live.engine as live_engine
from provisa.live.engine import LiveEngine, _LiveJob


class _FakeEngineRuntime:
    def __init__(self, rows):
        self.rows = rows
        self.column_names = ["id", "v"]

    async def execute_engine(self, _sql):
        from types import SimpleNamespace

        return SimpleNamespace(column_names=self.column_names, rows=list(self.rows))


class _RecordingFanout:
    def __init__(self):
        self.sends = 0

    async def send(self, _rows):
        self.sends += 1


class _NullTenantDB:
    @asynccontextmanager
    async def acquire(self):
        yield object()


@pytest.fixture
def shared_data():
    return {}


def _run_replace_poll(rows, monkeypatch, watermark_state):
    async def _get(_conn, query_id, output_type):
        return watermark_state.get((query_id, output_type))

    async def _set(_conn, query_id, output_type, value):
        watermark_state[(query_id, output_type)] = value

    monkeypatch.setattr(live_engine, "get_watermark", _get, raising=False)
    monkeypatch.setattr(live_engine, "set_watermark", _set, raising=False)
    # _poll_replace imports get/set_watermark from provisa.live.watermark at call time.
    import provisa.live.watermark as wm

    monkeypatch.setattr(wm, "get_watermark", _get)
    monkeypatch.setattr(wm, "set_watermark", _set)

    fanout = _RecordingFanout()
    job = _LiveJob(
        query_id="q1",
        sql="SELECT * FROM t",
        watermark_column="",
        poll_interval=5,
        fanout=fanout,  # type: ignore[arg-type]  # test double records send() calls
        kafka_outputs=[],
        mode="replace",
    )
    eng = LiveEngine(tenant_db=_NullTenantDB(), engine=_FakeEngineRuntime(rows))
    asyncio.run(eng._poll_replace(job))
    return fanout


@given("a poll-signal table with no watermark column and 100 rows")
def given_poll_signal_table(shared_data):
    shared_data["rows"] = [{"id": i, "v": i} for i in range(100)]
    shared_data["watermark_state"] = {}


@when("the full table is re-scanned and the content hash matches the previous scan")
def when_rescan_hash_matches(shared_data, monkeypatch):
    # First poll delivers and records the digest; second poll re-scans identical rows.
    _run_replace_poll(shared_data["rows"], monkeypatch, shared_data["watermark_state"])
    shared_data["fanout"] = _run_replace_poll(
        shared_data["rows"], monkeypatch, shared_data["watermark_state"]
    )


@then("no snapshot is delivered to downstream subscribers")
def then_no_snapshot_delivered(shared_data):
    # The second (unchanged) scan is suppressed — 0 sends on this run.
    assert shared_data["fanout"].sends == 0


@when("the content hash differs (row added, deleted, or modified)")
def when_hash_differs(shared_data, monkeypatch):
    changed = shared_data["rows"] + [{"id": 100, "v": 100}]
    shared_data["fanout"] = _run_replace_poll(changed, monkeypatch, shared_data["watermark_state"])


@then("a replace snapshot is delivered")
def then_replace_snapshot_delivered(shared_data):
    assert shared_data["fanout"].sends == 1


scenarios("../features/REQ-992.feature")
