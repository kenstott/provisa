# Copyright (c) 2026 Kenneth Stott
# Canary: 7e14c2a9-08bd-4f36-a5c7-93b0d6e21f4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The deployment-wide scheduled-maintenance notice (REQ-1466).

The endpoints are what the banner, the admin tab and `provisa maintenance` all read and write, so
what is pinned here is the wording contract (the server composes it, so one deployment says one
thing), the ``started_at`` semantics (stamped by the transition into active, never restarted by a
re-arm, cleared on off) and the gate on the write.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from provisa.api.admin import maintenance_router as mr


class _FakeConn:
    """One row keyed by id, enough of the core API for the router's select + upsert."""

    def __init__(self, row: SimpleNamespace | None) -> None:
        self.row = row

    async def execute_core(self, _stmt):
        row = self.row
        return SimpleNamespace(first=lambda: row)

    async def upsert(self, _table, values, index_elements, update_columns):  # noqa: ARG002
        self.row = SimpleNamespace(**values)


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def _wire(monkeypatch, row: SimpleNamespace | None) -> _FakeConn:
    conn = _FakeConn(row)
    monkeypatch.setattr(mr, "_admin_pool", lambda: _FakePool(conn))
    return conn


def _request(user_id: str = "admin@example.com"):
    return SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id=user_id)))


def _row(**overrides) -> SimpleNamespace:
    base = {
        "id": "current",
        "active": True,
        "message": None,
        "ends_at": None,
        "started_at": datetime(2026, 8, 14, 10, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.mark.asyncio
async def test_an_absent_row_reads_as_no_window(monkeypatch):
    # A deployment that has never had a maintenance window has no row at all; that is "off", not
    # an error the banner has to interpret.
    _wire(monkeypatch, None)
    assert await mr.read_maintenance() == {
        "active": False,
        "message": None,
        "ends_at": None,
        "started_at": None,
    }


@pytest.mark.asyncio
async def test_an_unset_message_resolves_to_the_deployments_standard_wording(monkeypatch):
    _wire(monkeypatch, _row())
    assert (await mr.read_maintenance())["message"] == mr._DEFAULT_MESSAGE


@pytest.mark.asyncio
async def test_a_supplied_message_is_kept(monkeypatch):
    _wire(monkeypatch, _row(message="Swapping the engine cluster."))
    assert (await mr.read_maintenance())["message"] == "Swapping the engine cluster."


@pytest.mark.asyncio
async def test_turning_it_on_stamps_the_start_and_records_who(monkeypatch):
    conn = _wire(monkeypatch, None)
    monkeypatch.setattr(mr, "require_platform_settings", lambda _r: None)
    before = datetime.now(timezone.utc)
    result = await mr.set_maintenance(_request(), mr.NoticeBody(active=True))
    assert result["active"] is True
    assert result["message"] == mr._DEFAULT_MESSAGE
    assert conn.row is not None
    assert conn.row.started_at >= before
    assert conn.row.updated_by == "admin@example.com"


@pytest.mark.asyncio
async def test_re_arming_an_open_window_does_not_restart_its_clock(monkeypatch):
    # Correcting the wording or extending the end time mid-window must not make the banner claim
    # the work only just began.
    started = datetime(2026, 8, 14, 10, tzinfo=timezone.utc)
    conn = _wire(monkeypatch, _row(started_at=started))
    monkeypatch.setattr(mr, "require_platform_settings", lambda _r: None)
    ends = started + timedelta(hours=2)
    await mr.set_maintenance(_request(), mr.NoticeBody(active=True, message="Longer", ends_at=ends))
    assert conn.row is not None
    assert conn.row.started_at == started
    assert conn.row.ends_at == ends


@pytest.mark.asyncio
async def test_turning_it_off_clears_the_stamp_so_the_next_window_starts_its_own(monkeypatch):
    conn = _wire(monkeypatch, _row())
    monkeypatch.setattr(mr, "require_platform_settings", lambda _r: None)
    result = await mr.set_maintenance(_request(), mr.NoticeBody(active=False))
    assert result["active"] is False
    assert conn.row is not None
    assert conn.row.started_at is None


@pytest.mark.asyncio
async def test_the_write_is_gated_before_anything_is_stored(monkeypatch):
    # A false maintenance banner is itself an outage, so the gate is checked before the row is
    # touched — a rejected caller must leave no trace.
    conn = _wire(monkeypatch, None)

    def deny(_request):
        raise PermissionError("platform_settings capability required")

    monkeypatch.setattr(mr, "require_platform_settings", deny)
    with pytest.raises(PermissionError):
        await mr.set_maintenance(_request(), mr.NoticeBody(active=True))
    assert conn.row is None
