# Copyright (c) 2026 Kenneth Stott
# Canary: 62703c1e-b417-4177-ac78-bdb7649fdc52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A load-protected table's replica is refreshed only in its off-peak window (REQ-1141).

The build runner asks, before it claims a build, whether the build may run now. A refresh of a
replica that stands in this store, of a load-protected table whose window is closed, is held
until the window opens: the record goes idle, due at the opening, saying why. Its first build is
never held -- reads of it wait on that one. A window-only table is due once per window.
"""

# Requirements: REQ-1141

from __future__ import annotations

import uuid

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from provisa.federation import replica_builds
from provisa.federation import replica_state as build_state
from provisa.federation.replica_errors import WAITING_WINDOW
from provisa.federation.replica_locks import BuildLocks
from provisa.federation.replica_runner import BuildOutcome, ReplicaRunner
from provisa.federation.replica_state import RetryPolicy
from provisa.federation.scheduled_refresh import (
    next_window_open,
    parse_off_peak_window,
    window_open,
)

# PostgreSQL advisory locks are held server-wide, not per database, and every xdist worker's
# control plane is a database on one server: each worker process names an org of its own, or a
# replica lock another worker's test holds reads here as another build's.
ORG = f"org1{uuid.uuid4().hex[:8]}"

# PostgreSQL advisory locks are held server-wide, not per database: every xdist worker's control
# plane shares one server, so each worker process names an engine of its own, or a slot another
# worker's test holds reads here as taken.
ENGINE = f"trino@engine-{uuid.uuid4().hex[:8]}:8080"

ORDERS = ("shop", "public", "orders")


def _at(text: str) -> float:
    return datetime.fromisoformat(text).timestamp()


# -- when the window next opens -------------------------------------------------------------------


def test_the_next_opening_is_today_before_the_start_and_tomorrow_after_it():
    window = parse_off_peak_window("22:00-04:00", "UTC")
    assert next_window_open(window, _at("2026-10-14T12:00:00+00:00")) == _at(
        "2026-10-14T22:00:00+00:00"
    )
    # Inside the window: its next opening is the next day's.
    assert next_window_open(window, _at("2026-10-14T23:00:00+00:00")) == _at(
        "2026-10-15T22:00:00+00:00"
    )
    assert window_open(window, _at("2026-10-15T01:00:00+00:00"))


def test_the_next_opening_keeps_its_wall_clock_time_across_a_dst_change():
    window = parse_off_peak_window("22:00-04:00", "America/New_York")
    # US clocks go back on 1 November 2026: 22:00 is 02:00 UTC before and 03:00 UTC after.
    assert next_window_open(window, _at("2026-10-31T12:00:00-04:00")) == _at(
        "2026-11-01T02:00:00+00:00"
    )
    assert next_window_open(window, _at("2026-11-01T12:00:00-05:00")) == _at(
        "2026-11-02T03:00:00+00:00"
    )


# -- the runner holds a refresh the window does not allow ----------------------------------------


def _runner(control_plane, tmp_path, *, build, deferred_until, tasks):
    locks = BuildLocks(control_plane.url)
    locks._slots = tmp_path / "slots"

    async def none(*_args):
        return None

    return ReplicaRunner(
        db=control_plane.org(),
        org_id=ORG,
        locks=locks,
        engine_key=lambda: ENGINE,
        build=build,
        source_cap=none,
        permits=None,
        next_refresh_at=none,
        store=lambda: "store-a",
        retry=lambda: RetryPolicy(60.0, 3600),
        builds_per_node=lambda: 1,
        engine_jobs=lambda: 1,
        spawn=lambda coro, name: tasks.append(asyncio.ensure_future(coro)),
        deferred_until=deferred_until,
    )


async def _drain(tasks):
    while tasks:
        await asyncio.gather(*tasks[:])
        tasks[:] = [t for t in tasks if not t.done()]


@pytest.mark.asyncio
async def test_a_refresh_the_window_does_not_allow_is_held_until_it_opens(control_plane, tmp_path):
    db = control_plane.org()
    opens = datetime(2026, 10, 15, 22, tzinfo=UTC)
    async with db.acquire() as conn:
        await build_state.request_build(conn, ORDERS, build_state.REASON_REFRESH)
    built: list = []

    async def build(key, progress):
        built.append(key)
        return BuildOutcome(rows_copied=1, method="stream_batches")

    tasks: list = []
    window: list[datetime | None] = [opens]

    async def deferred_until(key, now):
        return window[0]

    runner = _runner(
        control_plane, tmp_path, build=build, deferred_until=deferred_until, tasks=tasks
    )
    assert await runner.run_pass() == 0
    async with db.acquire() as conn:
        held = await build_state.read(conn, ORDERS)
    assert held is not None
    assert (held.build_state, held.next_refresh_at, held.waiting_on) == (
        build_state.IDLE,
        opens,
        WAITING_WINDOW,
    )
    assert built == []

    # The window opens: the held refresh is due, and runs.
    window[0] = None
    async with db.acquire() as conn:
        await build_state.request_build(conn, ORDERS, build_state.REASON_REFRESH)
    assert await runner.run_pass() == 1
    await _drain(tasks)
    assert built == [ORDERS]


# -- what the policy says --------------------------------------------------------------------------


def _model(*, load_protected=True, window="22:00-04:00", cache_ttl=None):
    source = SimpleNamespace(
        id="shop",
        load_protected=load_protected,
        off_peak_window=window,
        off_peak_tz="UTC",
        cache_ttl=None,
        change_signal="ttl",
    )
    table = SimpleNamespace(
        schema_name="public",
        table_name="orders",
        load_protected=None,
        off_peak_window=None,
        off_peak_tz=None,
        cache_ttl=cache_ttl,
        change_signal=None,
        live=None,
    )
    return source, table


@pytest.fixture
def model(monkeypatch):
    held: dict = {"model": _model(), "record": None}

    async def _row(state, key):
        source, table = held["model"]
        return source, table, [source]

    async def _probe(state, key, source, table):
        return False

    class _Conn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    async def _read(conn, key):
        return held["record"]

    monkeypatch.setattr(replica_builds, "_model_row", _row)
    monkeypatch.setattr(replica_builds, "_probe_decides", _probe)
    monkeypatch.setattr(replica_builds, "store_identity", lambda state: "store-a")
    monkeypatch.setattr(build_state, "read", _read)
    state = SimpleNamespace(tenant_db=SimpleNamespace(acquire=lambda: _Conn()))
    return held, state


def _stands(store: str = "store-a"):
    return SimpleNamespace(exists_in=lambda s: s == store)


@pytest.mark.asyncio
async def test_a_load_protected_refresh_waits_for_its_window_and_a_first_build_does_not(model):
    held, state = model
    until = replica_builds._deferred_until(state)
    noon = datetime(2026, 10, 14, 12, tzinfo=UTC)
    night = datetime(2026, 10, 14, 23, tzinfo=UTC)
    assert await until(("shop", "public", "orders"), noon) is None  # never built here
    held["record"] = _stands()
    assert await until(("shop", "public", "orders"), noon) == datetime(2026, 10, 14, 22, tzinfo=UTC)
    assert await until(("shop", "public", "orders"), night) is None  # the window is open
    held["record"] = _stands("store-b")  # its replica stands in another store
    assert await until(("shop", "public", "orders"), noon) is None


@pytest.mark.asyncio
async def test_a_table_not_load_protected_is_never_held(model):
    held, state = model
    held["model"] = _model(load_protected=False)
    held["record"] = _stands()
    until = replica_builds._deferred_until(state)
    assert await until(("shop", "public", "orders"), datetime(2026, 10, 14, 12, tzinfo=UTC)) is None


@pytest.mark.asyncio
async def test_a_window_only_table_is_due_once_per_window(model):
    held, state = model
    due = replica_builds._next_refresh_at(state)
    built_at = datetime(2026, 10, 14, 23, tzinfo=UTC)  # inside tonight's window
    assert await due(("shop", "public", "orders"), built_at) == datetime(
        2026, 10, 15, 22, tzinfo=UTC
    )
    held["model"] = _model(cache_ttl=600)  # a cadence is its own clock
    assert await due(("shop", "public", "orders"), built_at) == built_at + timedelta(seconds=600)


@pytest.mark.asyncio
async def test_a_table_that_never_expires_is_never_due_again(model):
    """REQ-2031: a cache_ttl of -1 lands once; its replica is never due on any clock."""
    held, state = model
    held["model"] = _model(cache_ttl=-1)
    due = replica_builds._next_refresh_at(state)
    assert await due(("shop", "public", "orders"), datetime(2026, 10, 14, 23, tzinfo=UTC)) is None
