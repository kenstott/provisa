# Copyright (c) 2026 Kenneth Stott
# Canary: 9c8b7a6d-5e4f-3021-b0a9-8c7d6e5f4a3b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1266: per-org materialized-view background refresh on the embedded scheduler.

Two invariants keep a second org's MV wiring from corrupting the first's:
  1. every APScheduler job id is org-namespaced (``:org_<id>``), so a second org's
     ``register_runtime``/``register_poll_job`` never clobbers the first under
     ``replace_existing``;
  2. each job fire BINDS the org's ``current_org`` ContextVar for its duration (APScheduler
     fires with the ContextVar unset), so any routed ``state.X`` read inside a processor
     resolves that org's runtime — not the default's.
The default/single-org path (``org_id=None``) keeps bare ids and binds nothing."""

from __future__ import annotations

from typing import Any

import pytest

from provisa.api.org_runtime import current_org
from provisa.events import supervisor
from provisa.events.boot import register_runtime
from provisa.events.processor import TableProcessor


class _FakeScheduler:
    def __init__(self) -> None:
        self.jobs: dict[str, Any] = {}

    def add_job(self, func, *_args, id: str, **_kwargs) -> None:
        self.jobs[id] = func


def test_register_runtime_namespaces_job_ids_per_org():
    sched = _FakeScheduler()
    register_runtime(sched, db=None, processors=[], specs=[], org_id="acme")
    assert set(sched.jobs) == {
        "events:boot:org_acme",
        "events:tick:org_acme",
        "events:reaper:org_acme",
    }


def test_register_runtime_default_org_keeps_bare_ids():
    sched = _FakeScheduler()
    register_runtime(sched, db=None, processors=[], specs=[], org_id=None)
    assert set(sched.jobs) == {"events:boot", "events:tick", "events:reaper"}


def test_two_orgs_do_not_clobber_each_other():
    sched = _FakeScheduler()
    register_runtime(sched, db=None, processors=[], specs=[], org_id="acme")
    register_runtime(sched, db=None, processors=[], specs=[], org_id="beta")
    assert "events:tick:org_acme" in sched.jobs
    assert "events:tick:org_beta" in sched.jobs
    assert len(sched.jobs) == 6


@pytest.mark.asyncio
async def test_tick_job_binds_current_org_for_the_fire(monkeypatch):
    seen: list[str | None] = []

    async def _capture_tick(_db, _processors):
        seen.append(current_org.get())

    monkeypatch.setattr(supervisor, "tick", _capture_tick)

    sched = _FakeScheduler()
    register_runtime(sched, db=None, processors=[], specs=[], org_id="acme", seed=False)
    assert current_org.get() is None  # unbound before the fire (as APScheduler would fire it)
    await sched.jobs["events:tick:org_acme"]()
    assert seen == ["acme"]
    assert current_org.get() is None  # reset in finally


@pytest.mark.asyncio
async def test_default_org_tick_binds_nothing(monkeypatch):
    seen: list[str | None] = []

    async def _capture_tick(_db, _processors):
        seen.append(current_org.get())

    monkeypatch.setattr(supervisor, "tick", _capture_tick)

    sched = _FakeScheduler()
    register_runtime(sched, db=None, processors=[], specs=[], org_id=None, seed=False)
    await sched.jobs["events:tick"]()
    assert seen == [None]


class _FakeProc:
    node = "sales.orders"

    def __init__(self) -> None:
        self.bound_during_inject: str | None = None

    async def inject(self, _probe) -> None:
        self.bound_during_inject = current_org.get()


@pytest.mark.asyncio
async def test_poll_job_namespaces_id_and_binds_org():
    proc = _FakeProc()
    sched = _FakeScheduler()
    # register_poll_job only touches self.node / self.inject — call it unbound on the fake.
    TableProcessor.register_poll_job(
        proc, sched, seconds=5, probe_factory=lambda: None, org_id="acme"
    )
    assert "poll:sales.orders:org_acme" in sched.jobs
    await sched.jobs["poll:sales.orders:org_acme"]()
    assert proc.bound_during_inject == "acme"
    assert current_org.get() is None


@pytest.mark.asyncio
async def test_poll_job_default_org_bare_id_no_bind():
    proc = _FakeProc()
    sched = _FakeScheduler()
    TableProcessor.register_poll_job(
        proc, sched, seconds=5, probe_factory=lambda: None, org_id=None
    )
    assert "poll:sales.orders" in sched.jobs
    await sched.jobs["poll:sales.orders"]()
    assert proc.bound_during_inject is None
