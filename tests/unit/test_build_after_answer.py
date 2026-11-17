# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7d3e52-6c19-4a8f-9e24-3f5a1d8c7b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A build answers once its model is published; what nobody waits on runs after the answer, in
the build's after-answer pass (REQ-1914, REQ-2129; provisa.api.build_after_answer).

Each build here is a request's: the real entry (``app._rebuild_schemas``) on a connection loop,
the model build stood in for (``model_builds``), so what ran after the answer and for which build
stamp is counted, never timed.
"""

# Requirements: REQ-1914, REQ-2129

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, cast

import pytest

from provisa.api import rebuild_trace
from provisa.api.org_runtime import OrgRuntime
from provisa.core import config_stamp
from provisa.core.connection_loop import run_on_own_thread

if TYPE_CHECKING:
    from provisa.core.database import Database

_ALL = list(rebuild_trace.AFTER_PHASES)


@pytest.fixture
def stored(monkeypatch) -> dict[str, int]:
    """The stamps the control plane holds, read by each build."""
    held = {config_stamp.MODEL: 9, config_stamp.BUILD: 4}

    async def _read(db):
        del db
        return dict(held)

    monkeypatch.setattr(config_stamp, "read", _read)
    return held


@pytest.fixture
def runtime(monkeypatch, stored):
    from provisa.api.app import AppState

    del stored
    state = AppState()
    state.org_id = "default"
    rt = OrgRuntime(org_id="default")
    rt.model_db = cast("Database", object())  # read only through the stood-in stamp read
    state.org_registry.set("default", rt)
    monkeypatch.setattr("provisa.api.app.state", state)
    return rt


async def _request_build() -> None:
    """A build a request asks for: on a connection loop, as every request runs. It returns when
    the request has its answer."""
    from provisa.api.app import _rebuild_schemas

    await asyncio.wrap_future(
        run_on_own_thread(lambda: _rebuild_schemas(cause="a test"), name="a request's build")
    )


class _Gate:
    """Holds the first pass to reach ``phase`` there until opened; later passes go through."""

    def __init__(self) -> None:
        self._open = threading.Event()
        self.reached = threading.Event()
        self._used = False

    async def __call__(self) -> None:
        if self._used:
            return
        self._used = True
        self.reached.set()
        await asyncio.to_thread(self._open.wait, 30)

    def wait_reached(self) -> None:
        assert self.reached.wait(30), "the pass never reached the gate"

    def open(self) -> None:
        self._open.set()


def _ran_for(ran: list[tuple[str, int]], stamp: int) -> list[str]:
    return [phase for phase, at in ran if at == stamp]


@pytest.mark.usefixtures("runtime")
async def test_the_four_phases_and_the_warm_up_run_after_the_answer_in_order(model_builds):
    gate = _Gate()
    model_builds.after.before[rebuild_trace.DERIVED_WRITES] = gate

    await _request_build()
    await asyncio.to_thread(gate.wait_reached)

    # The request has its answer while the pass is held at its first phase: nothing of it ran.
    (build,) = model_builds.builds
    assert build.after is not None
    assert build.after.outcome == rebuild_trace.PENDING
    assert model_builds.after.ran == []

    gate.open()
    await asyncio.to_thread(model_builds.after.settle)

    assert model_builds.after.ran == [(phase, 4) for phase in _ALL]
    assert _ALL == [
        rebuild_trace.DERIVED_WRITES,
        rebuild_trace.NAMES_TABLE,
        rebuild_trace.ANNOUNCE,
        rebuild_trace.LIFECYCLE,
        rebuild_trace.VIEW_REBUILDS,
        rebuild_trace.WARM_UP,
    ]
    assert build.after.outcome == rebuild_trace.RAN
    assert build.after.phase_names == _ALL
    assert build.after.stamps == {config_stamp.MODEL: 9, config_stamp.BUILD: 4}
    assert build.after.answered_s >= 0
    assert model_builds.passes == [build.after]


@pytest.mark.usefixtures("runtime")
async def test_two_quick_builds_give_one_pass_for_the_newer_stamp(model_builds, stored):
    gate = _Gate()
    model_builds.after.before[rebuild_trace.DERIVED_WRITES] = gate

    await _request_build()  # build 4: its pass is held at its first phase
    await asyncio.to_thread(gate.wait_reached)
    stored[config_stamp.BUILD] = 5
    await _request_build()  # build 5: published while build 4's pass runs
    stored[config_stamp.BUILD] = 6
    await _request_build()  # build 6: published before build 5's pass began
    gate.open()
    await asyncio.to_thread(model_builds.after.settle)

    first, second, third = (b.after for b in model_builds.builds)
    assert first is not None and second is not None and third is not None
    # Build 5's pass never began; build 6's ran every phase, once.
    assert (second.outcome, second.stopped_before, second.phases) == (
        rebuild_trace.SUPERSEDED,
        rebuild_trace.DERIVED_WRITES,
        [],
    )
    assert _ran_for(model_builds.after.ran, 5) == []
    assert (third.outcome, third.phase_names) == (rebuild_trace.RAN, _ALL)
    assert _ran_for(model_builds.after.ran, 6) == _ALL
    # Build 4's pass ended at the phase it was in.
    assert first.outcome == rebuild_trace.SUPERSEDED
    assert _ran_for(model_builds.after.ran, 4) == [rebuild_trace.DERIVED_WRITES]


@pytest.mark.usefixtures("runtime")
async def test_a_superseded_pass_stops_at_its_next_phase_boundary(model_builds, stored):
    gate = _Gate()
    model_builds.after.before[rebuild_trace.NAMES_TABLE] = gate

    await _request_build()  # build 4: its pass is held in the names table
    await asyncio.to_thread(gate.wait_reached)
    stored[config_stamp.BUILD] = 5
    await _request_build()
    gate.open()
    await asyncio.to_thread(model_builds.after.settle)

    first, second = (b.after for b in model_builds.builds)
    assert first is not None and second is not None
    # The phase it was in finished; it stopped before the next one.
    assert first.outcome == rebuild_trace.SUPERSEDED
    assert first.phase_names == [rebuild_trace.DERIVED_WRITES, rebuild_trace.NAMES_TABLE]
    assert first.stopped_before == rebuild_trace.ANNOUNCE
    assert _ran_for(model_builds.after.ran, 4) == [
        rebuild_trace.DERIVED_WRITES,
        rebuild_trace.NAMES_TABLE,
    ]
    assert (second.outcome, _ran_for(model_builds.after.ran, 5)) == (rebuild_trace.RAN, _ALL)


@pytest.mark.usefixtures("runtime")
async def test_a_failing_phase_is_recorded_and_the_next_pass_runs_every_phase(
    model_builds, stored, caplog
):
    failed = []

    async def fails_once() -> None:
        if not failed:
            failed.append(True)
            raise RuntimeError("the catalog would not take the notice")

    model_builds.after.before[rebuild_trace.ANNOUNCE] = fails_once

    await _request_build()
    await asyncio.to_thread(model_builds.after.settle)
    (first,) = (b.after for b in model_builds.builds)
    assert first is not None
    assert first.outcome == rebuild_trace.FAILED
    assert first.failed_in == rebuild_trace.ANNOUNCE
    assert first.error is not None and "the catalog would not take the notice" in first.error
    assert first.phase_names == [
        rebuild_trace.DERIVED_WRITES,
        rebuild_trace.NAMES_TABLE,
        rebuild_trace.ANNOUNCE,
    ]
    assert _ran_for(model_builds.after.ran, 4) == [
        rebuild_trace.DERIVED_WRITES,
        rebuild_trace.NAMES_TABLE,
    ]
    assert any(
        "after-answer pass" in r.getMessage() and "failed in announce" in r.getMessage()
        for r in caplog.records
    )

    # The next build's pass runs every phase again, the ones before the failure among them.
    stored[config_stamp.BUILD] = 5
    await _request_build()
    await asyncio.to_thread(model_builds.after.settle)
    second = model_builds.builds[1].after
    assert second is not None
    assert (second.outcome, _ran_for(model_builds.after.ran, 5)) == (rebuild_trace.RAN, _ALL)
