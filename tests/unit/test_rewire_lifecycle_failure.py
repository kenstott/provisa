# Copyright (c) 2026 Kenneth Stott
# Canary: 781f7f82-b7e6-45ef-bf5e-78e9f44899da
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1865, REQ-1745, REQ-1770: a schema rebuild's lifecycle wiring that fails is the rebuild's
failure.

Each step was logged and the rebuild carried on, so a deadlock in its read of the model left the
org's push listeners, ingest engines, poll jobs or row-materialize drain unwired, and nothing
failed (J710). Inline at boot the failure fails the rebuild; detached, spawn_background reports it
by the task's name."""

# Requirements: REQ-1865, REQ-1745, REQ-1770

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api import app
from provisa.core.runtime_gone import RuntimeNotBuilt
from provisa.events import app_wiring, push_wiring, row_materialize_lifecycle


class _Deadlock(Exception):
    pass


async def _nothing(*args, **kwargs):
    return None


async def _deadlocked(*args, **kwargs):
    raise _Deadlock("deadlock detected")


_STEPS = {
    "push listeners": (push_wiring, "wire_push_listeners"),
    "ingest engines": (app, "_init_ingest_engines"),
    "poll jobs": (app_wiring, "wire_new_poll_jobs"),
    "row materialize": (row_materialize_lifecycle, "wire_row_materialize_background"),
}


@pytest.fixture
def wiring(monkeypatch):
    """Every wiring step succeeds, on a runtime with a scheduler and row-materialize config."""
    for module, name in _STEPS.values():
        monkeypatch.setattr(module, name, _nothing)
    rm = SimpleNamespace(
        refresh_tick_seconds=5,
        reap_interval_seconds=60,
        reap_grace_period=60.0,
        reap_batch_size=100,
    )
    monkeypatch.setattr(
        app,
        "state",
        SimpleNamespace(_scheduler=object(), config=SimpleNamespace(row_materialize=rm)),
    )
    return monkeypatch


@pytest.mark.asyncio
@pytest.mark.parametrize("step", list(_STEPS))
async def test_a_wiring_step_failure_is_raised(wiring, step):
    module, name = _STEPS[step]
    wiring.setattr(module, name, _deadlocked)
    with pytest.raises(_Deadlock, match="deadlock detected"):
        await app._rewire_lifecycle()


@pytest.mark.asyncio
async def test_poll_jobs_for_a_dropped_runtime_are_left_to_the_next_one(wiring):
    """The runtime the rebuild is for was dropped while it ran: the one built in its place wires
    its own poll jobs, and the rebuild's remaining wiring still runs."""
    ran: list[str] = []

    async def _gone(*args, **kwargs):
        raise RuntimeNotBuilt("acme", None, "dropped")

    async def _row_materialize(*args, **kwargs):
        ran.append("row materialize")

    wiring.setattr(app_wiring, "wire_new_poll_jobs", _gone)
    wiring.setattr(row_materialize_lifecycle, "wire_row_materialize_background", _row_materialize)
    await app._rewire_lifecycle()
    assert ran == ["row materialize"]
