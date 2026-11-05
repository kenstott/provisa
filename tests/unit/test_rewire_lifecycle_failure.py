# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1865: a schema rebuild's row-materialize wiring that fails is the rebuild's failure.

It was logged and the rebuild carried on, so a deadlock in its read of the model left the org's
row-materialize tables with no refresh drain and no reaper, and nothing failed (J710)."""

# Requirements: REQ-1865

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api import app
from provisa.events import app_wiring, push_wiring, row_materialize_lifecycle


class _Deadlock(Exception):
    pass


@pytest.mark.asyncio
async def test_a_row_materialize_wiring_failure_is_raised(monkeypatch):
    async def _nothing(*args, **kwargs):
        return None

    async def _deadlocked(*args, **kwargs):
        raise _Deadlock("deadlock detected")

    monkeypatch.setattr(push_wiring, "wire_push_listeners", _nothing)
    monkeypatch.setattr(app, "_init_ingest_engines", _nothing)
    monkeypatch.setattr(app_wiring, "wire_new_poll_jobs", _nothing)
    monkeypatch.setattr(row_materialize_lifecycle, "wire_row_materialize_background", _deadlocked)
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
    with pytest.raises(_Deadlock, match="deadlock detected"):
        await app._rewire_lifecycle()
