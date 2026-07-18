# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4e3b90-9c44-4018-9e05-5f6a1a4f1ca6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1141: the out-of-band scheduler driver — window/cadence pre-gate, probe short-circuit."""

from __future__ import annotations

import pytest

from provisa.federation.scheduled_refresh import OffPeakWindow, RefreshPolicy
from provisa.federation.scheduler import ScheduledEntry, refresh_due_entries


def _policy(*, window=None, cadence=None, probe_capable=False) -> RefreshPolicy:
    return RefreshPolicy(
        load_protected=True, window=window, cadence=cadence, probe_capable=probe_capable
    )


async def _run(entries, *, now, probe=None, land=None):
    landed_keys: list[str] = []

    async def _land(entry):
        landed_keys.append(entry.key)
        return now

    async def _probe(entry):
        return probe(entry) if probe is not None else None

    outcomes = await refresh_due_entries(
        entries=lambda: entries,
        probe=_probe,
        land=land or _land,
        now=lambda: now,
    )
    return outcomes, landed_keys


@pytest.mark.asyncio
async def test_cadence_not_elapsed_skips_no_probe():
    probed = []
    e = ScheduledEntry("a", _policy(cadence=3600, probe_capable=True), last_refresh_at=100.0, stored_token="t")
    outcomes, landed = await _run([e], now=200.0, probe=lambda x: probed.append(x) or "t2")
    assert outcomes[0].action == "skipped"
    assert landed == [] and probed == []  # too soon: never probed, never landed


@pytest.mark.asyncio
async def test_cadence_elapsed_no_probe_lands():
    e = ScheduledEntry("a", _policy(cadence=60), last_refresh_at=100.0, stored_token=None)
    outcomes, landed = await _run([e], now=1000.0)
    assert outcomes[0].action == "landed" and landed == ["a"]
    assert outcomes[0].new_refresh_at == 1000.0


@pytest.mark.asyncio
async def test_probe_unchanged_resets_clock_no_land():
    e = ScheduledEntry("a", _policy(cadence=60, probe_capable=True), last_refresh_at=100.0, stored_token="tok")
    outcomes, landed = await _run([e], now=1000.0, probe=lambda x: "tok")
    assert outcomes[0].action == "unchanged"
    assert landed == []  # unchanged → zero data pull
    assert outcomes[0].new_refresh_at == 1000.0  # clock reset so cadence measures from the probe
    assert outcomes[0].new_token == "tok"


@pytest.mark.asyncio
async def test_probe_changed_lands():
    e = ScheduledEntry("a", _policy(cadence=60, probe_capable=True), last_refresh_at=100.0, stored_token="old")
    outcomes, landed = await _run([e], now=1000.0, probe=lambda x: "new")
    assert outcomes[0].action == "landed" and landed == ["a"]
    assert outcomes[0].new_token == "new"


@pytest.mark.asyncio
async def test_off_window_skips_before_probe():
    w = OffPeakWindow(60, 120, "UTC")  # 01:00-02:00 UTC
    probed = []
    # now = 2026-07-18 12:00 UTC (out of window)
    import datetime
    from zoneinfo import ZoneInfo

    noon = datetime.datetime(2026, 7, 18, 12, tzinfo=ZoneInfo("UTC")).timestamp()
    e = ScheduledEntry("a", _policy(window=w, probe_capable=True), last_refresh_at=None, stored_token=None)
    outcomes, landed = await _run([e], now=noon, probe=lambda x: probed.append(x) or "t")
    assert outcomes[0].action == "skipped"
    assert probed == [] and landed == []  # off-window: probe never runs (source untouched)
