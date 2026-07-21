# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1f8c53-2d47-4e19-b83a-0f5c9d2e7b41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit: the bitemporal event-loop generate handler + grain-spec parsing (REQ-1166/1167/1168).

The handler is the runtime binding — per fire it appends a version stamped by the calendar
``window.end`` (deterministic, as-of-addressable) when periodic, or wall-clock when live. Here the
``append`` collaborator is a recorder so we assert exactly which system-time stamp the handler picks.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from provisa.events.calendars import Grain, NthWeekday, parse_grain_spec
from provisa.events.handlers import make_mv_bitemporal_generate

UTC = timezone.utc


@pytest.mark.asyncio
async def test_periodic_fire_stamps_window_end():
    """A fire carrying a calendar window seals at window.end — a DETERMINISTIC, addressable stamp."""
    seen: list[str | None] = []

    async def _append(system_ts):
        seen.append(system_ts)

    gen = make_mv_bitemporal_generate(_append)
    ctx = SimpleNamespace(
        window=(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)),
        window_id="2026-01",
    )
    result = await gen([], prior_hash=None, ctx=ctx, forced=False)
    assert seen == ["TIMESTAMP '2026-02-01 00:00:00.000000'"]  # window.end, not wall-clock
    assert result is not None
    event_type, payload, content_hash = result
    assert event_type == "replace"  # ripples downstream (append is always a new version)
    assert payload["window_id"] == "2026-01" and payload["sealed"] is True
    assert content_hash is None  # never gated by the REQ-981 output hash


@pytest.mark.asyncio
async def test_live_fire_stamps_wall_clock_none():
    """A fire with no window (a live bitemporal MV) passes system_ts=None → wall-clock stamp."""
    seen: list[str | None] = []

    async def _append(system_ts):
        seen.append(system_ts)

    gen = make_mv_bitemporal_generate(_append)
    ctx = SimpleNamespace(window=None, window_id=None)
    await gen([], prior_hash=None, ctx=ctx, forced=False)
    assert seen == [None]


@pytest.mark.asyncio
async def test_no_ctx_is_wall_clock():
    seen: list[str | None] = []

    async def _append(system_ts):
        seen.append(system_ts)

    await make_mv_bitemporal_generate(_append)([], prior_hash=None, ctx=None)
    assert seen == [None]


def test_parse_grain_spec_nesting_and_recurrence():
    assert parse_grain_spec("daily") is Grain.DAILY
    assert parse_grain_spec("quarterly") is Grain.QUARTERLY
    assert parse_grain_spec("3WE") == NthWeekday(weekday=2, n=3)  # 3rd Wednesday
    assert parse_grain_spec("1MO") == NthWeekday(weekday=0, n=1)
    assert parse_grain_spec("LFR") == NthWeekday(weekday=4, n=-1)  # last Friday
    assert parse_grain_spec(Grain.WEEKLY) is Grain.WEEKLY  # idempotent on a Grain
    assert parse_grain_spec(NthWeekday(weekday=1, n=2)) == NthWeekday(weekday=1, n=2)


def test_parse_grain_spec_fails_loud():
    with pytest.raises(ValueError, match="unknown grain"):
        parse_grain_spec("hourly")
    with pytest.raises(ValueError):
        parse_grain_spec("6WE")  # no 6th weekday ordinal → not a recurrence, not a grain
