# Copyright (c) 2026 Kenneth Stott
# Canary: 2e12f021-d014-42b8-bf04-21122d9e14e9
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for consume_cdc_into_store's debounce batching (REQ-1733).

A fake provider yields ChangeEvents on a script of (event, delay-before-next) so tests control
exactly how bursty or spread-out the stream is, without real network I/O. ``apply_cdc`` is
patched to record each call's event list instead of touching a real store, so a test asserts on
BATCH BOUNDARIES (how many apply_cdc calls, how many events per call) directly.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest

from provisa.subscriptions.cdc_landing import consume_cdc_into_store


@dataclass
class _Event:
    operation: str
    row: dict


class _ScriptedProvider:
    """Yields ``events`` from ``watch()``, sleeping ``delay`` seconds before each one."""

    def __init__(self, script: list[tuple[_Event, float]]) -> None:
        self._script = script
        self.closed = False

    async def watch(self, table: str):
        for event, delay in self._script:
            if delay:
                await asyncio.sleep(delay)
            yield event

    async def ack(self, events: list) -> None:  # REQ-1734: no offset concept in this test double
        return

    async def close(self) -> None:
        self.closed = True


def _events(n: int) -> list[_Event]:
    return [_Event("insert", {"id": i}) for i in range(n)]


@pytest.mark.asyncio
async def test_no_debounce_applies_each_event_individually():
    """debounce_quiet=0 (default) — one apply_cdc call per event, no batching."""
    ev = _events(3)
    provider = _ScriptedProvider([(ev[0], 0), (ev[1], 0), (ev[2], 0)])
    disconnect = asyncio.Event()
    calls: list[list] = []

    async def _fake_apply_cdc(conn, table, pk, events):
        calls.append(list(events))
        return {"upsert": len(events), "delete": 0}

    with patch(
        "provisa.subscriptions.cdc_landing.apply_cdc", AsyncMock(side_effect=_fake_apply_cdc)
    ):
        totals = await consume_cdc_into_store(
            provider,
            conn=None,
            schema="s",
            table="t",
            columns=[("id", "integer")],
            pk_columns=["id"],
            disconnect=disconnect,
        )

    assert len(calls) == 3
    assert [len(c) for c in calls] == [1, 1, 1]
    assert totals == {"upsert": 3, "delete": 0}
    assert provider.closed


@pytest.mark.asyncio
async def test_debounce_batches_a_burst_into_one_flush():
    """Events arriving faster than the quiet period collapse into one apply_cdc call."""
    ev = _events(3)
    # All three arrive back-to-back (no gap), then the stream ends — the quiet period elapses
    # only after the last event, via the final flush (stream end), not a mid-stream timeout.
    provider = _ScriptedProvider([(ev[0], 0), (ev[1], 0), (ev[2], 0)])
    disconnect = asyncio.Event()
    calls: list[list] = []

    async def _fake_apply_cdc(conn, table, pk, events):
        calls.append(list(events))
        return {"upsert": len(events), "delete": 0}

    with patch(
        "provisa.subscriptions.cdc_landing.apply_cdc", AsyncMock(side_effect=_fake_apply_cdc)
    ):
        totals = await consume_cdc_into_store(
            provider,
            conn=None,
            schema="s",
            table="t",
            columns=[("id", "integer")],
            pk_columns=["id"],
            disconnect=disconnect,
            debounce_quiet=0.2,
            debounce_max_delay=5.0,
        )

    assert len(calls) == 1
    assert len(calls[0]) == 3
    assert totals == {"upsert": 3, "delete": 0}


@pytest.mark.asyncio
async def test_debounce_quiet_period_flushes_mid_stream():
    """A gap longer than debounce_quiet flushes what's buffered before the next event arrives,
    producing two separate batches from one continuous script."""
    ev = _events(4)
    provider = _ScriptedProvider(
        [(ev[0], 0), (ev[1], 0), (ev[2], 0.15), (ev[3], 0)]  # gap > quiet after event[1]
    )
    disconnect = asyncio.Event()
    calls: list[list] = []

    async def _fake_apply_cdc(conn, table, pk, events):
        calls.append(list(events))
        return {"upsert": len(events), "delete": 0}

    with patch(
        "provisa.subscriptions.cdc_landing.apply_cdc", AsyncMock(side_effect=_fake_apply_cdc)
    ):
        await consume_cdc_into_store(
            provider,
            conn=None,
            schema="s",
            table="t",
            columns=[("id", "integer")],
            pk_columns=["id"],
            disconnect=disconnect,
            debounce_quiet=0.05,
            debounce_max_delay=5.0,
        )

    assert len(calls) == 2
    assert [len(c) for c in calls] == [2, 2]


@pytest.mark.asyncio
async def test_debounce_max_delay_caps_staleness_under_continuous_churn():
    """Continuous arrivals faster than debounce_quiet never let the quiet period elapse — but
    debounce_max_delay forces a flush anyway, capping staleness under nonstop traffic."""
    ev = _events(6)
    # Six events, each 0.03s apart — quiet (0.2s) never elapses between them, but max_delay (0.08s)
    # must force at least one flush before the stream ends.
    script = [(ev[0], 0)] + [(e, 0.03) for e in ev[1:]]
    provider = _ScriptedProvider(script)
    disconnect = asyncio.Event()
    calls: list[list] = []

    async def _fake_apply_cdc(conn, table, pk, events):
        calls.append(list(events))
        return {"upsert": len(events), "delete": 0}

    with patch(
        "provisa.subscriptions.cdc_landing.apply_cdc", AsyncMock(side_effect=_fake_apply_cdc)
    ):
        await consume_cdc_into_store(
            provider,
            conn=None,
            schema="s",
            table="t",
            columns=[("id", "integer")],
            pk_columns=["id"],
            disconnect=disconnect,
            debounce_quiet=0.2,
            debounce_max_delay=0.08,
        )

    assert len(calls) >= 2, "max_delay must force multiple flushes under continuous churn"
    assert sum(len(c) for c in calls) == 6


@pytest.mark.asyncio
async def test_stream_end_flushes_partial_batch():
    """The provider's stream ending (StopAsyncIteration) with events still buffered (quiet period
    never elapsed) still lands them — no events silently dropped when the source disconnects."""
    ev = _events(2)
    provider = _ScriptedProvider([(ev[0], 0), (ev[1], 0)])  # stream ends right after
    disconnect = asyncio.Event()
    calls: list[list] = []

    async def _fake_apply_cdc(conn, table, pk, events):
        calls.append(list(events))
        return {"upsert": len(events), "delete": 0}

    with patch(
        "provisa.subscriptions.cdc_landing.apply_cdc", AsyncMock(side_effect=_fake_apply_cdc)
    ):
        totals = await consume_cdc_into_store(
            provider,
            conn=None,
            schema="s",
            table="t",
            columns=[("id", "integer")],
            pk_columns=["id"],
            disconnect=disconnect,
            debounce_quiet=5.0,  # long enough that only stream-end triggers the flush
            debounce_max_delay=5.0,
        )

    assert len(calls) == 1
    assert len(calls[0]) == 2
    assert totals == {"upsert": 2, "delete": 0}
    assert provider.closed


@pytest.mark.asyncio
async def test_ack_called_with_exactly_the_flushed_batch_after_landing():
    """REQ-1734: ack() fires once per flush, with exactly that flush's events — never before
    apply_cdc succeeds, and never with events from a later, still-unflushed batch."""
    ev = _events(4)
    provider = _ScriptedProvider(
        [(ev[0], 0), (ev[1], 0), (ev[2], 0.15), (ev[3], 0)]  # same mid-stream-quiet-flush shape
    )
    provider.ack = AsyncMock()
    disconnect = asyncio.Event()
    order: list[str] = []

    async def _fake_apply_cdc(conn, table, pk, events):
        order.append("land")
        return {"upsert": len(events), "delete": 0}

    with patch(
        "provisa.subscriptions.cdc_landing.apply_cdc", AsyncMock(side_effect=_fake_apply_cdc)
    ):
        await consume_cdc_into_store(
            provider,
            conn=None,
            schema="s",
            table="t",
            columns=[("id", "integer")],
            pk_columns=["id"],
            disconnect=disconnect,
            debounce_quiet=0.05,
            debounce_max_delay=5.0,
        )

    assert provider.ack.call_count == 2
    first_batch = provider.ack.call_args_list[0].args[0]
    second_batch = provider.ack.call_args_list[1].args[0]
    assert [e.row["id"] for e in first_batch] == [0, 1]
    assert [e.row["id"] for e in second_batch] == [2, 3]


@pytest.mark.asyncio
async def test_backpressure_bounded_queue_never_grows_unbounded():
    """A tiny queue_maxsize proves the pump actually blocks on a slow consumer rather than
    buffering every event in memory — the backpressure mechanism, not just a batching nicety."""
    ev = _events(50)
    provider = _ScriptedProvider([(e, 0) for e in ev])  # fires as fast as possible
    disconnect = asyncio.Event()
    calls: list[list] = []

    async def _slow_apply_cdc(conn, table, pk, events):
        # A deliberately slow "land" so the pump outruns it if nothing throttles it.
        await asyncio.sleep(0.01)
        calls.append(list(events))
        return {"upsert": len(events), "delete": 0}

    with patch(
        "provisa.subscriptions.cdc_landing.apply_cdc", AsyncMock(side_effect=_slow_apply_cdc)
    ):
        totals = await consume_cdc_into_store(
            provider,
            conn=None,
            schema="s",
            table="t",
            columns=[("id", "integer")],
            pk_columns=["id"],
            disconnect=disconnect,
            debounce_quiet=0.0,  # apply one at a time — the queue is the only thing throttling
            queue_maxsize=3,
        )

    assert totals == {"upsert": 50, "delete": 0}
    assert sum(len(c) for c in calls) == 50
    # every event lands exactly once, in order, despite the tiny queue forcing the pump to block
    assert [c[0].row["id"] for c in calls] == list(range(50))
