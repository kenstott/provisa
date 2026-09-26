# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: per-key row_lock registry — disjoint keys never contend, same key serializes and
re-checks, and the registry is bounded (evicted once every holder/waiter has drained)."""

from __future__ import annotations

import asyncio

import pytest

from provisa.events.row_lock import _registry_size, row_lock


@pytest.mark.asyncio
async def test_lock_evicted_after_release():
    assert _registry_size() == 0
    async with row_lock("mat.orders", (1,)):
        assert _registry_size() == 1
    assert _registry_size() == 0


@pytest.mark.asyncio
async def test_disjoint_keys_never_contend():
    order = []

    async def hold(key, delay):
        async with row_lock("mat.orders", key):
            order.append(("enter", key))
            await asyncio.sleep(delay)
            order.append(("exit", key))

    await asyncio.gather(hold((1,), 0.02), hold((2,), 0.01))
    # Both entered before either exited — disjoint keys ran concurrently, not serialized.
    assert order.index(("enter", (1,))) < order.index(("exit", (2,)))
    assert order.index(("enter", (2,))) < order.index(("exit", (1,)))


@pytest.mark.asyncio
async def test_same_key_serializes():
    events: list[str] = []

    async def hold(tag, delay):
        async with row_lock("mat.orders", (1,)):
            events.append(f"{tag}-enter")
            await asyncio.sleep(delay)
            events.append(f"{tag}-exit")

    await asyncio.gather(hold("a", 0.02), hold("b", 0.01))
    # The second holder's enter never happens before the first's exit — same key, serialized.
    first_exit = min(i for i, e in enumerate(events) if e.endswith("exit"))
    second_enter = max(i for i, e in enumerate(events) if e.endswith("enter"))
    assert second_enter > first_exit


@pytest.mark.asyncio
async def test_registry_bounded_under_many_distinct_keys():
    """The registry never grows past the number of keys CURRENTLY contended, regardless of how
    many distinct keys were ever requested (the design doc's own flagged gap, closed here)."""
    for i in range(500):
        async with row_lock("mat.orders", (i,)):
            pass
    assert _registry_size() == 0
