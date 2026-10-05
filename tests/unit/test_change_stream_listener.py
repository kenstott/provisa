# Copyright (c) 2026 Kenneth Stott
# Canary: 28291b85-1187-4602-9c3c-f69821bd05c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1861: a MongoDB table that opts into its source's change feed (change signal ``native``)
has its replica rebuilt when the collection changes.

The listener carries no rows. Each burst of changes asks for one build, through the request a
write through Provisa also makes; the replicator is the replica's only writer. Before this, a
``native`` MongoDB table served from a replica was built once and never refreshed: residency
treats ``native`` as kept current by a listener, and there was none."""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from provisa.events import push_wiring
from provisa.events.push_wiring import follow_changes


class _Stream:
    """A change stream whose changes arrive at given clock times; ``try_next`` returns None
    (nothing within the wait) until the next one is due."""

    def __init__(self, clock: list[float], arrivals: list[float], stop: threading.Event) -> None:
        self.clock, self.arrivals, self.stop = clock, list(arrivals), stop
        self.alive = True

    async def try_next(self):
        self.clock[0] += 0.5  # each wait is half a second
        if self.arrivals and self.arrivals[0] <= self.clock[0]:
            return {"at": self.arrivals.pop(0)}
        if not self.arrivals and self.clock[0] > 30:
            self.stop.set()
        return None


async def _asked_at(arrivals: list[float], *, quiet: float, max_delay: float) -> list[float]:
    clock = [0.0]
    stop = threading.Event()
    asked: list[float] = []

    async def on_change() -> None:
        asked.append(clock[0])

    await follow_changes(
        _Stream(clock, arrivals, stop),
        on_change,
        stop,
        quiet=quiet,
        max_delay=max_delay,
        clock=lambda: clock[0],
    )
    return asked


async def test_one_change_asks_for_one_build():
    assert await _asked_at([1.0], quiet=0.0, max_delay=5.0) == [1.0]


async def test_a_burst_of_changes_asks_once_when_it_goes_quiet():
    assert await _asked_at([1.0, 1.5, 2.0], quiet=2.0, max_delay=60.0) == [4.0]


async def test_a_collection_that_never_goes_quiet_still_asks_every_max_delay():
    arrivals = [1.0 + 0.5 * i for i in range(20)]  # a change every wait, for ten seconds
    asked = await _asked_at(arrivals, quiet=2.0, max_delay=4.0)
    assert asked[:2] == [5.0, 9.5]  # four seconds after the first change of each burst


async def test_nothing_is_asked_while_nothing_changes():
    assert await _asked_at([], quiet=0.0, max_delay=5.0) == []


async def test_a_stream_the_server_closed_is_watched_again():
    class _Closed:
        alive = False

        async def try_next(self):
            return None

    with pytest.raises(ConnectionError, match="closed by the server"):
        await follow_changes(_Closed(), None, threading.Event(), quiet=0.0, max_delay=5.0)


# -- which tables get a listener -------------------------------------------------------------------


def _wired(monkeypatch, *, table_signal, source_signal):
    started: list[str] = []

    def _spawn(coro, *, name):
        coro.close()
        started.append(name)
        return SimpleNamespace(name=name)

    monkeypatch.setattr(push_wiring, "spawn_long_lived", _spawn)
    state = SimpleNamespace(
        push_listener_disconnects={}, push_listener_tasks=[], push_listener_handles={}
    )
    src = SimpleNamespace(id="shop", database="shop", change_signal=source_signal)
    tbl = {
        "id": 7,
        "source_id": "shop",
        "schema_name": "shop",
        "table_name": "orders",
        "change_signal": table_signal,
    }
    log = SimpleNamespace(info=lambda *a, **k: None)
    first = push_wiring._start_change_stream(state, src, tbl, log=log)
    again = push_wiring._start_change_stream(state, src, tbl, log=log)
    return started, first, again


@pytest.mark.parametrize(
    ("table_signal", "source_signal", "listens"),
    [
        ("native", None, True),
        (None, "native", True),  # the table inherits its source's signal
        ("ttl", "native", False),  # the table's own signal wins
        (None, "ttl", False),
        (None, None, False),
    ],
)
def test_a_table_gets_a_listener_only_when_its_change_signal_opts_it_in(
    monkeypatch, table_signal, source_signal, listens
):
    started, first, again = _wired(
        monkeypatch, table_signal=table_signal, source_signal=source_signal
    )
    assert (first is not None) is listens
    assert again is None  # never a second listener for the same table
    assert started == (["change-stream:shop/shop.orders"] if listens else [])


def test_mongodb_is_a_change_feed_source_and_not_a_row_carrying_one():
    assert "mongodb" in push_wiring._CHANGE_STREAM_SOURCE_TYPES
    assert "mongodb" not in push_wiring._LISTENER_SOURCE_TYPES
