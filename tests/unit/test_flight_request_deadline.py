# Copyright (c) 2026 Kenneth Stott
# Canary: 2f8c0b46-9d13-4e7a-b6a5-1c4e7d9f3b82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One request deadline covers the whole Flight call (REQ-1905).

A Flight ticket had no deadline at all: its wait for a stream slot was bounded by the server's
request budget, and then its execution and its stream ran for as long as they took. The wait,
the execution and the stream now share ONE deadline (``server.limits.request_timeout``), as a
request on any other transport does, and a stream that outlives it is cancelled with an error
that names it."""

# Requirements: REQ-1905

from __future__ import annotations

import json
import time

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight import deadline as flight_deadline
from provisa.api.flight import stream_slots
from provisa.api.flight.server import ProvisaFlightServer
from provisa.core import request_deadline, settings_registry


class _State:
    def __init__(self, cap: int | None, request_timeout: float) -> None:
        self.org_id = "default"
        self.roles = {"analyst": {}}
        self.rate_limiter = None
        self.flight_global_cap = cap
        _flight_timeout(request_timeout)
        self.security_high = False


@pytest.fixture(autouse=True)
def _fresh_slots(monkeypatch):
    monkeypatch.setattr(stream_slots, "_slots", None)
    monkeypatch.setattr(settings_registry, "_config", {})  # restored after the test


def _flight_timeout(seconds: float) -> None:
    """Flight's own request timeout (REQ-1905), set the way a deployment's config file sets it."""
    settings_registry.bind_config({"server": {"limits": {"request_timeouts": {"flight": seconds}}}})


def _server(state, execute) -> ProvisaFlightServer:
    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    server._state = state
    server._execute_query = execute  # type: ignore[method-assign]
    return server


def _ticket() -> flight.Ticket:
    return flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())


def _batches(n: int, each: float, clock=None):
    """``n`` batches, each taking ``each`` seconds to produce: on ``clock`` when one is given
    (the request deadline's injected clock moves, nothing sleeps), else in real time."""
    schema = pa.schema([("n", pa.int64())])
    for i in range(n):
        if clock is None:
            time.sleep(each)
        else:
            clock.advance(each)
        yield pa.record_batch([pa.array([i])], schema=schema)


# The three cases below are about ONE budget shared by a request's wait for a stream slot, its
# execution and its stream. They used to hold a 1-second budget against the real clock, with a
# second thread holding the slot and real sleeps for the work: on a loaded machine the thread
# start, the sleeps and the test's own setup spent the budget before the point being measured,
# and the cases failed for a reason that had nothing to do with the rule. The deadline's clock
# is injected (tests/deadline_clock.py) and the wait for a slot is a stand-in that takes a
# stated time on that clock, so each case is the same arithmetic on any machine.


class _SlotAfter:
    """The server's stream slots, with the one slot freeing after ``waits`` seconds for each
    successive request (on the deadline's clock): what a request finds when another holds it."""

    def __init__(self, clock, *waits: float) -> None:
        self._clock = clock
        self._waits = list(waits)
        self.asked: list[float] = []

    def acquire(self, timeout: float | None = None) -> bool:
        self.asked.append(timeout if timeout is not None else float("inf"))
        wait = self._waits.pop(0)
        if timeout is not None and wait > timeout:
            self._clock.advance(timeout)
            return False  # no slot freed within what the request had left
        self._clock.advance(wait)
        return True

    def release(self) -> None:
        return None


def _one_slot(monkeypatch, clock, *waits: float) -> _SlotAfter:
    slot = _SlotAfter(clock, *waits)
    monkeypatch.setattr(stream_slots.slots_for(1), "_free", slot)
    return slot


@pytest.fixture
def stream_clock(deadline_clock, monkeypatch):
    """The deadline's clock, with the stream's own check as the only thing that acts on it: the
    process watchdog (a separate mechanism with its own tests) raises into a request's thread
    from another thread at a moment the machine chooses, and is kept out of these cases."""
    monkeypatch.setattr(request_deadline._watchdog, "watch", lambda *args, **kwargs: None)
    return deadline_clock


def test_execution_gets_what_the_wait_left_of_the_one_deadline(stream_clock, monkeypatch):
    """A request that waited 0.6s of a 1s budget for a slot runs with 0.4s left — not a fresh
    1s."""
    seen: list[float | None] = []

    def _execute(_request):
        # What every execution path does on a cache miss: wait for a stream slot, then run.
        release_slot = server._acquire_stream_slot()
        try:
            seen.append(request_deadline.remaining())
            return "ok"
        finally:
            release_slot()

    server = _server(_State(cap=1, request_timeout=1.0), _execute)
    slot = _one_slot(monkeypatch, stream_clock, 0.0, 0.6)
    assert server.do_get(None, _ticket()) == "ok"  # the slot is free: no wait
    assert server.do_get(None, _ticket()) == "ok"  # another request holds it for 0.6s

    assert seen == [pytest.approx(1.0), pytest.approx(0.4)]
    # ... and the wait itself was bounded by the request's own budget, not a separate one.
    assert slot.asked == [pytest.approx(1.0), pytest.approx(1.0)]


def test_a_stream_that_waited_is_cut_off_at_the_deadline_not_at_wait_plus_budget(
    stream_clock, monkeypatch
):
    budget = 1.0

    def _execute(_request):
        release_slot = server._acquire_stream_slot()  # waits 0.3s for the slot
        # A lazy stream of 20 batches, 0.1s apart — 2s of work — holding its slot until the
        # stream ends.
        return flight_deadline.stream_within_deadline(
            stream_slots.SlotHeldBatches(release_slot, _batches(20, 0.1, stream_clock))
        )

    server = _server(_State(cap=1, request_timeout=budget), _execute)
    _one_slot(monkeypatch, stream_clock, 0.3)
    started = stream_clock.monotonic()
    stream = server.do_get(None, _ticket())
    assert stream_clock.monotonic() - started == pytest.approx(0.3)  # the wait, and nothing else

    got = 0
    with pytest.raises(flight.FlightServerError) as raised:
        for _batch in stream:
            got += 1
    elapsed = stream_clock.monotonic() - started

    # Cut when the ONE budget ran out, 1s after the request began: the 0.3s wait left 0.7s of
    # stream, which is 7 batches. A budget that started again after the wait would have run
    # to 1.3s and delivered 10.
    assert got == 7
    assert elapsed == pytest.approx(budget)
    message = str(raised.value)
    assert "request deadline" in message and "request_timeout" in message and "1s" in message


def test_a_request_that_finds_no_slot_within_its_budget_is_refused_at_the_deadline(
    stream_clock, monkeypatch
):
    """The wait for a slot draws on the same budget: one that would take longer than the
    request has is given up when the budget ends, naming the limit."""
    server = _server(
        _State(cap=1, request_timeout=1.0), lambda _r: server._acquire_stream_slot() and "ok"
    )
    _one_slot(monkeypatch, stream_clock, 5.0)
    started = stream_clock.monotonic()
    with pytest.raises(flight.FlightServerError):
        server.do_get(None, _ticket())
    assert stream_clock.monotonic() - started == pytest.approx(1.0)


def test_a_stream_within_its_deadline_is_unaffected():
    def _execute(_request):
        return flight_deadline.stream_within_deadline(_batches(5, 0.01))

    server = _server(_State(cap=2, request_timeout=5.0), _execute)
    stream = server.do_get(None, _ticket())
    assert [b.column(0)[0].as_py() for b in stream] == [0, 1, 2, 3, 4]
    # Nothing of the request is left bound on this thread for the next RPC to inherit.
    assert request_deadline.current() is None


def test_a_materialized_result_within_its_deadline_is_unaffected():
    server = _server(_State(cap=2, request_timeout=5.0), lambda _r: "table")
    assert server.do_get(None, _ticket()) == "table"
    assert request_deadline.current() is None


def test_execution_cancelled_at_the_deadline_names_it(stream_clock):
    def _execute(_request):
        dl = request_deadline.current()
        assert dl is not None
        stream_clock.advance(0.25)  # the statement runs past its 0.2s budget
        raise dl.expired_error()  # what a statement cancelled by the deadline's watchdog raises

    server = _server(_State(cap=2, request_timeout=0.2), _execute)
    with pytest.raises(flight.FlightServerError) as raised:
        server.do_get(None, _ticket())
    assert "request deadline" in str(raised.value) and "request_timeout" in str(raised.value)


def test_an_error_before_the_deadline_is_not_reported_as_the_deadline():
    def _execute(_request):
        raise RuntimeError("engine down")

    server = _server(_State(cap=2, request_timeout=5.0), _execute)
    with pytest.raises(RuntimeError, match="engine down"):
        server.do_get(None, _ticket())


def test_a_tighter_deadline_the_caller_bound_is_kept(deadline_clock):
    seen: list[float | None] = []
    server = _server(
        _State(cap=2, request_timeout=30.0), lambda _r: seen.append(request_deadline.remaining())
    )
    with request_deadline.within(0.5):
        server.do_get(None, _ticket())
    assert seen[0] is not None and seen[0] <= 0.5


def test_every_lazy_stream_do_get_returns_is_under_the_deadline():
    """A lazy stream leaves do_get as a GeneratorStream in two places — the SQL routes' common
    exit (engine AND direct-source streams) and the GraphQL engine stream. Both wrap."""
    import inspect

    from provisa.api.flight import server

    src = inspect.getsource(server)
    # Every stream is built through provisa.api.flight.compression (the operator's codec).
    assert "flight.GeneratorStream(" not in src
    assert src.count("generator_stream(") == 3  # the third streams a materialized table
    assert "_metered_batches(stream_within_deadline(batch_gen))" in src
    assert "generator_stream(arrow_schema, stream_within_deadline(batch_gen), plan.warnings)" in src
