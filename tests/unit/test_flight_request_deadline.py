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
import threading
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


def _batches(n: int, each: float):
    schema = pa.schema([("n", pa.int64())])
    for i in range(n):
        time.sleep(each)
        yield pa.record_batch([pa.array([i])], schema=schema)


def test_execution_gets_what_the_wait_left_of_the_one_deadline():
    """A request that waited 0.6s of a 1s budget for a slot runs with ~0.4s left — not a fresh
    1s."""
    release = threading.Event()
    seen: list[float | None] = []

    def _execute(_request):
        # What every execution path does on a cache miss: wait for a stream slot, then run.
        release_slot = server._acquire_stream_slot()
        try:
            seen.append(request_deadline.remaining())
            if len(seen) == 1:
                release.wait(timeout=0.6)  # the first request holds the slot this long
            return "ok"
        finally:
            release_slot()

    server = _server(_State(cap=1, request_timeout=1.0), _execute)
    first = threading.Thread(target=lambda: server.do_get(None, _ticket()))
    first.start()
    time.sleep(0.05)  # `first` holds the only slot for ~0.6s
    assert server.do_get(None, _ticket()) == "ok"
    first.join(timeout=10)

    waited_remaining = seen[1]
    assert seen[0] is not None and seen[0] > 0.9  # the first request: nearly its whole budget
    assert waited_remaining is not None and 0.2 < waited_remaining < 0.5


def test_a_stream_that_waited_is_cut_off_at_the_deadline_not_at_wait_plus_budget():
    budget = 1.0
    calls = 0
    guard = threading.Lock()

    def _execute(_request):
        nonlocal calls
        with guard:
            calls += 1
            mine = calls
        release_slot = server._acquire_stream_slot()
        if mine == 1:
            try:
                time.sleep(0.3)  # the first request: holds the only slot, then finishes
                return "ok"
            finally:
                release_slot()
        # The second request: a lazy stream of 20 batches, 0.1s apart — 2s of work — holding
        # its slot until the stream ends.
        return flight_deadline.stream_within_deadline(
            stream_slots.SlotHeldBatches(release_slot, _batches(20, 0.1))
        )

    server = _server(_State(cap=1, request_timeout=budget), _execute)
    first = threading.Thread(target=lambda: server.do_get(None, _ticket()))
    first.start()
    time.sleep(0.05)
    started = time.monotonic()
    stream = server.do_get(None, _ticket())  # waits ~0.25s for the slot
    assert 0.15 < time.monotonic() - started < 0.6
    first.join(timeout=10)

    got = 0
    with pytest.raises(flight.FlightServerError) as raised:
        for _batch in stream:
            got += 1
    elapsed = time.monotonic() - started

    assert budget <= elapsed < budget + 0.5  # cut at the deadline, not at 0.3s + 1s
    assert 0 < got < 10
    message = str(raised.value)
    assert "request deadline" in message and "request_timeout" in message and "1s" in message


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


def test_execution_cancelled_at_the_deadline_names_it():
    def _execute(_request):
        dl = request_deadline.current()
        assert dl is not None
        time.sleep(0.25)
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
