# Copyright (c) 2026 Kenneth Stott
# Canary: 0d5a7c93-1e4f-4b28-9c60-f3b8e1d2a475
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The server-wide Flight stream limit: what takes a slot, for how long, and what waits (REQ-1905).

The limit used to REJECT a stream over it, to take its slot before the response-cache check (so a
cache hit that never reaches the engine took one), and to give the slot back when ``do_get``
returned — before pyarrow pulled a single batch of a lazy stream, so it bounded nothing for the
scans it exists to bound. Now:

* a stream over the limit WAITS for a slot, within the request's deadline;
* the slot is taken only when the engine or the source is about to be reached (a cache HIT takes
  none);
* a lazy stream HOLDS its slot until it is drained, fails, or is dropped by the client."""

# Requirements: REQ-1905

from __future__ import annotations

import gc
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight import stream_slots
from provisa.api.flight.server import ProvisaFlightServer
from provisa.core import request_deadline, settings_registry
from provisa.core.rpc_loop import run_rpc

_SCHEMA = pa.schema([("n", pa.int64())])


@pytest.fixture(autouse=True)
def _fresh_slots(monkeypatch):
    monkeypatch.setattr(stream_slots, "_slots", None)
    monkeypatch.setattr(settings_registry, "_config", {})  # restored after the test


def _flight_timeout(seconds: float) -> None:
    """Flight's own request timeout (REQ-1905), set the way a deployment's config file sets it."""
    settings_registry.bind_config({"server": {"limits": {"request_timeouts": {"flight": seconds}}}})


def _batches(n: int, each: float = 0.0):
    for i in range(n):
        time.sleep(each)
        yield pa.record_batch([pa.array([i])], schema=_SCHEMA)


class _Engine:
    """The engine's lazy Arrow terminal: counts how many streams are open at once."""

    def __init__(self, each: float = 0.0, batches: int = 3) -> None:
        self.open = 0
        self.peak = 0
        self.opened = 0
        self._each = each
        self._batches = batches
        self._guard = threading.Lock()

    def execute_engine_stream(self, _sql, _params):
        with self._guard:
            self.open += 1
            self.opened += 1
            self.peak = max(self.peak, self.open)

        def _gen():
            try:
                yield from _batches(self._batches, self._each)
            finally:
                with self._guard:
                    self.open -= 1

        return _SCHEMA, _gen()


def _server(cap: int | None, engine: _Engine, request_timeout: float = 30.0):
    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    _flight_timeout(request_timeout)
    server._state = SimpleNamespace(flight_global_cap=cap, federation_engine=engine)
    return server


_cache_answer = threading.local()


@pytest.fixture(autouse=True)
def _no_real_pipeline():
    """The engine route's collaborators, replaced ONCE for the test (not per call: the tests
    call from several threads, and a patch entered and left per thread would be undone under
    another thread's call)."""

    async def _check(_plan, _state):
        return getattr(_cache_answer, "table", None)

    async def _residency(_state, _plan):
        return None

    with (
        patch("provisa.pgwire._pipeline.check_response_cache_arrow", _check),
        patch("provisa.api.flight.server._prepare_engine_residency", _residency),
        patch("provisa.pgwire._pipeline.response_cache_tee", lambda *_a, **_k: None),
    ):
        yield


def _stream(server, cached=None):
    """One ticket through the engine route: (cached_table, schema, batches)."""
    # REQ-1909: as _Plan — no capped live source bound at mint.
    plan = SimpleNamespace(physical_sql="SELECT 1", live_caps=(), live_caps_org=None)
    _cache_answer.table = cached
    try:
        return run_rpc(lambda: server._engine_arrow_through_cache(plan, []))
    finally:
        _cache_answer.table = None


def _free(cap: int) -> int:
    return stream_slots.slots_for(cap).free()


def test_streams_over_the_limit_wait_and_all_complete():
    limit, extra = 3, 5
    engine = _Engine(each=0.03)
    server = _server(limit, engine)
    rows: list[int] = []
    failures: list[BaseException] = []
    start = threading.Barrier(limit + extra)

    def _one() -> None:
        try:
            start.wait(timeout=10)
            _hit, _schema, batches = _stream(server)
            rows.append(sum(b.num_rows for b in batches))
        except BaseException as exc:  # collected and asserted on below
            failures.append(exc)

    threads = [threading.Thread(target=_one) for _ in range(limit + extra)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert failures == []
    assert rows == [3] * (limit + extra)
    assert engine.peak == limit  # never more than `limit` streams open on the engine at once
    assert _free(limit) == limit


def test_a_cache_hit_takes_no_slot_even_when_every_slot_is_taken():
    engine = _Engine()
    server = _server(1, engine, request_timeout=0.3)
    _hit, _schema, held = _stream(server)  # holds the only slot: not drained yet
    assert _free(1) == 0

    table = pa.table({"n": [1]})
    started = time.monotonic()
    hit, schema, batches = _stream(server, cached=table)
    assert time.monotonic() - started < 0.2  # no wait for a slot
    assert hit is table and schema is None and batches is None
    assert engine.opened == 1
    list(held)


def test_a_lazy_stream_holds_its_slot_until_it_is_drained():
    server = _server(1, _Engine())
    _hit, _schema, batches = _stream(server)
    assert _free(1) == 0  # do_get would have returned by now; the slot is still held
    assert sum(b.num_rows for b in batches) == 3
    assert _free(1) == 1


def test_a_stream_the_client_dropped_gives_its_slot_back():
    server = _server(1, _Engine())
    _hit, _schema, batches = _stream(server)
    next(iter(batches))
    assert _free(1) == 0
    del batches
    gc.collect()
    assert _free(1) == 1


def test_a_stream_never_pulled_gives_its_slot_back():
    server = _server(1, _Engine())
    result = _stream(server)
    assert _free(1) == 0
    del result
    gc.collect()
    assert _free(1) == 1


def test_a_failing_engine_call_gives_the_slot_back():
    class _Broken:
        def execute_engine_stream(self, _sql, _params):
            raise RuntimeError("engine down")

    server = _server(1, _Broken())
    with pytest.raises(flight.FlightServerError, match="engine down"):
        _stream(server)
    assert _free(1) == 1


def test_a_stream_that_gets_no_slot_within_its_budget_fails_naming_the_limit():
    server = _server(1, _Engine(), request_timeout=0.4)
    _hit, _schema, held = _stream(server)  # holds the only slot
    started = time.monotonic()
    with pytest.raises(flight.FlightServerError) as raised:
        _stream(server)
    waited = time.monotonic() - started
    assert 0.3 <= waited < 2.0
    message = str(raised.value)
    assert "max concurrent Arrow Flight streams reached (server-wide)" in message
    assert "flight_max_concurrent_streams=1" in message
    list(held)


def test_the_wait_is_bounded_by_the_requests_remaining_deadline_when_one_is_bound():
    server = _server(1, _Engine(), request_timeout=30.0)
    _hit, _schema, held = _stream(server)
    started = time.monotonic()
    # Either end is the 0.3 s deadline: the slot wait giving up at it (a Flight error), or the
    # deadline's own raise landing in this thread as the wait returns.
    with pytest.raises((flight.FlightServerError, TimeoutError)):
        with request_deadline.within(0.3):
            _stream(server)
    assert time.monotonic() - started < 2.0  # the 0.3s deadline, not the 30s server budget
    list(held)


def test_no_limit_configured_means_no_gate():
    engine = _Engine()
    server = _server(None, engine)
    streams = [_stream(server)[2] for _ in range(8)]
    assert engine.open == 8
    for s in streams:
        list(s)


def test_the_default_limit_divides_the_hosts_budget_among_the_launchs_workers(monkeypatch):
    from provisa.core.limits import flight_stream_default_limit as default_limit

    monkeypatch.setattr("os.cpu_count", lambda: 16)
    monkeypatch.delenv("PROVISA_LAUNCH_ID", raising=False)
    assert default_limit() == 6  # one process: a third of min(32, 16 + 4)
    monkeypatch.setenv("PROVISA_LAUNCH_ID", "l")
    monkeypatch.setenv("PROVISA_WORKERS", "2")
    assert default_limit() == 3
    monkeypatch.setenv("PROVISA_WORKERS", "16")
    assert default_limit() == 2  # never below 2 per worker


def test_every_place_do_get_reaches_the_engine_or_a_source_takes_a_slot():
    """Five acquisitions cover six places: the engine stream, the direct-source stream, the
    direct buffered read (one helper, used by SQL and by GraphQL), and the Cypher pipeline
    terminal and engine read. None at the top of do_get."""
    import inspect

    from provisa.api.flight import server

    src = inspect.getsource(server)
    assert src.count("self._acquire_stream_slot()") == 5
    assert src.count("self._native_read_in_slot(") == 2
    assert "slots_for(global_cap).slot(" not in src
