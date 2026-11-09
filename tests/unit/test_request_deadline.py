# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4b2d7a-1c6f-4a3e-b8d5-6f0a2c9e1b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request's budget holds even while blocking work owns its thread (REQ-1882, amended
2026-09-29): the watchdog cancels the in-flight statement at the deadline."""

# Requirements: REQ-1882, REQ-2093

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from provisa.core import request_deadline
from provisa.core.connection_loop import connection_loop


class _BlockingStatement:
    """Stands in for a driver call: blocks until cancelled or ``duration`` elapses."""

    def __init__(self, duration: float) -> None:
        self.duration = duration
        self._cancelled = threading.Event()

    def cancel(self) -> None:
        self._cancelled.set()

    def execute(self) -> str:
        if self._cancelled.wait(self.duration):
            raise RuntimeError("canceling statement due to user request")
        return "done"


def test_one_second_budget_cancels_a_five_second_blocking_statement() -> None:
    stmt = _BlockingStatement(5.0)

    async def _request() -> str:
        # No await while it blocks: an asyncio timer alone could never fire here.
        with request_deadline.cancel_on_deadline(stmt.cancel):
            return stmt.execute()

    t0 = time.monotonic()
    with connection_loop() as cl, pytest.raises(TimeoutError, match="1s budget"):
        cl.run(_request(), timeout=1.0)
    elapsed = time.monotonic() - t0
    assert 0.9 <= elapsed < 2.0


def test_statement_within_budget_is_not_cancelled() -> None:
    stmt = _BlockingStatement(0.05)

    async def _request() -> str:
        with request_deadline.cancel_on_deadline(stmt.cancel):
            return stmt.execute()

    with connection_loop() as cl:
        assert cl.run(_request(), timeout=2.0) == "done"


def test_remaining_reflects_the_request_budget() -> None:
    async def _request() -> float | None:
        await asyncio.sleep(0)
        return request_deadline.remaining()

    with connection_loop() as cl:
        left = cl.run(_request(), timeout=3.0)
    assert left is not None and 2.5 < left <= 3.0
    assert request_deadline.remaining() is None  # unbound outside a request


def test_statement_started_after_expiry_fails_immediately(deadline_clock) -> None:
    with request_deadline.within(0.05) as dl:
        # Inside the thread's shield the watchdog raises nothing, so what is seen here is the
        # statement's own refusal at its start.
        with request_deadline.shielded().lock:
            deadline_clock.advance(0.15)
            assert dl.fired
            with pytest.raises(TimeoutError):
                with request_deadline.cancel_on_deadline(lambda: None):
                    pytest.fail("must not start a statement after the budget expired")


def test_tighter_enclosing_deadline_wins(deadline_clock) -> None:
    with request_deadline.within(1.0) as outer:
        with request_deadline.within(30.0) as inner:
            assert inner is outer


def test_a_budget_timeout_at_an_await_point_names_the_budget() -> None:
    """wait_for's own TimeoutError is empty; the request loop re-raises the deadline's message."""

    async def _slow() -> None:
        await asyncio.sleep(5)

    with connection_loop() as cl, pytest.raises(TimeoutError, match=r"exceeded its 0\.2s budget"):
        cl.run(_slow(), timeout=0.2)


@pytest.fixture
def thread_starts(monkeypatch) -> list[str]:
    """Every thread the deadline module starts once its one watchdog is running, by name."""
    with request_deadline.within(60):
        pass  # the process's watchdog thread exists from the first deadline on
    started: list[str] = []
    real = threading.Thread

    class _CountingThread(real):  # type: ignore[misc, valid-type]
        def start(self) -> None:
            started.append(self.name)
            super().start()

    monkeypatch.setattr(request_deadline.threading, "Thread", _CountingThread)
    return started


def _watchdog_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "provisa-deadline-watchdog"]


def test_budgeted_runs_start_no_thread_of_their_own(thread_starts) -> None:
    """One watchdog thread watches every deadline of the process. A pgwire point lookup makes four
    budgeted runs (describe, plan, cache check, audit): none of them starts a thread. The budget
    holds at the await points and by the clock."""

    async def _cpu_only() -> float | None:
        await asyncio.sleep(0)
        return request_deadline.remaining()

    with connection_loop() as cl:
        for budget in (120, 120, 30, 30):
            left = cl.run(_cpu_only(), timeout=budget)
            assert left is not None and left <= budget
    assert thread_starts == []
    assert len(_watchdog_threads()) == 1


def test_a_request_of_several_statements_is_watched_by_the_one_watchdog(thread_starts) -> None:
    async def _request() -> list[str]:
        out = []
        for _ in range(3):
            stmt = _BlockingStatement(0.01)
            with request_deadline.cancel_on_deadline(stmt.cancel):
                out.append(stmt.execute())
        return out

    with connection_loop() as cl:
        assert cl.run(_request(), timeout=2.0) == ["done"] * 3
    assert thread_starts == []
    assert len(_watchdog_threads()) == 1


def test_a_50ms_deadline_cancels_a_source_that_does_not_answer_and_starts_no_thread(
    thread_starts,
) -> None:
    """REQ-2093: the request's deadline is watched by the process's one watchdog, not by a timer
    of its own: at expiry the statement is cancelled and the request fails with the deadline's
    error, and no thread was started for it."""
    stmt = _BlockingStatement(5.0)  # a source that does not answer

    async def _request() -> str:
        with request_deadline.cancel_on_deadline(stmt.cancel):
            return stmt.execute()

    t0 = time.monotonic()
    with connection_loop() as cl, pytest.raises(TimeoutError, match=r"0\.05s budget"):
        cl.run(_request(), timeout=0.05)
    assert time.monotonic() - t0 < 1.0
    assert thread_starts == []
    assert len(_watchdog_threads()) == 1


def test_a_statement_registered_late_is_still_cancelled_at_the_deadline() -> None:
    stmt = _BlockingStatement(5.0)

    async def _request() -> str:
        time.sleep(0.4)  # the budget is already partly spent when the statement starts
        with request_deadline.cancel_on_deadline(stmt.cancel):
            return stmt.execute()

    t0 = time.monotonic()
    with connection_loop() as cl, pytest.raises(TimeoutError, match="1s budget"):
        cl.run(_request(), timeout=1.0)
    assert 0.9 <= time.monotonic() - t0 < 2.0


def test_expiry_is_by_the_clock_when_nothing_was_registered(thread_starts, deadline_clock) -> None:
    with request_deadline.within(0.05) as dl, request_deadline.shielded().lock:
        assert not dl.fired
        deadline_clock.advance(0.1)
        assert dl.fired
    assert thread_starts == []


def test_an_expiry_on_request_gives_the_statement_in_flight_its_drivers_grace(monkeypatch) -> None:
    """A CancelRequest (Deadline.expire) cancels the statement through its driver at once, and
    raises in the request's own thread only after the grace a clock expiry gives: raised at once,
    the raise landed while the driver's cancel error was being answered (pgspec Cancel read half a
    message)."""
    watched: list[float] = []
    monkeypatch.setattr(
        request_deadline._watchdog, "watch", lambda dl, *, at=None: watched.append(at)
    )
    cancelled: list[bool] = []
    dl = request_deadline.Deadline(30.0)
    try:
        # The statement returns after the cancel; its scope then reports the request cancelled.
        with pytest.raises(TimeoutError, match="canceling statement due to user request"):
            with dl._registered(lambda: cancelled.append(True)):
                before = time.monotonic()
                assert dl.expire("canceling statement due to user request")
        assert cancelled == [True]
        assert watched and watched[-1] >= before + request_deadline._CANCEL_GRACE_S
    finally:
        dl.stop()


# --- REQ-2224: a wait with a limit of its own is not charged to the request ----------------


def test_a_stopped_clock_is_not_charged_and_what_was_left_is_left_afterwards():
    with request_deadline.within(0.3) as dl:
        left = dl.remaining()
        with request_deadline.clock_stopped(5.0):
            time.sleep(0.5)  # longer than the whole budget
            assert not dl.fired
        assert dl.remaining() == pytest.approx(left, abs=0.15)
        assert not dl.fired


def test_a_wait_with_no_limit_of_its_own_is_charged_to_the_request():
    with request_deadline.within(5.0) as dl:
        left = dl.remaining()
        with request_deadline.clock_stopped(0.0):
            time.sleep(0.2)
        assert dl.remaining() <= left - 0.15


def test_a_stopped_clock_stops_the_deadlines_around_it():
    with request_deadline.within(0.3) as outer:
        with request_deadline.within(0.1) as inner:
            assert inner.outer is outer
            with request_deadline.clock_stopped(5.0):
                time.sleep(0.5)
            assert not inner.fired and not outer.fired


def test_a_deadline_fires_at_its_restored_time_after_the_clock_restarts():
    cancelled = threading.Event()
    with pytest.raises(TimeoutError):
        with request_deadline.within(0.15) as dl:
            with request_deadline.clock_stopped(5.0):
                time.sleep(0.2)
            with dl._registered(cancelled.set):
                assert cancelled.wait(2.0)  # the watchdog acted on the restored expiry
    assert cancelled.is_set()


def test_an_expired_request_stays_expired_across_a_clock_restart():
    with pytest.raises(TimeoutError, match="stopping"):
        with request_deadline.within(30) as dl:
            with request_deadline.clock_stopped(5.0):
                dl.expire("stopping")
                time.sleep(0.5)
    assert dl.ended_early


def test_timing_out_follows_a_stopped_clock():
    async def run():
        with request_deadline.within(0.2) as dl:
            async with request_deadline.timing_out(dl):
                with request_deadline.clock_stopped(5.0):
                    await asyncio.sleep(0.4)
                return "answered"

    assert asyncio.run(run()) == "answered"


def test_timing_out_still_ends_a_request_at_its_deadline():
    async def run():
        with request_deadline.within(0.1) as dl:
            async with request_deadline.timing_out(dl):
                await asyncio.sleep(5)

    with pytest.raises(TimeoutError):
        asyncio.run(run())
