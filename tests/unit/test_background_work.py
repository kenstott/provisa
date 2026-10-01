# Copyright (c) 2026 Kenneth Stott
# Canary: 8a3f6c21-7d4e-4b19-9e52-3c0d1f7a6b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Background work never runs on the process (front) loop (REQ-1882, amended 2026-09-29).

Detached work runs on background worker threads with their own connection loops; delayed work
waits on a timer thread, not a worker; long-lived loops get their own thread; APScheduler jobs run
on workers too. The process loop keeps relaying request I/O while any of them blocks."""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
import time

import pytest

from provisa.core import connection_loop as cl_mod
from provisa.core.connection_loop import (
    DEFAULT_BACKGROUND_WORKERS,
    configure_background_workers,
    shutdown_background,
    spawn_after,
    spawn_background,
    spawn_long_lived,
)

pytestmark = pytest.mark.asyncio

_probe: contextvars.ContextVar[str | None] = contextvars.ContextVar("bg_probe", default=None)


@pytest.fixture(autouse=True)
def _fresh_background():
    shutdown_background(timeout=5)
    configure_background_workers(DEFAULT_BACKGROUND_WORKERS)
    yield
    shutdown_background(timeout=5)
    configure_background_workers(DEFAULT_BACKGROUND_WORKERS)


async def _tick_while(fut, interval: float = 0.05) -> int:
    ticks = 0
    wrapped = asyncio.wrap_future(fut)
    while not wrapped.done():
        await asyncio.sleep(interval)
        ticks += 1
    await wrapped
    return ticks


async def test_a_blocking_background_task_does_not_block_the_process_loop():
    ran_on: list[int] = []

    async def _blocking() -> None:
        ran_on.append(threading.get_ident())
        time.sleep(1.0)  # a synchronous control-plane / engine call

    started = time.monotonic()
    ticks = await _tick_while(spawn_background(_blocking(), name="blocking"))

    assert time.monotonic() - started >= 1.0
    assert ticks >= 10  # the process loop kept ticking every 50ms throughout the 1s block
    assert ran_on and ran_on[0] != threading.get_ident()


async def test_the_spawned_task_sees_the_callers_context():
    seen: list[str | None] = []

    async def _read() -> None:
        seen.append(_probe.get())

    token = _probe.set("org-42")
    try:
        fut = spawn_background(_read(), name="ctx")
    finally:
        _probe.reset(token)
    await asyncio.wrap_future(fut)
    assert seen == ["org-42"]


async def test_a_failure_is_logged_with_the_task_name(caplog):
    async def _boom() -> None:
        raise RuntimeError("kaput")

    with caplog.at_level(logging.ERROR, logger="provisa.core.connection_loop"):
        await asyncio.wrap_future(spawn_background(_boom(), name="named-job"))

    assert any(
        "background task named-job failed" in r.getMessage() and r.exc_info for r in caplog.records
    )


async def test_a_delayed_task_does_not_hold_a_worker_during_its_delay():
    shutdown_background(timeout=5)
    configure_background_workers(1)  # one worker: a sleeping delay would starve everything else
    order: list[str] = []
    done = threading.Event()

    async def _delayed() -> None:
        order.append("delayed")
        done.set()

    async def _immediate() -> None:
        order.append("immediate")

    started = time.monotonic()
    spawn_after(1.0, _delayed(), name="delayed-drop")
    await asyncio.wrap_future(spawn_background(_immediate(), name="immediate"))
    assert time.monotonic() - started < 0.5  # ran at once — the delay pinned no worker
    assert await asyncio.to_thread(done.wait, 5)
    assert order == ["immediate", "delayed"]
    assert time.monotonic() - started >= 1.0


async def test_delayed_entries_not_yet_due_are_dropped_at_shutdown(caplog):
    ran: list[str] = []

    async def _later() -> None:
        ran.append("ran")

    spawn_after(60, _later(), name="far-future")
    with caplog.at_level(logging.WARNING, logger="provisa.core.connection_loop"):
        shutdown_background(timeout=5)
    assert ran == []
    assert any("far-future" in r.getMessage() for r in caplog.records)


async def test_a_long_lived_loop_runs_on_its_own_thread_and_stops_at_shutdown():
    beats: list[int] = []

    async def _forever() -> None:
        while True:
            beats.append(threading.get_ident())
            await asyncio.sleep(0.02)

    handle = spawn_long_lived(_forever(), name="heartbeat")
    await asyncio.sleep(0.2)
    assert beats and beats[0] != threading.get_ident()
    assert not handle.done()

    await asyncio.to_thread(shutdown_background, 5)
    assert handle.done()


async def test_a_long_lived_handle_cancels_and_is_awaitable_from_a_loop():
    async def _forever() -> None:
        while True:
            await asyncio.sleep(0.02)

    handle = spawn_long_lived(_forever(), name="cancellable")
    await asyncio.sleep(0.05)
    handle.cancel()
    assert await handle.wait(5)


async def test_scheduled_jobs_run_on_a_worker_not_the_scheduler_loop():
    from apscheduler.events import EVENT_JOB_EXECUTED

    from provisa.scheduler.executor import background_scheduler

    ran_on: list[int] = []
    executed = threading.Event()

    async def _job() -> None:
        ran_on.append(threading.get_ident())
        time.sleep(0.5)  # blocking, as a real refresh tick would be

    scheduler = background_scheduler()
    scheduler.add_listener(lambda _e: executed.set(), EVENT_JOB_EXECUTED)
    scheduler.start()
    try:
        scheduler.add_job(_job, id="probe")
        loop_ident = threading.get_ident()
        ticks = 0
        deadline = time.monotonic() + 5
        while not executed.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            ticks += 1
        assert executed.is_set()
        assert ran_on and ran_on[0] != loop_ident
        assert ticks >= 8  # the scheduler's loop ticked through the job's 0.5s block
    finally:
        scheduler.shutdown(wait=False)


async def test_resizing_a_running_pool_is_refused():
    await asyncio.wrap_future(spawn_background(asyncio.sleep(0), name="start-pool"))
    with pytest.raises(RuntimeError, match="configure it before first use"):
        configure_background_workers(DEFAULT_BACKGROUND_WORKERS + 1)
    assert cl_mod._bg_workers == DEFAULT_BACKGROUND_WORKERS


def test_run_on_own_thread_returns_result_on_a_separate_thread_in_parallel():
    """REQ-1882: fanned-out units each run on their own thread + loop, in parallel, and the
    future carries each unit's result or exception."""
    import asyncio
    import threading

    import pytest

    from provisa.core.connection_loop import run_on_own_thread

    caller = threading.get_ident()
    both_running = threading.Barrier(2)

    async def _unit(value: int) -> tuple[int, int]:
        both_running.wait(timeout=5)  # blocks this unit's thread; proves the two run in parallel
        return value, threading.get_ident()

    async def _boom() -> None:
        raise ValueError("unit failed")

    async def _main() -> None:
        a = run_on_own_thread(lambda: _unit(1), name="unit-a")
        b = run_on_own_thread(lambda: _unit(2), name="unit-b")
        (va, ta), (vb, tb) = await asyncio.gather(asyncio.wrap_future(a), asyncio.wrap_future(b))
        assert (va, vb) == (1, 2)
        assert ta != tb and caller not in (ta, tb)
        with pytest.raises(ValueError, match="unit failed"):
            await asyncio.wrap_future(run_on_own_thread(_boom, name="unit-boom"))

    asyncio.run(_main())
