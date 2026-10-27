# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9b7c15-8a42-4d60-b1f7-c05d2e6a9f84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882: what every request is awaiting, on demand.

A stalled server whose loops are alive shows nothing in a thread dump: each request thread sits in
its loop's select. The task dump names each task on each live loop and where its coroutine is
waiting, so a stall says what it waits on.
"""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import threading

from provisa.core import connection_loop, task_dump


async def _awaiting_the_pool(gate: asyncio.Event) -> None:
    await gate.wait()


def test_the_dump_names_each_task_and_where_it_waits(monkeypatch) -> None:
    loop = asyncio.new_event_loop()
    gate_box: list[asyncio.Event] = []
    started = threading.Event()

    async def main() -> None:
        gate = asyncio.Event()
        gate_box.append(gate)
        task = asyncio.ensure_future(_awaiting_the_pool(gate))
        task.set_name("POST /admin/graphql")
        started.set()
        await task

    thread = threading.Thread(target=lambda: loop.run_until_complete(main()), daemon=True)
    thread.start()
    started.wait(5)
    monkeypatch.setattr(connection_loop, "live_loops", lambda: [loop])
    try:
        out = task_dump.dump_tasks()
    finally:
        loop.call_soon_threadsafe(gate_box[0].set)
        thread.join(5)
        loop.close()
    assert "POST /admin/graphql" in out
    assert "_awaiting_the_pool" in out
    assert "test_task_dump.py" in out


def test_no_interval_starts_nothing(monkeypatch) -> None:
    monkeypatch.delenv("PROVISA_TASK_DUMP_S", raising=False)
    assert task_dump.start_from_env() is None


def test_an_interval_starts_one_daemon_thread(monkeypatch) -> None:
    monkeypatch.setenv("PROVISA_TASK_DUMP_S", "3600")
    thread = task_dump.start_from_env()
    assert thread is not None and thread.daemon and thread.is_alive()


def test_one_dump_writes_every_threads_stack_beside_the_tasks(capfd, caplog) -> None:
    """A loop whose own thread is blocked (a lock taken without awaiting) has tasks that cannot
    say so: the dump also writes every thread's Python stack."""
    with caplog.at_level("WARNING", logger="provisa.core.task_dump"):
        task_dump.dump_once()
    assert any("task dump" in r.getMessage() for r in caplog.records)
    err = capfd.readouterr().err
    assert "Current thread" in err or "Thread 0x" in err
    assert "test_one_dump_writes_every_threads_stack_beside_the_tasks" in err
