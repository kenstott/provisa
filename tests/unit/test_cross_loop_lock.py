# Copyright (c) 2026 Kenneth Stott
# Canary: 25c97f45-eba4-45e4-885b-a09da287b615
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A CrossLoopLock is never held for a waiter that cannot take it. A connection loop runs only
while it serves a request, so a waiter's loop may not be running when the lock is released; the
lock was handed to that waiter regardless and stayed held until its loop next ran, and every
other acquirer -- a demo template's build among them -- waited behind it."""

from __future__ import annotations

import asyncio
import threading

from provisa.core.connection_loop import CrossLoopLock


def test_a_release_while_a_waiters_loop_is_not_running_leaves_the_lock_to_the_next_acquirer():
    lock = CrossLoopLock()
    idle, active = asyncio.new_event_loop(), asyncio.new_event_loop()
    try:
        active.run_until_complete(lock.acquire())
        waiter = idle.create_task(lock.acquire())
        idle.run_until_complete(asyncio.sleep(0))  # queued behind the holder; its loop then stops
        assert not waiter.done()

        async def _release_then_take() -> None:
            lock.release()
            await asyncio.wait_for(lock.acquire(), 2.0)
            lock.release()

        active.run_until_complete(_release_then_take())
        waiter.cancel()
        idle.run_until_complete(asyncio.gather(waiter, return_exceptions=True))
    finally:
        idle.close()
        active.close()


def test_a_waiter_on_a_running_loop_takes_the_lock_once_it_is_released():
    lock = CrossLoopLock()
    holder = asyncio.new_event_loop()
    holder.run_until_complete(lock.acquire())
    took = threading.Event()

    def _wait() -> None:
        async def _take() -> None:
            await lock.acquire()
            took.set()
            lock.release()

        asyncio.run(asyncio.wait_for(_take(), 5.0))

    thread = threading.Thread(target=_wait)
    thread.start()
    try:
        assert not took.wait(0.2)  # held: the waiter waits
        lock.release()
        assert took.wait(5.0)
    finally:
        thread.join(10)
        holder.close()
    assert not lock.locked()


def test_a_cancelled_waiter_does_not_keep_the_lock_from_the_one_behind_it():
    async def _run() -> None:
        lock = CrossLoopLock()
        await lock.acquire()
        first = asyncio.create_task(lock.acquire())
        second = asyncio.create_task(lock.acquire())
        await asyncio.sleep(0)
        lock.release()
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        await asyncio.wait_for(second, 2.0)
        lock.release()
        assert not lock.locked()

    asyncio.run(_run())
