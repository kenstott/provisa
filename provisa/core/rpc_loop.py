# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1f93d2-8b57-4e0c-b2a4-5d7e9c3f1b86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One Arrow Flight RPC on its handler thread's own loop (REQ-1882, amended 2026-09-29).

pyarrow.flight serves each RPC on a gRPC handler thread, recycled between calls (thread-local state
does not survive from one call to the next). The Flight SQL server and the airport Flight service
wrap each RPC in :func:`run_rpc`, which checks a ConnectionLoop out for the RPC and binds it to the
handler thread, so every coroutine the RPC runs executes on that thread. A do_get whose batches are
pulled after the handler returns hands the loop to the stream (:func:`hold_loop_for_stream`), which
checks it in once drained.
"""

# Requirements: REQ-1882

from __future__ import annotations

import threading as _threading
from collections.abc import Callable, Iterator
from typing import TypeVar

from provisa.core.connection_loop import LOOP_POOL, ConnectionLoop, bound, current_connection_loop

T = TypeVar("T")


class LoopHeldBatches:
    """A record-batch iterator that runs on — and finally checks in — its RPC's connection loop.

    Built inside do_get while the loop is bound; pyarrow drains it on the same handler thread
    after do_get returns, when the thread's binding is gone, so each pull re-binds the loop."""

    def __init__(self, cl: ConnectionLoop, batches: Iterator) -> None:
        self._cl = cl
        self._batches = batches
        self._released = False

    def __iter__(self) -> "LoopHeldBatches":
        return self

    def __next__(self):
        if self._released:
            raise StopIteration
        try:
            with bound(self._cl):
                return next(self._batches)
        except BaseException:
            # StopIteration (drained), a failed fetch, or GeneratorExit — the stream is over.
            self.release()
            raise

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            with bound(self._cl):
                close = getattr(self._batches, "close", None)
                if close is not None:
                    close()  # a DIRECT stream releases its server-side cursor on this loop
        finally:
            LOOP_POOL.checkin(self._cl)

    def __del__(self) -> None:
        # pyarrow dropped the stream without draining it (client cancelled mid-stream).
        self.release()


def hold_loop_for_stream(batches: Iterator) -> LoopHeldBatches:
    """Hand this RPC's connection loop to ``batches``, which checks it in when the stream ends."""
    cl = current_connection_loop()
    _rpc.loop_handed_off = True
    return LoopHeldBatches(cl, batches)


_rpc = _threading.local()


def run_rpc(body: Callable[[], T]) -> T:
    """Run one RPC's body with a connection loop checked out and bound to this handler thread.

    The loop is checked back in when the body returns — unless the body handed it to a stream
    (:func:`hold_loop_for_stream`), which checks it in once drained."""
    cl = LOOP_POOL.checkout()
    _rpc.loop_handed_off = False
    try:
        with bound(cl):
            return body()
    finally:
        handed_off = getattr(_rpc, "loop_handed_off", False)
        _rpc.loop_handed_off = False
        if not handed_off:
            LOOP_POOL.checkin(cl)
