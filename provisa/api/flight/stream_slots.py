# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3e9f04-2b61-4a8d-b5d7-9e0f1a6c4d32
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The server-wide limit on concurrent Arrow Flight query streams (REQ-1905).

PER WORKER PROCESS. What the limit protects is the capacity the other transports share with
Flight inside one process — one interpreter executing on one core at a time — so each worker
bounds its own streams; a deployment of N workers runs up to N times the limit. (With the Flight
port shared by every worker, REQ-1900, a deployment-wide count would also mean a round trip to a
shared store before every stream, and a shared count cannot be WAITED on without polling.)

A stream over the limit WAITS for a slot. The limit is a semaphore; the wait is bounded by the
request's remaining deadline and ends in an error naming the limit only when no slot frees in that
time. It used to reject immediately, which turned any client concurrency above the limit into
errors rather than queueing.

A slot is taken where the engine or a source is about to be reached — after the response-cache
check, so a HIT takes none — and is held for as long as that reach lasts. For a lazy stream that
is until pyarrow has pulled the last batch, well after ``do_get`` returned: ``SlotHeldBatches``
carries the slot through the pull and gives it back when the stream is drained, fails, or is
dropped by the client.
"""

# Requirements: REQ-1905

from __future__ import annotations

import threading
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from typing import Any

from provisa.core import request_deadline


class StreamLimitTimeout(RuntimeError):
    """No Flight stream slot freed within the request's budget."""

    def __init__(self, limit: int, waited: float) -> None:
        super().__init__(
            "max concurrent Arrow Flight streams reached (server-wide): all "
            f"{limit} stream slots of this worker (flight_max_concurrent_streams={limit}) "
            f"stayed busy for the {waited:.1f}s this request could wait"
        )


class StreamSlots:
    """``limit`` slots; ``slot`` holds one for the block, waiting for it if all are taken."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._free = threading.BoundedSemaphore(limit)

    def acquire(self, budget: float) -> Callable[[], None]:
        """Take a slot and return what gives it back (once; later calls do nothing). Waits at
        most the current request deadline's remaining time, or ``budget`` (the server's request
        budget) when the caller has bound no deadline."""
        remaining = request_deadline.remaining()
        wait = budget if remaining is None else remaining
        if not self._free.acquire(timeout=wait):
            raise StreamLimitTimeout(self.limit, wait)
        released = threading.Lock()

        def release() -> None:
            if released.acquire(blocking=False):
                self._free.release()

        return release

    @contextmanager
    def slot(self, budget: float) -> Generator[None]:
        """Hold a slot for the block."""
        release = self.acquire(budget)
        try:
            yield
        finally:
            release()

    def free(self) -> int:
        """How many slots are free right now (for tests and diagnostics)."""
        return self._free._value  # noqa: SLF001 - the semaphore's own count; no public reader


class SlotHeldBatches:
    """A lazy stream that holds its slot until it ends: drained, failed, or dropped (pyarrow
    releases the stream when the client cancels, and may never have pulled from it)."""

    def __init__(self, release: Callable[[], None], batches: Iterator[Any]) -> None:
        self._release = release
        self._batches = batches

    def __iter__(self) -> "SlotHeldBatches":
        return self

    def __next__(self) -> Any:
        try:
            return next(self._batches)
        except BaseException:
            # StopIteration (drained) or a failed fetch: the stream is over either way.
            self._release()
            raise

    def close(self) -> None:
        close = getattr(self._batches, "close", None)
        try:
            if close is not None:
                close()
        finally:
            self._release()

    def __del__(self) -> None:
        self.close()


_slots: StreamSlots | None = None
_slots_guard = threading.Lock()


def slots_for(limit: int) -> StreamSlots:
    """This process's slots for ``limit`` (rebuilt when the configured limit changes; streams
    already running release into the set they were admitted by)."""
    global _slots
    with _slots_guard:
        if _slots is None or _slots.limit != limit:
            _slots = StreamSlots(limit)
        return _slots
