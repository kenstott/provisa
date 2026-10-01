# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-ef01-345678901234
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SSE fan-out output for live queries (Phase AM).

Each connected SSE client registers an asyncio queue.  When new rows arrive
from a poll, they are pushed to every registered queue.  The SSE endpoint
reads from the queue and yields ``data:`` lines.

Usage::

    fanout = SSEFanout(query_id="abc-123")
    queue = fanout.subscribe()

    # In a separate task, the live engine calls:
    await fanout.send([{"id": 1, "amount": 42}])

    # The SSE endpoint reads:
    async for row_batch in queue_reader(queue):
        ...

    fanout.unsubscribe(queue)
"""

from __future__ import annotations

import asyncio
import logging
import threading

from provisa.live.outputs.base import LiveOutput

log = logging.getLogger(__name__)

# Requirements: REQ-258, REQ-260, REQ-286


class SSEFanout(LiveOutput):  # REQ-258, REQ-260, REQ-286
    """Fan-out new rows to all subscribed SSE client queues."""

    def __init__(self, query_id: str) -> None:
        self.query_id = query_id
        # REQ-1882 (amended 2026-09-29): an SSE client is served on its own request thread and loop,
        # while polls push from the process loop. Each queue is paired with its subscriber's loop
        # and filled there (call_soon_threadsafe); the list is guarded by a thread lock.
        self._lock = threading.Lock()
        self._queues: list[tuple[asyncio.AbstractEventLoop, asyncio.Queue]] = []

    def subscribe(self) -> asyncio.Queue:  # REQ-258, REQ-286
        """Register a new client queue (on the calling loop) and return it."""
        q: asyncio.Queue = asyncio.Queue()
        with self._lock:
            self._queues.append((asyncio.get_running_loop(), q))
            total = len(self._queues)
        log.debug("[SSE FANOUT] client subscribed to %s (total=%d)", self.query_id, total)
        return q

    def unsubscribe(self, queue: asyncio.Queue) -> None:  # REQ-565
        """Remove a client queue when the client disconnects."""
        with self._lock:
            self._queues = [(lp, q) for lp, q in self._queues if q is not queue]
            remaining = len(self._queues)
        log.debug(
            "[SSE FANOUT] client unsubscribed from %s (remaining=%d)", self.query_id, remaining
        )

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._queues)

    def _deliver(self, item: list[dict] | None) -> None:
        with self._lock:
            targets = list(self._queues)
        running = asyncio.get_running_loop()
        for loop, q in targets:
            # Unbounded queues: put_nowait never raises QueueFull.
            if loop is running:
                q.put_nowait(item)
            elif not loop.is_closed():  # closed: the subscriber's request ended with its loop
                loop.call_soon_threadsafe(q.put_nowait, item)

    async def send(self, rows: list[dict]) -> None:  # REQ-565
        """Push *rows* to every subscriber queue (non-blocking)."""
        if not rows:
            return
        self._deliver(rows)

    async def close(self) -> None:  # REQ-565
        """Signal all subscribers that the stream ended (send sentinel None)."""
        self._deliver(None)
        with self._lock:
            self._queues.clear()
