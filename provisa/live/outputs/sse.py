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

from provisa.live.outputs.base import LiveOutput

log = logging.getLogger(__name__)


class SSEFanout(LiveOutput):
    """Fan-out new rows to all subscribed SSE client queues."""

    def __init__(self, query_id: str) -> None:
        self.query_id = query_id
        self._queues: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        """Register a new client queue and return it."""
        q: asyncio.Queue = asyncio.Queue()
        self._queues.append(q)
        log.debug("[SSE FANOUT] client subscribed to %s (total=%d)", self.query_id, len(self._queues))
        return q

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Remove a client queue when the client disconnects."""
        try:
            self._queues.remove(queue)
        except ValueError:
            pass
        log.debug("[SSE FANOUT] client unsubscribed from %s (remaining=%d)", self.query_id, len(self._queues))

    @property
    def subscriber_count(self) -> int:
        return len(self._queues)

    async def send(self, rows: list[dict]) -> None:
        """Push *rows* to every subscriber queue (non-blocking)."""
        if not rows:
            return
        for q in list(self._queues):
            try:
                q.put_nowait(rows)
            except asyncio.QueueFull:
                log.warning("[SSE FANOUT] queue full for %s, dropping batch", self.query_id)

    async def close(self) -> None:
        """Signal all subscribers that the stream ended (send sentinel None)."""
        for q in list(self._queues):
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass
        self._queues.clear()
