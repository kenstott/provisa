# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2b9c41-5d3a-4f86-a1c7-8b0e4d6f2a95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Store writes on an engine runtime's ONE connection, run on the caller's own thread (REQ-1882).

A runtime that lands through a single shared connection (Postgres, Snowflake, Databricks,
Fabric/Synapse, ClickHouse's one client) must not have two writes on it at once. That used to be
enforced by a one-worker thread pool every land was handed to — which took a read-triggered land
(REQ-1661) off its request's thread. A :class:`LandGuard` serializes the connection with a lock
instead, and the write runs on the thread that asked for it.
"""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import TypeVar

from provisa.core import request_deadline

T = TypeVar("T")


class LandGuard:
    """Serializes writes on one shared store connection; each write runs on its caller's thread."""

    def __init__(self, what: str) -> None:
        self._what = what
        self._lock = threading.Lock()

    def _guarded(self, fn: Callable[[], T], budget: float | None) -> T:
        # The wait for the connection is bounded by the request's remaining budget; outside a
        # request (startup, the event loop's own lands) there is no budget and the write queues.
        acquired = self._lock.acquire() if budget is None else self._lock.acquire(timeout=budget)
        if not acquired:
            raise TimeoutError(
                f"{self._what}: the store connection stayed busy with another land for the "
                f"rest of this request's budget ({budget:.1f}s)"
            )
        try:
            return fn()
        finally:
            self._lock.release()

    async def run(self, fn: Callable[[], T]) -> T:
        """Run ``fn`` holding the connection.

        On a request's (or a background worker's) own connection loop the default executor runs
        ``fn`` inline, on that thread (``provisa.core.connection_loop``): the land is part of the
        request and never leaves it. On the process loop — lifespan startup wiring, the only work
        that lands from there — the default executor is the loop's thread pool. That hop is async
        by necessity: a blocking store write on the process loop would stall every connection it
        accepts and relays for."""
        # Read on the caller's context: a pool thread does not carry the request's ContextVars.
        budget = request_deadline.remaining()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._guarded, fn, budget)
