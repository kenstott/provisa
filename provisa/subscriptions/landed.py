# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Subscriptions on a table a push listener lands (Kafka, WebSocket): told when a batch lands.

A subscription re-runs its query on each change, and the query reads the landed table. Read
from the source's own feed it would race the listener, re-running before the batch is in the
table. So the listener says when a batch has landed (:func:`landed`, after the batch is written),
and a subscription on the table waits for that (:class:`LandedNotificationProvider`).

The listener runs on a thread of its own and a subscription on its request's, so a landing is
handed to each waiting subscription on its own loop. A table is the org's and environment's that
the landing is for: one process serves many orgs, each with a table of the same name.
"""

# Requirements: REQ-258, REQ-1733

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncGenerator
from datetime import datetime, timezone

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

#: (org, env, source id, schema, table) -> the waiting subscriptions: (their loop, their queue)
_Key = tuple[str, str | None, str, str, str]
_waiting: dict[_Key, set[tuple[asyncio.AbstractEventLoop, asyncio.Queue]]] = {}
_lock = threading.Lock()


def _key(source_id: str, schema: str, table: str) -> _Key:
    from provisa.core.request_context import current_env, require_current_org

    return require_current_org(), current_env.get(), source_id, schema, table


def landed(source_id: str, schema: str, table: str) -> None:
    """A batch of the bound org and environment's ``source_id.schema.table`` has landed."""
    with _lock:
        waiting = list(_waiting.get(_key(source_id, schema, table), ()))
    for loop, queue in waiting:
        if not loop.is_closed():
            loop.call_soon_threadsafe(queue.put_nowait, None)


class LandedNotificationProvider(NotificationProvider):  # REQ-258
    """One change a landed batch of one table, for the org and environment bound when it
    watches."""

    def __init__(self, source_id: str, schema: str) -> None:
        self._source_id = source_id
        self._schema = schema
        self._registered: tuple[_Key, tuple[asyncio.AbstractEventLoop, asyncio.Queue]] | None = None

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        key = _key(self._source_id, self._schema, table)
        queue: asyncio.Queue = asyncio.Queue()
        entry = (asyncio.get_running_loop(), queue)
        with _lock:
            _waiting.setdefault(key, set()).add(entry)
        self._registered = (key, entry)
        try:
            while True:
                await queue.get()
                yield ChangeEvent(
                    operation="landed", table=table, row={}, timestamp=datetime.now(timezone.utc)
                )
        finally:
            self._unregister()

    def _unregister(self) -> None:
        if self._registered is None:
            return
        key, entry = self._registered
        with _lock:
            waiting = _waiting.get(key)
            if waiting is not None:
                waiting.discard(entry)
                if not waiting:
                    _waiting.pop(key)
        self._registered = None

    async def close(self) -> None:
        self._unregister()
