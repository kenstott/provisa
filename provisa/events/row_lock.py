# Copyright (c) 2026 Kenneth Stott
# Canary: bbf9933b-f438-4af0-b016-f2af80eb72a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-key locks for row-level materialize (REQ-1865, design doc section 4).

``land_lock`` (``provisa/events/land_lock.py``) is keyed per physical NODE (one lock for the whole
table) — correct for a whole-table REPLACE but wrong-grained for a row cache: locking the entire
table for every single-row fetch would serialize unrelated point lookups against millions of
independent rows, destroying exactly the concurrency a row-level cache exists to provide.

This module locks per ``(node, pk_tuple)`` instead. Two concurrent callers needing the SAME key
serialize (the second, after acquiring, is expected to re-check ``_row_expires_at`` and skip its
own fetch when the first already refreshed it — that re-check lives in ``ensure_rows_resident``,
not here). Two callers needing DISJOINT keys never contend.

The design doc explicitly flags this dict as unbounded by construction (up to one entry per row
ever requested) and asks the implementer to close that gap rather than leave it open. This module
closes it with a refcounted wrapper: acquiring increments a shared entry's holder/waiter count
before awaiting the underlying ``asyncio.Lock``, and releasing decrements it and deletes the dict
entry the instant the count reaches zero -- there is no window where a dict entry outlives every
holder AND waiter of it, so the registry never grows past the number of keys CURRENTLY contended,
regardless of how many distinct keys have ever been requested over the process's lifetime.

The SAME lock (by the SAME (node, pk_tuple) key) must be used by every caller that can touch a
row-materialized key: a query-driven fetch (``ensure_rows_resident``), the CDC-triggered background
refresh (section 5/6b), and the reaper sweep (section 6c) -- one registry, three callers, never
racing each other on one key."""

from __future__ import annotations

import asyncio
from typing import Any


class _RefcountedLock:
    __slots__ = ("lock", "refcount")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.refcount = 0


_locks: dict[tuple[str, tuple[Any, ...]], _RefcountedLock] = {}


class _RowLockHandle:
    """An async context manager for one ``(node, pk_tuple)`` key. Not itself an ``asyncio.Lock`` --
    a thin handle so acquire/release can maintain the shared entry's refcount and evict it. Safe to
    construct repeatedly for the same key (each construction is a fresh acquire attempt); never
    share one handle instance across two concurrent ``async with`` blocks."""

    __slots__ = ("_key", "_entry")

    def __init__(self, key: tuple[str, tuple[Any, ...]]) -> None:
        self._key = key
        self._entry: _RefcountedLock | None = None

    async def __aenter__(self) -> "_RowLockHandle":
        # No `await` between the dict lookup/insert and the refcount bump: asyncio is single-
        # threaded cooperative, so this block runs atomically with respect to every other task —
        # two concurrent callers for the same key are guaranteed to see and share the SAME entry,
        # never each create their own.
        entry = _locks.get(self._key)
        if entry is None:
            entry = _RefcountedLock()
            _locks[self._key] = entry
        entry.refcount += 1
        self._entry = entry
        await entry.lock.acquire()
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        entry = self._entry
        assert entry is not None  # __aenter__ always sets it before returning
        entry.lock.release()
        entry.refcount -= 1
        # Same atomicity argument as __aenter__: no `await` between the decrement and the delete,
        # so this can't race a concurrent acquire that just created a fresh entry for this key.
        if entry.refcount == 0 and _locks.get(self._key) is entry:
            del _locks[self._key]
        self._entry = None
        return False


def row_lock(node: str, pk_values: tuple[Any, ...]) -> _RowLockHandle:
    """The lock for one row-materialized key: ``node`` is the physical node (``schema.table``, the
    SAME string ``land_lock``'s own callers use), ``pk_values`` the row's PK column values in a
    fixed order. Use as ``async with row_lock(node, pk_values): ...``."""
    return _RowLockHandle((node, pk_values))


def _registry_size() -> int:
    """Test/diagnostic hook: the number of currently-contended keys (never every key ever
    requested) — asserts the bounded-eviction property."""
    return len(_locks)
