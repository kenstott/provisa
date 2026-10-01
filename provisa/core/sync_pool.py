# Copyright (c) 2026 Kenneth Stott
# Canary: 5c0e9a41-2d7b-4f63-9b8e-7a1d3c6e4f20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One shared, thread-safe, bounded connection pool per resource per worker (REQ-1882).

Every request runs on its own thread (REQ-1882, amended 2026-09-29), so a source's pool is a shared
resource used by many threads at once. When every connection is checked out, the next borrower
WAITS for one to be returned (bounded by ``wait_s``) — it never fails with a pool-exhausted error.
The wait bound turns a leaked connection into a clear error instead of an indefinite hang.
"""

# Requirements: REQ-1882

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Generic, TypeVar

from provisa.core import request_deadline

C = TypeVar("C")


class PoolTimeout(RuntimeError):
    """No pooled connection was returned within the pool's wait bound."""


class BlockingPool(Generic[C]):
    """A LIFO pool of at most ``maxsize`` connections; ``getconn`` waits when all are checked out."""

    def __init__(
        self,
        create: Callable[[], C],
        close: Callable[[C], None],
        *,
        minsize: int,
        maxsize: int,
        wait_s: float,
        name: str,
    ) -> None:
        if maxsize < 1:
            raise ValueError(f"{name}: maxsize must be >= 1, got {maxsize}")
        self._create = create
        self._close = close
        self._maxsize = maxsize
        self._wait_s = wait_s
        self._name = name
        self._idle: list[C] = []
        self._created = 0
        self._closed = False
        self._cond = threading.Condition(threading.Lock())
        for _ in range(minsize):
            self._idle.append(self._new())

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def _new(self) -> C:
        conn = self._create()
        self._created += 1
        return conn

    def getconn(self) -> C:
        wait_s = self._wait_s
        budget = request_deadline.remaining()
        if budget is not None:
            wait_s = min(wait_s, budget)
        deadline = time.monotonic() + wait_s
        with self._cond:
            while True:
                if self._closed:
                    raise RuntimeError(f"{self._name}: pool is closed")
                if self._idle:
                    return self._idle.pop()
                if self._created < self._maxsize:
                    # Reserve the slot before connecting (outside the lock) so concurrent
                    # borrowers never overshoot maxsize.
                    self._created += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._cond.wait(remaining):
                    raise PoolTimeout(
                        f"{self._name}: no connection returned within {wait_s:.1f}s "
                        f"(all {self._maxsize} checked out)"
                    )
        try:
            return self._create()
        except BaseException:
            with self._cond:
                self._created -= 1
                self._cond.notify()
            raise

    def putconn(self, conn: C) -> None:
        with self._cond:
            if not self._closed:
                self._idle.append(conn)
                self._cond.notify()
                return
            self._created -= 1
        self._close(conn)

    def discard(self, conn: C) -> None:
        """Close a broken connection instead of returning it; frees its slot."""
        try:
            self._close(conn)
        finally:
            with self._cond:
                self._created -= 1
                self._cond.notify()

    @contextmanager
    def connection(self, *, is_broken: Callable[[BaseException], bool]) -> Iterator[C]:
        """Borrow a connection; a failure ``is_broken`` classifies as fatal discards it."""
        conn = self.getconn()
        try:
            yield conn
        except BaseException as exc:
            if is_broken(exc):
                self.discard(conn)
            else:
                self.putconn(conn)
            raise
        self.putconn(conn)

    def closeall(self) -> None:
        with self._cond:
            self._closed = True
            idle, self._idle = self._idle, []
            self._created -= len(idle)
            self._cond.notify_all()
        for conn in idle:
            self._close(conn)
