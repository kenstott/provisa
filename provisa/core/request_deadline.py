# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a41-5b3f-4d8e-a6c0-1f9b4e2d7a53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-request deadlines that hold even while blocking work owns the request thread.

Every request runs on its own thread, and its driver calls block that thread (REQ-1882). An
asyncio timer cannot fire while the thread is inside a blocking call, so a request's budget is a
:class:`Deadline`: a watchdog ``threading.Timer`` that, at expiry, invokes the cancel callback of
whatever blocking statement the request is running (``psycopg2`` ``conn.cancel()``, ``oracledb``
``conn.cancel()``, ``pyodbc`` ``cursor.cancel()``, ...). Drivers wrap each blocking call in
:func:`cancel_on_deadline`; the cancelled call raises in its own thread and the request fails with
``TimeoutError``. :func:`remaining` gives drivers that take a timeout argument (httpx) the budget
left."""

# Requirements: REQ-1882

from __future__ import annotations

import contextlib
import contextvars
import logging
import threading
import time
from collections.abc import Callable, Generator

log = logging.getLogger(__name__)


class Deadline:
    """One request's budget: expires ``timeout`` seconds after construction."""

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        self.expires = time.monotonic() + timeout
        self._lock = threading.Lock()
        self._cancels: dict[int, Callable[[], None]] = {}
        self._next = 0
        # The watchdog is armed by the first statement that registers a cancel (``_registered``),
        # not here: a budgeted run that makes no blocking driver call has nothing for it to
        # cancel, and expiry itself is read off the clock (``fired``).
        self._timer: threading.Timer | None = None
        self._stopped = False

    @property
    def fired(self) -> bool:
        """Whether the budget has expired — by the clock, whether or not a watchdog was armed."""
        return time.monotonic() >= self.expires

    def remaining(self) -> float:
        return max(0.0, self.expires - time.monotonic())

    def _fire(self) -> None:
        with self._lock:
            cancels = list(self._cancels.values())
        for cancel in cancels:
            try:
                cancel()
            except Exception:
                # One statement's cancel failing must not stop the others being cancelled; the
                # failure is reported and that statement ends at its own driver's limit.
                log.exception("request deadline: cancelling an in-flight statement failed")

    def stop(self) -> None:
        with self._lock:
            self._stopped = True
            timer = self._timer
        if timer is not None:
            timer.cancel()

    def expired_error(self) -> TimeoutError:
        return TimeoutError(f"request exceeded its {self.timeout:g}s budget")

    @contextlib.contextmanager
    def _registered(self, cancel: Callable[[], None]) -> Generator[None]:
        with self._lock:
            if self.fired:
                raise self.expired_error()
            key = self._next
            self._next += 1
            self._cancels[key] = cancel
            if self._timer is None and not self._stopped:
                # A second thread by necessity (REQ-1882): the request thread is about to enter a
                # blocking driver call and cannot time itself out when the budget expires. The
                # timer runs none of the request's work — at expiry it only calls the in-flight
                # statement's cancel.
                self._timer = threading.Timer(self.remaining(), self._fire)
                self._timer.daemon = True
                self._timer.start()
        try:
            yield
        except Exception as exc:
            if self.fired:
                raise self.expired_error() from exc
            raise
        finally:
            with self._lock:
                self._cancels.pop(key, None)


_current: contextvars.ContextVar[Deadline | None] = contextvars.ContextVar(
    "provisa_request_deadline", default=None
)


def current() -> Deadline | None:
    return _current.get()


def bind(dl: Deadline) -> None:
    """Bind ``dl`` in the current context (used to seed a caller-supplied task context)."""
    _current.set(dl)


def remaining() -> float | None:
    """Seconds left in the current request's budget, or ``None`` outside a request."""
    dl = _current.get()
    return None if dl is None else dl.remaining()


@contextlib.contextmanager
def within(timeout: float) -> Generator[Deadline]:
    """Bind a deadline of ``timeout`` seconds for the enclosed work (the tighter of this and any
    enclosing deadline wins)."""
    outer = _current.get()
    if outer is not None and outer.remaining() <= timeout:
        yield outer
        return
    dl = Deadline(timeout)
    token = _current.set(dl)
    try:
        yield dl
    finally:
        _current.reset(token)
        dl.stop()


@contextlib.contextmanager
def cancel_on_deadline(cancel: Callable[[], None]) -> Generator[None]:
    """Run the enclosed blocking call so the current deadline can cancel it via ``cancel``.

    Outside a request (no deadline bound) the call runs unbounded by a request budget, as
    startup/background work does."""
    dl = _current.get()
    if dl is None:
        yield
        return
    with dl._registered(cancel):  # noqa: SLF001 - module-private collaborator
        yield
