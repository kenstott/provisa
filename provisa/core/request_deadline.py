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
:class:`Deadline`, watched by ONE watchdog thread for the whole process (:class:`_Watchdog`): at
expiry it invokes the cancel callback of whatever blocking statement the request is running
(``psycopg`` ``conn.cancel()``, ``oracledb`` ``conn.cancel()``, ``pyodbc`` ``cursor.cancel()``,
...). Drivers wrap each blocking call in :func:`cancel_on_deadline`; the cancelled call raises in
its own thread and the request fails with ``TimeoutError``. :func:`remaining` gives drivers that
take a timeout argument (httpx) the budget left.

ONE DEADLINE PER REQUEST (REQ-1905). A transport binds it where its request begins and ends —
:func:`request` for a request that is one block, :func:`open_request` for one that is several
protocol messages long — so the wait for a slot, governance, execution, the fetch, shaping,
encoding and the send all draw on the same budget, the transport's own request timeout
(``provisa.core.limits.request_timeout_for``). Its expiry is :class:`RequestTimedOut`, naming the
transport and the setting. A tighter budget inside it (a role's ``max_query_time_ms``) is
:func:`within`.

WHERE EXPIRY IS NOTICED. A blocking driver call is cancelled by the watchdog, and one that
returns after expiry raises on its way out (:meth:`Deadline._registered`). A stream checks
between batches (``provisa.executor.result.StreamingQueryResult``). The transport checks before
it answers (:func:`check`, :meth:`Deadline.check`): a request whose deadline has passed is
answered with the timeout and nothing else."""

# Requirements: REQ-1882, REQ-1905

from __future__ import annotations

import contextlib
import contextvars
import heapq
import logging
import signal
import threading
import time
import weakref
from collections.abc import Callable, Generator
from types import FrameType

log = logging.getLogger(__name__)


class RequestTimedOut(TimeoutError):
    """A statement outran its request timeout (REQ-1905). Carries what the message names — the
    transport, the timeout in seconds and the setting it comes from — so an HTTP surface can
    report them as the params of ``data.query_timeout``."""

    def __init__(self, transport: str, timeout_s: float, setting: str) -> None:
        super().__init__(
            f"{transport} request exceeded its {timeout_s:g}s request timeout ({setting})"
        )
        self.transport = transport
        self.timeout_s = timeout_s
        self.setting = setting


class Deadline:
    """One request's budget: expires ``timeout`` seconds after construction.

    ``transport`` and ``setting`` are what its expiry names (REQ-1905): the transport the request
    arrived on and the operator setting the timeout comes from. A deadline that names neither is
    a budget inside a request (:func:`within`), whose owner words its own error."""

    def __init__(
        self, timeout: float, *, transport: str | None = None, setting: str | None = None
    ) -> None:
        self.timeout = timeout
        self.expires = time.monotonic() + timeout
        self.transport = transport
        self.setting = setting
        self._lock = threading.Lock()
        self._cancels: dict[int, Callable[[], None]] = {}
        self._next = 0
        self._stopped = False
        # Why the budget ended early, when it was not the clock (the process is stopping).
        self._ended: str | None = None
        with _live_lock:
            _live.add(self)
        # Watched from creation: expiry is acted on whatever the request is doing at that moment.
        _watchdog.watch(self)

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

    def _at_expiry(self) -> None:
        """What the watchdog does when the budget runs out, in order. Each step ends work the
        request's own thread cannot end by itself. Runs on the watchdog thread; a request that
        has already ended is left alone."""
        with self._lock:
            if self._stopped:
                return
        # 1. The blocking statement in flight, through its driver's cancel.
        self._fire()

    def stop(self) -> None:
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
        _watchdog.forget()

    def expired_error(self) -> TimeoutError:
        if self._ended is not None:
            return TimeoutError(f"request cancelled: {self._ended}")
        if self.transport is not None and self.setting is not None:
            return RequestTimedOut(self.transport, self.timeout, self.setting)
        return TimeoutError(f"request exceeded its {self.timeout:g}s budget")

    def check(self) -> None:
        """Raise the expiry error if the budget has passed. For the points where a request can
        be ended between two pieces of its own work: a stream between batches, a transport
        before it answers."""
        if self.fired:
            raise self.expired_error()

    @property
    def ended_early(self) -> bool:
        """Whether the budget was ended by :meth:`expire` rather than by the clock."""
        return self._ended is not None

    def expire(self, reason: str) -> bool:
        """End this budget NOW for ``reason``: the statement in flight is cancelled through its
        driver and every later statement of the request is refused with ``reason``. False when
        the request already finished."""
        with self._lock:
            if self._stopped:
                return False
            self._ended = reason
            self.expires = time.monotonic()
        self._fire()
        return True

    @contextlib.contextmanager
    def _registered(self, cancel: Callable[[], None]) -> Generator[None]:
        with self._lock:
            if self.fired:
                raise self.expired_error()
            key = self._next
            self._next += 1
            self._cancels[key] = cancel
        try:
            try:
                yield
            except Exception as exc:
                if self.fired:
                    raise self.expired_error() from exc
                raise
            # A call that came back after the budget passed — a driver whose cancel arrived too
            # late to interrupt it, or work a cancel does not reach (rows already received being
            # converted) — ends the request here rather than handing it more to do.
            self.check()
        finally:
            with self._lock:
                self._cancels.pop(key, None)


class _Watchdog:
    """The one thread that watches every live deadline of this process.

    A second thread by necessity (REQ-1882): a request thread inside a blocking driver call
    cannot time itself out. It runs none of any request's work — at a deadline's expiry it only
    carries out that deadline's :meth:`Deadline._at_expiry`. One thread and a heap, rather than a
    timer thread per request: watching a deadline is a heap push, and a request that makes no
    blocking call costs no thread."""

    # Ended deadlines are dropped from the heap in bulk once this many are waiting to be.
    _COMPACT_AT = 256

    def __init__(self) -> None:
        self._wake = threading.Condition()
        self._heap: list[tuple[float, int, Deadline]] = []
        self._seq = 0
        self._ended = 0
        self._thread: threading.Thread | None = None

    def watch(self, dl: Deadline) -> None:
        with self._wake:
            self._seq += 1
            heapq.heappush(self._heap, (dl.expires, self._seq, dl))
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._run, name="provisa-deadline-watchdog", daemon=True
                )
                self._thread.start()
            elif self._heap[0][2] is dl:
                self._wake.notify()  # it is now the next to expire

    def forget(self) -> None:
        """A watched deadline has ended. Its entry stays until it reaches the top of the heap or
        ended entries are half of it — then they are all dropped, so a long timeout does not
        keep every request it ever timed in memory until that timeout passes."""
        with self._wake:
            self._ended += 1
            if self._ended >= self._COMPACT_AT and self._ended * 2 >= len(self._heap):
                self._heap = [entry for entry in self._heap if not entry[2]._stopped]  # noqa: SLF001
                heapq.heapify(self._heap)
                self._ended = 0

    def watched(self) -> int:
        """Entries in the heap (ended ones not yet dropped included)."""
        with self._wake:
            return len(self._heap)

    def _next_expired(self) -> Deadline:
        with self._wake:
            while True:
                while self._heap and self._heap[0][2]._stopped:  # noqa: SLF001
                    heapq.heappop(self._heap)
                    self._ended = max(0, self._ended - 1)
                if not self._heap:
                    self._wake.wait()
                    continue
                wait = self._heap[0][0] - time.monotonic()
                if wait <= 0:
                    return heapq.heappop(self._heap)[2]
                self._wake.wait(wait)

    def _run(self) -> None:
        while True:
            dl = self._next_expired()
            try:
                dl._at_expiry()  # noqa: SLF001 - module-private collaborator
            except Exception:
                # One request's expiry failing must not stop every other request being watched.
                log.exception("request deadline: acting on an expired deadline failed")


_watchdog = _Watchdog()


# Every deadline still in use, so a stopping process can end the requests that hold them. Weak:
# a deadline is dropped with its request.
_live: weakref.WeakSet[Deadline] = weakref.WeakSet()
_live_lock = threading.Lock()

_SHUTTING_DOWN = "the server is shutting down"


def expire_all(reason: str) -> int:
    """Expire every live request deadline (see :meth:`Deadline.expire`). Returns how many requests
    were ended."""
    with _live_lock:
        deadlines = list(_live)
    return sum(1 for dl in deadlines if dl.expire(reason))


@contextlib.contextmanager
def expire_on_stop_signals() -> Generator[None]:
    """While the block runs, SIGTERM and SIGINT first expire every live request deadline and then
    run the handler that was installed before (the server's own shutdown).

    Every request runs on its own thread and blocks it in driver calls (REQ-1882). The server's
    shutdown waits for in-flight requests, and a request thread inside a long run of statements
    has no way to learn the process is stopping — so without this a worker ignores the signal
    until the request ends on its own. Signal handlers can be installed only from the main
    thread; entered anywhere else (an in-process test client's lifespan) this installs nothing."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous: dict[int, object] = {}

    def _chain(sig: int) -> Callable[[int, FrameType | None], None]:
        before = signal.getsignal(sig)
        previous[sig] = before

        def _handler(signum: int, frame: FrameType | None) -> None:
            ended = expire_all(_SHUTTING_DOWN)
            if ended:
                log.warning("stop signal %s: ended %d in-flight request(s)", signum, ended)
            if callable(before):
                before(signum, frame)

        return _handler

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _chain(sig))
    try:
        yield
    finally:
        for sig, before in previous.items():
            signal.signal(sig, before)  # type: ignore[arg-type]


_current: contextvars.ContextVar[Deadline | None] = contextvars.ContextVar(
    "provisa_request_deadline", default=None
)


def current() -> Deadline | None:
    return _current.get()


def bind(dl: Deadline) -> None:
    """Bind ``dl`` in the current context (used to seed a caller-supplied task context)."""
    _current.set(dl)


def unbind() -> None:
    """Leave the request's deadline behind in this context: for work a request STARTED that is
    not the request's own and must outlive it (a replica build, ``federation.replica_build``).
    Background work is started in a copy of its caller's context, deadline included; without
    this its control-plane and store calls are cancelled when that request's deadline passes."""
    _current.set(None)


@contextlib.contextmanager
def bound(dl: Deadline) -> Generator[None]:
    """Bind ``dl`` for the enclosed work and unbind it after, WITHOUT stopping it: for a deadline
    whose owner outlives the block (a Flight stream pulled batch by batch, REQ-1905)."""
    token = _current.set(dl)
    try:
        yield
    finally:
        _current.reset(token)


def remaining() -> float | None:
    """Seconds left in the current request's budget, or ``None`` outside a request."""
    dl = _current.get()
    return None if dl is None else dl.remaining()


def check() -> None:
    """Raise if the current request's deadline has passed (:meth:`Deadline.check`). Outside a
    request there is no deadline and nothing to raise."""
    dl = _current.get()
    if dl is not None:
        dl.check()


def open_request(transport: str) -> Deadline:
    """THE deadline of a request arriving on ``transport`` (REQ-1905): that transport's own
    request timeout, its expiry naming the transport and the setting. Not bound: for a request
    that is several protocol messages long, whose handler binds it around each message
    (:func:`bound`) and stops it when the request ends. A request that is one block uses
    :func:`request`."""
    from provisa.core.limits import request_timeout_for, request_timeout_setting

    return Deadline(
        request_timeout_for(transport),
        transport=transport,
        setting=request_timeout_setting(transport),
    )


@contextlib.contextmanager
def request(transport: str) -> Generator[Deadline]:
    """Bind the deadline of a request arriving on ``transport`` for the whole of it (REQ-1905).

    Whatever the request then fails with, once its deadline has passed the failure it reports is
    the timeout: the enclosed work ends in :class:`RequestTimedOut`. A deadline the caller has
    already bound that is at least as tight is kept."""
    from provisa.core.limits import request_timeout_for

    outer = _current.get()
    if outer is not None and outer.remaining() <= request_timeout_for(transport):
        yield outer
        return
    dl = open_request(transport)
    token = _current.set(dl)
    try:
        yield dl
    except Exception as exc:
        if dl.fired and not dl.ended_early and not isinstance(exc, RequestTimedOut):
            raise dl.expired_error() from exc
        raise
    finally:
        _current.reset(token)
        dl.stop()


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
