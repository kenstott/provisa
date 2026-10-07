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

WHAT ENDS A REQUEST AT EXPIRY. Two things the request's own thread cannot do for itself, both
done by the watchdog. A blocking driver call is cancelled through its driver. Inline work —
rows being shaped, a response being encoded — is ended by RAISING THE TIMEOUT IN THE REQUEST'S
OWN THREAD (``PyThreadState_SetAsyncExc``; one request, one thread, REQ-1882), again every
:data:`_RAISE_AGAIN_S` until the request's deadline scope has ended, because a raise that lands
where exceptions are discarded (a weakref callback, a broad ``except``) ends nothing. The raise
lands at the thread's next bytecode; a single long C call ends when it returns.

A RELEASE SECTION IS NOT INTERRUPTED. Code that gives a resource back (a pooled connection, a
lock slot, a stream's cursor) runs inside the thread's shield (:func:`shielded`): the watchdog
raises only while it holds that same lock, so it raises nothing into a thread that is inside
one, and a raise set a moment earlier is dropped on the way in (it comes again once the section
is over). The scope's own exit is such a section. A connection whose statement was interrupted
this way is not reused (:func:`interrupted`).

The deterministic checks remain: a blocking call that returns after expiry raises on its way out
(:meth:`Deadline._registered`), a stream checks between batches
(``provisa.executor.result.StreamingQueryResult``), and a transport checks before it answers
(:func:`check`, :meth:`Deadline.check`) — a request whose deadline has passed is answered with
the timeout and nothing else."""

# Requirements: REQ-1882, REQ-1905

from __future__ import annotations

import contextlib
import contextvars
import ctypes
import functools
import heapq
import logging
import signal
import threading
import time
import traceback
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


class DeadlinePassed(TimeoutError):
    """What the watchdog raises in a request's own thread once its deadline has passed. Raised by
    class (the interpreter builds it with no arguments), so it carries no detail: whoever ends
    the request's deadline scope reports the deadline's own error (:meth:`Deadline.expired_error`)
    in its place."""

    def __init__(self) -> None:
        super().__init__("the request's deadline passed")


def interrupted(exc: BaseException) -> bool:
    """Whether ``exc`` is, or was caused by, the watchdog's raise into the thread — work cut at
    an arbitrary point, as opposed to a statement its driver cancelled. A connection that was
    mid-statement then is in an unknown protocol state and must not go back into a pool."""
    seen = 0
    cause: BaseException | None = exc
    while cause is not None and seen < 8:
        if isinstance(cause, DeadlinePassed):
            return True
        cause = cause.__cause__ or cause.__context__
        seen += 1
    return False


def let_go(exc: BaseException) -> None:
    """Release what the frames a raise cut through were still holding, now.

    The raise can land in the doorstep of a context manager — after its generator has taken a
    connection and before the ``with`` body owns it, or before its exit has resumed the
    generator. Nothing leaks: the abandoned generator's ``finally`` runs when it is finalized.
    But the exception's traceback keeps those frames alive, and an exception thrown through any
    generator-based context manager is part of a reference cycle, so "when it is finalized" is
    the cyclic collector's next run. Clearing the finished frames of the traceback (frames still
    executing are left alone) finalizes such a generator at once, on the request's own thread,
    at the point the request's scope ends. A no-op unless ``exc`` came from the watchdog's raise."""
    if not interrupted(exc):
        return
    seen = 0
    link: BaseException | None = exc
    while link is not None and seen < 8:
        traceback.clear_frames(link.__traceback__)
        link = link.__cause__ or link.__context__
        seen += 1


# The watchdog raises again this often until the request's deadline scope has ended.
_RAISE_AGAIN_S = 0.25
# ... and tries again this soon when the thread was inside a release section.
_RAISE_SOON_S = 0.002
# Between the expiry (statement cancelled) and the first raise: with a statement in flight, the
# time its driver's cancel is given to end it; without one, enough for a wait that was bounded
# by this same deadline to report its own timeout.
_CANCEL_GRACE_S = 0.25
_EXPIRY_GRACE_S = 0.05

_set_async_exc = ctypes.pythonapi.PyThreadState_SetAsyncExc


class _ThreadShield:
    """One thread's shield: the lock the watchdog must hold to raise in this thread, and the two
    calls on the thread's id. ``lock`` and ``settle`` are C callables on purpose — entering
    ``with shield.lock:`` and calling ``shield.settle()`` execute no Python frame, so there is no
    point between the end of the work and the start of the release at which a raise can land."""

    __slots__ = ("inside", "lock", "raise_now", "settle")

    def __init__(self) -> None:
        tid = ctypes.c_ulong(threading.get_ident())
        self.lock = threading.RLock()
        # The deadlines this thread is inside the scope of.
        self.inside: set[Deadline] = set()
        # Drops a raise that was set but not yet delivered (NULL clears it).
        self.settle = functools.partial(_set_async_exc, tid, None)
        self.raise_now = functools.partial(_set_async_exc, tid, ctypes.py_object(DeadlinePassed))

    def quiesce(self) -> None:
        """This thread has finished a request: it is inside no deadline's scope. Called inside
        the shield (``with shield.lock: shield.settle(); shield.quiesce()``) by whatever hands
        the thread its requests, as the first thing after one ends.

        A scope ends itself; this is for the one case it cannot. A raise that lands in the very
        door of a scope's exit (the context manager's own first bytecode) skips that exit, and
        the watchdog would go on raising into this thread — into its next request — until the
        abandoned scope was collected."""
        while self.inside:
            dl = self.inside.pop()
            dl._working = None  # noqa: SLF001
            dl._depth = 0  # noqa: SLF001


_thread = threading.local()


def shielded() -> _ThreadShield:
    """The calling thread's shield, for a release section the watchdog must not interrupt::

        shield = request_deadline.shielded()      # where the resource is taken
        conn = pool.getconn()
        try:
            ...
        finally:
            with shield.lock:      # the watchdog raises only while holding this lock
                shield.settle()    # drop a raise set just before the lock was taken
                pool.putconn(conn)

    Take the shield BEFORE the work and enter it as the first statement of the ``finally``: both
    steps run without a Python frame, so nothing can land between the work and the release. The
    dropped raise is not lost — the watchdog raises again once the section is over. Outside a
    request the same code runs and shields against nothing."""
    shield = getattr(_thread, "shield", None)
    if shield is None:
        shield = _thread.shield = _ThreadShield()
    return shield


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
        # The shield of the thread working under this deadline, while it is inside the deadline's
        # scope (``_enter`` / ``_leave``), and how many scopes deep. The watchdog raises there.
        self._working: _ThreadShield | None = None
        self._depth = 0
        self._cancelled = False
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

    def _at_expiry(self) -> float | None:
        """What the watchdog does when the budget runs out, in order. Each step ends work the
        request's own thread cannot end by itself. Runs on the watchdog thread; a request that
        has already ended is left alone. Returns the seconds after which to do it again, or
        None when there is nothing left to do."""
        with self._lock:
            if self._stopped:
                return None
            first = not self._cancelled
            self._cancelled = True
            in_statement = bool(self._cancels)
        # 1. The blocking statement in flight, through its driver's cancel — and then time for
        # that to end the request the clean way: the driver raises its own error, the connection
        # stays usable, and whatever was waiting on exactly this deadline (a pool, a queue, a
        # slot) reports its own timeout. The raise below is for what none of that reaches.
        if first:
            self._fire()
            return _CANCEL_GRACE_S if in_statement else _EXPIRY_GRACE_S
        # 2. Inline work, by raising the timeout in the request's own thread.
        shield = self._working
        if shield is None:
            return None  # no thread is inside the scope; one that enters later is seen then
        if not shield.lock.acquire(blocking=False):
            # It is inside a release section: raise as soon as it is out, not a period later.
            return _RAISE_SOON_S
        try:
            if self._working is shield and not self._stopped:
                shield.raise_now()
        finally:
            shield.lock.release()
        return _RAISE_AGAIN_S

    def _enter(self, shield: _ThreadShield) -> None:
        """The calling thread starts working inside this deadline's scope."""
        with self._lock:
            if self._working is None:
                self._working, self._depth = shield, 1
                shield.inside.add(self)
            elif self._working is shield:
                self._depth += 1
            else:
                return  # another thread's scope (work fanned out under the same deadline)
            already_passed = self.fired and not self._stopped
        if already_passed:
            _watchdog.watch(self, at=time.monotonic())

    def _leave(self, shield: _ThreadShield) -> None:
        """The calling thread leaves this deadline's scope. Called with ``shield.lock`` held and
        ``shield.settle()`` done, so no raise is pending and none can be set."""
        if self._working is shield:
            self._depth -= 1
            if self._depth == 0:
                self._working = None
                shield.inside.discard(self)

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
            self._cancelled = True
        self._fire()
        # ... and the request's own thread is raised in, by the watchdog, as at an expiry.
        _watchdog.watch(self, at=self.expires)
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

    def watch(self, dl: Deadline, *, at: float | None = None) -> None:
        with self._wake:
            self._seq += 1
            heapq.heappush(self._heap, (dl.expires if at is None else at, self._seq, dl))
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._run, name="provisa-deadline-watchdog", daemon=True
                )
                self._thread.start()
            elif self._heap[0][2] is dl and threading.current_thread() is not self._thread:
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
                again = dl._at_expiry()  # noqa: SLF001 - module-private collaborator
            except Exception:
                # One request's expiry failing must not stop every other request being watched.
                log.exception("request deadline: acting on an expired deadline failed")
                continue
            if again is not None:
                self.watch(dl, at=time.monotonic() + again)


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
    not the request's own and must outlive it (a replica build, ``federation.replica_builds``).
    Background work is started in a copy of its caller's context, deadline included; without
    this its control-plane and store calls are cancelled when that request's deadline passes."""
    _current.set(None)


def _enter_scope(dl: Deadline, shield: _ThreadShield) -> None:
    """Put the calling thread inside ``dl``'s scope, inside the shield: no raise is set while the
    scope is half entered. The deadline may already have passed — a loaded machine can hold the
    thread between creating the deadline and getting here for longer than the budget — and then
    the watchdog raises as soon as the shield is let go."""
    with shield.lock:
        shield.settle()
        dl._enter(shield)  # noqa: SLF001


class _Scope:
    """A thread's stay inside a deadline's scope, as a ``with`` block (REQ-1905).

    A class rather than a generator, for where a raise can land. A generator-based context
    manager is suspended at its ``yield`` while ``contextlib`` hands its value to the ``with``:
    a raise landing there — the door between entering the scope and the block — is outside every
    ``try`` the generator has and outside the block. It escapes as a bare :class:`DeadlinePassed`
    and leaves the thread inside the scope, raised into until the abandoned generator is
    collected. Here the scope is entered by the last call of ``__enter__``, inside its ``try``;
    from there to the block the interpreter executes no point at which a raise is delivered, and
    the block's own exception handling begins as ``__enter__`` returns.

    ``every_failure``: whatever the work fails with once the deadline has passed is reported as the
    timeout (a request's own deadline); otherwise only the watchdog's raise is. ``stops``: the
    deadline ends with the block (its owner does not outlive it)."""

    __slots__ = ("_dl", "_every_failure", "_outer", "_shield", "_stops")

    def __init__(self, dl: Deadline, *, every_failure: bool, stops: bool) -> None:
        self._dl = dl
        self._every_failure = every_failure
        self._stops = stops
        self._shield = shielded()
        self._outer: Deadline | None = None

    def __enter__(self) -> Deadline:
        self._outer = _current.get()
        try:
            _current.set(self._dl)
            _enter_scope(self._dl, self._shield)
        except BaseException as exc:
            self._end(exc)
            raise
        return self._dl

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> bool:
        self._end(exc)
        return False

    def _end(self, exc: BaseException | None) -> None:
        """Leave the scope; raise the deadline's own error in place of ``exc`` when it is what
        ``exc`` stands for."""
        dl, shield = self._dl, self._shield
        try:
            if isinstance(exc, Exception):
                let_go(exc)
                if self._reports_timeout(exc):
                    raise dl.expired_error() from exc
        finally:
            with shield.lock:
                shield.settle()
                dl._leave(shield)  # noqa: SLF001
            _current.set(self._outer)
            if self._stops:
                dl.stop()

    def _reports_timeout(self, exc: Exception) -> bool:
        if not self._every_failure:
            return isinstance(exc, DeadlinePassed)
        dl = self._dl
        return dl.fired and not dl.ended_early and not isinstance(exc, RequestTimedOut)


def bound(dl: Deadline) -> contextlib.AbstractContextManager[Deadline]:
    """Bind ``dl`` for the enclosed work and unbind it after, WITHOUT stopping it: for a deadline
    whose owner outlives the block (a Flight stream pulled batch by batch, REQ-1905)."""
    return _Scope(dl, every_failure=False, stops=False)


def hold(dl: Deadline) -> _ThreadShield:
    """Bind ``dl`` in the calling thread's context and put the thread inside its scope, for a
    request that is several protocol messages long and has no block to wrap (pgwire). Returns the
    thread's shield; the holder ends the scope with :func:`release` inside it::

        with shield.lock:
            shield.settle()
            request_deadline.release(dl, shield)
    """
    shield = shielded()
    _current.set(dl)
    _enter_scope(dl, shield)
    return shield


def release(dl: Deadline, shield: _ThreadShield) -> None:
    """End a scope begun with :func:`hold` (called inside the shield, see there)."""
    dl._leave(shield)  # noqa: SLF001
    _current.set(None)
    dl.stop()


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
    from provisa.core import process_mode
    from provisa.core.limits import request_timeout_for, request_timeout_setting

    # REQ-1916: a coordinator serves no data requests; every transport's request starts here.
    process_mode.refuse_data_request(transport)
    return Deadline(
        request_timeout_for(transport),
        transport=transport,
        setting=request_timeout_setting(transport),
    )


def request(transport: str) -> contextlib.AbstractContextManager[Deadline]:
    """Bind the deadline of a request arriving on ``transport`` for the whole of it (REQ-1905).

    Whatever the request then fails with, once its deadline has passed the failure it reports is
    the timeout: the enclosed work ends in :class:`RequestTimedOut`. A deadline the caller has
    already bound that is at least as tight is kept."""
    from provisa.core.limits import request_timeout_for

    outer = _current.get()
    if outer is not None and outer.remaining() <= request_timeout_for(transport):
        return contextlib.nullcontext(outer)
    return _Scope(open_request(transport), every_failure=True, stops=True)


def within(timeout: float) -> contextlib.AbstractContextManager[Deadline]:
    """Bind a deadline of ``timeout`` seconds for the enclosed work (the tighter of this and any
    enclosing deadline wins)."""
    outer = _current.get()
    if outer is not None and outer.remaining() <= timeout:
        return contextlib.nullcontext(outer)
    return _Scope(Deadline(timeout), every_failure=False, stops=True)


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
