# Copyright (c) 2026 Kenneth Stott
# Canary: 27225416-34e3-41de-ae37-c1c694ed5bdb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Connection-thread event loops (REQ-1882, amended 2026-09-29).

pgwire, Bolt and Arrow Flight each serve a connection (Flight: an RPC) on a dedicated OS thread.
The request runs ENTIRELY on that thread: its coroutines execute on an event loop the thread owns
and runs itself via ``loop.run_until_complete`` — no second thread, no hop to a shared loop. Two
connections therefore govern and execute in parallel instead of queueing on one process loop.

A :class:`ConnectionLoop` is checked out of :data:`LOOP_POOL` for the life of a connection (pgwire,
Bolt) or an RPC (Flight, whose handler threads are recycled between calls) and checked back in
when it ends. A checked-out loop is run by exactly one thread; an idle one is run by nobody. Idle
loops are reused rather than rebuilt per connection; they are closed once idle past
:data:`_IDLE_TTL_S` or beyond :data:`_MAX_IDLE`.

The default executor of a connection loop runs work INLINE on the connection thread: the loop
serves one connection, so a blocking call there blocks nobody else, and ``run_in_executor`` /
``asyncio.to_thread`` inside the request stay on the request's thread too.

Shared resources — the control-plane and source pools, the Redis client — are synchronous and
thread-safe, one per worker, shared by every request thread (REQ-1882). What remains loop-bound is
asyncio machinery itself:

- :class:`CrossLoopLock` replaces a process-global ``asyncio.Lock`` whose holders may now be on
  different loops and threads.
- Work that outlives the request never runs on the process (front) loop, which only accepts
  connections and relays ASGI receive/send — a blocking call there would stall every request.
  :func:`spawn_background` runs a coroutine on a bounded pool of background worker threads, each
  with its own connection loop; :func:`spawn_after` defers one to a timer thread without holding a
  worker during the delay; :func:`spawn_long_lived` gives a loop that runs for the process (a CDC
  listener, a reaper) a dedicated thread. :func:`shutdown_background` stops all three.
"""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import collections
import concurrent.futures
import contextvars
import logging
import threading
import time
import weakref

from provisa.core import request_deadline
from collections.abc import Callable, Coroutine, Generator
from contextlib import contextmanager
from types import TracebackType
from typing import Any, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")

# Idle connection loops kept for reuse, and how long one may sit idle before it is closed.
_MAX_IDLE = 64
_IDLE_TTL_S = 300.0

_REGISTRY_LOCK = threading.Lock()
# Every live connection loop.
_LIVE: "weakref.WeakSet[asyncio.AbstractEventLoop]" = weakref.WeakSet()

_bound = threading.local()

# The process (ASGI) loop. It accepts connections and relays ASGI I/O; no blocking work runs on it.
_process_loop: asyncio.AbstractEventLoop | None = None


class _InlineExecutor(concurrent.futures.ThreadPoolExecutor):
    """Runs submitted work immediately on the submitting thread (the connection thread).

    Subclasses ThreadPoolExecutor only because ``loop.set_default_executor`` requires one; it
    never starts a worker thread — ``submit`` is replaced outright."""

    def submit(self, fn, /, *args, **kwargs):  # type: ignore[override]
        fut: concurrent.futures.Future[Any] = concurrent.futures.Future()
        try:
            fut.set_result(fn(*args, **kwargs))
        except BaseException as exc:  # handed to the awaiting coroutine, which re-raises it
            fut.set_exception(exc)
        return fut

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        del wait, cancel_futures  # nothing runs asynchronously, so there is nothing to wait for


def is_connection_loop(loop: asyncio.AbstractEventLoop) -> bool:
    with _REGISTRY_LOCK:
        return loop in _LIVE


class ConnectionLoop:
    """An event loop run only by the connection thread that has it checked out."""

    def __init__(self) -> None:
        loop = asyncio.new_event_loop()
        loop.set_default_executor(_InlineExecutor())
        self.loop = loop
        self._owner: int | None = None
        self._idle_since = time.monotonic()
        with _REGISTRY_LOCK:
            _LIVE.add(loop)

    @property
    def owner(self) -> int | None:
        return self._owner

    def _claim(self) -> None:
        if self.loop.is_running():
            raise RuntimeError("connection loop is running on another thread")
        self._owner = threading.get_ident()

    def run(
        self,
        coro: Coroutine[Any, Any, T],
        *,
        timeout: float | None = None,
        context: contextvars.Context | None = None,
    ) -> T:
        """Run ``coro`` to completion on this loop, on the calling (owning) thread.

        ``context``: run the task IN that context (not a copy), so ContextVars set by one run are
        seen by the next — a streamed RPC advanced one message per run keeps its bindings.

        ``timeout`` is the request's budget: a :mod:`provisa.core.request_deadline` watchdog cancels
        whatever blocking statement is in flight when it expires (an asyncio timer alone cannot
        fire while inline blocking work holds this thread), and ``wait_for`` enforces it at the
        coroutine's await points."""
        if self._owner != threading.get_ident():
            coro.close()
            raise RuntimeError("connection loop run from a thread that has not checked it out")
        if timeout is None:
            return self._run_body(coro, context)
        with request_deadline.within(timeout) as dl:
            if context is not None:
                context.run(request_deadline.bind, dl)
            try:
                return self._run_body(asyncio.wait_for(coro, dl.remaining()), context)
            except TimeoutError as exc:
                request_deadline.let_go(exc)  # what the interrupted frames held is released now
                # wait_for raises a bare TimeoutError() whose str() is empty — a client then sees
                # a blank error. When the request's budget is what ran out, say so.
                if dl.remaining() <= 0:
                    raise dl.expired_error() from exc
                raise

    def _run_body(self, body: Coroutine[Any, Any, T], context: contextvars.Context | None) -> T:
        if context is None:
            return self.loop.run_until_complete(body)
        return self.loop.run_until_complete(self.loop.create_task(body, context=context))

    def _reap_detached(self) -> None:
        """Cancel tasks a finished request left behind on this loop.

        A connection loop only runs while a request is being served, so a task spawned with a bare
        ``create_task`` would be frozen between requests and destroyed with the loop. Work that
        must outlive the request goes through :func:`spawn_background`; anything still pending
        here is a spawn site that did not, and is reported as the defect it is."""
        pending = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
        if not pending:
            return
        log.error(
            "connection loop left %d detached task(s) pending at request end; cancelling "
            "(spawn work that outlives a request with provisa.core.connection_loop."
            "spawn_background): %s",
            len(pending),
            [t.get_name() for t in pending],
        )
        for t in pending:
            t.cancel()
        self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

    def close(self) -> None:
        """Close the loop. Owning thread only."""
        if self._owner != threading.get_ident():
            raise RuntimeError("connection loop closed from a thread that has not checked it out")
        loop = self.loop
        if loop.is_closed():
            return
        self._reap_detached()
        with _REGISTRY_LOCK:
            _LIVE.discard(loop)
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        self._owner = None


class ConnectionLoopPool:
    """Idle :class:`ConnectionLoop` s, checked out by a connection thread for its lifetime."""

    def __init__(self, *, max_idle: int = _MAX_IDLE, idle_ttl_s: float = _IDLE_TTL_S) -> None:
        self._lock = threading.Lock()
        self._idle: list[ConnectionLoop] = []
        self._max_idle = max_idle
        self._idle_ttl_s = idle_ttl_s

    def checkout(self) -> ConnectionLoop:
        with self._lock:
            cl = self._idle.pop() if self._idle else None
        if cl is None:
            cl = ConnectionLoop()
        cl._claim()
        return cl

    def checkin(self, cl: ConnectionLoop) -> None:
        if cl.owner != threading.get_ident():
            raise RuntimeError("connection loop checked in by a thread that does not own it")
        cl._reap_detached()
        now = time.monotonic()
        with self._lock:
            stale = [c for c in self._idle if now - c._idle_since > self._idle_ttl_s]
            self._idle = [c for c in self._idle if now - c._idle_since <= self._idle_ttl_s]
            keep = len(self._idle) < self._max_idle
            if keep:
                cl._owner = None
                cl._idle_since = now
                self._idle.append(cl)
        # Loops removed from the idle list are unreachable by any other thread, so this one may
        # claim and close them.
        for c in stale:
            c._claim()
            c.close()
        if not keep:
            cl.close()

    def close_idle(self) -> None:
        """Close every idle loop (process shutdown)."""
        with self._lock:
            idle, self._idle = self._idle, []
        for c in idle:
            c._claim()
            c.close()

    def idle_count(self) -> int:
        with self._lock:
            return len(self._idle)


LOOP_POOL = ConnectionLoopPool()


@contextmanager
def bound(cl: ConnectionLoop) -> Generator[ConnectionLoop]:
    """Bind ``cl`` as this thread's connection loop for the block (claims it for this thread)."""
    previous = getattr(_bound, "cl", None)
    if previous is not None and previous is not cl:
        raise RuntimeError("a different connection loop is already bound to this thread")
    if cl.owner != threading.get_ident():
        cl._claim()
    _bound.cl = cl
    try:
        yield cl
    finally:
        _bound.cl = previous


@contextmanager
def connection_loop() -> Generator[ConnectionLoop]:
    """Check a loop out for the block and bind it to this thread; check it back in after."""
    shield = request_deadline.shielded()
    cl: ConnectionLoop | None = None
    try:
        # Checking the loop out and back in are each a section the request's deadline does not
        # interrupt (REQ-1905, request_deadline.shielded).
        with shield.lock:
            shield.settle()
            cl = LOOP_POOL.checkout()
        with bound(cl):
            yield cl
    finally:
        with shield.lock:
            shield.settle()
            if cl is not None:
                LOOP_POOL.checkin(cl)


def current_connection_loop() -> ConnectionLoop:
    cl = getattr(_bound, "cl", None)
    if cl is None:
        raise RuntimeError("no connection loop is bound to this thread")
    return cl


def run_on_connection_loop(coro: Coroutine[Any, Any, T], *, timeout: float | None = None) -> T:
    """Run ``coro`` on this thread's bound connection loop, on this thread."""
    try:
        cl = current_connection_loop()
    except RuntimeError:
        coro.close()
        raise
    return cl.run(coro, timeout=timeout)


def run_on_own_thread(
    make_coro: Callable[[], Coroutine[Any, Any, T]], *, name: str
) -> concurrent.futures.Future[T]:
    """Run ``make_coro()`` on a NEW thread with its own connection loop; return its result future.

    For work a request fans out in parallel (each unit on its own thread, one task per loop —
    sibling tasks on one loop can deadlock under the inline executor). Unlike ``spawn_background``
    the future carries the coroutine's result or exception, and a dedicated thread (not the
    bounded background pool) is used so units that await one another cannot starve the pool.
    The coroutine runs in a copy of the caller's context. Await it with ``asyncio.wrap_future``."""
    ctx = contextvars.copy_context()
    fut: concurrent.futures.Future[T] = concurrent.futures.Future()

    def _target() -> None:
        if not fut.set_running_or_notify_cancel():
            return
        try:
            with connection_loop() as cl:
                fut.set_result(cl.run(make_coro(), context=ctx))
        except BaseException as exc:  # handed to whoever awaits the future, which re-raises it
            fut.set_exception(exc)

    threading.Thread(target=_target, name=name, daemon=True).start()
    return fut


# --------------------------------------------------------------------------------------------- #
# Cross-loop lock
# --------------------------------------------------------------------------------------------- #


class CrossLoopLock:
    """A FIFO async lock whose holder and waiters may be on different event loops and threads.

    ``asyncio.Lock`` binds to one loop on first contention and is not thread-safe, so a
    process-global one breaks the moment two connection loops contend for it. Here a thread lock
    guards the state, each waiter waits on a future of its OWN loop, and release hands the lock
    straight to the next waiter via ``call_soon_threadsafe`` on that waiter's loop."""

    def __init__(self) -> None:
        self._mutex = threading.Lock()
        self._held = False
        self._waiters: collections.deque[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = (
            collections.deque()
        )

    def locked(self) -> bool:
        return self._held

    async def acquire(self) -> bool:
        loop = asyncio.get_running_loop()
        with self._mutex:
            if not self._held and not self._waiters:
                self._held = True
                return True
            fut: asyncio.Future[None] = loop.create_future()
            self._waiters.append((loop, fut))
        try:
            await fut
        except asyncio.CancelledError:
            with self._mutex:
                granted = fut.done() and not fut.cancelled()
                if not granted and (loop, fut) in self._waiters:
                    self._waiters.remove((loop, fut))
                # Otherwise the waiter was already chosen: _grant sees the cancelled future
                # and passes the lock on.
            if granted:
                self.release()
            raise
        return True

    def release(self) -> None:
        with self._mutex:
            if not self._held:
                raise RuntimeError("CrossLoopLock.release() on an unlocked lock")
            self._hand_off_locked()

    def _hand_off_locked(self) -> None:
        while self._waiters:
            loop, fut = self._waiters.popleft()
            try:
                loop.call_soon_threadsafe(self._grant, fut)
            except RuntimeError:
                # The waiter's loop closed while it waited: the waiter is gone with it, so the
                # lock goes to the next one.
                continue
            return  # _held stays True: ownership passes directly to that waiter
        self._held = False

    def _grant(self, fut: asyncio.Future[None]) -> None:
        if fut.cancelled():
            with self._mutex:
                self._hand_off_locked()
            return
        fut.set_result(None)

    async def __aenter__(self) -> "CrossLoopLock":
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        del exc_type, exc, tb
        self.release()


# --------------------------------------------------------------------------------------------- #
# Work that outlives the request (REQ-1882, amended 2026-09-29: background work off the front loop)
# --------------------------------------------------------------------------------------------- #

# Background worker threads. A detached unit of work (a hot-cache promote, a TTL drop, an NL job,
# a scheduled job) runs to completion on one of these, on the worker's own connection loop. The
# control plane and source drivers block their calling thread, so this work must never run on the
# process loop. Configured once at startup (server.background_workers); the documented default
# applies to processes that never configure it (the CLI, tests).
DEFAULT_BACKGROUND_WORKERS = 16
_bg_lock = threading.Lock()
_bg_workers = DEFAULT_BACKGROUND_WORKERS
_bg_pool: concurrent.futures.ThreadPoolExecutor | None = None
_bg_inflight: "dict[concurrent.futures.Future[Any], tuple[str, _TaskSlot]]" = {}
_bg_timer: "_TimerThread | None" = None
_bg_long_lived: "set[LongLived]" = set()


def set_process_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Record the process (ASGI) loop. Called once at startup."""
    global _process_loop
    _process_loop = loop


def process_loop() -> asyncio.AbstractEventLoop:
    """The registered process loop (see :func:`set_process_loop`)."""
    loop = _process_loop
    if loop is None or loop.is_closed():
        raise RuntimeError(
            "no process loop is registered (provisa.core.connection_loop.set_process_loop)"
        )
    return loop


def configure_background_workers(workers: int) -> None:
    """Size the background pool. Must run before the first background submission."""
    global _bg_workers
    if workers < 1:
        raise ValueError(f"server.background_workers must be >= 1, got {workers}")
    with _bg_lock:
        if _bg_pool is not None and workers != _bg_workers:
            raise RuntimeError(
                "background pool already started with "
                f"{_bg_workers} workers; configure it before first use"
            )
        _bg_workers = workers


def _pool() -> concurrent.futures.ThreadPoolExecutor:
    global _bg_pool
    with _bg_lock:
        if _bg_pool is None:
            _bg_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=_bg_workers, thread_name_prefix="provisa-bg"
            )
        return _bg_pool


class _TaskSlot:
    """The task a background thread runs on its own connection loop, cancellable from any thread.

    Shutdown cancels running work through this before the databases and engines it uses close:
    a replica build still streaming when its Flight client was closed, or still connecting when
    its pools were, died in native code and took the process with it. Cancellation lands at the
    task's next await -- between batches, after a bounded connect."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[Any] | None = None
        self._cancel_requested = False

    def run(
        self,
        cl: "ConnectionLoop",
        coro: Coroutine[Any, Any, Any],
        name: str,
        ctx: contextvars.Context,
    ) -> None:
        """Run ``coro`` to completion as ``cl``'s task; a cancellation ends it quietly."""
        with self._lock:
            task = cl.loop.create_task(coro, name=name, context=ctx)
            self._loop, self._task = cl.loop, task
            if self._cancel_requested:
                task.cancel()
        try:
            cl.loop.run_until_complete(task)
        except asyncio.CancelledError:
            log.info("background task %s cancelled at shutdown", name)
        finally:
            with self._lock:
                self._loop = None

    def cancel(self) -> None:
        with self._lock:
            self._cancel_requested = True
            loop, task = self._loop, self._task
        if loop is not None and task is not None:
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass  # the loop closed as the task finished on its own — nothing left to cancel


def _run_detached(
    coro: Coroutine[Any, Any, Any], name: str, ctx: contextvars.Context, slot: _TaskSlot
) -> None:
    """Worker-thread body: run ``coro`` to completion on this worker's own connection loop."""
    try:
        with connection_loop() as cl:
            slot.run(cl, coro, name, ctx)
    except BaseException:
        log.exception("background task %s failed", name)


def _submit(
    coro: Coroutine[Any, Any, Any], name: str, ctx: contextvars.Context
) -> concurrent.futures.Future[None]:
    slot = _TaskSlot()
    try:
        fut = _pool().submit(_run_detached, coro, name, ctx, slot)
    except RuntimeError:
        coro.close()
        raise
    with _bg_lock:
        _bg_inflight[fut] = (name, slot)
    fut.add_done_callback(_forget_inflight)
    return fut


def _forget_inflight(fut: concurrent.futures.Future[Any]) -> None:
    with _bg_lock:
        _bg_inflight.pop(fut, None)


def _caller_context() -> contextvars.Context:
    """A copy of the caller's context for work that OUTLIVES it: its org binding and audit
    identity travel, its request deadline does not (REQ-1905). Every request now carries one
    deadline for its whole life; detached work that kept it would have its statements refused
    once that request's timeout had passed — a TTL drop scheduled hours ahead, a cache write
    finishing after the response left. Nor does its model change (REQ-1524): that closes when the
    caller finishes, so detached work that writes the model opens a change of its own."""
    from provisa.core import model_change

    ctx = contextvars.copy_context()
    ctx.run(request_deadline.unbind)
    ctx.run(model_change.unbind)
    return ctx


def spawn_background(
    coro: Coroutine[Any, Any, Any], *, name: str | None = None
) -> concurrent.futures.Future[None]:
    """Run ``coro`` to completion on a background worker thread, detached from the caller.

    The coroutine runs on the worker's own connection loop, in a copy of the caller's context
    (org binding, audit identity). When every worker is busy the submission queues; it is never
    dropped. A failure is logged with its name. The returned future resolves when the work ends
    (``None`` either way — the failure is reported by the log, not re-raised to a caller that
    has moved on); await it from a loop with ``asyncio.wrap_future``."""
    label = name or getattr(coro, "__qualname__", "background")
    return _submit(coro, label, _caller_context())


def spawn_after(delay: float, coro: Coroutine[Any, Any, Any], *, name: str) -> None:
    """Run ``coro`` on a background worker once ``delay`` seconds have passed.

    The delay is held by one timer thread, not by a worker: a TTL of hours pins nothing."""
    ctx = _caller_context()
    _timer().schedule(time.monotonic() + max(0.0, delay), coro, name, ctx)


class _TimerThread:
    """One thread holding every pending :func:`spawn_after` entry, earliest deadline first."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._heap: list[tuple[float, int, Coroutine[Any, Any, Any], str, contextvars.Context]] = []
        self._seq = 0
        self._stopped = False
        self._thread = threading.Thread(target=self._main, name="provisa-bg-timer", daemon=True)
        self._thread.start()

    def schedule(
        self, due: float, coro: Coroutine[Any, Any, Any], name: str, ctx: contextvars.Context
    ) -> None:
        import heapq

        with self._cond:
            if self._stopped:
                coro.close()
                raise RuntimeError(f"cannot schedule {name!r}: background work is shut down")
            self._seq += 1
            heapq.heappush(self._heap, (due, self._seq, coro, name, ctx))
            self._cond.notify()

    def _main(self) -> None:
        import heapq

        while True:
            with self._cond:
                while not self._stopped and (not self._heap or self._heap[0][0] > time.monotonic()):
                    wait = None if not self._heap else self._heap[0][0] - time.monotonic()
                    self._cond.wait(wait)
                if self._stopped:
                    return
                _due, _seq, coro, name, ctx = heapq.heappop(self._heap)
            try:
                _submit(coro, name, ctx)
            except RuntimeError:
                log.exception("delayed background task %s could not be started", name)

    def stop(self) -> list[str]:
        """Stop the timer; returns the names of the entries that never came due."""
        with self._cond:
            self._stopped = True
            pending, self._heap = self._heap, []
            self._cond.notify()
        for _due, _seq, coro, _name, _ctx in pending:
            coro.close()
        self._thread.join(timeout=5)
        return [name for _due, _seq, _coro, name, _ctx in pending]

    def pending(self) -> int:
        with self._cond:
            return len(self._heap)


def _timer() -> _TimerThread:
    global _bg_timer
    with _bg_lock:
        if _bg_timer is None:
            _bg_timer = _TimerThread()
        return _bg_timer


class LongLived:
    """A coroutine that runs for the life of the process on its own dedicated thread.

    The thread owns a connection loop and runs the coroutine as that loop's one task. ``cancel``
    is thread-safe (it cancels the task on its own loop); ``join`` waits for the thread."""

    def __init__(self, coro: Coroutine[Any, Any, Any], name: str, ctx: contextvars.Context):
        self.name = name
        self._coro: Coroutine[Any, Any, Any] | None = coro
        self._ctx = ctx
        self._slot = _TaskSlot()
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._main, name=f"provisa-bg:{name}", daemon=True)

    def start(self) -> "LongLived":
        self._thread.start()
        return self

    def _main(self) -> None:
        try:
            with connection_loop() as cl:
                coro, self._coro = self._coro, None
                assert coro is not None
                try:
                    self._slot.run(cl, coro, self.name, self._ctx)
                except BaseException:
                    log.exception("long-lived task %s failed", self.name)
        finally:
            self._done.set()
            with _bg_lock:
                _bg_long_lived.discard(self)

    def cancel(self) -> None:
        self._slot.cancel()

    def done(self) -> bool:
        return self._done.is_set()

    def join(self, timeout: float | None = None) -> bool:
        return self._done.wait(timeout)

    async def wait(self, timeout: float | None = None) -> bool:
        """Await the thread's end from any event loop without blocking it."""
        return await asyncio.to_thread(self._done.wait, timeout)


def spawn_long_lived(coro: Coroutine[Any, Any, Any], *, name: str) -> LongLived:
    """Start ``coro`` on a dedicated thread (see :class:`LongLived`); stopped at shutdown."""
    handle = LongLived(coro, name, _caller_context())
    with _bg_lock:
        _bg_long_lived.add(handle)
    return handle.start()


def shutdown_background(timeout: float = 10.0) -> None:
    """Stop background work (process shutdown): drop delayed entries not yet due, cancel queued
    submissions, cancel running ones and long-lived threads and give them ``timeout`` seconds to
    end, and log whatever did not finish. Resets so a later start (tests re-running the lifespan) begins clean."""
    global _bg_pool, _bg_timer
    with _bg_lock:
        timer, _bg_timer = _bg_timer, None
        pool, _bg_pool = _bg_pool, None
        long_lived = list(_bg_long_lived)
        inflight = dict(_bg_inflight)
    if timer is not None:
        dropped = timer.stop()
        if dropped:
            log.warning(
                "shutdown: %d delayed background task(s) dropped: %s", len(dropped), dropped
            )
    for handle in long_lived:
        handle.cancel()
    deadline = time.monotonic() + timeout
    if pool is not None:
        cancelled = [name for fut, (name, _slot) in inflight.items() if fut.cancel()]
        if cancelled:
            log.warning(
                "shutdown: %d queued background task(s) cancelled: %s", len(cancelled), cancelled
            )
        running = {fut: name for fut, (name, _slot) in inflight.items() if not fut.cancelled()}
        # Running work is cancelled too, and waited for: what it uses closes once this returns.
        for fut in running:
            inflight[fut][1].cancel()
        _done, not_done = concurrent.futures.wait(
            running, timeout=max(0.0, deadline - time.monotonic())
        )
        if not_done:
            log.warning(
                "shutdown: %d background task(s) still running: %s",
                len(not_done),
                [running[f] for f in not_done],
            )
        pool.shutdown(wait=False, cancel_futures=True)
    for handle in long_lived:
        if not handle.join(max(0.0, deadline - time.monotonic())):
            log.warning("shutdown: long-lived task %s did not stop in time", handle.name)


async def run_lifecycle_work(coro: Coroutine[Any, Any, Any], *, name: str) -> None:
    """Await process-lifecycle wiring inline at startup; detach it everywhere else.

    Called during lifespan startup (on the process loop, before any request is served), the wiring
    runs inline so boot is ordered. Called from a request or background thread's connection loop (an
    org runtime built on first access, a schema rebuild), it runs on a background worker and the
    caller continues — it does not depend on the wiring finishing, and blocking control-plane reads
    inside it must not run on the process loop once requests are being served."""
    if is_connection_loop(asyncio.get_running_loop()):
        spawn_background(coro, name=name)
        return
    await coro
