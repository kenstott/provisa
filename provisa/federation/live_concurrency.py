# Copyright (c) 2026 Kenneth Stott
# Canary: 5e81c3a4-2f7b-4d19-8a06-9b3c7e1d4f25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-source cap on concurrent LIVE reads (REQ-1909).

A source's ``max_live_concurrency`` bounds how many queries read it live at once. A permit is a
lease in a Redis sorted set keyed per (org, source): the member is the permit's token and the score
its lease expiry, read off the Redis server's own clock so every instance agrees on it. Acquiring
drops expired leases and adds one only while fewer than the cap remain, inside a WATCH/MULTI
transaction; a holder renews its leases while its query runs, so a permit held by a crashed
instance lapses on its own. With a real Redis the cap is cluster-wide; with embedded fakeredis (no
Redis URL) it holds per process.

Everything here runs on the request's own thread (REQ-1882): acquiring blocks that thread, which
serves only its own request, and the wait is bounded by the request's remaining deadline.
"""

# Requirements: REQ-1909

from __future__ import annotations

import logging
import threading
import time
import uuid
import weakref
from collections.abc import Iterable, Iterator
from typing import Any

log = logging.getLogger(__name__)

# A lease outlives its last renewal by LEASE_S; the renewer thread renews every RENEW_EVERY_S, so a
# live holder's lease never lapses while a crashed holder's does within LEASE_S.
LEASE_S = 30.0
RENEW_EVERY_S = LEASE_S / 3
# REQ-1909: a waiter with no request deadline (a background caller) still waits a bounded time —
# the same bound the pooled engine connections use (pg_runtime._POOL_WAIT_S).
NO_DEADLINE_WAIT_S = 120.0
# Poll interval while waiting for a permit: short first so an uncontended hand-off is fast, then
# capped so a long queue does not hammer Redis.
_POLL_START_S = 0.02
_POLL_MAX_S = 0.25


class LiveConcurrencyExceeded(TimeoutError):
    """No permit freed on a capped source before the request's wait bound (REQ-1909)."""

    def __init__(self, source_id: str, cap: int, waited_s: float) -> None:
        self.source_id = source_id
        self.cap = cap
        super().__init__(
            f"source {source_id!r} is at its live-read cap of {cap} concurrent "
            f"quer{'y' if cap == 1 else 'ies'} (max_live_concurrency); no permit freed within "
            f"{waited_s:.1f}s (REQ-1909)"
        )


class LivePermitStore:
    """The permit sets for every capped source, on one Redis (real or embedded)."""

    def __init__(self, redis_url: str | None) -> None:
        self._url = redis_url
        self._client: Any = None
        self._client_lock = threading.Lock()
        self._held: dict[tuple[str, str], int] = {}  # (key, token) -> holder count (always 1)
        self._held_lock = threading.Lock()
        self._renewer: threading.Thread | None = None

    @property
    def cluster_wide(self) -> bool:
        return bool(self._url)

    def _redis(self) -> Any:
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    from provisa.core.redis_factory import make_redis

                    self._client = make_redis(self._url, decode_responses=True).client()
        return self._client

    @staticmethod
    def key(org_id: str | None, source_id: str) -> str:
        return f"provisa:live_permits:{org_id or '-'}:{source_id}"

    def _now(self, r: Any) -> float:
        secs, micros = r.time()
        return float(secs) + float(micros) / 1_000_000

    def try_acquire(self, key: str, cap: int) -> str | None:
        """One attempt: a new lease token when fewer than ``cap`` unexpired leases exist, else None."""
        from provisa.core.redis_factory import watch_error

        conflict = watch_error()
        r = self._redis()
        token = uuid.uuid4().hex
        while True:
            with r.pipeline() as pipe:
                try:
                    pipe.watch(key)
                    now = self._now(r)
                    pipe.zremrangebyscore(key, "-inf", now)
                    if pipe.zcard(key) >= cap:
                        pipe.unwatch()
                        return None
                    pipe.multi()
                    pipe.zadd(key, {token: now + LEASE_S})
                    pipe.execute()
                except conflict:
                    continue  # another holder changed the set between WATCH and EXEC; re-read it
            self._track(key, token)
            return token

    def release(self, key: str, token: str) -> None:
        with self._held_lock:
            self._held.pop((key, token), None)
        self._redis().zrem(key, token)

    def holders(self, key: str) -> int:
        """Unexpired leases on ``key`` (for tests and the admin view)."""
        r = self._redis()
        return int(r.zcount(key, self._now(r), "+inf"))

    def _track(self, key: str, token: str) -> None:
        with self._held_lock:
            self._held[(key, token)] = 1
            if self._renewer is None or not self._renewer.is_alive():
                self._renewer = threading.Thread(
                    target=self._renew_loop, name="live-permit-renewer", daemon=True
                )
                self._renewer.start()

    def renew_all(self) -> None:
        """Push every lease this process holds LEASE_S past now (the renewer thread's one step)."""
        with self._held_lock:
            held = list(self._held)
        if not held:
            return
        r = self._redis()
        now = self._now(r)
        for key, token in held:
            # XX: renew only a lease that still exists. A lease already dropped (Redis lost it, or
            # this process stalled past LEASE_S) is not re-created: that could exceed the cap.
            if not r.zadd(key, {token: now + LEASE_S}, xx=True, ch=True):
                log.error(
                    "live-read permit %s on %s lapsed before release; the cap may briefly be "
                    "exceeded (REQ-1909)",
                    token,
                    key,
                )
                with self._held_lock:
                    self._held.pop((key, token), None)

    def _renew_loop(self) -> None:
        while True:
            time.sleep(RENEW_EVERY_S)
            with self._held_lock:
                if not self._held:
                    self._renewer = None
                    return
            try:
                self.renew_all()
            except Exception:
                # The renewer must outlive one failed round (a Redis blip); the next round retries
                # every lease, and a lease that lapsed meanwhile is reported by renew_all itself.
                log.exception("live-read permit renewal failed (REQ-1909)")


class LivePermits:
    """The permits one query holds, one per capped live source; release them exactly once."""

    def __init__(self, store: LivePermitStore | None, held: list[tuple[str, str]]) -> None:
        self._store = store
        self._held = held
        self._released = False

    @property
    def count(self) -> int:
        return len(self._held)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        if self._store is None:
            return  # no permit was taken (the read touches no capped live source)
        for key, token in reversed(self._held):
            self._store.release(key, token)

    def __enter__(self) -> LivePermits:
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

    async def __aenter__(self) -> LivePermits:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.release()

    def guard(self, rows: Iterable[Any]) -> Iterator[Any]:
        """``rows`` (e.g. Arrow record batches) yielded through; the permits release when the
        stream is drained, fails, or is closed early — and, for a stream abandoned before its first
        pull (whose ``finally`` never runs), when the generator is garbage-collected."""
        if not self._held:
            return iter(rows)

        def _gen() -> Iterator[Any]:
            try:
                yield from rows
            finally:
                self.release()

        gen = _gen()
        weakref.finalize(gen, self.release)
        return gen

    def wrap_stream(self, stream: Any) -> Any:
        """A ``ResultStream`` whose permits release when it drains, fails, or closes (REQ-1909).
        Closing the wrapper still closes ``stream``'s own source (server-side cursor, pooled
        connection), exactly as the unwrapped stream would."""
        if not self._held:
            return stream
        from provisa.executor.result import StreamingQueryResult

        source_close = getattr(stream, "close", None)

        def _release() -> None:
            try:
                if source_close is not None:
                    source_close()
            finally:
                self.release()

        return StreamingQueryResult(
            self.guard(stream.batches()),
            column_names=list(stream.column_names),
            column_types=stream.column_types,
            on_release=_release,
        )


def acquire(
    store: LivePermitStore, org_id: str | None, capped: list[tuple[str, int]]
) -> LivePermits:
    """Acquire one permit per ``(source_id, cap)`` in sorted source-id order (no deadlock between
    two queries each holding what the other needs). Waits bounded by the request's remaining
    deadline (NO_DEADLINE_WAIT_S without one), then raises LiveConcurrencyExceeded — never a
    fallback to another route (REQ-1909)."""
    from provisa.core import request_deadline

    held: list[tuple[str, str]] = []
    permits = LivePermits(store, held)
    try:
        for source_id, cap in sorted(capped):
            key = LivePermitStore.key(org_id, source_id)
            budget = request_deadline.remaining()
            bound = NO_DEADLINE_WAIT_S if budget is None else budget
            started = time.monotonic()
            pause = _POLL_START_S
            while True:
                token = store.try_acquire(key, cap)
                if token is not None:
                    held.append((key, token))
                    break
                waited = time.monotonic() - started
                if waited >= bound:
                    raise LiveConcurrencyExceeded(source_id, cap, waited)
                time.sleep(min(pause, bound - waited))
                pause = min(pause * 2, _POLL_MAX_S)
    except BaseException:
        permits.release()
        raise
    return permits


def _live_source_ids(
    state: Any, plan: Any, sources_by_id: dict[str, Any], replicated: set[str]
) -> list[str]:
    """The sources ``plan`` reads LIVE: the DIRECT / API route's one source, or the ENGINE route's
    sources (of ``sources_by_id``) the bound engine reads in place through its attach connector
    and that are not in ``replicated`` — the sources whose tables this statement reads are put on
    their replicas by the operator's settings (REQ-826/REQ-1141). The same classification
    query_residency.ensure_resident lands by, so the two never disagree."""
    from provisa.federation.strategy import engine_attaches
    from provisa.transpiler.router import Route

    if plan.route in (Route.DIRECT, Route.API):
        # DIRECT dials the source; an API route may call the source's upstream for this read.
        return [plan.source_id] if plan.source_id else []
    if plan.route != Route.ENGINE:
        return []
    engine = getattr(state, "federation_engine", None)
    out: list[str] = []
    for sid in plan.sources:
        src = sources_by_id.get(sid)
        if src is None or sid in replicated:
            continue
        if engine_attaches(engine, src.type.value):
            out.append(sid)
    return out


async def live_caps_for_plan(state: Any, plan: Any) -> tuple[str | None, list[tuple[str, int]]]:
    """``(org, [(source_id, cap), ...])`` for the capped sources ``plan`` reads live (REQ-1909).
    Async only for the registry reads; the (blocking) acquisition is :func:`acquire`, run on the
    request's own thread, so a synchronous terminal resolves the caps on its loop and then waits
    for permits on its own thread under the request's deadline.

    A source is read live unless the tables of it that the statement reads (``plan.table_ids``)
    are served from their replicas (``registry_view.operator_floor``, the floor routing applied
    to this same statement)."""
    from provisa.core.request_context import current_org
    from provisa.federation.registry_view import operator_floor, registered_sources

    org_id = current_org.get(None)
    wanted = set(plan.sources) | ({plan.source_id} if plan.source_id else set())
    capped_sources = {
        s.id: s
        for s in await registered_sources(state)
        if s.id in wanted and s.max_live_concurrency is not None
    }
    if not capped_sources:
        return org_id, []
    caps = {
        sid: s.max_live_concurrency
        for sid, s in capped_sources.items()
        if s.max_live_concurrency is not None
    }
    # A source is off the live path for this statement only when EVERY table of it the statement
    # reads is on its replica: a statement that also reads one of its tables in place still holds
    # the source's live permit.
    unfloored = state.replica_routes.unfloored
    read_in_place = {unfloored[t] for t in plan.table_ids if t in unfloored}
    replicated = set(operator_floor(state, plan.table_ids)) - read_in_place
    live = _live_source_ids(state, plan, capped_sources, replicated)
    return org_id, [(sid, caps[sid]) for sid in live if sid in caps]


def acquire_plan_permits(state: Any, plan: Any) -> LivePermits:
    """Acquire the live-read permits a pipeline plan carries (``plan.live_caps``, bound when the
    plan was minted — see ``_pipeline._attach_live_caps``), on the calling request thread, bounded
    by the request's deadline. No loop dispatch: a streaming terminal pays no extra hop (REQ-1887).
    Holds none — and touches no store — when the plan reads no capped source live (REQ-1909)."""
    if not plan.live_caps:
        return LivePermits(None, [])
    return acquire(state.live_permit_store, plan.live_caps_org, list(plan.live_caps))


async def acquire_for_route(
    state: Any, route: Any, source_id: str, sources: Iterable[str], table_ids: Iterable[int]
) -> LivePermits:
    """Acquire permits for a read that has a route decision but no pipeline plan (GraphQL's field
    executor, the REST / JSON:API helper): the caps are resolved here, then acquired on this
    request thread (REQ-1909). ``table_ids``: the registered tables the statement reads."""
    from types import SimpleNamespace

    view = SimpleNamespace(
        route=route,
        source_id=source_id or "",
        sources=frozenset(sources),
        table_ids=frozenset(table_ids),
    )
    org_id, capped = await live_caps_for_plan(state, view)
    if not capped:
        return LivePermits(None, [])
    return acquire(state.live_permit_store, org_id, capped)
