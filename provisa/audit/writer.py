# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0c8a61-3e7b-4f29-9b14-6a2f7c1e8d35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The audit row's INSERT, taken off the request path (REQ-074/REQ-1386).

A request thread that finishes a governed statement ENQUEUES its finished audit record and
returns; one writer thread per process inserts the queued records in batches. The request no
longer pays the control-plane round trips (checkout, INSERT, COMMIT) for a row it never reads.

No record is lost quietly:

- the queue is bounded. When it is full the enqueue WAITS — for the request's remaining deadline —
  and then raises :class:`AuditQueueFull`. A record is never dropped to make room.
- a batch whose insert fails is logged at error and retried until it lands; while it is held the
  writer takes no more than a batch's worth of further records, so a store that stays down fills
  the queue and the requests behind it fail loudly rather than run unaudited.
- shutdown (and a worker's exit, which runs the same lifespan shutdown) writes whatever is queued
  before the control-plane pools close. A failing insert is retried only for the shutdown budget;
  records still unwritten then are logged at error with their count and the writer stops — it
  does not outlive its application retrying a store that is gone.

The writer's lifetime is the application's: ``start_audit_writer`` at startup (the lifespan),
``shutdown_audit_writer`` at shutdown. A request only enqueues; it never starts the writer, and an
enqueue with no writer running raises.

Durability: a record is in memory from the enqueue until its batch commits, so a hard crash
(SIGKILL, power loss) loses the records enqueued within the last batch window — at most
``batch_size`` per window while the writer keeps up, and never more than the queue's capacity.

Each worker process has its own queue and writer; rows from every worker land in the same
``query_audit_log``, each stamped with the time its statement finished, not the time of its batch.
"""

# Requirements: REQ-074, REQ-689, REQ-1386, REQ-1454, REQ-1882

from __future__ import annotations

import asyncio
import atexit
import contextvars
import hashlib
import logging
import queue
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from provisa.core import request_deadline

if TYPE_CHECKING:
    from provisa.core.connection_loop import LongLived
    from provisa.encryption import EncryptionService

log = logging.getLogger(__name__)

# Records the queue holds before an enqueue has to wait.
QUEUE_CAPACITY = 10_000
# Records one INSERT carries.
BATCH_SIZE = 500
# How long the writer gathers a batch after its first record arrives: the longest a record waits
# in memory while the writer is keeping up.
BATCH_INTERVAL_S = 0.05
# Pause before a failed batch is tried again.
RETRY_INTERVAL_S = 1.0
# The enqueue's wait on a full queue when the caller has no request deadline bound (a denial
# written by a scheduled job): the deployment's control-plane pool wait, the same bound an
# unbudgeted synchronous INSERT waited for a connection.
_NO_DEADLINE_WAIT_S = 30.0
# How often a gathering writer looks at its stop flag.
_STOP_POLL_S = 0.05


class AuditQueueFull(RuntimeError):
    """The audit queue stayed full for the request's whole remaining deadline."""


@dataclass(frozen=True)
class AuditRecord:
    """One finished statement's audit row, plus where it goes and what it meters."""

    record_db: Any  # Any: the org's record handle in this region (query_audit_log, REQ-1922)
    tenant_id: str | None
    user_id: str
    role_id: str
    query_text: str
    # Resolved ids, or a resolver the writer thread calls (see PendingAudit.table_ids).
    table_ids: "tuple[int | str, ...] | Callable[[], tuple[int, ...]]"
    source: str
    status_code: int
    duration_ms: int
    logged_at: datetime
    trace_id: str | None
    encryption: "EncryptionService"
    # How the statement was answered (cache | direct | engine) and the rows it delivered; None
    # for a refused statement, which reached neither.
    route: str | None = None
    row_count: int | None = None
    # Provenance (provisa/audit/provenance.py). ``enforced`` may be a resolver the writer thread
    # calls, like ``table_ids``.
    # Required, by keyword: every record says what it knows of these, None included.
    model_stamp: int | None = field(kw_only=True)
    # The environment the statement ran in: with ``meter_pool`` (the control plane) and the org,
    # where the writer looks up the commit the model at ``model_stamp`` equals (see _model_commit).
    model_env: str = field(kw_only=True)
    enforced: "dict[str, Any] | Callable[[], dict[str, Any]] | None" = field(kw_only=True)
    route_reason: str | None = field(kw_only=True)
    sources: tuple[str, ...] = field(kw_only=True)
    data_age: dict[str, Any] | None = field(kw_only=True)
    # REQ-1922: the region whose data answered the statement (query_audit_log.region).
    region: str = field(kw_only=True)
    # REQ-1454: the control-plane pool and org the statement is metered against; no pool = a
    # deployment without a control plane, which has nothing to meter.
    meter_pool: Any = None  # Any: the control-plane Database handle
    meter_org: str = ""
    # REQ-826: where this statement's tables are counted toward Hot replication — the
    # deployment's count store and the org environment the table ids belong to
    # (federation/replica_hot.py). No store = a caller that counts nothing (a denial).
    hot_counts: Any = None  # Any: replica_hot.HotCounts
    hot_scope: str = ""

    def row(self, model_commit: str | None) -> dict[str, Any]:
        """The ``query_audit_log`` row: query text encrypted (REQ-689), its plaintext hash kept.

        ``model_commit``: the environment repository's commit the model at ``model_stamp`` equals,
        when that is proven (None otherwise). With a commit, the columns each role could see are
        recoverable from it; without one the row keeps the visible columns it was given."""
        enforced = self.enforced() if callable(self.enforced) else self.enforced
        if model_commit is not None and enforced is not None:
            enforced = {k: v for k, v in enforced.items() if k != "visible_columns"}
        return {
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "role_id": self.role_id,
            "query_hash": hashlib.sha256(self.query_text.encode()).hexdigest(),
            "query_text_enc": self.encryption.encrypt(self.query_text.encode("utf-8")),
            "table_ids": list(self.table_ids() if callable(self.table_ids) else self.table_ids),
            "source": self.source,
            "status_code": self.status_code,
            "duration_ms": self.duration_ms,
            "route": self.route,
            "row_count": self.row_count,
            "model_stamp": self.model_stamp,
            "model_commit": model_commit,
            "enforced": enforced,
            "route_reason": self.route_reason,
            "sources": sorted(self.sources),
            "data_age": self.data_age,
            "region": self.region,
            "trace_id": self.trace_id,
            "logged_at": self.logged_at,
        }


async def _insert_rows(record_db: Any, rows: list[dict[str, Any]]) -> None:
    from provisa.audit.query_log import log_queries

    await log_queries(record_db, rows)


async def _deployed_commit(pool: Any, org_id: str, env: str, stamp: int) -> str | None:
    """The commit the environment's model equals at ``stamp``, or None when that is not proven:
    the environment stands at a commit recorded at exactly this stamp, and has not drifted."""
    from provisa.core.env_store import get_env

    row = await get_env(pool, org_id, env)
    if row is None or row["drifted"] or row["deployed_stamp"] != stamp:
        return None
    return row["deployed_sha"]


async def _meter(pool: Any, org_id: str) -> None:
    from provisa.core.commerce import meter_op

    await meter_op(pool, org_id)


def _count(records: "list[AuditRecord]") -> None:
    """Add a landed batch to its tables' Hot counts (REQ-826), one round trip per count store."""
    from provisa.core import settings_registry
    from provisa.federation.replica_hot import batch_hits

    interval = settings_registry.value("replication.hot_interval")
    by_store: dict[int, list[AuditRecord]] = {}
    for rec in records:
        if rec.hot_counts is not None:
            by_store.setdefault(id(rec.hot_counts), []).append(rec)
    for group in by_store.values():
        group[0].hot_counts.add(batch_hits(group), interval)


class AuditWriter:
    """A bounded queue of finished audit records and the one thread that inserts them."""

    def __init__(
        self,
        *,
        capacity: int = QUEUE_CAPACITY,
        batch_size: int = BATCH_SIZE,
        interval_s: float = BATCH_INTERVAL_S,
        retry_s: float = RETRY_INTERVAL_S,
        insert: Callable[[Any, list[dict[str, Any]]], Awaitable[None]] = _insert_rows,
        meter: Callable[[Any, str], Awaitable[None]] = _meter,
        deployed_commit: Callable[[Any, str, str, int], Awaitable[str | None]] = _deployed_commit,
        count: "Callable[[list[AuditRecord]], None]" = _count,
    ) -> None:
        self._queue: queue.Queue[AuditRecord] = queue.Queue(maxsize=capacity)
        self._batch_size = batch_size
        self._interval_s = interval_s
        self._retry_s = retry_s
        self._insert = insert
        self._meter = meter
        self._deployed_commit = deployed_commit
        # (control plane, org, env, stamp) → the commit proven for that stamp. A stamp names one
        # model, so once proven it does not change; an unproven stamp is asked again next batch.
        self._proven_commits: dict[tuple[int, str, str, int], str] = {}
        self._count = count
        # Taken off the queue, not yet landed: rows whose insert failed, meters whose call failed.
        self._held_rows: list[AuditRecord] = []
        self._held_meters: list[tuple[Any, str]] = []
        # Progress, for flush(): records accepted and records fully written.
        self._progress = threading.Condition()
        self._accepted = 0
        self._settled = 0
        self._thread_lock = threading.Lock()
        self._thread: "LongLived | None" = None
        self._closed = False
        self._stopping = threading.Event()
        # Why the writer is holding records, when it is: the most recent insert or meter failure.
        self.last_error: str | None = None
        # When a shutdown stops retrying a failing insert (set by close()).
        self._drain_until = 0.0

    # -- request side ---------------------------------------------------------------------------

    def enqueue(self, record: AuditRecord) -> None:
        """Hand ``record`` to the writer and return. Waits only when the queue is full, bounded by
        the request's remaining deadline, and then raises :class:`AuditQueueFull`."""
        # REQ-1882 sanctioned hop: the INSERT of an audit row is the one piece of a request's
        # work nothing in the request reads back, so it is handed here to the writer's own thread
        # (maintainer decision 2026-10-01). The request thread does the queue put and nothing
        # else — it never starts the writer (see ``start``).
        if not self.running():
            raise RuntimeError(
                "audit writer is not running: it is started at application startup "
                "(start_audit_writer) and a record cannot be accepted before that, after "
                "shutdown, or once its thread has ended"
            )
        with self._progress:
            self._accepted += 1
        try:
            try:
                self._queue.put_nowait(record)
            except queue.Full:
                budget = request_deadline.remaining()
                wait = _NO_DEADLINE_WAIT_S if budget is None else budget
                try:
                    self._queue.put(record, timeout=max(wait, 0.0))
                except queue.Full:
                    raise AuditQueueFull(
                        f"audit queue full ({self._queue.maxsize} records) for {wait:.1f}s: the "
                        "audit writer is not keeping up or its store is unreachable; the "
                        "statement's audit record was not accepted"
                    ) from None
        except BaseException:
            with self._progress:
                self._accepted -= 1
                self._progress.notify_all()
            raise

    def flush(self, timeout: float) -> bool:
        """Wait until every record accepted before this call is written. False on timeout."""
        deadline = time.monotonic() + timeout
        with self._progress:
            target = self._accepted
            while self._settled < target:
                left = deadline - time.monotonic()
                if left <= 0:
                    return False
                self._progress.wait(left)
        return True

    def pending(self) -> int:
        with self._progress:
            return self._accepted - self._settled

    def running(self) -> bool:
        """Whether a record handed to this writer will be read: its thread was started, has not
        ended (shutdown, or a background shutdown that stopped every long-lived thread) and the
        writer has not been closed."""
        thread = self._thread
        return thread is not None and not self._closed and not thread.done()

    def status(self) -> str:
        """What the writer is holding and why — the text a failed flush is reported with."""
        pending = self.pending()
        if self._thread is not None and self._thread.done():
            return f"audit writer: its thread has stopped with {pending} record(s) unwritten"
        if not pending:
            return "audit writer: nothing pending"
        why = self.last_error or "no insert has failed; the writer has not reached them yet"
        return f"audit writer: {pending} record(s) unwritten — {why}"

    # -- writer side ----------------------------------------------------------------------------

    def start(self) -> "AuditWriter":
        """Start the writer's thread. Called once, at application startup — never by a request."""
        with self._thread_lock:
            if self._closed:
                raise RuntimeError("audit writer is shut down; it cannot be started again")
            if self._thread is None:
                from provisa.core.connection_loop import spawn_long_lived

                # An EMPTY context: the writer serves every request and must not carry the org
                # binding, audit identity or deadline of whatever context started it.
                self._thread = contextvars.Context().run(
                    spawn_long_lived, self._run(), name="audit-writer"
                )
        return self

    def _take(self) -> list[AuditRecord]:
        """Block for the next batch: up to ``batch_size`` records gathered over one interval from
        the first one's arrival. Fewer when rows are being held for retry. Returns early, with what
        it has, once shutdown is requested."""
        room = self._batch_size - len(self._held_rows) - len(self._held_meters)
        if room <= 0:
            self._stopping.wait(self._retry_s)
            return []
        batch: list[AuditRecord] = []
        until = time.monotonic() + self._interval_s
        while len(batch) < room and not self._stopping.is_set():
            left = until - time.monotonic()
            if left <= 0:
                break
            try:
                batch.append(self._queue.get(timeout=min(left, _STOP_POLL_S)))
            except queue.Empty:
                continue
            if len(batch) == 1:
                until = time.monotonic() + self._interval_s  # the window opens at the first record
        return batch

    def _settle(self, n: int) -> None:
        with self._progress:
            self._settled += n
            self._progress.notify_all()

    async def _write(self, batch: list[AuditRecord]) -> bool:
        """Insert held + new rows (one INSERT per tenant database), then meter each. Whatever fails
        stays held for the next attempt; returns whether everything landed."""
        # Everything is HELD until it lands, so an interruption mid-write loses nothing.
        self._held_rows = self._held_rows + batch
        by_db: dict[int, list[AuditRecord]] = {}
        for rec in self._held_rows:
            by_db.setdefault(id(rec.record_db), []).append(rec)
        for group in by_db.values():
            try:
                rows = [rec.row(await self._model_commit(rec)) for rec in group]
                await self._insert(group[0].record_db, rows)
            except Exception as exc:
                self.last_error = f"insert failed: {type(exc).__name__}: {exc}"
                log.exception(
                    "audit batch insert failed: %d record(s) held and retried", len(group)
                )
                continue
            landed = {id(rec) for rec in group}
            self._held_rows = [rec for rec in self._held_rows if id(rec) not in landed]
            self._count_landed(group)
            self._held_meters.extend((rec.meter_pool, rec.meter_org) for rec in group)
        todo, self._held_meters = self._held_meters, []
        settled = 0
        for i, (pool, org_id) in enumerate(todo):
            if pool is not None:
                try:
                    await self._meter(pool, org_id)
                except Exception as exc:
                    self.last_error = f"metering failed: {type(exc).__name__}: {exc}"
                    log.exception("audit metering failed for org %s: held and retried", org_id)
                    self._held_meters.append((pool, org_id))
                    continue
                except BaseException:
                    self._held_meters.extend(todo[i:])
                    self._settle(settled)
                    raise
            settled += 1
        self._settle(settled)
        return not self._held_rows and not self._held_meters

    async def _model_commit(self, rec: AuditRecord) -> str | None:
        """The commit ``rec``'s model equals, when proven. A deployment without a control plane,
        or a record without a stamp, has no environment position to prove one against."""
        if rec.meter_pool is None or rec.model_stamp is None:
            return None
        key = (id(rec.meter_pool), rec.meter_org, rec.model_env, rec.model_stamp)
        known = self._proven_commits.get(key)
        if known is not None:
            return known
        sha = await self._deployed_commit(
            rec.meter_pool, rec.meter_org, rec.model_env, rec.model_stamp
        )
        if sha is not None:
            self._proven_commits[key] = sha
        return sha

    def _count_landed(self, group: list[AuditRecord]) -> None:
        """Count a batch whose rows have landed toward Hot replication. Runs after the insert,
        so nothing here can cost an audit row."""
        from provisa.core.redis_factory import redis_error

        try:
            self._count(group)
        except redis_error():
            # REQ-826 / REQ-1920: Hot-N is best effort and the count is derived state — it may be
            # lost without loss of information (the audit rows just landed are the record). The
            # batch is not held or retried for it; the failure is logged with what it missed.
            log.exception(
                "Hot count not recorded for a batch of %d audit record(s); their statements are "
                "not counted toward replication",
                len(group),
            )

    async def _run(self) -> None:
        try:
            while not self._stopping.is_set():
                batch = self._take()
                if batch or self._held_rows or self._held_meters:
                    if not await self._write(batch):
                        self._stopping.wait(self._retry_s)
                # A suspension point per pass, so a cancel of this thread's task is delivered here.
                await asyncio.sleep(0)
        finally:
            await self._drain()

    async def _drain(self) -> None:
        """Shutdown: write everything still queued, retrying a failed insert until the shutdown
        budget is spent; report what could not be written."""
        remaining: list[AuditRecord] = []
        while True:
            try:
                remaining.append(self._queue.get_nowait())
            except queue.Empty:
                break
        while not await self._write(remaining):
            remaining = []
            if time.monotonic() + self._retry_s >= self._drain_until:
                break
            time.sleep(self._retry_s)
        lost = len(self._held_rows) + len(self._held_meters)
        if lost:
            log.error(
                "audit writer shut down with %d record(s) unwritten (%d row(s), %d meter(s))",
                lost,
                len(self._held_rows),
                len(self._held_meters),
            )

    def close(self, timeout: float) -> int:
        """Stop accepting records, write what is queued, stop the thread. Returns the number of
        records that could not be written within ``timeout`` (reported at error, then given up:
        a writer being shut down does not keep retrying a store that is gone).

        The writer is asked to stop by a flag it checks, not by cancelling its task: a task
        cancelled before its first step never runs at all, and the records already queued behind a
        writer that had not started yet would be dropped without a word."""
        with self._thread_lock:
            self._closed = True
            thread = self._thread
        if thread is None:
            return 0
        self._drain_until = time.monotonic() + timeout
        self._stopping.set()
        # The drain's last attempt may itself be waiting on the store; allow it one retry interval.
        if not thread.join(timeout + self._retry_s + 1.0):
            log.error(
                "audit writer did not finish within %.1fs: %d record(s) unwritten",
                timeout,
                self.pending(),
            )
        return self.pending()


_writer_lock = threading.Lock()
_writer: AuditWriter | None = None
_exit_hook_registered = False
# The budget an interpreter that exits without the application's own shutdown gives the writer.
_EXIT_BUDGET_S = 5.0


def start_audit_writer() -> AuditWriter:
    """Start this process's writer (application startup; each worker process has its own).
    Idempotent: a writer already running is returned as it is."""
    global _writer, _exit_hook_registered
    with _writer_lock:
        if _writer is not None and not _writer.running():
            # Its thread ended without the writer being shut down (a background shutdown stopped
            # every long-lived thread): report what it still held and start a fresh one.
            _writer.close(0.0)
            _writer = None
        if _writer is None:
            _writer = AuditWriter().start()
        if not _exit_hook_registered:
            # A process that exits without running the application's shutdown (a test session, a
            # tool) still writes what is queued — bounded, and then it exits.
            atexit.register(shutdown_audit_writer, _EXIT_BUDGET_S)
            _exit_hook_registered = True
        return _writer


def audit_writer() -> AuditWriter:
    """This process's running writer. Raises when the application has not started one: a record
    must not be accepted by a writer nothing will ever drain."""
    writer = _writer
    if writer is None:
        raise RuntimeError(
            "audit writer is not running: start_audit_writer() is called at application startup"
        )
    return writer


def audit_writer_running() -> bool:
    return _writer is not None


def flush_audit(timeout: float = 10.0) -> bool:
    """Wait until every audit record this process has accepted is written. False on timeout."""
    with _writer_lock:
        writer = _writer
    return True if writer is None else writer.flush(timeout)


def audit_writer_status() -> str:
    """Why a flush did not complete: how many records are unwritten and the last failure."""
    with _writer_lock:
        writer = _writer
    return "audit writer: not running" if writer is None else writer.status()


def shutdown_audit_writer(timeout: float = 10.0) -> int:
    """Application shutdown / worker exit: write what is queued and stop the writer. Returns the
    number of records left unwritten when the budget ran out (0 when there was no writer)."""
    global _writer
    with _writer_lock:
        writer, _writer = _writer, None
    return 0 if writer is None else writer.close(timeout)
