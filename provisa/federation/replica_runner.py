# Copyright (c) 2026 Kenneth Stott
# Canary: a97dceeb-cc3a-4e91-a07c-98acff4f6ba7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The replica build runner (REQ-1915, REQ-1916).

Every process that does background work (``process_mode.runs_background_work``) runs a runner.
A runner claims requested builds from the central record and runs them, so a deployment builds
more replicas at once by running more such nodes. Three limits apply, each a lock the holder's
death releases (``replica_locks``):

- ``replication.builds_per_node``: builds on this host at once;
- ``replication.engine_jobs``: background jobs on one engine at once, across every node;
- a source's live-read cap (REQ-1909): a build is one more reader of its source.

One pass tries, in order, a node slot, the engine's job slot, a replica's lock, and the source's
permit. Every one is a try. Whatever was taken is given back as soon as a later one is not
available, and the build stays requested with the reason recorded, so nothing is held while
waiting. The source permit is tried last because it is the only one of the four a request can
see being held.

Node count is capacity for streamed copies, whose rows pass through the building node. For a
copy the engine performs as its own statement the node only submits and follows it, and the
capacity is the engine's.
"""

# Requirements: REQ-1915, REQ-1916, REQ-1909

from __future__ import annotations

import logging
import os
import random
import socket
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from provisa.core import process_mode, request_deadline
from provisa.federation import replica_state as build_state
from provisa.federation.data_replicator import BuildOutcome, Progress
from provisa.federation.replica_errors import WAITING_ENGINE, WAITING_SOURCE, coded
from provisa.federation.replica_locks import BuildClaim, BuildLocks
from provisa.federation.replica_state import ReplicaKey

log = logging.getLogger(__name__)


# How many of the oldest candidates one pass considers, per free slot it is filling.
_CANDIDATES_PER_PASS = 16
# The running build's progress is written to the record no more often than this.
_PROGRESS_EVERY_S = 1.0


def engine_job_key(kind: str, address: str | None) -> str:
    """What identifies one engine for the job cap: its kind and the address jobs run against.
    No org is in the key, so an engine several orgs share counts once. An engine embedded in
    the Provisa process has no address; each host has its own, so the key carries the host."""
    return f"{kind}@{address if address is not None else socket.gethostname()}"


@dataclass
class _Job:
    key: ReplicaKey
    claim: BuildClaim
    permit: tuple[str, str] | None  # the source permit's (set key, token), when one was taken
    slot: Any = None


class ReplicaRunner:
    """One process's build runner for one org's replicas.

    ``build`` runs one replica's build and returns its outcome; it is the data replicator's
    job for that replica. ``source_cap`` gives a replica's source's live-read cap (None: no
    cap). ``next_refresh_at`` gives when a replica completed now is next due (None: only on
    request). ``store`` identifies the store the builds write into. ``housekeeping`` runs at
    the start of each pass (dropping retired replicas whose wait is over). ``spawn`` runs a build detached from the pass that claimed it."""

    def __init__(
        self,
        *,
        db: Any,
        org_id: str,
        locks: BuildLocks,
        engine_key: Callable[[], str],
        build: Callable[[ReplicaKey, Progress], Awaitable[BuildOutcome]],
        source_cap: Callable[[ReplicaKey], Awaitable[int | None]],
        permits: Any,
        next_refresh_at: Callable[[ReplicaKey, datetime], Awaitable[datetime | None]],
        store: Callable[[], str],
        retry_interval: Callable[[], float],
        housekeeping: Callable[[BuildLocks, str], Awaitable[Any]] | None = None,
        builds_per_node: Callable[[], int],
        engine_jobs: Callable[[], int],
        spawn: Callable[..., Any],
    ) -> None:
        self._db = db
        self._org_id = org_id
        self._locks = locks
        self._engine_key = engine_key
        self._build = build
        self._source_cap = source_cap
        self._permits = permits
        self._next_refresh_at = next_refresh_at
        self._store = store
        self._retry_interval = retry_interval
        self._housekeeping = housekeeping
        self._builds_per_node = builds_per_node
        self._engine_jobs = engine_jobs
        self._spawn = spawn
        self._holder = f"{socket.gethostname()}:{os.getpid()}"

    async def run_pass(self) -> int:
        """Start as many requested builds as the limits allow. Returns how many it started."""
        if not process_mode.runs_background_work():
            return 0
        if self._housekeeping is not None:
            # What the pass does besides building: dropping the replicas the model retired,
            # once their wait is over. A failure there must not stop the builds.
            try:
                await self._housekeeping(self._locks, self._org_id)
            except Exception as exc:  # allow-ble: logged with its cause; the next pass tries again, and builds must not wait on a drop
                log.error("replica housekeeping failed: %s", exc, exc_info=exc)
        started = 0
        while True:
            slot = self._locks.try_node_slot(self._builds_per_node())
            if slot is None:
                break
            job = None
            try:
                job = await self._claim_one()
            finally:
                if job is None:
                    slot.release()
            if job is None:
                break
            job.slot = slot
            self._spawn(self._run(job), name=f"replica-build:{'.'.join(job.key)}")
            started += 1
        return started

    async def _claim_one(self) -> _Job | None:
        """Claim one build: the engine's slot, a replica's lock and row, its source's permit."""
        now = datetime.now(UTC)
        async with self._db.acquire() as conn:
            keys = await build_state.candidates(
                conn, now=now, limit=_CANDIDATES_PER_PASS, retry_interval=self._retry_interval()
            )
        if not keys:
            return None
        # Every runner sees the same oldest candidates; a shuffle keeps them from all trying
        # the same one first.
        random.shuffle(keys)
        claim = self._locks.claim()
        job: _Job | None = None
        try:
            if not claim.try_engine_slot(self._engine_key(), self._engine_jobs()):
                async with self._db.acquire() as conn:
                    await build_state.set_waiting(conn, keys, waiting_on=WAITING_ENGINE)
                return None
            for key in keys:
                job = await self._claim_key(claim, key, now)
                if job is not None:
                    return job
            return None
        finally:
            if job is None:
                claim.close()

    async def _claim_key(self, claim: BuildClaim, key: ReplicaKey, now: datetime) -> _Job | None:
        if not claim.try_replica(self._org_id, key):
            return None  # another runner is building it
        started = False
        try:
            async with self._db.acquire() as conn:
                if not await build_state.claim(
                    conn, key, holder=self._holder, now=now, retry_interval=self._retry_interval()
                ):
                    return None  # built by another runner since this pass selected it
                cap = await self._source_cap(key)
                permit: tuple[str, str] | None = None
                if cap is not None:
                    permit_key = self._permits.key(self._org_id, key[0])
                    token = self._permits.try_acquire(permit_key, cap)
                    if token is None:
                        await build_state.unclaim(conn, key, waiting_on=WAITING_SOURCE)
                        return None
                    permit = (permit_key, token)
            started = True
            return _Job(key=key, claim=claim, permit=permit)
        finally:
            if not started:
                claim.release_replica(self._org_id, key)

    async def _run(self, job: _Job) -> None:
        """Run one claimed build to its end, record the outcome, release what it holds, and
        look for the next build."""
        last_write = 0.0

        async def progress(rows_copied: int) -> None:
            nonlocal last_write
            if time.monotonic() - last_write < _PROGRESS_EVERY_S:
                return
            last_write = time.monotonic()
            async with self._db.acquire() as conn:
                await build_state.record_progress(conn, job.key, rows_copied=rows_copied)

        shield = request_deadline.shielded()
        try:
            try:
                outcome = await self._build(job.key, progress)
            except BaseException as exc:  # allow-ble: a build's failure, whatever its type, is recorded on the replica for the operator and the next read; it is re-raised below when it is not an ordinary error
                log.error("replica build of %s failed: %s", ".".join(job.key), exc, exc_info=exc)
                code, params = coded(exc)
                async with self._db.acquire() as conn:
                    await build_state.record_failed(
                        conn,
                        job.key,
                        error=str(exc) or type(exc).__name__,
                        code=code,
                        params=params,
                        now=datetime.now(UTC),
                    )
                if not isinstance(exc, Exception):
                    raise
            else:
                now = datetime.now(UTC)
                due = await self._next_refresh_at(job.key, now)
                async with self._db.acquire() as conn:
                    await build_state.record_completed(
                        conn,
                        job.key,
                        rows_copied=outcome.rows_copied,
                        method=outcome.method,
                        content_hash=outcome.content_hash,
                        store=self._store(),
                        definition_hash=outcome.definition_hash,
                        built_columns=outcome.built_columns,
                        next_refresh_at=due,
                        now=now,
                    )
                log.info(
                    "replica of %s built: %d rows (%s)",
                    ".".join(job.key),
                    outcome.rows_copied,
                    outcome.method,
                )
        finally:
            with shield.lock:
                shield.settle()
                if job.permit is not None:
                    self._permits.release(*job.permit)
                job.claim.close()
                if job.slot is not None:
                    job.slot.release()
        await self.run_pass()
