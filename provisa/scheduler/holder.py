# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1f8c49-3d72-4e05-9b86-c0e4d7a2f513
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which worker process runs the deployment's scheduled jobs (REQ-1900).

Every worker of a `uvicorn --workers N` launch — and every node of a cluster — starts a scheduler,
because some jobs are the process's own (draining ITS in-memory egress counters, watching ITS
engine connection). The rest are the deployment's: an MV refresh tick, a source poll, a reaper,
OTEL compaction, a config trigger. Run in every worker, each of those fired N times.

One process is therefore the HOLDER, and only it runs the deployment's jobs. The holder is whoever
holds a PostgreSQL session advisory lock on the platform control plane — the one thing every worker
reaches. Nothing is elected and nothing is renewed: the lock belongs to a database session, so it
is held exactly as long as the holding process's connection lives and the server frees it the
moment that process dies. Every other worker asks for the lock (without waiting) each time one of
its own schedulers fires a deployment job, so the first firing after the holder is gone is run by
whoever asks first.

Control planes on other dialects are single-writer file stores run by one process, which is
therefore the holder.
"""

# Requirements: REQ-1900

from __future__ import annotations

import logging
import threading

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from provisa.core.database import create_engine_from_url

log = logging.getLogger(__name__)

# "PVSC" as ASCII bytes: the first half of the two-key advisory lock (the second is the scope).
_SCHEDULER_LOCK_CLASS = 0x50565343


class SchedulerHolder:
    """This process's claim on the deployment's scheduled jobs for ``scope`` (the org id the
    deployment boots as — two deployments sharing one control-plane server do not share a
    holder)."""

    def __init__(self, platform_url: str, scope: str) -> None:
        self._engine = create_engine_from_url(platform_url, pool_size=1, max_overflow=0)
        self._scope = scope
        self._conn = None
        self._held = False
        # Jobs run on background worker threads; the claim is one connection.
        self._guard = threading.Lock()

    def holds(self) -> bool:
        """Whether this process holds the lock now, taking it if nobody does."""
        if self._engine.dialect.name != "postgresql":
            return True
        with self._guard:
            try:
                return self._holds_locked()
            except DBAPIError:
                # The session is gone (control plane restarted, connection cut) and the lock with
                # it: this process is not the holder for THIS firing. The next firing opens a new
                # session and asks again.
                log.warning("scheduler holder: control-plane session lost", exc_info=True)
                self._drop()
                return False

    def _holds_locked(self) -> bool:
        if self._conn is None:
            # Autocommit: the lock is a session lock and must not sit in an open transaction.
            self._conn = self._engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        if self._held:
            # A statement on the holding session proves the session — and so the lock — is alive.
            self._conn.execute(text("SELECT 1"))
            return True
        self._held = bool(
            self._conn.execute(
                text("SELECT pg_try_advisory_lock(:cls, hashtext(:scope))"),
                {"cls": _SCHEDULER_LOCK_CLASS, "scope": self._scope},
            ).scalar()
        )
        if self._held:
            log.warning(
                "scheduler holder: this process now runs scheduled jobs for %r", self._scope
            )
        return self._held

    def _drop(self) -> None:
        conn, self._conn, self._held = self._conn, None, False
        if conn is not None:
            conn.invalidate()
            conn.close()

    def close(self) -> None:
        """End the session, releasing the lock if this process held it."""
        with self._guard:
            conn, self._conn, self._held = self._conn, None, False
            if conn is not None:
                conn.close()
        self._engine.dispose()
