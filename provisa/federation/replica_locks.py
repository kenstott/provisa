# Copyright (c) 2026 Kenneth Stott
# Canary: 6bc75753-5486-491e-873e-47c897117bcc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The locks a replica build holds (REQ-1915).

Three limits bound replica builds, each a lock the holder's death releases with nothing to renew
and nobody to notice:

- one build of a replica at a time, across every process and node: the REPLICA lock;
- at most ``replication.engine_jobs`` background jobs on one engine at once, across every node:
  an ENGINE SLOT, one of that many locks per engine;
- at most ``replication.builds_per_node`` builds on one host at once: a NODE SLOT.

The replica lock and the engine slot are session advisory locks on the platform control plane,
the one database every process of every org reaches, both taken on one connection the build
holds for as long as it runs (a :class:`BuildClaim`). PostgreSQL frees them when that session
ends. On a control plane that is a file on one host they are ``flock`` locks beside that file.
The node slot is always a ``flock`` on the host. Every attempt is a try: nothing here waits.
"""

# Requirements: REQ-1915

from __future__ import annotations

import asyncio
import logging
import random
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import text

from provisa.core import request_deadline
from provisa.core.host_lock import FileLock, control_plane_lock_dir, host_lock_dir, lock_name

log = logging.getLogger(__name__)

# First halves of the two-key advisory locks; the second half is the hash of the name.
_REPLICA_LOCK_CLASS = 0x50565242  # "PVRB"
_ENGINE_JOBS_LOCK_CLASS = 0x5056454A  # "PVEJ"

#: A replica's lock name: the org it belongs to and the table's registered identity.
ReplicaKey = tuple[str, str, str]


def replica_lock_name(org_id: str, key: ReplicaKey) -> str:
    return "|".join((org_id, *key))


class BuildClaim:
    """What one build holds on the control plane: an engine slot and a replica lock. Closing it
    releases both."""

    def __init__(self, platform_url: str) -> None:
        from sqlalchemy import make_url

        from provisa.core.database import control_plane_lock_engine, sync_store_url

        self._engine: Any = None
        self._conn: Any = None
        self._files: list[FileLock] = []
        self._dir: Path | None = None
        if make_url(sync_store_url(platform_url)).get_backend_name() != "postgresql":
            # A control plane that is a file on this host: the locks are files beside it.
            self._dir = control_plane_lock_dir(platform_url)
            return
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            # The engine is this claim's own, on the server itself (the session must be this
            # process's, which behind a pooling PgBouncer it is not), so disposing it ends the
            # session and with it the locks. Autocommit: a session lock must not sit inside an
            # open transaction.
            self._engine = control_plane_lock_engine(platform_url)
            self._conn = self._engine.connect().execution_options(isolation_level="AUTOCOMMIT")

    def _try_advisory(self, lock_class: int, name: str) -> bool:
        return bool(
            self._conn.execute(
                text("SELECT pg_try_advisory_lock(:cls, hashtext(:name))"),
                {"cls": lock_class, "name": name},
            ).scalar()
        )

    def _try_file(self, kind: str, name: str) -> bool:
        assert self._dir is not None  # a file-held control plane: set when the claim opened
        lock = FileLock(self._dir / lock_name(kind, name))
        if not lock.try_acquire():
            return False
        self._files.append(lock)
        return True

    def try_engine_slot(self, engine_key: str, cap: int) -> bool:
        """Take one of the ``cap`` job slots of the engine ``engine_key``, if one is free."""
        for slot in random.sample(range(cap), cap):
            name = f"{engine_key}:{slot}"
            if self._conn is not None:
                if self._try_advisory(_ENGINE_JOBS_LOCK_CLASS, name):
                    return True
            elif self._try_file("engine-job", name):
                return True
        return False

    def try_replica(self, org_id: str, key: ReplicaKey) -> bool:
        """Take the lock of one replica, if no build holds it."""
        name = replica_lock_name(org_id, key)
        if self._conn is not None:
            return self._try_advisory(_REPLICA_LOCK_CLASS, name)
        return self._try_file("replica", name)

    def release_replica(self, org_id: str, key: ReplicaKey) -> None:
        """Give back a replica lock taken on this claim (the build did not start)."""
        name = replica_lock_name(org_id, key)
        if self._conn is not None:
            self._conn.execute(
                text("SELECT pg_advisory_unlock(:cls, hashtext(:name))"),
                {"cls": _REPLICA_LOCK_CLASS, "name": name},
            )
            return
        assert self._dir is not None  # a file-held control plane: set when the claim opened
        path = self._dir / lock_name("replica", name)
        for lock in [f for f in self._files if f.path == path]:
            lock.release()
            self._files.remove(lock)

    def close(self) -> None:
        """Release everything this claim holds."""
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            conn, self._conn = self._conn, None
            engine, self._engine = self._engine, None
            files, self._files = self._files, []
            for lock in files:
                lock.release()
            if conn is not None:
                conn.close()
            if engine is not None:
                engine.dispose()  # ends the session: the server frees its advisory locks


class BuildLocks:
    """The build locks of one deployment, as this process takes them."""

    def __init__(self, platform_url: str) -> None:
        self._url = platform_url
        self._slots = host_lock_dir(platform_url)

    def try_node_slot(self, cap: int) -> FileLock | None:
        """One of this host's ``cap`` build slots, held, or None when all are taken."""
        for slot in range(cap):
            lock = FileLock(self._slots / f"build-slot-{slot}.lock")
            if lock.try_acquire():
                return lock
        return None

    def claim(self) -> BuildClaim:
        """A new claim: the connection a build holds its engine slot and replica lock on."""
        return BuildClaim(self._url)


# -- writers of deltas (REQ-1915) ----------------------------------------------------------------

#: How often a delta writer that found its replica being built looks again.
_DELTA_POLL_S = 0.5

_delta_claims: dict[str, BuildClaim] = {}
_delta_guard = threading.Lock()


def _delta_claim(platform_url: str) -> BuildClaim:
    """This process's one claim for its delta writers on ``platform_url``, opened on first use
    and kept: a delta is small and frequent, and a connection per delta would cost more than
    the delta. The locks on it are released one by one as each delta ends; the control plane
    releases whatever is left if the process dies."""
    claim = _delta_claims.get(platform_url)
    if claim is None:
        claim = _delta_claims[platform_url] = BuildClaim(platform_url)
    return claim


@asynccontextmanager
async def replica_write_lock(
    platform_url: str, org_id: str, key: ReplicaKey
) -> AsyncIterator[None]:
    """Hold the replica ``key``'s lock for a write of deltas into it: an append past the
    cursor, or a batch of change events applied by key.

    The same lock a build holds, so a delta is never written into a table a build is about to
    replace (it would be lost with the table) and a build never starts under a delta. A delta
    that finds a build running waits for it — the delta then lands in the new table. The wait
    is on the event loop's or a listener's own task, never on a request's thread. Writers of
    one replica in one process are already one at a time (``events.land_lock``)."""
    waited = 0.0
    while True:
        with _delta_guard:
            held = _delta_claim(platform_url).try_replica(org_id, key)
        if held:
            break
        if waited == 0.0:
            log.info("a delta for %s waits for the replica's build to finish", ".".join(key))
        await asyncio.sleep(_DELTA_POLL_S)
        waited += _DELTA_POLL_S
    try:
        yield
    finally:
        with _delta_guard:
            _delta_claim(platform_url).release_replica(org_id, key)
