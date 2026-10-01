# Copyright (c) 2026 Kenneth Stott
# Canary: 8b5d2f70-1e9c-4a37-b6d4-3c7a0e9f5d18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The boot sequence against the control plane under `uvicorn --workers N` (REQ-1900).

``uvicorn --workers N`` runs the whole startup in every worker at once. Startup has two halves:

* ONCE PER LAUNCH — everything that writes the control plane or the engine's shared catalogs:
  control-plane DDL, the built-in seeds, the config apply, role grants, the environment
  baselines. Idempotent one after another, but not concurrently: on a fresh control plane N
  sessions race each other's ``CREATE TABLE`` in the catalog, and N config applies delete and
  re-insert the same registry rows under each other (foreign-key violations, deadlocks).
* PER WORKER — everything that only builds this process: connection pools, the registry read into
  memory, the compiled schemas, the protocol listeners. It writes nothing shared, so every worker
  does it at the same time.

The workers are separate processes with no shared memory, so the one thing they all reach — the
platform control-plane database — carries both the lock and the record of what is done:

* a PostgreSQL session advisory lock, taken before the first control-plane statement. The server
  releases it itself if the holding process dies.
* ``boot_generations``: one row per org naming the generation whose once-per-launch work is
  COMPLETE (written last, under the lock). A generation is the launch (``$PROVISA_LAUNCH_ID``,
  exported once by the launcher and inherited by every worker it starts, respawns included)
  together with the schema, the config and the engine the work was done for.

The first worker to take the lock finds no row for its generation, does the once-per-launch work,
records the generation and releases. Each other worker then takes the lock only long enough to
read the row, finds its own generation recorded, and goes straight to its per-worker half.

A process with no launch id is not one of several workers of a launch (a single server, a
``--reload`` child): no generation is ever recorded for it, so it does the whole boot under the
lock, which is what every start did before workers existed. A new launch has a new id and does
the once-per-launch work again — a restart re-applies the config, as it always has.

``boot_workers`` is the launch's roll call: a worker adds its row when it starts serving, so any
worker can answer "how many of us are ready" from the control plane rather than from its own
memory.

Control planes on other dialects are single-writer file stores (SQLite) run by one process: they
take no lock and record no generation."""

# Requirements: REQ-1900

from __future__ import annotations

import hashlib
import json
import os
import socket
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    Table,
    Text,
    delete,
    func,
    select,
    text,
)

from provisa.core.database import create_engine_from_url

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection as SAConnection

    from provisa.core.database import Database

# "PROVISA3" as ASCII bytes — any fixed bigint works; this reads as app-specific in pg_locks.
_BOOT_LOCK_KEY = 0x50524F5649534133

_metadata = MetaData()

boot_generations = Table(
    "boot_generations",
    _metadata,
    Column("scope", Text, primary_key=True),
    Column("generation", Text, nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

boot_workers = Table(
    "boot_workers",
    _metadata,
    Column("launch_id", Text, primary_key=True),
    Column("host", Text, primary_key=True),
    Column("pid", Integer, primary_key=True),
    Column("ready_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)


def launch_id() -> str | None:
    """The launch this process is a worker of, or ``None`` when it is not one of several."""
    return os.environ.get("PROVISA_LAUNCH_ID") or None


def expected_workers() -> int:
    """How many worker processes the launch runs. The launcher that exports the launch id exports
    the count with it; a process outside a launch is the only one."""
    if launch_id() is None:
        return 1
    return int(os.environ["PROVISA_WORKERS"])


def boot_generation(launch: str, **inputs: Any) -> str:
    """The generation the once-per-launch work is done for: the launch plus everything the work is
    derived from (schema DDL, config content, engine). Any of them changing is a new generation."""
    payload = json.dumps({"launch": launch, **inputs}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


class BootLock:
    """The held boot lock. ``completed``/``mark_completed`` read and write the generation row on
    the connection that holds the lock, so no other worker can be between the two."""

    def __init__(self, conn: "SAConnection | None") -> None:
        self._conn = conn

    def completed(self, scope: str, generation: str | None) -> bool:
        """Whether ``generation``'s once-per-launch work is recorded complete for ``scope``."""
        if self._conn is None or generation is None:
            return False
        recorded = self._conn.execute(
            select(boot_generations.c.generation).where(boot_generations.c.scope == scope)
        ).scalar()
        return recorded == generation

    def mark_completed(self, scope: str, generation: str | None) -> None:
        """Record ``generation`` complete for ``scope``. Called last, after the work succeeded."""
        if self._conn is None or generation is None:
            return
        self._conn.execute(delete(boot_generations).where(boot_generations.c.scope == scope))
        self._conn.execute(boot_generations.insert().values(scope=scope, generation=generation))


@contextmanager
def control_plane_boot_lock(platform_url: str) -> Generator[BootLock]:
    """Hold the boot lock on ``platform_url`` for the block."""
    engine = create_engine_from_url(platform_url, pool_size=1, max_overflow=0)
    try:
        if engine.dialect.name != "postgresql":
            with engine.begin() as conn:
                _metadata.create_all(conn, tables=[boot_workers])
            yield BootLock(None)
            return
        # A session-level lock lives on this one connection, so it stays open (autocommit: the
        # lock must not sit inside a transaction that idles for the whole boot).
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"SELECT pg_advisory_lock({_BOOT_LOCK_KEY})"))
            try:
                # Under the lock, so the CREATE TABLE IF NOT EXISTS here never races another
                # worker's in the catalog.
                _metadata.create_all(conn)
                yield BootLock(conn)
            finally:
                conn.execute(text(f"SELECT pg_advisory_unlock({_BOOT_LOCK_KEY})"))
    finally:
        engine.dispose()


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def register_ready_worker(db: "Database", launch: str) -> None:
    """Add this process to its launch's roll call. Rows of this host's workers that no longer run
    (killed without a shutdown, since respawned by the supervisor) are removed first, so the count
    is of processes that exist."""
    host = socket.gethostname()
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(boot_workers.c.pid).where(
                    boot_workers.c.launch_id == launch, boot_workers.c.host == host
                )
            )
        ).fetchall()
        gone = [row[0] for row in rows if row[0] == os.getpid() or not _pid_alive(row[0])]
        if gone:
            await conn.execute_core(
                delete(boot_workers).where(
                    boot_workers.c.launch_id == launch,
                    boot_workers.c.host == host,
                    boot_workers.c.pid.in_(gone),
                )
            )
        await conn.execute_core(
            boot_workers.insert().values(launch_id=launch, host=host, pid=os.getpid())
        )


async def unregister_worker(db: "Database", launch: str) -> None:
    """Take this process off its launch's roll call (clean shutdown)."""
    async with db.acquire() as conn:
        await conn.execute_core(
            delete(boot_workers).where(
                boot_workers.c.launch_id == launch,
                boot_workers.c.host == socket.gethostname(),
                boot_workers.c.pid == os.getpid(),
            )
        )


async def ready_worker_count(db: "Database", launch: str) -> int:
    """How many workers of ``launch`` are serving."""
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(func.count()).select_from(boot_workers).where(boot_workers.c.launch_id == launch)
        )
        return int(result.scalar())
