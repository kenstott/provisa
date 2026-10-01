# Copyright (c) 2026 Kenneth Stott
# Canary: 7081fb98-3b54-4c82-a6d5-761d75fb7a31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-source SQLAlchemy engine management for ingest sources (Phase AS, REQ-331).

Each ingest source gets ONE shared, synchronous engine per worker (REQ-1882): every request runs
on its own thread and the engine's pool is thread-safe, so request threads share it; a borrower
waits for a free pooled connection when the pool is exhausted."""

from __future__ import annotations

import logging
import threading
from typing import Any

# Requirements: REQ-331, REQ-332

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

# Bounded wait for a pooled connection when all are checked out (REQ-1882: the extra request
# waits, never fails with "pool exhausted").
_POOL_WAIT_S = 30.0

# Engines are created once per source_id and reused across requests.
_engines: dict[str, Engine] = {}
# get_engine is a check-then-create-then-set on a shared dict, reachable from more than one thread
# (ingest workers / executor threads). Without this guard, concurrent first-hits for one source each
# build their own Engine and all but the last leak an undisposed connection pool — a silent
# resource race. Double-checked locking makes "one engine per source_id" an invariant.
_engines_lock = threading.Lock()


def get_engine(  # REQ-331, REQ-332
    source_id: str,
    dialect: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    search_path: str | None = None,
    *,
    use_pgbouncer: bool,
) -> Engine:
    """Return (or create) the Engine for *source_id*.

    ``dialect`` is a SQLAlchemy ``backend[+driver]`` string (``postgresql``, ``mysql``,
    ``sqlite`` …). It resolves to the sync driver the store runs on (``provisa.core.database.sync_store_url``).
    Defaults to ``postgresql`` when absent.

    ``search_path`` (REQ-1730): scopes every connection this engine opens to a Postgres schema at
    the driver level (libpq's ``options=-csearch_path=…``, applied on connect — unlike
    ``core.database.Database.acquire()``'s per-acquire ``SET search_path``, there is no
    application-level wrapper here to issue it per checkout). Only the tenant_db-mirroring caller
    (``app_loaders.py``'s ``_init_ingest_engines``) passes this, with the org's own schema — an
    ingest source with its OWN explicit host/database is a genuinely external DB and must keep
    that connection's ordinary default search_path, never forced into an org schema that has
    nothing to do with it.

    ``use_pgbouncer``: whether a PostgreSQL target sits behind PgBouncer; it sets the
    prepared-statement policy through ``provisa.core.database.pg_engine``, the same one the
    control plane runs on. Ignored for other backends.
    """
    cached = _engines.get(source_id)
    if cached is not None:
        return cached

    with _engines_lock:
        # Re-check under the lock: a peer may have created it while we waited.
        cached = _engines.get(source_id)
        if cached is not None:
            return cached
        from provisa.core.database import pg_engine, sync_store_url

        url = sync_store_url(_build_url(dialect, host, port, database, username, password))
        log.info("Creating ingest engine for source=%s url=%s", source_id, url.split("@")[-1])
        pool_kwargs: dict[str, Any] = {
            "pool_pre_ping": True,
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout": _POOL_WAIT_S,
        }
        if url.startswith("postgresql"):
            connect_args = {"options": f"-csearch_path={search_path}"} if search_path else {}
            engine = pg_engine(
                url, use_pgbouncer=use_pgbouncer, connect_args=connect_args, **pool_kwargs
            )
        else:
            engine = create_engine(url, **pool_kwargs)
        _engines[source_id] = engine
        return engine


def dispose_all() -> None:
    """Dispose all cached engines (called on app shutdown)."""
    for eng in list(_engines.values()):
        eng.dispose()
    _engines.clear()


def _build_url(
    dialect: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> str:
    if not dialect:
        dialect = "postgresql"
    if not host:
        raise ValueError("ingest DB host is required")
    if not port:
        raise ValueError("ingest DB port is required")
    import urllib.parse

    # REQ-1745: an ingest source with no connection fields of its own mirrors state.tenant_db's
    # own engine URL verbatim (app_loaders.py's _init_ingest_engines) -- and a trust/peer-auth
    # control-plane postgres (a docker-assigned test instance, or any deployment relying on the
    # OS's default postgres trust auth) legitimately has no password. Requiring one
    # unconditionally raised ValueError for EVERY ingest source on such a deployment, silently
    # swallowed by tolerate_startup_failure in _init_ingest_engines, which aborted that
    # function's per-source loop before state.ingest_tables/state.ingest_engines were ever
    # populated for that source -- every POST to /data/ingest then 404'd "source not found" no
    # matter how many times the schema rebuilt. A DB that genuinely requires a password still
    # fails, just at connect time from the driver's own auth error, not this pre-emptive guess.
    pw = urllib.parse.quote_plus(password) if password else ""
    return f"{dialect}://{username}:{pw}@{host}:{port}/{database}"
