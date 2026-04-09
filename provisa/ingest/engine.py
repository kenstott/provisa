# Copyright (c) 2026 Kenneth Stott
# Canary: 7081fb98-3b54-4c82-a6d5-761d75fb7a31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-source SQLAlchemy async engine management for ingest sources (Phase AS, REQ-331)."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

log = logging.getLogger(__name__)

# Engines are created once per source_id and reused across requests.
_engines: dict[str, AsyncEngine] = {}


def get_engine(
    source_id: str,
    dialect: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> AsyncEngine:
    """Return (or create) the AsyncEngine for *source_id*.

    ``dialect`` must be a SQLAlchemy async driver string, e.g.
    ``postgresql+asyncpg``, ``mysql+aiomysql``.  Defaults to
    ``postgresql+asyncpg`` when absent.
    """
    if source_id in _engines:
        return _engines[source_id]

    url = _build_url(dialect, host, port, database, username, password)
    log.info("Creating ingest engine for source=%s url=%s", source_id, url.split("@")[-1])
    engine = create_async_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)
    _engines[source_id] = engine
    return engine


async def dispose_all() -> None:
    """Dispose all cached engines (called on app shutdown)."""
    for eng in list(_engines.values()):
        await eng.dispose()
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
        dialect = "postgresql+asyncpg"
    if not host:
        host = "localhost"
    if not port:
        port = 5432
    import urllib.parse
    pw = urllib.parse.quote_plus(password or "")
    return f"{dialect}://{username}:{pw}@{host}:{port}/{database}"
