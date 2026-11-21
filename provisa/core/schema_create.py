# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a1d92-8c47-4e0b-9d15-a2b7e4c80f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Creating a schema of a PostgreSQL store, one creator at a time.

``CREATE SCHEMA IF NOT EXISTS`` does not protect against a concurrent creator: two sessions that
both find the schema absent both insert it into ``pg_namespace``, and the second fails on the
catalog's unique index (``pg_namespace_nspname_index``). An org's ``_mv_cache`` schema has many
creators on different connections -- the engine's store connection landing a view's staged rows,
the store writer reconciling a view's table at boot, the org's provisioning -- so every creation
of a store schema goes through here: an advisory lock keyed by the schema's name is held around
the create, and whoever comes second finds the schema the first created.

The key space is this module's own (:data:`SCHEMA_CREATE_LOCK_KEY`), not the schema-bootstrap key
of :func:`provisa.core.db.schema_lock_key`: a caller holding an org's bootstrap lock creates
schemas through here, and a shared key would make it wait on itself.
"""

from __future__ import annotations

import zlib
from typing import Any

#: The high word of every schema-creation lock key; the low word is the schema name's CRC-32.
SCHEMA_CREATE_LOCK_KEY = 7352


def schema_create_lock_key(schema: str) -> int:
    """The advisory-lock key that serializes creating ``schema``."""
    return (SCHEMA_CREATE_LOCK_KEY << 32) | zlib.crc32(schema.encode())


def _quoted(schema: str) -> str:
    return '"' + schema.replace('"', '""') + '"'


def create_schema_on_cursor(cur: Any, schema: str) -> None:
    """Create ``schema`` through ``cur``, a DB-API cursor of an AUTOCOMMIT PostgreSQL connection
    (psycopg2), holding the schema's session advisory lock around the create. The create commits
    as it runs, so the lock is released only after the schema is visible to every session."""
    if not cur.connection.autocommit:
        raise ValueError(
            f"create_schema_on_cursor({schema!r}) needs an autocommit connection: the create "
            "must commit before the lock that serializes it is released"
        )
    key = schema_create_lock_key(schema)
    cur.execute("SELECT pg_advisory_lock(%s)", (key,))
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quoted(schema)}")
    finally:
        cur.execute("SELECT pg_advisory_unlock(%s)", (key,))


async def create_schema(conn: Any, schema: str) -> None:
    """Create ``schema`` through ``conn``, a :class:`provisa.core.database.Connection`, in a
    transaction holding the schema's transaction advisory lock (a savepoint inside a caller's
    transaction, whose commit then releases it). Only PostgreSQL takes the lock: its
    ``IF NOT EXISTS`` is the one that loses to a concurrent creator. A backend without schemas
    (SQLite) creates nothing."""
    from sqlalchemy.schema import CreateSchema

    if not conn.capabilities.schemas:
        return
    async with conn.transaction():
        if conn.capabilities.dialect == "postgresql":
            await conn.advisory_xact_lock(schema_create_lock_key(schema))
        await conn.execute_core(CreateSchema(schema, if_not_exists=True))


def create_schema_sync(sa_conn: Any, schema: str) -> None:
    """Create ``schema`` through ``sa_conn``, a SQLAlchemy ``Connection`` inside a transaction
    (``engine.begin()``), holding the schema's transaction advisory lock on PostgreSQL until that
    transaction commits."""
    from sqlalchemy import text
    from sqlalchemy.schema import CreateSchema

    if sa_conn.dialect.name == "postgresql":
        sa_conn.execute(
            text("SELECT pg_advisory_xact_lock(:key)").bindparams(
                key=schema_create_lock_key(schema)
            )
        )
    sa_conn.execute(CreateSchema(schema, if_not_exists=True))
