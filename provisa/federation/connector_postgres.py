# Copyright (c) 2026 Kenneth Stott
# Canary: a32317a3-6418-4b02-8876-143b55949d88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL connector: describe-only probe helper for the admin source-meta contract.

Mirrors connector_sqlite.py's shape. A single one-shot async helper that owns
``import asyncpg`` directly, so the driver import never lives inline in an API router.
"""

from __future__ import annotations

_COMMENT_SQL = """
SELECT pg_catalog.shobj_description(d.oid, 'pg_database')
FROM pg_catalog.pg_database d
WHERE d.datname = current_database()
"""


async def fetch_database_comment(  # REQ-012
    host: str, port: int, database: str, username: str, password: str
) -> str:
    """Connect to a PostgreSQL database and return its database-level comment, if any."""
    import asyncpg

    conn = await asyncpg.connect(
        host=host,
        port=port,
        database=database,
        user=username,
        password=password,
        timeout=5,
    )
    try:
        row = await conn.fetchrow(_COMMENT_SQL)
        return (row[0] or "") if row else ""
    finally:
        await conn.close()
