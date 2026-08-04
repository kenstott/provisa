# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MySQL/MariaDB connector: describe-only probe helper for the admin source-meta contract.

Mirrors connector_sqlite.py's shape. A single one-shot async helper that owns
``import aiomysql`` directly, so the driver import never lives inline in an API router.
"""

from __future__ import annotations

_COMMENT_SQL = """
SELECT SCHEMA_COMMENT
FROM information_schema.SCHEMATA
WHERE SCHEMA_NAME = DATABASE()
"""


async def fetch_database_comment(  # REQ-012
    host: str, port: int, database: str, username: str, password: str
) -> str:
    """Connect to a MySQL/MariaDB database and return its schema-level comment, if any."""
    import aiomysql  # pyright: ignore[reportMissingImports]

    conn = await aiomysql.connect(
        host=host,
        port=port,
        db=database,
        user=username,
        password=password,
        connect_timeout=5,
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(_COMMENT_SQL)
            row = await cur.fetchone()
            return (row[0] or "") if row else ""
    finally:
        conn.close()
