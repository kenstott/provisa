# Copyright (c) 2026 Kenneth Stott
# Canary: cd3bc4e3-a652-425f-b5ec-3d0094e37b2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL Server connector: describe-only probe helper for the admin source-meta contract.

Mirrors connector_sqlite.py's shape. A single one-shot async helper that owns
``import aioodbc`` directly, so the driver import never lives inline in an API router.
"""

from __future__ import annotations

_COMMENT_SQL = (
    "SELECT CAST(value AS NVARCHAR(MAX)) FROM sys.extended_properties "
    "WHERE class = 0 AND name = 'MS_Description'"
)


async def fetch_database_comment(  # REQ-012
    host: str, port: int, database: str, username: str, password: str
) -> str:
    """Connect to a SQL Server database and return its MS_Description extended property, if any."""
    import aioodbc  # pyright: ignore[reportMissingImports]

    dsn = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={username};PWD={password}"
    )
    conn = await aioodbc.connect(dsn=dsn, timeout=5)
    try:
        async with conn.cursor() as cur:
            await cur.execute(_COMMENT_SQL)
            row = await cur.fetchone()
            return (row[0] or "") if row else ""
    finally:
        await conn.close()
