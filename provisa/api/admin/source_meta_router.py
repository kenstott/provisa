# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3a9c12-4b8e-4d2f-a1c5-0e6b3d8f2a9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin route: fetch description metadata from a DB source before registration.

Endpoints:
  POST /admin/source-meta/db-description  — connect and return DB-level comment
"""

# Requirements: REQ-012

from __future__ import annotations
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/source-meta", tags=["admin", "source-meta"])

_PG_COMMENT_SQL = """
SELECT pg_catalog.shobj_description(d.oid, 'pg_database')
FROM pg_catalog.pg_database d
WHERE d.datname = current_database()
"""

_MYSQL_COMMENT_SQL = """
SELECT SCHEMA_COMMENT
FROM information_schema.SCHEMATA
WHERE SCHEMA_NAME = DATABASE()
"""


class DbDescriptionRequest(BaseModel):
    type: str  # postgresql | mysql | sqlite | mssql
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    path: str = ""  # sqlite


@router.post("/db-description")
async def get_db_description(body: DbDescriptionRequest) -> dict:  # REQ-012
    """Connect to the DB and return the database-level comment, if any."""
    description = ""

    if body.type == "postgresql":
        try:
            import asyncpg

            conn = await asyncpg.connect(
                host=body.host,
                port=body.port,
                database=body.database,
                user=body.username,
                password=body.password,
                timeout=5,
            )
            try:
                row = await conn.fetchrow(_PG_COMMENT_SQL)
                description = (row[0] or "") if row else ""
            finally:
                await conn.close()
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Connection failed: {exc}") from exc

    elif body.type in ("mysql", "mariadb"):
        try:
            import aiomysql  # pyright: ignore[reportMissingImports]

            conn = await aiomysql.connect(
                host=body.host,
                port=body.port,
                db=body.database,
                user=body.username,
                password=body.password,
                connect_timeout=5,
            )
            try:
                async with conn.cursor() as cur:
                    await cur.execute(_MYSQL_COMMENT_SQL)
                    row = await cur.fetchone()
                    description = (row[0] or "") if row else ""
            finally:
                conn.close()
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Connection failed: {exc}") from exc

    elif body.type == "sqlite":
        # SQLite has no database-level comments
        description = ""

    elif body.type in ("mssql", "sqlserver"):
        try:
            import aioodbc  # pyright: ignore[reportMissingImports]

            dsn = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={body.host},{body.port};"
                f"DATABASE={body.database};"
                f"UID={body.username};PWD={body.password}"
            )
            conn = await aioodbc.connect(dsn=dsn, timeout=5)
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT CAST(value AS NVARCHAR(MAX)) FROM sys.extended_properties "
                        "WHERE class = 0 AND name = 'MS_Description'"
                    )
                    row = await cur.fetchone()
                    description = (row[0] or "") if row else ""
            finally:
                await conn.close()
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Connection failed: {exc}") from exc

    return {"description": description}
