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

from provisa.federation import connector_mssql, connector_mysql, connector_postgres

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/source-meta", tags=["admin", "source-meta"])


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
            description = await connector_postgres.fetch_database_comment(
                host=body.host,
                port=body.port,
                database=body.database,
                username=body.username,
                password=body.password,
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Connection failed: {exc}") from exc

    elif body.type in ("mysql", "mariadb"):
        try:
            description = await connector_mysql.fetch_database_comment(
                host=body.host,
                port=body.port,
                database=body.database,
                username=body.username,
                password=body.password,
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Connection failed: {exc}") from exc

    elif body.type == "sqlite":
        # SQLite has no database-level comments
        description = ""

    elif body.type in ("mssql", "sqlserver"):
        try:
            description = await connector_mssql.fetch_database_comment(
                host=body.host,
                port=body.port,
                database=body.database,
                username=body.username,
                password=body.password,
            )
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Connection failed: {exc}") from exc

    return {"description": description}
