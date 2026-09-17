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

from fastapi import APIRouter
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
    """Connect to the DB and return the database-level comment, if any.

    Best-effort autofill, never a validation gate: the actual connectivity check a source must
    pass lives in create_source's _add_source_pool (schema_common.py), which runs later at submit
    time with the source's full config (including federation_hints this preview never sees, e.g.
    sqlserver's trust_server_certificate for a self-signed cert). A failure here — wrong
    credentials typed so far, a cert this bare preview DSN doesn't trust, a source not listening
    yet — means only "no free description to offer", which an empty string already says; raising
    422 turned an expected, harmless outcome into a hard browser-console error for every caller
    (verified live 2026-09-16: SourcesPage.tsx's own caller already treats a non-OK response as
    "skip the autofill", so the 422 bought nothing but console noise no caller acted on)."""
    description = ""

    try:
        if body.type == "postgresql":
            description = await connector_postgres.fetch_database_comment(
                host=body.host,
                port=body.port,
                database=body.database,
                username=body.username,
                password=body.password,
            )
        elif body.type in ("mysql", "mariadb"):
            description = await connector_mysql.fetch_database_comment(
                host=body.host,
                port=body.port,
                database=body.database,
                username=body.username,
                password=body.password,
            )
        elif body.type in ("mssql", "sqlserver"):
            description = await connector_mssql.fetch_database_comment(
                host=body.host,
                port=body.port,
                database=body.database,
                username=body.username,
                password=body.password,
            )
        # sqlite has no database-level comments; every other type falls through with description=""
    except Exception:
        log.debug(
            "db-description preview failed for %r; no autofill offered", body.type, exc_info=True
        )

    return {"description": description}
