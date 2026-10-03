# Copyright (c) 2026 Kenneth Stott
# Canary: a7b8c9d0-e1f2-3456-0123-567890123456
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes for Neo4j source registration (Phase AO).

Endpoints:
  POST /admin/sources/neo4j            — register a Neo4j source
  POST /admin/sources/neo4j/{id}/preview — preview a Cypher query (sample rows)
  POST /admin/sources/neo4j/{id}/tables  — register a table (runs preview+validate)
"""

# Requirements: REQ-295, REQ-296, REQ-298, REQ-299

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.api_source.persist import persist_api_source
from provisa.neo4j.source import (
    Neo4jSourceConfig,
    build_api_source,
)
from provisa.api.admin.capabilities import require_capability_request

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/sources/neo4j", tags=["admin", "neo4j"])


def _control_plane(state):
    """The tenant control plane the rows land in; registration without one is a 503, not a dict."""
    db = getattr(state, "tenant_db", None)
    if db is None:
        raise ApiError(503, "neo4j.database_not_connected", "Database not connected")
    return db


class Neo4jSourceRequest(BaseModel):
    source_id: str
    host: str
    port: int = 7474
    database: str = "neo4j"
    use_https: bool = False
    # Optional basic auth; prefer ApiAuth in production
    username: str | None = None
    password: str | None = None


class Neo4jPreviewRequest(BaseModel):
    cypher: str


@router.post("")
async def register_neo4j_source(body: Neo4jSourceRequest, request: Request):  # REQ-295
    """Register a Neo4j source."""
    require_capability_request(request, "source_registration")
    state = request.app.state
    cfg = Neo4jSourceConfig(
        source_id=body.source_id,
        host=body.host,
        port=body.port,
        database=body.database,
        use_https=body.use_https,
    )
    api_source = build_api_source(cfg)
    # REQ-1668: the source is a control-plane row, not a process-lifetime dict entry.
    async with _control_plane(state).acquire() as conn:
        await persist_api_source(conn, api_source)
    if not hasattr(state, "api_sources"):
        state.api_sources = {}
    state.api_sources[api_source.id] = api_source
    if not hasattr(state, "neo4j_configs"):
        state.neo4j_configs = {}
    state.neo4j_configs[api_source.id] = cfg
    log.info("Registered Neo4j source %s at %s:%d", body.source_id, body.host, body.port)
    return {"source_id": api_source.id, "base_url": api_source.base_url}


@router.post("/{source_id}/preview")
async def preview_neo4j_query(  # REQ-296, REQ-298, REQ-299
    source_id: str,
    body: Neo4jPreviewRequest,
    request: Request,
):
    """Preview a Cypher query (LIMIT 5).

    Returns sample rows or a shape validation error if node objects are returned.
    """
    require_capability_request(request, "source_registration")
    # REQ-1670: the same preview the registerTable form runs (a Source row registered through
    # createSource resolves here too, not only one registered through this router).
    from provisa.api.admin._neo4j_registration import preview_neo4j

    state = request.app.state
    async with _control_plane(state).acquire() as conn:
        result = await preview_neo4j(conn, source_id, body.cypher)
    if result.error is not None:
        raise HTTPException(status_code=422, detail=result.error)
    return {
        "rows": result.rows,
        "columns": [{"name": c.name, "data_type": c.data_type} for c in result.columns],
    }
