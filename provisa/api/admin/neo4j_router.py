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

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from provisa.neo4j.preview import Neo4jNodeObjectError, preview_query, validate_shape
from provisa.neo4j.source import (
    Neo4jSourceConfig,
    build_api_source,
    build_endpoint,
    infer_columns,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/sources/neo4j", tags=["admin", "neo4j"])


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


class Neo4jTableRequest(BaseModel):
    table_name: str
    cypher: str
    ttl: int = 300


@router.post("")
async def register_neo4j_source(body: Neo4jSourceRequest, request: Request):
    """Register a Neo4j source."""
    state = request.app.state
    cfg = Neo4jSourceConfig(
        source_id=body.source_id,
        host=body.host,
        port=body.port,
        database=body.database,
        use_https=body.use_https,
    )
    api_source = build_api_source(cfg)
    # Persist to state — mirrors how api_discovery registers sources
    if not hasattr(state, "api_sources"):
        state.api_sources = {}
    state.api_sources[api_source.id] = api_source
    log.info("Registered Neo4j source %s at %s:%d", body.source_id, body.host, body.port)
    return {"source_id": api_source.id, "base_url": api_source.base_url}


@router.post("/{source_id}/preview")
async def preview_neo4j_query(
    source_id: str,
    body: Neo4jPreviewRequest,
    request: Request,
):
    """Preview a Cypher query (LIMIT 5).

    Returns sample rows or a shape validation error if node objects are returned.
    """
    state = request.app.state
    api_source = getattr(state, "api_sources", {}).get(source_id)
    if api_source is None:
        raise HTTPException(status_code=404, detail=f"Neo4j source {source_id!r} not found")

    try:
        rows = await preview_query(
            base_url=api_source.base_url,
            database="neo4j",  # TODO: store database on source config
            cypher=body.cypher,
        )
        validate_shape(rows)
    except Neo4jNodeObjectError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    columns = infer_columns(rows)
    return {"rows": rows, "columns": [c.model_dump() for c in columns]}


@router.post("/{source_id}/tables")
async def register_neo4j_table(
    source_id: str,
    body: Neo4jTableRequest,
    request: Request,
):
    """Register a Neo4j table (runs preview+validate before persisting).

    The table appears in the GraphQL schema via the api_source schema integration.
    """
    state = request.app.state
    api_source = getattr(state, "api_sources", {}).get(source_id)
    if api_source is None:
        raise HTTPException(status_code=404, detail=f"Neo4j source {source_id!r} not found")

    # Preview + validate before persisting
    try:
        rows = await preview_query(
            base_url=api_source.base_url,
            database="neo4j",
            cypher=body.cypher,
        )
        validate_shape(rows)
    except Neo4jNodeObjectError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    columns = infer_columns(rows)
    cfg = Neo4jSourceConfig(source_id=source_id, host="")  # base_url already on api_source
    endpoint = build_endpoint(cfg, body.table_name, body.cypher, columns, body.ttl)

    # Persist endpoint
    if not hasattr(state, "api_endpoints"):
        state.api_endpoints = []
    state.api_endpoints.append(endpoint)
    log.info("Registered Neo4j table %s on source %s", body.table_name, source_id)

    return {
        "table_name": body.table_name,
        "columns": [c.model_dump() for c in columns],
    }
