# Copyright (c) 2026 Kenneth Stott
# Canary: b8c9d0e1-f2a3-4567-1234-678901234567
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes for SPARQL source registration (Phase AO).

Endpoints:
  POST /admin/sources/sparql            — register a SPARQL source
  POST /admin/sources/sparql/{id}/tables — register a table (probe validates endpoint)
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.api_source.persist import persist_api_source

from provisa.sparql.source import (
    SparqlSourceConfig,
    build_api_source,
)
from provisa.api.admin.capabilities import require_capability_request

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/sources/sparql", tags=["admin", "sparql"])

# Requirements: REQ-297, REQ-298, REQ-299


def _control_plane(state):
    """The tenant control plane the rows land in; registration without one is a 503, not a dict."""
    db = getattr(state, "model_db", None)
    if db is None:
        raise ApiError(503, "sparql.database_not_connected", "Database not connected")
    return db


class SparqlSourceRequest(BaseModel):
    source_id: str
    endpoint_url: str
    default_graph_uri: str | None = None


@router.post("")
async def register_sparql_source(body: SparqlSourceRequest, request: Request):  # REQ-297, REQ-298
    """Register a SPARQL source."""
    require_capability_request(request, "source_registration")
    state = request.app.state
    cfg = SparqlSourceConfig(
        source_id=body.source_id,
        endpoint_url=body.endpoint_url,
        default_graph_uri=body.default_graph_uri,
    )
    api_source = build_api_source(cfg)
    # REQ-1683: the source is a control-plane row, not a process-lifetime dict entry.
    async with _control_plane(state).acquire() as conn:
        await persist_api_source(conn, api_source)
    if not hasattr(state, "api_sources"):
        state.api_sources = {}
    state.api_sources[api_source.id] = api_source
    if not hasattr(state, "sparql_endpoints"):
        state.sparql_endpoints = {}
    state.sparql_endpoints[api_source.id] = body.endpoint_url
    log.info("Registered SPARQL source %s at %s", body.source_id, body.endpoint_url)
    return {"source_id": api_source.id, "base_url": api_source.base_url}
