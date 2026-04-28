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

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from provisa.sparql.source import (
    SparqlSourceConfig,
    build_api_source,
    build_endpoint,
    extract_variables,
    infer_columns,
    probe_endpoint,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/sources/sparql", tags=["admin", "sparql"])


class SparqlSourceRequest(BaseModel):
    source_id: str
    endpoint_url: str
    default_graph_uri: str | None = None


class SparqlTableRequest(BaseModel):
    table_name: str
    sparql_query: str
    ttl: int = 300
    column_overrides: dict[str, str] | None = None  # var_name → type override


@router.post("")
async def register_sparql_source(body: SparqlSourceRequest, request: Request):
    """Register a SPARQL source."""
    state = request.app.state
    cfg = SparqlSourceConfig(
        source_id=body.source_id,
        endpoint_url=body.endpoint_url,
        default_graph_uri=body.default_graph_uri,
    )
    api_source = build_api_source(cfg)
    if not hasattr(state, "api_sources"):
        state.api_sources = {}
    state.api_sources[api_source.id] = api_source
    log.info("Registered SPARQL source %s at %s", body.source_id, body.endpoint_url)
    return {"source_id": api_source.id, "base_url": api_source.base_url}


@router.post("/{source_id}/tables")
async def register_sparql_table(
    source_id: str,
    body: SparqlTableRequest,
    request: Request,
):
    """Register a SPARQL table.

    Executes a LIMIT 5 probe to validate the endpoint and infer column names
    from the SPARQL SELECT variables. The table appears in the GraphQL schema
    via the api_source schema integration.
    """
    state = request.app.state
    api_source = getattr(state, "api_sources", {}).get(source_id)
    if api_source is None:
        raise HTTPException(status_code=404, detail=f"SPARQL source {source_id!r} not found")

    cfg = SparqlSourceConfig(
        source_id=source_id,
        endpoint_url=str(api_source.base_url),
    )

    try:
        rows = await probe_endpoint(cfg, body.sparql_query)
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"SPARQL endpoint probe failed: {exc}",
        ) from exc

    # Infer columns from probe results; fall back to SELECT variable names
    columns = infer_columns(rows) if rows else [
        _make_string_col(v) for v in extract_variables(body.sparql_query)
    ]
    if not columns:
        raise HTTPException(
            status_code=422,
            detail="Could not infer columns: probe returned no rows and no SELECT variables found.",
        )

    endpoint = build_endpoint(cfg, body.table_name, body.sparql_query, columns, body.ttl)

    if not hasattr(state, "api_endpoints"):
        state.api_endpoints = []
    state.api_endpoints.append(endpoint)
    log.info("Registered SPARQL table %s on source %s", body.table_name, source_id)

    return {
        "table_name": body.table_name,
        "columns": [c.model_dump() for c in columns],
    }


def _make_string_col(name: str):
    from provisa.api_source.models import ApiColumn, ApiColumnType
    return ApiColumn(name=name, type=ApiColumnType.string)
