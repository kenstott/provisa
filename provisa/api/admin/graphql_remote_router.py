# Copyright (c) 2026 Kenneth Stott
# Canary: dbd213fd-531e-44d6-941b-179405293d2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes for GraphQL Remote Schema Connector (Phase AP).

Endpoints:
  POST /admin/sources/graphql-remote          — register source (introspect + auto-register)
  POST /admin/sources/graphql-remote/{id}/refresh — re-introspect, update registrations
"""
from __future__ import annotations
import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/sources/graphql-remote", tags=["admin", "graphql-remote"])


class GraphQLRemoteSourceRequest(BaseModel):
    source_id: str
    url: str
    namespace: str
    domain_id: str = ""
    auth: dict | None = None
    cache_ttl: int = 300


class GraphQLRemoteRegistration(BaseModel):
    source_id: str
    url: str
    namespace: str
    domain_id: str = ""
    auth: dict | None = None
    cache_ttl: int = 300
    tables: list[dict] = []
    functions: list[dict] = []


async def _introspect_and_map(
    source_id: str,
    url: str,
    namespace: str,
    domain_id: str,
    auth: dict | None,
) -> tuple[list[dict], list[dict]]:
    from provisa.graphql_remote.introspect import introspect_schema
    from provisa.graphql_remote.mapper import map_schema
    schema = await introspect_schema(url, auth)
    tables, functions = map_schema(schema, namespace, source_id, domain_id)
    return tables, functions


@router.post("")
async def register_graphql_remote_source(
    body: GraphQLRemoteSourceRequest,
    request: Request,
):
    """Register a GraphQL remote source: introspect schema and auto-register tables/functions."""
    state = request.app.state

    try:
        tables, functions = await _introspect_and_map(
            body.source_id, body.url, body.namespace, body.domain_id, body.auth,
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Introspection failed: {exc}") from exc

    registration = GraphQLRemoteRegistration(
        source_id=body.source_id,
        url=body.url,
        namespace=body.namespace,
        domain_id=body.domain_id,
        auth=body.auth,
        cache_ttl=body.cache_ttl,
        tables=tables,
        functions=functions,
    )

    if not hasattr(state, "graphql_remote_sources"):
        state.graphql_remote_sources = {}
    state.graphql_remote_sources[body.source_id] = registration.model_dump()

    log.info(
        "Registered GraphQL remote source %s (%d tables, %d functions)",
        body.source_id, len(tables), len(functions),
    )
    return {
        "source_id": body.source_id,
        "tables": len(tables),
        "functions": len(functions),
        "table_names": [t["name"] for t in tables],
        "function_names": [f["name"] for f in functions],
    }


@router.post("/{source_id}/refresh")
async def refresh_graphql_remote_source(source_id: str, request: Request):
    """Re-introspect a registered remote source and update its table/function registrations."""
    state = request.app.state
    sources = getattr(state, "graphql_remote_sources", {})
    if source_id not in sources:
        raise HTTPException(status_code=404, detail=f"GraphQL remote source {source_id!r} not found")

    reg = sources[source_id]
    try:
        tables, functions = await _introspect_and_map(
            source_id, reg["url"], reg["namespace"], reg.get("domain_id", ""), reg.get("auth"),
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Re-introspection failed: {exc}") from exc

    reg["tables"] = tables
    reg["functions"] = functions
    state.graphql_remote_sources[source_id] = reg

    log.info("Refreshed GraphQL remote source %s", source_id)
    return {
        "source_id": source_id,
        "tables": len(tables),
        "functions": len(functions),
        "table_names": [t["name"] for t in tables],
        "function_names": [f["name"] for f in functions],
    }


@router.get("")
async def list_graphql_remote_sources(request: Request):
    """List all registered GraphQL remote sources."""
    state = request.app.state
    sources = getattr(state, "graphql_remote_sources", {})
    return list(sources.values())
