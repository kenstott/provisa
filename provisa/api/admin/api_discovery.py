# Copyright (c) 2026 Kenneth Stott
# Canary: 3d35d78d-cff2-4a89-ac31-cbc3a9499478
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin router for API source discovery and candidate management (Phase U)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from provisa.api_source.candidates import (
    accept_candidate,
    list_candidates,
    reject_candidate,
    store_candidates,
)
from provisa.api_source.introspect import (
    introspect_graphql,
    introspect_grpc,
    introspect_openapi,
)
from provisa.api_source.models import ApiSourceType

router = APIRouter(prefix="/admin/api-sources", tags=["api-sources"])


class DiscoverRequest(BaseModel):
    source_id: str
    type: ApiSourceType
    spec_url: str


class AcceptRequest(BaseModel):
    overrides: dict | None = None


@router.post("/discover")
async def discover(req: DiscoverRequest):
    """Trigger introspection of an API source."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    if req.type == ApiSourceType.openapi:
        candidates = await introspect_openapi(req.spec_url)
    elif req.type == ApiSourceType.graphql_api:
        candidates = await introspect_graphql(req.spec_url)
    elif req.type == ApiSourceType.grpc_api:
        candidates = await introspect_grpc(req.spec_url)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown source type: {req.type}")

    for c in candidates:
        c.source_id = req.source_id

    async with state.pg_pool.acquire() as conn:
        ids = await store_candidates(conn, req.source_id, candidates)

    return {"candidates_stored": len(ids), "ids": ids}


@router.get("/candidates")
async def get_candidates(source_id: str | None = None):
    """List discovered (pending) candidates."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    async with state.pg_pool.acquire() as conn:
        candidates = await list_candidates(conn, source_id)

    return [c.model_dump() for c in candidates]


@router.post("/candidates/{candidate_id}/accept")
async def accept(candidate_id: int, req: AcceptRequest | None = None):
    """Accept a candidate and register it as an endpoint."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    overrides = req.overrides if req else None
    async with state.pg_pool.acquire() as conn:
        endpoint = await accept_candidate(conn, candidate_id, overrides)

    return endpoint.model_dump()


@router.post("/candidates/{candidate_id}/reject")
async def reject(candidate_id: int):
    """Reject a candidate."""
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    async with state.pg_pool.acquire() as conn:
        await reject_candidate(conn, candidate_id)

    return {"status": "rejected"}
