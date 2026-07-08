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

# Requirements: REQ-307, REQ-308, REQ-311, REQ-314, REQ-316, REQ-317, REQ-321, REQ-322, REQ-329

from __future__ import annotations


from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from urllib.parse import urlparse

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
from sqlalchemy.exc import IntegrityError
from provisa.core.schema_org import api_sources
from provisa.otel_compat import get_tracer as _get_tracer

_tracer = _get_tracer(__name__)

router = APIRouter(prefix="/admin/api-sources", tags=["api-sources"])


class DiscoverRequest(BaseModel):
    source_id: str
    type: ApiSourceType
    spec_url: str
    base_url: str | None = None


class AcceptRequest(BaseModel):
    overrides: dict | None = None


@router.post("/discover")
async def discover(req: DiscoverRequest):  # REQ-307, REQ-314, REQ-322
    """Trigger introspection of an API source."""
    with _tracer.start_as_current_span("admin.api_discovery"):
        from provisa.api.app import state

        if state.tenant_db is None:
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

        parsed = urlparse(req.spec_url)
        base_url = req.base_url or f"{parsed.scheme}://{parsed.netloc}"

        pool = state.tenant_db
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.upsert(
                api_sources,
                {
                    "id": req.source_id,
                    "type": req.type.value,
                    "base_url": base_url,
                    "spec_url": req.spec_url,
                },
                index_elements=["id"],
                update_columns=["type", "base_url", "spec_url"],
            )
            ids = await store_candidates(conn, req.source_id, candidates)

    return {"candidates_stored": len(ids), "ids": ids}


@router.get("/candidates")
async def get_candidates(source_id: str | None = None):  # REQ-308, REQ-316, REQ-325
    """List discovered (pending) candidates."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as _conn:
        candidates = await list_candidates(_conn, source_id)

    return [c.model_dump() for c in candidates]


@router.post("/candidates/{candidate_id}/accept")
async def accept(candidate_id: int, req: AcceptRequest | None = None):  # REQ-311, REQ-321, REQ-329
    """Accept a candidate and register it as an endpoint."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    overrides = req.overrides if req else None
    pool = state.tenant_db
    assert pool is not None
    try:
        async with pool.acquire() as _conn:
            endpoint = await accept_candidate(_conn, candidate_id, overrides)
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Endpoint already registered: {e}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return endpoint.model_dump()


@router.post("/candidates/{candidate_id}/reject")
async def reject(candidate_id: int):  # REQ-311, REQ-321, REQ-329
    """Reject a candidate."""
    from provisa.api.app import state

    if state.tenant_db is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as _conn:
        await reject_candidate(_conn, candidate_id)

    return {"status": "rejected"}
