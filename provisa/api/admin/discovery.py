# Copyright (c) 2026 Kenneth Stott
# Canary: 1971faf0-92e4-46f1-8075-f8a18f370534
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin API router for LLM relationship discovery."""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from provisa.api.app import state
from provisa.discovery import candidates as candidates_repo
from provisa.discovery.analyzer import analyze
from provisa.discovery.collector import collect_metadata
from provisa.discovery.prompt import build_prompt

router = APIRouter(prefix="/admin/discover")


class DiscoverRequest(BaseModel):
    scope: str  # "table", "domain", "cross-domain"
    table_id: int | None = None
    domain_id: str | None = None
    domain_ids: list[str] | None = None


class RejectRequest(BaseModel):
    reason: str


class AcceptRequest(BaseModel):
    name: str | None = None


def _get_api_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not set")
    return key


import logging as _logging
_log = _logging.getLogger(__name__)


@router.post("/relationships")
async def trigger_discovery(body: DiscoverRequest):
    """Trigger LLM relationship discovery."""
    api_key = _get_api_key()

    scope_id: str | int | None = None
    if body.scope == "table":
        if body.table_id is None:
            raise HTTPException(status_code=400, detail="table_id required for table scope")
        scope_id = body.table_id
    elif body.scope == "domain":
        if body.domain_id is None:
            raise HTTPException(status_code=400, detail="domain_id required for domain scope")
        scope_id = body.domain_id

    async with state.pg_pool.acquire() as conn:
        discovery_input = await collect_metadata(
            state.trino_conn, conn, body.scope, scope_id,
        )
        _log.warning(
            "Discovery metadata: %d tables, columns per table: %s",
            len(discovery_input.tables),
            {t.table_name: len(t.columns) for t in discovery_input.tables},
        )
        prompt = build_prompt(discovery_input)
        candidates = analyze(prompt, api_key, discovery_input)
        _log.warning("LLM returned %d candidates after validation", len(candidates))
        stored_ids = await candidates_repo.store_candidates(conn, candidates, body.scope)

    return {"candidates_found": len(candidates), "stored_ids": stored_ids}


@router.get("/candidates")
async def list_candidates():
    """List pending relationship candidates."""
    async with state.pg_pool.acquire() as conn:
        return await candidates_repo.list_pending(conn)


@router.post("/candidates/{candidate_id}/accept")
async def accept_candidate(candidate_id: int, body: AcceptRequest | None = None):
    """Accept a relationship candidate."""
    async with state.pg_pool.acquire() as conn:
        return await candidates_repo.accept(conn, candidate_id, body.name if body else None)


@router.post("/candidates/{candidate_id}/reject")
async def reject_candidate(candidate_id: int, body: RejectRequest):
    """Reject a relationship candidate."""
    async with state.pg_pool.acquire() as conn:
        await candidates_repo.reject(conn, candidate_id, body.reason)
    return {"status": "rejected"}


@router.get("/candidates/rejected/count")
async def rejected_count():
    """Count rejected candidates."""
    async with state.pg_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM relationship_candidates WHERE status = 'rejected'"
        )
    return {"count": count}


@router.delete("/candidates/rejected")
async def clear_rejections():
    """Delete all rejected candidates."""
    async with state.pg_pool.acquire() as conn:
        count = await candidates_repo.clear_rejections(conn)
    return {"deleted": count}
