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

import logging as _logging
import os
from typing import cast

import asyncpg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from provisa.api.app import state
from provisa.discovery import candidates as candidates_repo
from provisa.discovery.analyzer import analyze
from provisa.discovery.collector import collect_fk_candidates, collect_metadata
from provisa.discovery.prompt import build_prompt
from provisa.otel_compat import get_tracer as _get_tracer

_tracer = _get_tracer(__name__)

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


_log = _logging.getLogger(__name__)


@router.post("/relationships")
async def trigger_discovery(body: DiscoverRequest):
    """Trigger relationship discovery: FK constraints always, LLM inference if ANTHROPIC_API_KEY set."""
    with _tracer.start_as_current_span("admin.discovery") as span:
        scope_id: str | int | None = None
        if body.scope == "table":
            if body.table_id is None:
                raise HTTPException(status_code=400, detail="table_id required for table scope")
            scope_id = body.table_id
        elif body.scope == "domain":
            if body.domain_id is None:
                raise HTTPException(status_code=400, detail="domain_id required for domain scope")
            scope_id = body.domain_id

        all_candidates = []
        pool = state.pg_pool
        assert pool is not None
        trino_conn = state.trino_conn
        assert trino_conn is not None
        async with pool.acquire() as _conn:
            conn = cast(asyncpg.Connection, _conn)
            fk_candidates = await collect_fk_candidates(
                trino_conn,
                conn,
                body.scope,
                scope_id,
            )
            _log.warning("FK introspection returned %d candidates", len(fk_candidates))
            all_candidates.extend(fk_candidates)

            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if api_key:
                discovery_input = await collect_metadata(
                    trino_conn,
                    conn,
                    body.scope,
                    scope_id,
                )
                _log.warning(
                    "LLM discovery metadata: %d tables, columns per table: %s",
                    len(discovery_input.tables),
                    {t.table_name: len(t.columns) for t in discovery_input.tables},
                )
                prompt = build_prompt(discovery_input)
                llm_candidates = analyze(prompt, api_key, discovery_input)
                _log.warning("LLM returned %d candidates after validation", len(llm_candidates))
                all_candidates.extend(llm_candidates)

            stored_ids = await candidates_repo.store_candidates(conn, all_candidates, body.scope)

        span.set_attribute("admin.source_count", len(all_candidates))
        return {"candidates_found": len(all_candidates), "stored_ids": stored_ids}


@router.get("/candidates")
async def list_candidates():
    """List pending relationship candidates."""
    pool = state.pg_pool
    assert pool is not None
    async with pool.acquire() as _conn:
        return await candidates_repo.list_pending(cast(asyncpg.Connection, _conn))


@router.post("/candidates/{candidate_id}/accept")
async def accept_candidate(candidate_id: int, body: AcceptRequest | None = None):
    """Accept a relationship candidate."""
    pool = state.pg_pool
    assert pool is not None
    async with pool.acquire() as _conn:
        return await candidates_repo.accept(
            cast(asyncpg.Connection, _conn), candidate_id, body.name if body else None
        )


@router.post("/candidates/{candidate_id}/reject")
async def reject_candidate(candidate_id: int, body: RejectRequest):
    """Reject a relationship candidate."""
    pool = state.pg_pool
    assert pool is not None
    async with pool.acquire() as _conn:
        await candidates_repo.reject(cast(asyncpg.Connection, _conn), candidate_id, body.reason)
    return {"status": "rejected"}


@router.get("/candidates/rejected/count")
async def rejected_count():
    """Count rejected candidates."""
    pool = state.pg_pool
    assert pool is not None
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM relationship_candidates WHERE status = 'rejected'"
        )
    return {"count": count}


@router.delete("/candidates/rejected")
async def clear_rejections():
    """Delete all rejected candidates."""
    pool = state.pg_pool
    assert pool is not None
    async with pool.acquire() as _conn:
        count = await candidates_repo.clear_rejections(cast(asyncpg.Connection, _conn))
    return {"deleted": count}
