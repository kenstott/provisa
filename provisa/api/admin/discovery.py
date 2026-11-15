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

# Requirements: REQ-018, REQ-167, REQ-413, REQ-612

from __future__ import annotations

import asyncio
import logging as _logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from provisa.api.app import state
from provisa.api.errors import ApiError
from provisa.discovery import candidates as candidates_repo
from provisa.discovery.analyzer import RelationshipCandidate, analyze_answer
from provisa.discovery.collector import collect_fk_candidates, collect_metadata
from provisa.discovery.prompt import build_prompt
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import stage as _stage
from provisa.api.admin.capabilities import require_capability_request

if TYPE_CHECKING:
    from provisa.core.database import Connection

_tracer = _get_tracer(__name__)

router = APIRouter(prefix="/admin/discover")

#: The confidence a semantic suggestion needs to be kept.
MIN_CONFIDENCE = 0.7

#: The kinds of discovery a run can be asked for: foreign keys read from the sources' own
#: constraints, and semantics inferred by the model assigned to relationship inference.
DiscoveryKind = Literal["foreign_keys", "semantics"]

#: Where a model and its key are set, as the admin UI names it.
WHERE_TO_SET = "Admin, AI Models"


class DiscoverRequest(BaseModel):
    scope: str  # "table", "domain", "cross-domain"
    kinds: list[DiscoveryKind] = Field(min_length=1)
    table_id: int | None = None
    domain_id: str | None = None
    domain_ids: list[str] | None = None


class RejectRequest(BaseModel):
    reason: str


class AcceptRequest(BaseModel):
    name: str | None = None


_log = _logging.getLogger(__name__)


@dataclass
class SemanticModel:  # REQ-413
    """The model assigned to relationship inference in the acting org, and why it cannot run
    (``problem``), judged by that model's own key; None when it can."""

    vendor: str
    model: str
    problem: str | None
    config: dict[str, Any]
    api_keys: dict[str, str]

    @property
    def label(self) -> str:
        return f"{self.vendor}:{self.model}"


async def semantic_model(pool: Any) -> SemanticModel:  # REQ-413
    """The acting org's model for relationship inference, with its config and keys resolved the
    way every org-bound model call resolves them."""
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.core.org_settings import resolve_org_config
    from provisa.llm.client import ProvisaLLMClient

    config = await resolve_org_config(pool)
    api_keys = await read_org_api_keys(pool)
    client = ProvisaLLMClient("relationship_inference", config=config, api_keys=api_keys)
    return SemanticModel(client.vendor, client.model, client.key_problem(), config, api_keys)


def _semantics_availability(model: SemanticModel) -> dict[str, Any]:
    return {
        "available": model.problem is None,
        "model": model.label,
        "reason": None if model.problem is None else "no_key",
    }


@router.get("/kinds")
async def discovery_kinds(request: Request):  # REQ-413
    """Which kinds of discovery can run: foreign keys always; semantics when the model assigned to
    relationship inference has its key."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    return {
        "foreign_keys": {"available": True},
        "semantics": _semantics_availability(await semantic_model(pool)),
    }


def _key(rel: dict | RelationshipCandidate) -> tuple[int, str, int, str]:
    """The columns a relationship joins, the identity of a candidate."""
    if isinstance(rel, RelationshipCandidate):
        return (rel.source_table_id, rel.source_column, rel.target_table_id, rel.target_column)
    return (
        int(rel["source_table_id"]),
        str(rel["source_column"]),
        int(rel["target_table_id"]),
        str(rel["target_column"]),
    )


async def _semantics(
    conn: "Connection",
    engine: Any,
    scope: str,
    scope_id: str | int | None,
    model: SemanticModel,
    found_by_fk: set[tuple[int, str, int, str]],
) -> tuple[dict[str, Any], list[RelationshipCandidate]]:  # REQ-018, REQ-167, REQ-413
    """Semantic suggestions from the assigned model, and what this kind found: how many, and when
    none, why. A failure of the model is raised as an error, not reported as none found."""
    discovery_input = await collect_metadata(engine, conn, scope, scope_id)
    rejected = await conn.fetch(
        "SELECT source_table_id, source_column, target_table_id, target_column "
        "FROM relationship_candidates WHERE status = 'rejected'"
    )
    discovery_input.rejected_pairs = [dict(r) for r in rejected]
    prompt = build_prompt(discovery_input)
    try:
        analysis = await asyncio.to_thread(
            analyze_answer,
            prompt,
            model.api_keys,
            discovery_input,
            MIN_CONFIDENCE,
            config=model.config,
        )
    except Exception as exc:
        raise ApiError(
            502,
            "discovery.model_failed",
            f"The model {model.label} failed: {exc}",
            model=model.label,
            error=str(exc),
        ) from exc
    kept = analysis.candidates
    below_threshold = analysis.below_threshold

    # Optional: when a Jev key is configured, replace each candidate's self-reported confidence
    # with Jev's calibrated score (REQ-1789); those it drops fell below the threshold.
    from provisa.core.org_secrets import resolve_jev_api_key

    jev_key = await resolve_jev_api_key(state.model_db)
    if jev_key and kept:
        from provisa.jev.decisions import refine_relationship_confidence

        refined = await refine_relationship_confidence(kept, jev_key, min_confidence=MIN_CONFIDENCE)
        below_threshold += len(kept) - len(refined)
        kept = refined

    known = (
        {_key(r) for r in discovery_input.existing_relationships}
        | {_key(r) for r in discovery_input.rejected_pairs}
        | found_by_fk
    )
    fresh = [c for c in kept if _key(c) not in known]
    if fresh:
        empty_reason = None
    elif analysis.unreadable:
        empty_reason = "unreadable"
    elif analysis.proposed == 0:
        empty_reason = "none_proposed"
    else:
        empty_reason = "filtered"
    return {
        "suggested": len(fresh),
        "proposed": analysis.proposed,
        "below_threshold": below_threshold,
        "threshold": MIN_CONFIDENCE,
        "already_known": len(kept) - len(fresh),
        "invalid": analysis.invalid,
        "empty_reason": empty_reason,
    }, fresh


@router.post("/relationships")
async def trigger_discovery(
    request: Request, body: DiscoverRequest
):  # REQ-018, REQ-167, REQ-413, REQ-612
    """Run the kinds of relationship discovery asked for, and say what each found."""
    require_capability_request(request, "create_relationship")
    with _stage(_tracer, "admin.discovery") as span:
        scope_id: str | int | None = None
        if body.scope == "table":
            if body.table_id is None:
                raise ApiError(
                    400, "discovery.table_id_required", "table_id required for table scope"
                )
            scope_id = body.table_id
        elif body.scope == "domain":
            if body.domain_id is None:
                raise ApiError(
                    400, "discovery.domain_id_required", "domain_id required for domain scope"
                )
            scope_id = body.domain_id

        kinds = set(body.kinds)
        pool = state.model_db
        assert pool is not None
        model: SemanticModel | None = None
        if "semantics" in kinds:
            model = await semantic_model(pool)
            if model.problem is not None:
                raise ApiError(
                    409,
                    "discovery.semantics_unavailable",
                    f"Semantics cannot run: {model.label}, the model assigned to relationship "
                    f"inference, has no key. Set one under {WHERE_TO_SET}.",
                    model=model.label,
                    where=WHERE_TO_SET,
                )

        report: dict[str, Any] = {}
        all_candidates: list[RelationshipCandidate] = []
        async with pool.acquire() as _conn:
            conn = cast("Connection", _conn)
            found_by_fk: set[tuple[int, str, int, str]] = set()
            if "foreign_keys" in kinds:
                fk_candidates = await collect_fk_candidates(
                    state.source_pools,
                    state.source_types,
                    conn,
                    body.scope,
                    scope_id,
                )
                _log.warning("FK introspection returned %d candidates", len(fk_candidates))
                found_by_fk = {_key(c) for c in fk_candidates}
                all_candidates.extend(fk_candidates)
                report["foreign_keys"] = {"found": len(fk_candidates)}
            if model is not None:
                engine = state.federation_engine
                assert engine is not None
                report["semantics"], suggested = await _semantics(
                    conn, engine, body.scope, scope_id, model, found_by_fk
                )
                all_candidates.extend(suggested)

            stored_ids = await candidates_repo.store_candidates(conn, all_candidates, body.scope)

        span.set_attribute("admin.source_count", len(all_candidates))
        return {"candidates_found": len(all_candidates), "stored_ids": stored_ids, **report}


@router.get("/candidates")
async def list_candidates(request: Request):  # REQ-612
    """List pending relationship candidates."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        return await candidates_repo.list_pending(cast("Connection", _conn))


@router.post("/candidates/{candidate_id}/accept")
async def accept_candidate(
    request: Request, candidate_id: int, body: AcceptRequest | None = None
):  # REQ-612
    """Accept a relationship candidate."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        return await candidates_repo.accept(
            cast("Connection", _conn), candidate_id, body.name if body else None
        )


@router.post("/candidates/{candidate_id}/reject")
async def reject_candidate(request: Request, candidate_id: int, body: RejectRequest):  # REQ-612
    """Reject a relationship candidate."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        await candidates_repo.reject(cast("Connection", _conn), candidate_id, body.reason)
    return {"status": "rejected"}


@router.get("/candidates/rejected/count")
async def rejected_count(request: Request):
    """Count rejected candidates."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM relationship_candidates WHERE status = 'rejected'"
        )
    return {"count": count}


@router.delete("/candidates/rejected")
async def clear_rejections(request: Request):
    """Delete all rejected candidates."""
    require_capability_request(request, "create_relationship")
    pool = state.model_db
    assert pool is not None
    async with pool.acquire() as _conn:
        count = await candidates_repo.clear_rejections(cast("Connection", _conn))
    return {"deleted": count}
