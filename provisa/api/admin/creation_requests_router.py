# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REST endpoints for the creation-request queue (REQ-063/434/480)."""

# Requirements: REQ-042, REQ-060, REQ-063, REQ-366, REQ-434

from __future__ import annotations

import json
from typing import cast

import asyncpg
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

router = APIRouter(prefix="/admin/creation-requests", tags=["admin"])

_REJECTION_REASONS: dict[str, list[str]] = {
    "relationship": [
        "duplicate",
        "incorrect_join_columns",
        "wrong_cardinality",
        "source_not_registered",
        "insufficient_detail",
    ],
    "view": [
        "duplicate",
        "query_invalid",
        "governance_violation",
        "out_of_scope",
        "insufficient_detail",
    ],
    "webhook_registration": [
        "duplicate",
        "endpoint_unreachable",
        "schema_mismatch",
        "governance_violation",
        "insufficient_detail",
    ],
}

_REQUIRED_APPROVALS: dict[str, int] = {
    "relationship": 2,
}


def _get_pool() -> asyncpg.Pool:
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


def _user_id(request: Request) -> str | None:
    identity = getattr(request.state, "identity", None)
    return getattr(identity, "user_id", None) if identity is not None else None


def _require_capability(request: Request, capability: str) -> None:
    """Raise HTTPException 403 if caller lacks capability. Dev/no-auth mode skips enforcement."""
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    user_id = getattr(identity, "user_id", None) if identity is not None else None
    # Dev / no-auth mode — skip enforcement
    if not user_id or user_id == "anonymous":
        return
    roles: dict[str, dict] = getattr(state, "roles", {})
    caps: set[str] = set()
    for claim in getattr(identity, "roles", []):
        role_id = claim.strip().split(":")[0] if ":" in claim.strip() else claim.strip()
        role = roles.get(role_id) or {}
        for c in role.get("capabilities") or []:
            caps.add(c)
    if "superadmin" not in caps and "admin" not in caps and capability not in caps:
        raise HTTPException(status_code=403, detail=f"Missing capability: {capability!r}")


def _deserialize(row: dict) -> dict:
    out = dict(row)
    if isinstance(out.get("approvals"), str):
        out["approvals"] = json.loads(out["approvals"])
    if isinstance(out.get("payload"), str):
        out["payload"] = json.loads(out["payload"])
    return out


class SubmitBody(BaseModel):
    request_type: str
    capability: str
    payload: dict


class RejectBody(BaseModel):
    reason: str


@router.post("/")
async def submit_request(body: SubmitBody, request: Request):  # REQ-063, REQ-434
    if body.request_type not in _REJECTION_REASONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown request_type {body.request_type!r}. Must be one of {list(_REJECTION_REASONS)}",
        )
    # No default approval count — an unlisted request_type must be rejected
    # rather than silently requiring a single approval.
    if body.request_type not in _REQUIRED_APPROVALS:
        raise HTTPException(
            status_code=400,
            detail=f"No approval policy for request_type {body.request_type!r}",
        )
    required = _REQUIRED_APPROVALS[body.request_type]
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast(asyncpg.Connection, _conn)
        rid = await conn.fetchval(
            """
            INSERT INTO creation_requests
                (request_type, capability, payload, requested_by, required_approvals)
            VALUES ($1, $2, $3::jsonb, $4, $5)
            RETURNING id
            """,
            body.request_type,
            body.capability,
            json.dumps(body.payload),
            _user_id(request),
            required,
        )
    return {"id": rid, "status": "pending"}


@router.get("/rejection-reasons")
async def rejection_reasons():
    return _REJECTION_REASONS


@router.get("/")
async def list_requests(  # REQ-063, REQ-434  # pyright: ignore[reportUnusedParameter]
    _request: Request,
    status: str | None = Query(None),
    request_type: str | None = Query(None),
):
    conditions: list[str] = []
    params: list[object] = []
    if status:
        params.append(status)
        conditions.append(f"status = ${len(params)}")
    if request_type:
        params.append(request_type)
        conditions.append(f"request_type = ${len(params)}")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast(asyncpg.Connection, _conn)
        rows = await conn.fetch(
            f"SELECT * FROM creation_requests {where} ORDER BY created_at DESC",
            *params,
        )
    return [_deserialize(dict(r)) for r in rows]


@router.post("/{request_id}/approve")
async def approve_request(request_id: int, request: Request):  # REQ-063, REQ-366, REQ-434
    user_id = _user_id(request)
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast(asyncpg.Connection, _conn)
        row = await conn.fetchrow("SELECT * FROM creation_requests WHERE id = $1", request_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Request not found")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"Request is already {row['status']}")
        _require_capability(request, row["capability"])
        entry = json.dumps([{"approver": user_id, "approved_at": "now"}])
        updated_row = await conn.fetchrow(
            """
            UPDATE creation_requests
            SET approvals = approvals || $2::jsonb
            WHERE id = $1 AND status = 'pending'
            RETURNING *
            """,
            request_id,
            entry,
        )
        if updated_row is None:
            raise HTTPException(status_code=409, detail="Could not record approval")
        updated = _deserialize(dict(updated_row))
        approvals = updated.get("approvals") or []
        required = updated.get("required_approvals", 1)
        if len(approvals) >= required:
            await conn.execute(
                "UPDATE creation_requests SET status = 'executed', resolved_by = $2, resolved_at = NOW() "
                "WHERE id = $1 AND status = 'pending'",
                request_id,
                user_id,
            )
            updated["status"] = "executed"
    return updated


@router.post("/{request_id}/reject")
async def reject_request(
    request_id: int, body: RejectBody, request: Request
):  # REQ-063, REQ-366, REQ-434
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast(asyncpg.Connection, _conn)
        row = await conn.fetchrow("SELECT * FROM creation_requests WHERE id = $1", request_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Request not found")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"Request is already {row['status']}")
        _require_capability(request, row["capability"])
        valid = _REJECTION_REASONS.get(row["request_type"], [])
        if body.reason not in valid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid reason {body.reason!r} for type {row['request_type']!r}. Valid: {valid}",
            )
        result = await conn.execute(
            "UPDATE creation_requests SET status = 'rejected', rejection_reason = $2, "
            "resolved_by = $3, resolved_at = NOW() WHERE id = $1 AND status = 'pending'",
            request_id,
            body.reason,
            _user_id(request),
        )
        if result != "UPDATE 1":
            raise HTTPException(status_code=409, detail="Could not reject request")
    return {"id": request_id, "status": "rejected", "reason": body.reason}


@router.post("/{request_id}/execute")
async def execute_request(request_id: int, request: Request):  # REQ-063, REQ-366, REQ-434
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast(asyncpg.Connection, _conn)
        row = await conn.fetchrow("SELECT * FROM creation_requests WHERE id = $1", request_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Request not found")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"Request is already {row['status']}")
        _require_capability(request, row["capability"])
        result = await conn.execute(
            "UPDATE creation_requests SET status = 'executed', resolved_by = $2, resolved_at = NOW() "
            "WHERE id = $1 AND status = 'pending'",
            request_id,
            _user_id(request),
        )
        if result != "UPDATE 1":
            raise HTTPException(status_code=409, detail="Could not execute request")
    return {"id": request_id, "status": "executed"}
