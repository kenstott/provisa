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
from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import and_, func, select, update

from provisa.core.schema_org import creation_requests

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

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


def _get_pool() -> "Database":
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
        conn = cast("Connection", _conn)
        rid = await conn.insert_returning(
            creation_requests,
            {
                "request_type": body.request_type,
                "capability": body.capability,
                "payload": body.payload,
                "requested_by": _user_id(request),
                "required_approvals": required,
            },
            returning="id",
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
    stmt = select(creation_requests)
    if status:
        stmt = stmt.where(creation_requests.c.status == status)
    if request_type:
        stmt = stmt.where(creation_requests.c.request_type == request_type)
    stmt = stmt.order_by(creation_requests.c.created_at.desc())
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        result = await conn.execute_core(stmt)
        rows = result.fetchall()
    return [_deserialize(dict(r._mapping)) for r in rows]


@router.post("/{request_id}/approve")
async def approve_request(request_id: int, request: Request):  # REQ-063, REQ-366, REQ-434
    user_id = _user_id(request)
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        result = await conn.execute_core(
            select(creation_requests).where(creation_requests.c.id == request_id)
        )
        row = result.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Request not found")
        row = _deserialize(dict(row._mapping))
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"Request is already {row['status']}")
        _require_capability(request, row["capability"])
        new_approvals = list(row.get("approvals") or []) + [
            {"approver": user_id, "approved_at": "now"}
        ]
        upd = await conn.execute_core(
            update(creation_requests)
            .where(
                and_(
                    creation_requests.c.id == request_id,
                    creation_requests.c.status == "pending",
                )
            )
            .values(approvals=new_approvals)
        )
        if (upd.rowcount or 0) == 0:
            raise HTTPException(status_code=409, detail="Could not record approval")
        result = await conn.execute_core(
            select(creation_requests).where(creation_requests.c.id == request_id)
        )
        updated = _deserialize(dict(result.fetchone()._mapping))
        approvals = updated.get("approvals") or []
        required = updated.get("required_approvals", 1)
        if len(approvals) >= required:
            await conn.execute_core(
                update(creation_requests)
                .where(
                    and_(
                        creation_requests.c.id == request_id,
                        creation_requests.c.status == "pending",
                    )
                )
                .values(status="executed", resolved_by=user_id, resolved_at=func.now())
            )
            updated["status"] = "executed"
    return updated


@router.post("/{request_id}/reject")
async def reject_request(
    request_id: int, body: RejectBody, request: Request
):  # REQ-063, REQ-366, REQ-434
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        result = await conn.execute_core(
            select(creation_requests).where(creation_requests.c.id == request_id)
        )
        row = result.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Request not found")
        row = dict(row._mapping)
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"Request is already {row['status']}")
        _require_capability(request, row["capability"])
        valid = _REJECTION_REASONS.get(row["request_type"], [])
        if body.reason not in valid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid reason {body.reason!r} for type {row['request_type']!r}. Valid: {valid}",
            )
        result = await conn.execute_core(
            update(creation_requests)
            .where(
                and_(
                    creation_requests.c.id == request_id,
                    creation_requests.c.status == "pending",
                )
            )
            .values(
                status="rejected",
                rejection_reason=body.reason,
                resolved_by=_user_id(request),
                resolved_at=func.now(),
            )
        )
        if (result.rowcount or 0) != 1:
            raise HTTPException(status_code=409, detail="Could not reject request")
    return {"id": request_id, "status": "rejected", "reason": body.reason}


@router.post("/{request_id}/execute")
async def execute_request(request_id: int, request: Request):  # REQ-063, REQ-366, REQ-434
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        result = await conn.execute_core(
            select(creation_requests).where(creation_requests.c.id == request_id)
        )
        row = result.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Request not found")
        row = dict(row._mapping)
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"Request is already {row['status']}")
        _require_capability(request, row["capability"])
        result = await conn.execute_core(
            update(creation_requests)
            .where(
                and_(
                    creation_requests.c.id == request_id,
                    creation_requests.c.status == "pending",
                )
            )
            .values(status="executed", resolved_by=_user_id(request), resolved_at=func.now())
        )
        if (result.rowcount or 0) != 1:
            raise HTTPException(status_code=409, detail="Could not execute request")
    return {"id": request_id, "status": "executed"}
