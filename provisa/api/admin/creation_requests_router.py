# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REST endpoints for the creation-request queue (REQ-063/434/480).

A relationship request is decided by the domains it touches (REQ-1948); the rule is
``relationship_approvals``, asked here on every approve, reject, execute and list.
"""

# Requirements: REQ-042, REQ-060, REQ-063, REQ-366, REQ-434, REQ-1948

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import and_, func, select, update

from provisa.api.admin import relationship_approvals as approvals_rule
from provisa.api.errors import ApiError
from provisa.core.repositories import creation_request as cr_repo
from provisa.core.repositories.creation_request import (
    REQUIRED_APPROVALS as _REQUIRED_APPROVALS,
)
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
    # REQ-1814: keyed by the stored request_type ("webhook" — see actions_router.py's
    # cr_repo.create call), not by its capability name ("webhook_registration"). The old key here
    # never matched any real row's request_type, so this dropdown was empty too.
    "webhook": [
        "duplicate",
        "endpoint_unreachable",
        "schema_mismatch",
        "governance_violation",
        "insufficient_detail",
    ],
    # REQ-1814: propose_source/propose_table (REQ-1792/1798) land in this same queue but had no
    # entry here — the Requests page's reject dialog requires picking a reason, and an unknown
    # request_type resolves to an empty list, so the dropdown had nothing to offer and rejection
    # was impossible for every MCP-proposed source/table.
    "source": [
        "duplicate",
        "endpoint_unreachable",
        "governance_violation",
        "out_of_scope",
        "insufficient_detail",
    ],
    "table": [
        "duplicate",
        "schema_mismatch",
        "source_not_registered",
        "governance_violation",
        "insufficient_detail",
    ],
}


def _get_pool() -> "Database":
    from provisa.api.app import state

    assert state.model_db is not None
    return state.model_db


def _identity(request: Request):
    return getattr(request.state, "identity", None)


def _user_id(request: Request) -> str | None:
    identity = _identity(request)
    return getattr(identity, "user_id", None) if identity is not None else None


def _holds(request: Request, capability: str) -> bool:
    """Whether the caller holds ``capability``. Dev/no-auth mode skips enforcement."""
    from provisa.api.app import state

    identity = _identity(request)
    user_id = getattr(identity, "user_id", None) if identity is not None else None
    # Dev / no-auth mode — skip enforcement
    if not user_id or user_id == "anonymous":
        return True
    from provisa.api.admin.capabilities import _resolved_capabilities

    return capability in _resolved_capabilities(identity, state)


def _require_capability(request: Request, capability: str) -> None:
    """Raise HTTPException 403 if caller lacks capability. Dev/no-auth mode skips enforcement."""
    if not _holds(request, capability):
        raise HTTPException(status_code=403, detail=f"Missing capability: {capability!r}")


def _reach(request: Request) -> frozenset[str] | None:
    """The domains the caller's right to create relationships reaches (REQ-1944, REQ-1948)."""
    from provisa.api.admin.capabilities import right_reach
    from provisa.api.app import state

    return right_reach(_identity(request), state, approvals_rule.RIGHT)


async def _audit(
    request: Request,
    row: dict,
    action: str,
    involved: frozenset[str],
    refusal: "approvals_rule.Refusal | None" = None,
) -> None:  # REQ-1948
    await approvals_rule.record(
        _get_pool(),
        action=action,
        request=row,
        actor=_user_id(request),
        involved=involved,
        # Only a relationship request is decided by domains; any other touches none here.
        reach=_reach(request) if _is_relationship(row) else None,
        refusal=refusal,
    )


async def _refuse(
    request: Request,
    row: dict,
    action: str,
    involved: frozenset[str],
    refusal: "approvals_rule.Refusal | None",
    status_code: int = 403,
) -> None:
    """Raise ``refusal`` (when there is one) after writing the refused attempt to the trail."""
    if refusal is None:
        return
    await _audit(request, row, action, involved, refusal)
    raise ApiError(status_code, refusal.code, refusal.message, **refusal.params)


def _deserialize(row: dict) -> dict:
    out = dict(row)
    if isinstance(out.get("approvals"), str):
        out["approvals"] = json.loads(out["approvals"])
    if isinstance(out.get("payload"), str):
        out["payload"] = json.loads(out["payload"])
    return out


def _is_relationship(row: dict) -> bool:
    return row["request_type"] == approvals_rule.REQUEST_TYPE


async def _pending(conn: "Connection", request_id: int) -> dict:
    result = await conn.execute_core(
        select(creation_requests).where(creation_requests.c.id == request_id)
    )
    found = result.fetchone()
    if found is None:
        raise HTTPException(status_code=404, detail="Request not found")
    row = _deserialize(dict(found._mapping))
    if row["status"] != "pending":
        raise HTTPException(status_code=409, detail=f"Request is already {row['status']}")
    return row


async def _carry_out(
    conn: "Connection", row: dict, request: Request, involved: frozenset[str]
) -> None:  # REQ-434, REQ-1948
    """Create what a request that has had its approvals asks for, and mark it executed.

    Every type goes through ``perform_request_creation`` — the code the direct mutation uses.
    A relationship is stored on its approvals' authority (REQ-1948); a view, table or source is
    created as the caller, whose own capability and domain gates apply. A creation that fails
    leaves the request pending, answered with the failure's own code.
    """
    import types

    from provisa.api.admin.schema_mutation import perform_request_creation, request_carried_out

    info = types.SimpleNamespace(context={"request": request})
    try:
        result = await perform_request_creation(cast("Any", info), row)
    except PermissionError as e:
        # The direct mutation's own gate refused the caller (a view, table or source is created
        # as the caller): the same answer, and the same codes, the REST gates give.
        code = "auth.missing_capability" if str(e).startswith("Missing") else "auth.domain_denied"
        await _refuse(request, row, "execute", involved, approvals_rule.Refusal(code, str(e)), 403)
        raise
    if not result.success:
        if result.code is None:
            raise HTTPException(status_code=422, detail=result.message)
        failed = approvals_rule.Refusal(result.code, result.message, dict(result.params or {}))
        await _refuse(request, row, "execute", involved, failed, status_code=422)
    if not await cr_repo.mark_executed(conn, row["id"], _user_id(request)):
        raise HTTPException(status_code=409, detail="Could not execute request")
    await request_carried_out(row)
    await _audit(request, row, "execute", involved)


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
    payload = body.payload
    if body.request_type == approvals_rule.REQUEST_TYPE:
        # REQ-1948: the request is decided by its tables' domains, so it must name its tables.
        import dataclasses

        from provisa.api.admin.schema_common import _rebuild_relationship_input

        try:
            payload = dataclasses.asdict(_rebuild_relationship_input(body.payload))
        except TypeError as e:
            raise HTTPException(status_code=400, detail=f"Not a relationship: {e}")
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        rid = await cr_repo.create(
            conn, body.request_type, body.capability, payload, _user_id(request)
        )
    return {"id": rid, "status": "pending"}


@router.get("/rejection-reasons")
async def rejection_reasons():
    return _REJECTION_REASONS


@router.get("/")
async def list_requests(  # REQ-063, REQ-434, REQ-1948
    request: Request,
    status: str | None = Query(None),
    request_type: str | None = Query(None),
):
    """The requests the caller can decide and the ones they made (REQ-1948).

    Each row says which domains it touches, which of them it still waits on, whether the caller
    is one of the users who may decide it, and why the caller's approval would be refused.
    """
    stmt = select(creation_requests)
    if status:
        stmt = stmt.where(creation_requests.c.status == status)
    if request_type:
        stmt = stmt.where(creation_requests.c.request_type == request_type)
    stmt = stmt.order_by(creation_requests.c.created_at.desc())
    user_id = _user_id(request)
    reach = _reach(request)
    pool = _get_pool()
    out: list[dict] = []
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        result = await conn.execute_core(stmt)
        for found in result.fetchall():
            row = _deserialize(dict(found._mapping))
            mine = user_id is not None and row["requested_by"] == user_id
            if _is_relationship(row):
                involved = await approvals_rule.domains_involved(conn, row["payload"])
                tables = approvals_rule.tables_named(row["payload"])
                decides = (
                    approvals_rule.rejection_refusal(
                        user_id=user_id,
                        requested_by=row["requested_by"],
                        involved=involved,
                        reach=reach,
                        tables=tables,
                    )
                    is None
                )
                cannot_approve = approvals_rule.approval_refusal(
                    user_id=user_id,
                    requested_by=row["requested_by"],
                    approvals=row["approvals"],
                    involved=involved,
                    reach=reach,
                    tables=tables,
                )
                # Why this user's approval would be refused, so the page says it before asking.
                row["approve_refusal"] = (
                    None
                    if cannot_approve is None
                    else {
                        "code": cannot_approve.code,
                        "params": cannot_approve.params,
                        "detail": cannot_approve.message,
                    }
                )
                row["domains"] = sorted(involved)
                row["waiting_on"] = (
                    approvals_rule.waiting_on(involved, row["approvals"], row["requested_by"])
                    if row["status"] == "pending"
                    else []
                )
            else:
                decides = _holds(request, row["capability"])
                repeat = approvals_rule.repeat_refusal(
                    user_id=user_id, requested_by=row["requested_by"], approvals=row["approvals"]
                )
                row["approve_refusal"] = (
                    None
                    if repeat is None
                    else {"code": repeat.code, "params": repeat.params, "detail": repeat.message}
                )
                row["domains"] = []
                row["waiting_on"] = []
            if not (decides or mine):
                continue
            row["can_decide"] = decides
            out.append(row)
    return out


@router.post("/{request_id}/approve")
async def approve_request(request_id: int, request: Request):  # REQ-063, REQ-366, REQ-434, REQ-1948
    user_id = _user_id(request)
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        row = await _pending(conn, request_id)
        if _is_relationship(row):
            return await _approve_relationship(conn, row, request)
        # Every type: the right the request names, never the requester's own yes, one yes per
        # user; the approval that completes the count creates what the request asks for.
        _require_capability(request, row["capability"])
        none: frozenset[str] = frozenset()
        await _refuse(
            request,
            row,
            "approve",
            none,
            approvals_rule.repeat_refusal(
                user_id=user_id, requested_by=row["requested_by"], approvals=row["approvals"]
            ),
        )
        stored = await cr_repo.add_approval(conn, request_id, user_id, [])
        if stored is None:
            raise HTTPException(status_code=409, detail="Could not record approval")
        updated = _deserialize(stored)
        await _audit(request, updated, "approve", none)
        if (
            approvals_rule.count_refusal(
                updated["approvals"], updated["requested_by"], updated["required_approvals"]
            )
            is None
        ):
            await _carry_out(conn, updated, request, none)
            updated["status"] = "executed"
    return updated


async def _approve_relationship(conn: "Connection", row: dict, request: Request) -> dict:
    """REQ-1948: count the approval if the caller's right reaches a domain the request touches;
    the approval that completes the rule carries the request out."""
    user_id = _user_id(request)
    involved = await approvals_rule.domains_involved(conn, row["payload"])
    reach = _reach(request)
    await _refuse(
        request,
        row,
        "approve",
        involved,
        approvals_rule.approval_refusal(
            user_id=user_id,
            requested_by=row["requested_by"],
            approvals=row["approvals"],
            involved=involved,
            reach=reach,
            tables=approvals_rule.tables_named(row["payload"]),
        ),
    )
    assert user_id is not None  # an approval without a user was refused above
    stored = await cr_repo.add_approval(
        conn, row["id"], user_id, sorted(approvals_rule.reached(reach, involved))
    )
    if stored is None:
        raise HTTPException(status_code=409, detail="Could not record approval")
    updated = _deserialize(stored)
    await _audit(request, updated, "approve", involved)
    if approvals_rule.executable(involved, updated["approvals"], updated["requested_by"]):
        await _carry_out(conn, updated, request, involved)
        updated["status"] = "executed"
    updated["domains"] = sorted(involved)
    updated["waiting_on"] = approvals_rule.waiting_on(
        involved, updated["approvals"], updated["requested_by"]
    )
    return updated


@router.post("/{request_id}/reject")
async def reject_request(
    request_id: int, body: RejectBody, request: Request
):  # REQ-063, REQ-366, REQ-434, REQ-1948
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        row = await _pending(conn, request_id)
        involved: frozenset[str] = frozenset()
        if _is_relationship(row):
            # REQ-1948: a rejection comes from any user who could approve.
            involved = await approvals_rule.domains_involved(conn, row["payload"])
            await _refuse(
                request,
                row,
                "reject",
                involved,
                approvals_rule.rejection_refusal(
                    user_id=_user_id(request),
                    requested_by=row["requested_by"],
                    involved=involved,
                    reach=_reach(request),
                    tables=approvals_rule.tables_named(row["payload"]),
                ),
            )
        else:
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
        if _is_relationship(row):
            await _audit(request, row, "reject", involved)
    return {"id": request_id, "status": "rejected", "reason": body.reason}


@router.post("/{request_id}/execute")
async def execute_request(request_id: int, request: Request):  # REQ-063, REQ-366, REQ-434, REQ-1948
    pool = _get_pool()
    async with pool.acquire() as _conn:
        conn = cast("Connection", _conn)
        row = await _pending(conn, request_id)
        if _is_relationship(row):
            # REQ-1948: executing is not a way around the approvals. It is open to the users who
            # may decide the request, and only once the request is fully approved.
            involved = await approvals_rule.domains_involved(conn, row["payload"])
            await _refuse(
                request,
                row,
                "execute",
                involved,
                approvals_rule.rejection_refusal(
                    user_id=_user_id(request),
                    requested_by=row["requested_by"],
                    involved=involved,
                    reach=_reach(request),
                    tables=approvals_rule.tables_named(row["payload"]),
                ),
            )
            await _refuse(
                request,
                row,
                "execute",
                involved,
                approvals_rule.incomplete_refusal(involved, row["approvals"], row["requested_by"]),
                status_code=409,
            )
            await _carry_out(conn, row, request, involved)
            return {"id": request_id, "status": "executed"}
        # Every other type: executing is not a way around the approvals either.
        _require_capability(request, row["capability"])
        none: frozenset[str] = frozenset()
        await _refuse(
            request,
            row,
            "execute",
            none,
            approvals_rule.repeat_refusal(
                user_id=_user_id(request), requested_by=row["requested_by"], approvals=[]
            ),
        )
        await _refuse(
            request,
            row,
            "execute",
            none,
            approvals_rule.count_refusal(
                row["approvals"], row["requested_by"], row["required_approvals"]
            ),
            status_code=409,
        )
        await _carry_out(conn, row, request, none)
    return {"id": request_id, "status": "executed"}
