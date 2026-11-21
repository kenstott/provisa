# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3e5d71-0c2a-4f86-b4d9-6e1a7c8f2b05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Access reviews over REST (REQ-2266).

- ``POST /admin/access-reviews`` takes a review now (``user_management``).
- ``GET /admin/access-reviews`` lists the reviews, ``GET /admin/access-reviews/{id}`` one review's
  rows: every row for a holder of ``user_management``, else the rows of the domains the caller
  owns (``domain_steward``). Each row says whether the caller may decide it.
- ``POST /admin/access-reviews/rows/{row_id}`` confirms or revokes a row, with a reason. Only the
  owner of the row's domain decides it, never on their own access; revoking a role that carries
  platform rights is still its holder's act (``rights.check_role_grant``).
- ``GET /admin/access-reviews/{id}/export`` is the review as CSV (``user_management``).
- ``GET`` and ``PUT /admin/access-reviews/schedule`` read and set the org's schedule
  (``org_settings``).
"""

# Requirements: REQ-2266

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import Response
from pydantic import BaseModel

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings, require_roles_removable
from provisa.api.admin.capabilities import has_capability_request, require_capability_request
from provisa.api.errors import ApiError
from provisa.core.database import Database
from provisa.core.org_membership import is_self_role_change
from provisa.entitlements import access_review as ar
from provisa.security.rights import Capability

router = APIRouter(prefix="/admin/access-reviews", tags=["admin"])

_ANONYMOUS = "anonymous"
_STEWARD = {Capability.DOMAIN_STEWARD.value}


class DecisionBody(BaseModel):
    decision: str
    reason: str


class ScheduleBody(BaseModel):
    # A five-field cron expression in UTC; null or blank: no scheduled review.
    cron: str | None = None


def _model_db() -> Database:
    from provisa.api.app import state

    assert state.model_db is not None, "the org's model store is bound with its runtime"
    return state.model_db


def _caller(request: Request) -> str | None:
    """The signed-in caller; None in dev/no-auth mode, where every gate is open."""
    user_id = getattr(getattr(request.state, "identity", None), "user_id", _ANONYMOUS)
    return None if user_id == _ANONYMOUS else user_id


def _refused(exc: ar.ReviewRefused) -> ApiError:
    status = 404 if exc.code == "access_review.row_not_found" else 409
    if exc.code in (
        "access_review.reason_required",
        "access_review.decision_invalid",
        "access_review.cron_invalid",
    ):
        status = 422
    return ApiError(status, exc.code, str(exc), **exc.params)


def _owns(request: Request, spellings: tuple[str, ...]) -> bool:
    """Whether the caller owns the domain spelled ``spellings``: ``domain_steward`` reaches it."""
    from provisa.api.app import state
    from provisa.entitlements.runtime import reaches_domain

    if _caller(request) is None:
        return True
    return reaches_domain(state, spellings, _STEWARD)


def _require_reader(request: Request) -> bool:
    """Admit a holder of user_management (True: reads every row) or of domain_steward (False:
    reads the rows of the domains they own); refuse anyone else."""
    if has_capability_request(request, Capability.USER_MANAGEMENT.value):
        return True
    require_capability_request(request, Capability.DOMAIN_STEWARD.value)
    return False


@router.post("")
async def start_review(request: Request) -> dict[str, Any]:
    """Take a review of every grant Provisa holds in force now."""
    from provisa.core.org_membership import record_admin_action
    from provisa.entitlements import store

    require_capability_request(request, Capability.USER_MANAGEMENT.value)
    actor = _caller(request) or _ANONYMOUS
    db = _model_db()
    async with db.acquire() as conn:
        async with conn.transaction():
            review_id = await ar.start(conn, actor, await store.load_held(conn))
        rows = await ar.rows_of(conn, review_id)
    await record_admin_action(
        db,
        org_id=require_active_org_id(request),
        action="access_review.started",
        actor_id=actor,
        subject_id=str(review_id),
        detail={"review_id": review_id, "rows": len(rows)},
    )
    return {"id": review_id, "rows": len(rows)}


@router.get("")
async def list_reviews(request: Request) -> list[dict[str, Any]]:
    _require_reader(request)
    async with _model_db().acquire() as conn:
        found = await ar.reviews(conn)
    return [
        {
            "id": r.id,
            "started_by": r.started_by,
            "started_at": r.started_at,
            "rows": r.rows,
            "decided": r.decided,
        }
        for r in found
    ]


@router.get("/schedule")
async def get_schedule(request: Request) -> dict[str, Any]:
    from provisa.core.org_settings import read_org_overrides

    require_org_settings(request)
    return {"cron": ar.cron_of(await read_org_overrides(_model_db()))}


@router.put("/schedule")
async def set_schedule(body: ScheduleBody, request: Request) -> dict[str, Any]:
    from provisa.core.org_settings import write_org_overrides

    require_org_settings(request)
    given = (body.cron or "").strip()
    try:
        cron = ar.check_cron(given) if given else None
    except ar.ReviewRefused as exc:
        raise _refused(exc) from None
    await write_org_overrides(
        _model_db(),
        {ar.SETTINGS_KEY: None if cron is None else {ar.CRON_FIELD: cron}},
        updated_by=_caller(request) or _ANONYMOUS,
    )
    return {"cron": cron}


async def _rows_for(request: Request, review_id: int) -> list[dict[str, Any]]:
    every = _require_reader(request)
    async with _model_db().acquire() as conn:
        if not await ar.review_exists(conn, review_id):
            raise ApiError(
                404, "access_review.not_found", "No such access review", review=review_id
            )
        rows = await ar.rows_of(conn, review_id)
        spellings = await ar.domain_spellings(conn)
    caller = _caller(request)
    owned: dict[str, bool] = {}
    out = []
    for r in rows:
        domain = r["domain_id"]
        if domain not in owned:
            owned[domain] = _owns(request, spellings.get(domain, (domain,)))
        if not (every or owned[domain]):
            continue
        r["can_decide"] = (
            owned[domain]
            and r["decision"] is None
            and not is_self_role_change(caller, r["user_id"])
        )
        out.append(r)
    return out


@router.get("/{review_id}")
async def get_review(review_id: int, request: Request) -> dict[str, Any]:
    return {"id": review_id, "rows": await _rows_for(request, review_id)}


@router.get("/{review_id}/export")
async def export_review(review_id: int, request: Request) -> Response:
    require_capability_request(request, Capability.USER_MANAGEMENT.value)
    rows = await _rows_for(request, review_id)
    return Response(
        content=ar.export_csv(rows),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="access-review-{review_id}.csv"'},
    )


@router.post("/rows/{row_id}")
async def decide_row(row_id: int, body: DecisionBody, request: Request) -> dict[str, Any]:
    """Confirm or revoke one row, as the owner of its domain. A revocation removes the grant at
    once and recomputes the member's effective rows."""
    from provisa.api.app import state
    from provisa.core.org_membership import SELF_ROLE_CHANGE_MESSAGE, record_admin_action
    from provisa.entitlements import effective_store

    require_capability_request(request, Capability.DOMAIN_STEWARD.value)
    caller = _caller(request)
    db = _model_db()
    async with db.acquire() as conn:
        held = await ar.row(conn, row_id)
        if held is None:
            raise ApiError(404, "access_review.row_not_found", "No such review row", row=row_id)
        if is_self_role_change(caller, held["user_id"]):  # REQ-1308
            raise ApiError(403, "access_review.own_access", SELF_ROLE_CHANGE_MESSAGE)
        spellings = (await ar.domain_spellings(conn)).get(held["domain_id"], (held["domain_id"],))
        if not _owns(request, spellings):
            raise ApiError(
                403,
                "access_review.not_owner",
                f"Only the owner of domain {held['domain_id']!r} decides this row",
                domain=held["domain_id"],
            )
        if body.decision == ar.REVOKED:
            require_roles_removable(request, [held["role_id"]])
        try:
            async with conn.transaction():
                decided = await ar.decide(
                    conn, row_id, body.decision, caller or _ANONYMOUS, body.reason
                )
                if decided["revoked_grant"]:
                    await effective_store.write_person(conn, state, decided["user_id"])
        except ar.ReviewRefused as exc:
            raise _refused(exc) from None
    await record_admin_action(
        db,
        org_id=require_active_org_id(request),
        action=f"access_review.{decided['decision']}",
        actor_id=caller or _ANONYMOUS,
        subject_id=decided["user_id"],
        detail={
            "review_id": decided["review_id"],
            "row_id": row_id,
            "role_id": decided["role_id"],
            "domain_id": decided["domain_id"],
            "reason": decided["reason"],
        },
    )
    return decided
