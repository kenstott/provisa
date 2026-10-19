# Copyright (c) 2026 Kenneth Stott
# Canary: 3c45bea8-b5af-4757-b864-c52392aa7f73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Judgments under review (REQ-2094): the named bases a judgment cites, and the review queue.

- ``GET /admin/judgments/bases``, ``PUT /admin/judgments/bases/{id}``,
  ``POST /admin/judgments/bases/{id}/changed`` (declare a basis changed, with a note: the
  judgments citing it are reviewed at once). Writing a basis needs ``access_config``.
- ``GET /admin/judgments/findings``: the open findings the caller may see, in the queue's order.
- ``POST /admin/judgments/findings/{id}/reaffirm``: re-record the judgment's why with today's
  evidence; needs the right that edits the decision.
- ``POST /admin/judgments/findings/{id}/dismiss``: a posture finding, with a note;
  ``access_config``.
"""

# Requirements: REQ-2094

from __future__ import annotations

import datetime as dt
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.capabilities import has_capability_request, require_capability_request
from provisa.api.errors import ApiError

router = APIRouter(prefix="/admin/judgments", tags=["admin", "judgments"])

#: Who may read the bases: anyone who may give a judgment a why.
_BASIS_READERS = (
    "view_governance",
    "access_config",
    "table_registration",
    "masking_config",
    "column_grant",
    "sensitive_data",
)


class BasisBody(BaseModel):
    name: str
    description: str | None = None
    owner: str | None = None


class NoteBody(BaseModel):
    note: str


def _db() -> Any:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "judgments.database_unavailable", "Database unavailable")
    return state.model_db


def _actor(request: Request) -> str | None:
    identity = getattr(request.state, "identity", None)
    user = getattr(identity, "user_id", None)
    return None if user in (None, "anonymous") else user


def _require_any(request: Request, rights: tuple[str, ...]) -> None:
    if not any(has_capability_request(request, r) for r in rights):
        raise ApiError(
            403,
            "judgments.right_required",
            f"one of {', '.join(rights)} is required",
            rights=list(rights),
        )


def _iso(row: dict) -> dict:
    return {k: v.isoformat() if isinstance(v, dt.datetime) else v for k, v in row.items()}


@router.get("/bases")
async def list_bases(request: Request) -> list[dict[str, Any]]:
    from provisa.judgments.bases import list_bases as _list

    _require_any(request, _BASIS_READERS)
    async with _db().acquire() as conn:
        return await _list(conn)


@router.put("/bases/{basis_id}")
async def put_basis(basis_id: str, body: BasisBody, request: Request) -> dict[str, Any]:
    from provisa.judgments.bases import BasisRefused
    from provisa.judgments.bases import put_basis as _put

    require_capability_request(request, "access_config")
    try:
        async with _db().acquire() as conn:
            return await _put(
                conn, basis_id, name=body.name, description=body.description, owner=body.owner
            )
    except BasisRefused as exc:
        raise ApiError(400, "judgments.basis_invalid", str(exc)) from exc


@router.post("/bases/{basis_id}/changed")
async def declare_changed(basis_id: str, body: NoteBody, request: Request) -> dict[str, Any]:
    """Declare the basis changed; every judgment citing it decided before now is dated by
    context at once."""
    from provisa.api.app import state
    from provisa.core.request_context import current_env
    from provisa.judgments.bases import BasisRefused
    from provisa.judgments.bases import declare_changed as _declare
    from provisa.judgments.review import review_org

    require_capability_request(request, "access_config")
    at = dt.datetime.now(dt.timezone.utc)
    try:
        async with _db().acquire() as conn:
            basis = await _declare(conn, basis_id, note=body.note, by=_actor(request), at=at)
    except BasisRefused as exc:
        raise ApiError(400, "judgments.basis_invalid", str(exc)) from exc
    if basis is None:
        raise ApiError(404, "judgments.basis_not_found", f"there is no basis {basis_id!r}")
    review = await review_org(
        state, require_active_org_id(request), current_env.get(), basis_id=basis_id
    )
    return {**basis, "review": review}


async def _visible(request: Request, conn: Any, finding: dict) -> bool:
    from provisa.judgments.resolution import is_sensitive, read_rights

    rights = read_rights(await is_sensitive(conn, finding))
    return any(has_capability_request(request, r) for r in rights)


@router.get("/findings")
async def list_findings(request: Request) -> list[dict[str, Any]]:
    """The open findings the caller may see: a finding on a sensitive column needs
    sensitive_data, any other view_governance."""
    from provisa.judgments.findings import open_findings

    async with _db().acquire() as conn:
        found = await open_findings(conn)
        return [_iso(f) for f in found if await _visible(request, conn, f)]


async def _finding(conn: Any, finding_id: int) -> dict:
    from provisa.judgments.findings import jf

    row = (await conn.execute_core(select(jf).where(jf.c.id == finding_id))).fetchone()
    if row is None:
        raise ApiError(404, "judgments.finding_not_found", f"there is no finding {finding_id}")
    finding = dict(row._mapping)
    if finding["resolved_at"] is not None:
        raise ApiError(409, "judgments.finding_resolved", f"finding {finding_id} is resolved")
    return finding


@router.post("/findings/{finding_id}/reaffirm")
async def reaffirm(finding_id: int, request: Request) -> dict[str, Any]:
    from provisa.judgments.resolution import ResolveRefused, is_sensitive, resolve_rights
    from provisa.judgments.resolution import reaffirm as _reaffirm

    async with _db().acquire() as conn:
        finding = await _finding(conn, finding_id)
        _require_any(
            request, resolve_rights(finding["judgment"], await is_sensitive(conn, finding))
        )
        try:
            resolved = await _reaffirm(
                conn, finding, by=_actor(request), at=dt.datetime.now(dt.timezone.utc)
            )
        except ResolveRefused as exc:
            raise ApiError(409, "judgments.not_reaffirmable", str(exc)) from exc
    return {"resolved": resolved}


@router.post("/findings/{finding_id}/dismiss")
async def dismiss(finding_id: int, body: NoteBody, request: Request) -> dict[str, Any]:
    from provisa.judgments.resolution import ResolveRefused
    from provisa.judgments.resolution import dismiss as _dismiss

    require_capability_request(request, "access_config")
    async with _db().acquire() as conn:
        finding = await _finding(conn, finding_id)
        try:
            await _dismiss(
                conn,
                finding,
                note=body.note,
                by=_actor(request),
                at=dt.datetime.now(dt.timezone.utc),
            )
        except ResolveRefused as exc:
            raise ApiError(409, "judgments.not_dismissible", str(exc)) from exc
    return {"dismissed": finding_id}
