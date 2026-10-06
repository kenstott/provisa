# Copyright (c) 2026 Kenneth Stott
# Canary: e7c4a1b8-3f92-4d50-b6a1-2d9e8f0c4b13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes of a profile's constraints and of the checks it hands to checkers (REQ-1934).

* ``GET    /admin/tables/{table_id}/profile-constraints`` — the operator's decisions, and the
  checker tables an accepted constraint could be exported to.
* ``POST   /admin/tables/{table_id}/profile-constraints`` — accept (as proposed or edited) or
  dismiss a proposed constraint.
* ``DELETE /admin/tables/{table_id}/profile-constraints/{constraint_id}`` — withdraw a decision.
* ``POST   /admin/tables/{table_id}/profile-constraints/{constraint_id}/export`` — add an accepted
  constraint to a checker whose contract scans the table.
* ``GET    /admin/tables/{table_id}/profile-checks`` — the checkers scanning the member's registered
  drift table, and the registered tables of the expectations shape.
* ``POST   /admin/tables/{table_id}/profile-checks/drift`` — Create drift check.
* ``POST   /admin/tables/{table_id}/profile-checks/expectation`` — Create expectation check.

All are for those who may edit the table (``table_registration``). Every export is refused by name
where no checker can take it (``provisa.profiler.export``).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.admin.profiler_router import _db, _member
from provisa.api.app import state
from provisa.api.errors import ApiError

router = APIRouter(prefix="/admin", tags=["admin", "profiler"])


class DecisionInput(BaseModel):
    kind: str
    column: str
    otherColumn: str | None = None
    definition: dict
    evidence: str
    share: float | None
    sampled: bool
    status: str  # accepted | dismissed
    runId: str


class ExportInput(BaseModel):
    # The checker table to add to; may be left out where exactly one scans the target.
    checkerTableId: int | None = None


class ExpectationInput(ExportInput):
    expectationsTableId: int


def _checker_doc(c: Any) -> dict:
    return {"id": c.id, "tableName": c.table_name, "sourceId": c.source_id, "checker": c.checker}


@router.get("/tables/{table_id}/profile-constraints")
async def list_constraints(request: Request, table_id: int) -> dict:
    require_capability_request(request, "table_registration")
    from provisa.profiler import constraints, export

    async with _db().acquire() as conn:
        await _member(conn, table_id)
        decided = await constraints.decisions(conn, table_id)
        checkers = await export.checkers_scanning(conn, state.contexts, {table_id})
    return {
        "decisions": [
            {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in d.items()}
            for d in decided
        ],
        "checkers": [_checker_doc(c) for c in checkers],
    }


@router.post("/tables/{table_id}/profile-constraints")
async def decide_constraint(request: Request, table_id: int, body: DecisionInput) -> dict:
    require_capability_request(request, "table_registration")
    from provisa.profiler import constraints

    try:
        constraint = constraints.Constraint(
            body.kind, body.column, body.otherColumn, dict(body.definition)
        )
    except ValueError as exc:
        raise ApiError(422, "profile.constraint_invalid", str(exc)) from exc
    async with _db().acquire() as conn:
        await _member(conn, table_id)
        try:
            cid = await constraints.decide(
                conn,
                table_id,
                constraint,
                status=body.status,
                evidence=body.evidence,
                share=body.share,
                sampled=body.sampled,
                run_id=body.runId,
            )
        except ValueError as exc:
            raise ApiError(422, "profile.constraint_invalid", str(exc)) from exc
    return {"id": cid}


@router.delete("/tables/{table_id}/profile-constraints/{constraint_id}")
async def forget_constraint(request: Request, table_id: int, constraint_id: str) -> dict:
    require_capability_request(request, "table_registration")
    from provisa.profiler import constraints

    async with _db().acquire() as conn:
        await _member(conn, table_id)
        if not await constraints.forget(conn, table_id, constraint_id):
            raise ApiError(
                404,
                "profile.constraint_not_found",
                f"No decision {constraint_id} on table {table_id}",
            )
    return {"id": constraint_id}


async def _added(conn: Any, checker: Any, check: dict) -> dict:
    from provisa.api.app import _rebuild_schemas
    from provisa.profiler import export

    added = await export.add_check(conn, checker, check)
    if added:
        await _rebuild_schemas()
    return {"checkerTable": _checker_doc(checker), "added": added}


@router.post("/tables/{table_id}/profile-constraints/{constraint_id}/export")
async def export_constraint(
    request: Request, table_id: int, constraint_id: str, body: ExportInput
) -> dict:
    require_capability_request(request, "table_registration")
    from provisa.profiler import constraints, export

    async with _db().acquire() as conn:
        member = await _member(conn, table_id)
        accepted = {c.id: c for c in await constraints.accepted_constraints(conn, table_id)}
        if constraint_id not in accepted:
            raise ApiError(
                404,
                "profile.constraint_not_accepted",
                f"Table {member['table_name']!r} has no accepted constraint {constraint_id}",
            )
        found = await export.checkers_scanning(conn, state.contexts, {table_id})
        try:
            checker = export.pick(found, body.checkerTableId, f"table {member['table_name']!r}")
        except export.ExportRefused as exc:
            raise ApiError(409, "profile.export_refused", str(exc)) from exc
        check = export.constraint_check(accepted[constraint_id].constraint, checker.checker)
        return await _added(conn, checker, check)


async def _registered_drift_ids(conn: Any, member: dict, table_id: int) -> set[int]:
    """The ids of the member's drift result table as registered on its profiler."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.profiler.schema import result_table_name

    name = result_table_name(member["table_name"], table_id, "drift")
    result = await conn.execute_core(
        select(rt.c.id).where(rt.c.table_name == name, rt.c.source_id == member["source_id"])
    )
    return {r[0] for r in result.fetchall()}


def _published(table_id: int) -> tuple[str, list[str]] | None:
    """A registered table as the org admin's pgwire publishes it: its domain.table and columns."""
    from provisa.profiler.run import PROFILE_ROLE, _published_name

    ctx = state.contexts.get(PROFILE_ROLE)
    if ctx is None:
        raise ApiError(503, "profile.schema_unavailable", "No compiled schema for the org admin")
    meta = next((m for m in ctx.tables.values() if m.table_id == table_id), None)
    if meta is None:
        return None
    columns = [
        ctx.physical_to_sql[(table_id, c.column_name)]
        for c in state.schema_build_cache["column_types"][table_id]
        if (table_id, c.column_name) in ctx.physical_to_sql
    ]
    return _published_name(meta), columns


@router.get("/tables/{table_id}/profile-checks")
async def check_candidates(request: Request, table_id: int) -> dict:
    require_capability_request(request, "table_registration")
    from provisa.core.schema_org import registered_tables as rt
    from provisa.profiler import export

    async with _db().acquire() as conn:
        member = await _member(conn, table_id)
        drift_ids = await _registered_drift_ids(conn, member, table_id)
        checkers = await export.checkers_scanning(conn, state.contexts, drift_ids)
        rows = (await conn.execute_core(select(rt.c.id, rt.c.table_name))).fetchall()
    shaped = []
    for tid, name in rows:
        published = _published(tid)
        if published is not None and not export.is_expectations_shape(published[1]):
            shaped.append({"id": tid, "tableName": name, "published": published[0]})
    return {
        "driftTableRegistered": bool(drift_ids),
        "checkers": [_checker_doc(c) for c in checkers],
        "expectationTables": shaped,
    }


async def _drift_checker(conn: Any, table_id: int, checker_table_id: int | None) -> Any:
    from provisa.profiler import export
    from provisa.profiler.schema import result_table_name

    member = await _member(conn, table_id)
    drift_ids = await _registered_drift_ids(conn, member, table_id)
    name = result_table_name(member["table_name"], table_id, "drift")
    if not drift_ids:
        raise ApiError(
            409,
            "profile.export_refused",
            f"the drift table {name!r} is not registered: register it on the profiler first",
        )
    found = await export.checkers_scanning(conn, state.contexts, drift_ids)
    try:
        return export.pick(found, checker_table_id, f"the registered drift table {name!r}")
    except export.ExportRefused as exc:
        raise ApiError(409, "profile.export_refused", str(exc)) from exc


@router.post("/tables/{table_id}/profile-checks/drift")
async def create_drift_check(request: Request, table_id: int, body: ExportInput) -> dict:
    require_capability_request(request, "table_registration")
    from provisa.profiler import export

    async with _db().acquire() as conn:
        checker = await _drift_checker(conn, table_id, body.checkerTableId)
        return await _added(conn, checker, export.drift_check(checker))


@router.post("/tables/{table_id}/profile-checks/expectation")
async def create_expectation_check(request: Request, table_id: int, body: ExpectationInput) -> dict:
    require_capability_request(request, "table_registration")
    from provisa.profiler import export

    published = _published(body.expectationsTableId)
    if published is None:
        raise ApiError(
            404,
            "profile.expectations_not_found",
            f"Table {body.expectationsTableId} is not a registered table the org admin reads",
        )
    missing = export.is_expectations_shape(published[1])
    if missing:
        raise ApiError(
            409,
            "profile.export_refused",
            f"{published[0]} is not of the expectations shape: it lacks {missing}; the shape is "
            f"{list(export.EXPECTATION_SHAPE)}",
        )
    async with _db().acquire() as conn:
        checker = await _drift_checker(conn, table_id, body.checkerTableId)
        return await _added(conn, checker, export.expectation_check(checker, published[0]))
