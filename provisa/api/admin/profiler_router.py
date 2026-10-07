# Copyright (c) 2026 Kenneth Stott
# Canary: 6c0d9f74-3e28-4b1a-95f6-a7d2e8b1c049
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes of the Data Profiler (REQ-1934).

* ``GET  /admin/profilers`` — the profiler sources with their schedules, for Add to Profiler.
* ``POST /admin/profilers/{source_id}/run`` — profile every member now.
* ``GET  /admin/profilers/{source_id}/catalog`` — the result relations it produces per member, each
  with the grants, masks and row rules the registration form starts from (the curation step; with
  no registration no reader sees a profile).
* ``POST /admin/tables/{table_id}/profile-runs`` — Run Profile Now for one member.
* ``GET  /admin/tables/{table_id}/profile-runs`` — its run history (View Profile Runs).
* ``GET  /admin/tables/{table_id}/profile-runs/{run_id}`` — one run's results, safe for its viewer.
* ``POST /admin/tables/{table_id}/declared-profiles`` — store a declared profile (REQ-1942).
* ``GET  /admin/tables/{table_id}/profile-runs/{run_id}/declared`` — a run as a declared profile,
  safe for its viewer, to change and store as one.
* ``GET  /admin/tables/{table_id}/profile-runs/{run_id}/history`` — one measure across the run's
  drift window, with the run's drift row (REQ-1934 DRIFT ACROSS RUNS), safe for its viewer.

The run history and results are shown only to those who may edit the table (the
``table_registration`` capability); they are read straight from the result relations, as the
profiler wrote them.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, Request
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.app import state
from provisa.api.errors import ApiError
from provisa.core.schema_org import registered_tables

router = APIRouter(prefix="/admin", tags=["admin", "profiler"])


def _db() -> Any:
    if state.model_db is None:
        raise ApiError(503, "profile.database_unavailable", "Database unavailable")
    return state.model_db


def _jsonable(row: dict) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}


@router.get("/profilers")
async def list_profilers(request: Request) -> list[dict]:
    require_capability_request(request, "table_registration")
    from provisa.profiler.source import members, profiler_sources

    async with _db().acquire() as conn:
        out = []
        for p in await profiler_sources(conn):
            settings = p["settings"]
            out.append(
                {
                    "id": p["id"],
                    "cron": settings.cron,
                    "sampleAboveCells": settings.sample_above_cells,
                    "lowCardinalityMax": settings.low_cardinality_max,
                    "driftWindow": settings.drift_window,
                    "driftSeason": settings.drift_season,
                    "members": [m["table_name"] for m in await members(conn, p["id"])],
                }
            )
    return out


@router.post("/profilers/{source_id}/run")
async def run_profiler(request: Request, source_id: str) -> list[dict]:
    require_capability_request(request, "source_registration")
    from provisa.profiler.source import run_source

    try:
        return await run_source(state, source_id)
    except ValueError as exc:
        raise ApiError(404, "profile.not_a_profiler", str(exc), source=source_id) from exc


@router.get("/profilers/{source_id}/catalog")
async def profiler_catalog(request: Request, source_id: str) -> list[dict]:
    require_capability_request(request, "table_registration")
    from provisa.profiler.governance import column_rules, prefill
    from provisa.profiler.schema import RESULT_KINDS, kind_fields, result_table_name
    from provisa.profiler.source import members

    out = []
    async with _db().acquire() as conn:
        for t in await members(conn, source_id):
            # The registration form's defaults, from the profiled table's rules as they are now.
            rules = await column_rules(conn, state, t["id"])
            entries = []
            for kind in RESULT_KINDS:
                fields = kind_fields(kind)
                defaults = prefill(kind, [n for n, _, _ in fields], rules)
                described = {c["name"]: c for c in defaults["columns"]}
                entries.append(
                    {
                        "kind": kind,
                        "tableName": result_table_name(t["table_name"], t["id"], kind),
                        "columns": [
                            {"dataType": dt, "description": d, **described[n]}
                            for n, dt, d in fields
                        ],
                        "rowRules": defaults["rowRules"],
                    }
                )
            out.append({"member": t["table_name"], "memberId": t["id"], "tables": entries})
    return out


async def _member(conn: Any, table_id: int) -> dict:
    row = (
        await conn.execute_core(
            select(registered_tables.c.table_name, registered_tables.c.profiler_source_id).where(
                registered_tables.c.id == table_id
            )
        )
    ).fetchone()
    if row is None:
        raise ApiError(
            404, "profile.table_not_found", f"Table {table_id} not found", table_id=table_id
        )
    if row[1] is None:
        raise ApiError(
            409,
            "profile.not_a_member",
            f"Table {row[0]!r} has not joined a profiler",
            table=row[0],
        )
    return {"table_name": row[0], "source_id": row[1]}


@router.post("/tables/{table_id}/profile-runs")
async def run_profile_now(request: Request, table_id: int) -> dict:
    require_capability_request(request, "table_registration")
    from provisa.profiler.run import ProfileError, profile_table

    async with _db().acquire() as conn:
        member = await _member(conn, table_id)
        settings = await _settings(conn, member["source_id"])
    try:
        run = await profile_table(
            state,
            table_id=table_id,
            table_name=member["table_name"],
            settings=settings,
        )
    except ProfileError as exc:
        raise ApiError(422, "profile.run_failed", str(exc), table=member["table_name"]) from exc
    return {"runId": run.run_id, "rowCount": run.row_count, "profiledRows": run.profiled_rows}


async def _relation(conn: Any, table_name: str, table_id: int, kind: str) -> Any:
    from provisa.profiler.history import relation

    return await relation(conn, table_name, table_id, kind)


async def _settings(conn: Any, source_id: str) -> Any:
    from provisa.core.schema_org import sources
    from provisa.profiler.source import profiler_settings

    mapping = (
        await conn.execute_core(select(sources.c.mapping).where(sources.c.id == source_id))
    ).fetchone()
    assert mapping is not None, "membership names a source the integrity guard keeps"
    return profiler_settings(source_id, dict(mapping[0]))


def _viewer_roles(request: Request, x_provisa_role: str | None) -> frozenset[str]:
    acting = getattr(request.state, "role", None) or x_provisa_role
    if not acting:
        raise ApiError(
            400,
            "profile.role_header_required",
            "X-Provisa-Role header required: a profile run is shown as its viewer may see it",
        )
    return frozenset(r.strip() for r in acting.split(",") if r.strip())


@router.get("/tables/{table_id}/profile-runs")
async def list_profile_runs(request: Request, table_id: int) -> list[dict]:
    require_capability_request(request, "table_registration")
    async with _db().acquire() as conn:
        member = await _member(conn, table_id)
        runs = await _relation(conn, member["table_name"], table_id, "runs")
        result = await conn.execute_core(select(runs).order_by(runs.c.run_time.desc()))
        return [_jsonable(dict(r._mapping)) for r in result.fetchall()]


@router.get("/tables/{table_id}/profile-runs/{run_id}")
async def get_profile_run(
    request: Request,
    table_id: int,
    run_id: str,
    x_provisa_role: str | None = Header(None),
) -> dict:
    """One run's results, safe for the viewer: read off the profiled table's current column rules
    and the viewer's roles (governance.safe_run), here on the server and never in the browser."""
    require_capability_request(request, "table_registration")
    from provisa.profiler.governance import column_rules, safe_run
    from provisa.profiler.schema import RESULT_KINDS

    roles = _viewer_roles(request, x_provisa_role)
    out: dict[str, list[dict]] = {}
    async with _db().acquire() as conn:
        member = await _member(conn, table_id)
        rules = await column_rules(conn, state, table_id)
        for kind in RESULT_KINDS:
            relation = await _relation(conn, member["table_name"], table_id, kind)
            result = await conn.execute_core(select(relation).where(relation.c.run_id == run_id))
            out[kind] = [_jsonable(dict(r._mapping)) for r in result.fetchall()]
    if not out["runs"]:
        raise ApiError(404, "profile.run_not_found", f"Run {run_id} not found", run_id=run_id)
    return safe_run(out, rules, roles)


def _matches(column: Any, value: str | None) -> Any:
    return column.is_(None) if value is None else column == value


@router.get("/tables/{table_id}/profile-runs/{run_id}/history")
async def get_measure_history(
    request: Request,
    table_id: int,
    run_id: str,
    scope: str,
    measure: str,
    column: str | None = None,
    subject: str | None = None,
    x_provisa_role: str | None = Header(None),
) -> dict:
    """One measure of a run across the run's drift window (REQ-1934 DRIFT ACROSS RUNS): the run's
    drift row for it, and the measure's value in each window run and in the run, oldest first --
    shown only when the viewer may see the drift row (governance.safe_run)."""
    require_capability_request(request, "table_registration")
    from provisa.profiler import compare
    from provisa.profiler.governance import column_rules, safe_run
    from provisa.profiler.history import MEASURE_KINDS, as_utc, previous_runs, run_results

    roles = _viewer_roles(request, x_provisa_role)
    async with _db().acquire() as conn:
        member = await _member(conn, table_id)
        settings = await _settings(conn, member["source_id"])
        rules = await column_rules(conn, state, table_id)
        drift = await _relation(conn, member["table_name"], table_id, "drift")
        found = (
            await conn.execute_core(
                select(drift).where(
                    drift.c.run_id == run_id,
                    drift.c.scope == scope,
                    drift.c.measure == measure,
                    _matches(drift.c.column_name, column),
                    _matches(drift.c.subject, subject),
                )
            )
        ).fetchone()
        shown = safe_run({"drift": [] if found is None else [dict(found._mapping)]}, rules, roles)
        if not shown["drift"]:
            raise ApiError(
                404,
                "profile.measure_not_found",
                f"Run {run_id} records no {scope} measure {measure!r} this viewer may see",
                run_id=run_id,
            )
        row = shown["drift"][0]
        at = as_utc(row["run_time"])
        earlier = await previous_runs(conn, member["table_name"], table_id, at, None)
        window = compare.window_of(settings.drift_season, settings.drift_window, at, earlier)
        timeline = [*reversed(window), (run_id, at)]
        stored = await run_results(
            conn, member["table_name"], table_id, [rid for rid, _ in timeline], MEASURE_KINDS
        )
    key = (scope, column, subject, measure)
    points = []
    for rid, at in timeline:
        value = compare.measures_of(stored[rid]).scalars.get(key)
        points.append(
            {
                "runId": rid,
                "runTime": at.isoformat(),
                "value": None if value is None else value.value,
                "current": rid == run_id,
            }
        )
    return {"drift": _jsonable(row), "points": points}


async def _registered(conn: Any, table_id: int) -> str:
    """The registered table's name; a declared profile needs no profiler membership."""
    row = (
        await conn.execute_core(
            select(registered_tables.c.table_name).where(registered_tables.c.id == table_id)
        )
    ).fetchone()
    if row is None:
        raise ApiError(
            404, "profile.table_not_found", f"Table {table_id} not found", table_id=table_id
        )
    return row[0]


class DeclaredProfileBody(BaseModel):
    """A declared profile (REQ-1942): the facts a profile run measures, written by hand --
    provisa.profiler.declared says their shape."""

    profile: dict[str, Any]


@router.post("/tables/{table_id}/declared-profiles")
async def declare_profile(request: Request, table_id: int, body: DeclaredProfileBody) -> dict:
    """Store a declared profile of the table in the active environment (REQ-1942): stored as a
    profile run is, so generation reads it as it reads a measured one."""
    require_capability_request(request, "table_registration")
    from provisa.profiler.declared import DeclaredProfileRefused, declare
    from provisa.profiler.run import ProfileError

    async with _db().acquire() as conn:
        table_name = await _registered(conn, table_id)
    try:
        run_id = await declare(state, table_id, table_name, body.profile)
    except (DeclaredProfileRefused, ProfileError) as exc:
        raise ApiError(422, "profile.declared_refused", str(exc), table=table_name) from exc
    return {"runId": run_id}


@router.get("/tables/{table_id}/profile-runs/{run_id}/declared")
async def run_as_declared(
    request: Request,
    table_id: int,
    run_id: str,
    x_provisa_role: str | None = Header(None),
) -> dict:
    """A run -- measured or declared -- as a declared profile document, as its viewer may see it
    (REQ-1942): to change and store as a declared profile of its own."""
    from provisa.profiler.declared import DeclaredProfileRefused, as_declared

    run = await get_profile_run(request, table_id, run_id, x_provisa_role)
    try:
        return {"profile": as_declared(run)}
    except DeclaredProfileRefused as exc:
        raise ApiError(422, "profile.declared_refused", str(exc), run_id=run_id) from exc
