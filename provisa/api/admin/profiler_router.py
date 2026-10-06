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

The run history and results are shown only to those who may edit the table (the
``table_registration`` capability); they are read straight from the result relations, as the
profiler wrote them.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, Request
from sqlalchemy import select
from sqlalchemy.schema import CreateTable

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
                    "sampleAboveRows": settings.sample_above_rows,
                    "lowCardinalityMax": settings.low_cardinality_max,
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
    from provisa.core.schema_org import sources
    from provisa.profiler.run import ProfileError, profile_table
    from provisa.profiler.source import profiler_settings

    async with _db().acquire() as conn:
        member = await _member(conn, table_id)
        mapping = (
            await conn.execute_core(
                select(sources.c.mapping).where(sources.c.id == member["source_id"])
            )
        ).fetchone()
    assert mapping is not None, "membership names a source the integrity guard keeps"
    settings = profiler_settings(member["source_id"], dict(mapping[0]))
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
    from provisa.profiler.schema import result_sa_table

    table = result_sa_table(table_name, table_id, kind)
    # A member that has not run yet has an empty history, not a missing one.
    await conn.execute_core(CreateTable(table, if_not_exists=True))
    return table


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

    acting = getattr(request.state, "role", None) or x_provisa_role
    if not acting:
        raise ApiError(
            400,
            "profile.role_header_required",
            "X-Provisa-Role header required: a profile run is shown as its viewer may see it",
        )
    roles = frozenset(r.strip() for r in acting.split(",") if r.strip())
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
