# Copyright (c) 2026 Kenneth Stott
# Canary: b944c5e0-a347-4436-8301-32eb7b858fb5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The review (REQ-2094): the staleness checks over an org's judgments, written to the queue
through ``record_finding``, with the posture checks and the sensitive columns that have no fake.

It runs hourly per org (``judgments:review``, as ``roles:sweep_grants`` runs), at the end of each
profile run for the profiled table, and at once for the judgments citing a basis declared
changed. A run clears the open findings it produces that it no longer sees, within what it
looked at.
"""

# Requirements: REQ-2094

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import select

from provisa.judgments import findings as fq
from provisa.judgments import staleness
from provisa.judgments.inventory import Judgment, judgments

JOB_ID = "judgments:review"
REVIEW_MINUTES = 60

#: The finding a sensitive column with no fake is listed under, fed by uncovered_sensitive.
NO_FAKE = "no_fake"


async def _members(admin_db: Any, org_id: str) -> set[str]:
    from provisa.core.schema_admin import user_org_memberships as m

    async with admin_db.acquire() as conn:
        rows = await conn.execute_core(select(m.c.user_id).where(m.c.org_id == org_id))
        return {r[0] for r in rows.fetchall()}


async def _bases(conn: Any) -> dict[str, dt.datetime | None]:
    from provisa.core.schema_org import judgment_bases as jb

    rows = await conn.execute_core(select(jb.c.id, jb.c.changed_at))
    return {r[0]: r[1] for r in rows.fetchall()}


async def _registrations(conn: Any, table_ids: set[int]) -> dict[int, dict[str, Any]]:
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import table_columns as tc

    regs = {
        r["id"]: {**r, "columns": {}}
        for r in (
            dict(x._mapping)
            for x in (
                await conn.execute_core(select(rt).where(rt.c.id.in_(sorted(table_ids))))
            ).fetchall()
        )
    }
    for tid, name, dtype in (
        await conn.execute_core(
            select(tc.c.table_id, tc.c.column_name, tc.c.data_type).where(
                tc.c.table_id.in_(sorted(table_ids))
            )
        )
    ).fetchall():
        if tid in regs:
            regs[tid]["columns"][name] = dtype
    return regs


async def _drifting(conn: Any, reg: dict, tid: int, latest: str, evidence_run: str) -> set[str]:
    """The columns the profiler's comparison of ``latest`` with ``evidence_run`` flags drifting
    by its own thresholds; empty when the evidence run is no longer held."""
    from provisa.core.schema_org import sources
    from provisa.profiler import compare
    from provisa.profiler.history import MEASURE_KINDS, run_results
    from provisa.profiler.source import profiler_settings

    stored = await run_results(conn, reg["table_name"], tid, [latest, evidence_run], MEASURE_KINDS)
    if not stored[evidence_run]["runs"] or not stored[latest]["runs"]:
        return set()  # pruned: the digest still says whether the type changed
    mapping = (
        await conn.execute_core(
            select(sources.c.mapping).where(sources.c.id == reg["profiler_source_id"])
        )
    ).scalar()
    settings = profiler_settings(reg["profiler_source_id"], dict(mapping or {}))
    rows = compare.compare(
        compare.measures_of(stored[latest]), compare.measures_of(stored[evidence_run])
    )
    return {
        r["column"]
        for r in rows
        if r.get("column")
        and (
            (r["ks_previous"] is not None and r["ks_previous"] > settings.drift_ks)
            or (r["psi_previous"] is not None and r["psi_previous"] > settings.drift_psi)
        )
    }


async def _table_now(
    conn: Any, schema: str, reg: dict, wanted_runs: set[str]
) -> staleness.TableNow:
    from provisa.judgments.evidence import digest, latest_run

    found = await latest_run(conn, schema, reg)
    if found is None:
        return staleness.TableNow(dict(reg["columns"]), None, {}, {})
    tid, run_id = found
    d = await digest(conn, schema, reg, tid, run_id)
    drifting = {
        run: await _drifting(conn, reg, tid, run_id, run) for run in wanted_runs if run != run_id
    }
    return staleness.TableNow(dict(reg["columns"]), run_id, d["facts"], drifting)


async def _review(
    conn: Any,
    schema: str,
    members: set[str],
    selected: list[Judgment],
    *,
    whole_org: bool,
    at: dt.datetime,
) -> dict[str, int]:
    from provisa.core.env_data import uncovered_sensitive

    bases = await _bases(conn)
    table_ids = {j.table_id for j in selected if j.table_id is not None}
    regs = await _registrations(conn, table_ids)
    nows: dict[int, staleness.TableNow] = {}
    for tid, reg in regs.items():
        runs = {
            (j.why.get("evidence") or {}).get("run_id") for j in selected if j.table_id == tid
        } - {None}
        nows[tid] = await _table_now(conn, schema, reg, runs)  # type: ignore[arg-type]
    today = at.date()
    seen: list[fq.Key] = []
    within: set[tuple[str, str]] = set()
    for j in selected:
        within.add((j.object_type, j.object_key))
        now = nows.get(j.table_id) if j.table_id is not None else None
        for f in staleness.check(j, now, today=today, members=members, bases=bases):
            seen.append(
                await fq.record_finding(
                    conn,
                    object_type=j.object_type,
                    object_key=j.object_key,
                    judgment=j.judgment,
                    category=f.category,
                    finding=f.finding,
                    observed=f.observed,
                    at=at,
                )
            )
    # Columns added to a table holding a sensitive tag or a fake: undecided.
    for tid, now in nows.items():
        on_table = [j for j in selected if j.table_id == tid]
        for column, observed in staleness.undecided_columns(on_table, now):
            key = ("column", f"{tid}:{column}")
            within.add(key)
            seen.append(
                await fq.record_finding(
                    conn,
                    object_type="column",
                    object_key=f"{tid}:{column}",
                    judgment=fq.FAKE,
                    category=fq.UNDECIDED,
                    finding=staleness.UNDECIDED_COLUMN,
                    observed=observed,
                    at=at,
                )
            )
    cleared = await fq.clear_unseen(
        conn,
        seen,
        judgments=fq.JUDGMENTS,
        findings=(*staleness.FINDINGS, staleness.UNDECIDED_COLUMN),
        at=at,
        within=None if whole_org else within,
    )
    if whole_org:
        cleared += await _record_uncovered(conn, await uncovered_sensitive(conn, schema), at)
    return {"recorded": len(seen), "cleared": cleared}


async def _record_uncovered(conn: Any, columns: list[str], at: dt.datetime) -> int:
    """Each sensitive column with no fake (``table.column``, as uncovered_sensitive names it) as
    one undecided finding, so the coverage count and the queue cannot disagree. Returns how many
    such findings cleared."""
    from provisa.core.schema_org import registered_tables as rt

    ids = {
        r[1]: r[0] for r in (await conn.execute_core(select(rt.c.id, rt.c.table_name))).fetchall()
    }
    seen = []
    for qualified in columns:
        table, column = qualified.split(".", 1)
        seen.append(
            await fq.record_finding(
                conn,
                object_type="column",
                object_key=f"{ids[table]}:{column}",
                judgment=fq.FAKE,
                category=fq.UNDECIDED,
                finding=NO_FAKE,
                observed={"table": table, "column": column},
                at=at,
            )
        )
    return await fq.clear_unseen(conn, seen, judgments=(fq.FAKE,), findings=(NO_FAKE,), at=at)


async def review_org(
    state: Any,
    org_id: str,
    env: str | None = None,
    *,
    table_id: int | None = None,
    basis_id: str | None = None,
) -> dict[str, int]:
    """Review ``org_id``'s judgments in ``env`` (both bound by the caller): all of them, or those
    on ``table_id``, or those citing ``basis_id``; with the posture checks on a whole-org run."""
    from provisa.core.environments import org_schema
    from provisa.judgments import posture

    db = state.model_db
    assert db is not None, "the org's model store is bound with its runtime"
    assert state.admin_db is not None, "the platform plane is brought up before any org runtime"
    members = await _members(state.admin_db, org_id)
    at = dt.datetime.now(dt.timezone.utc)
    schema = org_schema(org_id, env)
    whole = table_id is None and basis_id is None
    async with db.acquire() as conn:
        every = await judgments(conn)
        selected = [
            j
            for j in every
            if (table_id is None or j.table_id == table_id)
            and (basis_id is None or basis_id in (j.why.get("bases") or ()))
        ]
        counts = await _review(conn, schema, members, selected, whole_org=whole, at=at)
        if whole:
            counts["posture"] = await posture.check_org(state, conn, org_id, env, at)
    return counts


async def scheduled_review(org_id: str) -> dict[str, int]:
    """``org_id``'s review, run by the scheduler."""
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        return await review_org(state, org_id)
    finally:
        reset_current_org(token)


def job_id(org_id: str) -> str:
    return f"{JOB_ID}:org_{org_id}"


def register_org(scheduler: Any, org_id: str) -> None:
    """Review ``org_id`` every REVIEW_MINUTES. A job already held is kept."""
    from apscheduler.triggers.interval import IntervalTrigger

    if scheduler.get_job(job_id(org_id)) is not None:
        return
    scheduler.add_job(
        scheduled_review,
        trigger=IntervalTrigger(minutes=REVIEW_MINUTES),
        args=[org_id],
        id=job_id(org_id),
        name=JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
