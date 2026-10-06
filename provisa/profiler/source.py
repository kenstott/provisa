# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3c7a58-2b14-4d6f-a1e9-5f8b2c0d7e36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Data Profiler source: its settings, its member tables, its schedule (REQ-1934).

A profiler source holds a name, a schedule and its run default, and nothing else. The schedule is a
cron expression, the recurrence scheduled triggers use (REQ-1003), and fires on the same scheduler;
the run defaults are ``sample_above_rows`` -- None profiles every row, a number samples about that many
rows from a larger table. Both live in the source's ``mapping`` (REQ-251).

Membership is on the table (``registered_tables.profiler_source_id``); a table belongs to at most
one profiler.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

PROFILER_SOURCE_TYPE = "data_profiler"

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProfilerSettings:
    cron: str
    sample_above_rows: int | None
    # A column with no more distinct values than this has its full value-frequency table recorded
    # and may be inferred a category (REQ-1934). The source form supplies its default.
    low_cardinality_max: int


def profiler_settings(source_id: str, mapping: dict) -> ProfilerSettings:
    """The profiler's settings from its mapping, or ValueError naming the source."""
    unknown = set(mapping) - {"cron", "sample_above_rows", "low_cardinality_max"}
    if unknown:
        raise ValueError(
            f"profiler source {source_id!r}: unknown setting(s) {sorted(unknown)}; a profiler "
            f"holds cron, sample_above_rows and low_cardinality_max only"
        )
    cron = mapping.get("cron")
    if not isinstance(cron, str) or not cron.strip():
        raise ValueError(f"profiler source {source_id!r} needs a cron schedule")
    try:
        CronTrigger.from_crontab(cron)
    except ValueError as exc:
        raise ValueError(f"profiler source {source_id!r}: cron {cron!r} is invalid: {exc}") from exc
    sample = mapping.get("sample_above_rows")
    if sample is not None and (
        isinstance(sample, bool) or not isinstance(sample, int) or sample < 1
    ):
        raise ValueError(
            f"profiler source {source_id!r}: sample_above_rows must be a positive whole number "
            f"or absent (profile every row), got {sample!r}"
        )
    low = mapping.get("low_cardinality_max")
    if isinstance(low, bool) or not isinstance(low, int) or low < 1:
        raise ValueError(
            f"profiler source {source_id!r}: low_cardinality_max must be a positive whole number, "
            f"got {low!r}"
        )
    return ProfilerSettings(cron=cron.strip(), sample_above_rows=sample, low_cardinality_max=low)


async def profiler_sources(conn: Any) -> list[dict]:
    """``[{id, settings}]`` for every profiler source of the org."""
    from provisa.core.schema_org import sources

    result = await conn.execute_core(
        select(sources.c.id, sources.c.mapping)
        .where(sources.c.type == PROFILER_SOURCE_TYPE)
        .order_by(sources.c.id)
    )
    return [
        {"id": sid, "settings": profiler_settings(sid, dict(mapping))}
        for sid, mapping in result.fetchall()
    ]


async def members(conn: Any, source_id: str) -> list[dict]:
    """``[{id, table_name}]`` of the tables that joined ``source_id``."""
    from provisa.core.schema_org import registered_tables as rt

    result = await conn.execute_core(
        select(rt.c.id, rt.c.table_name)
        .where(rt.c.profiler_source_id == source_id)
        .order_by(rt.c.table_name)
    )
    return [{"id": tid, "table_name": name} for tid, name in result.fetchall()]


async def check_membership(conn: Any, table: Any) -> None:
    """Refuse a table joining something that is not a Data Profiler source, naming it."""
    from provisa.core.schema_org import sources

    sid = getattr(table, "profiler_source_id", None)
    if sid is None:
        return
    found = (await conn.execute_core(select(sources.c.type).where(sources.c.id == sid))).fetchone()
    if found is None or found[0] != PROFILER_SOURCE_TYPE:
        raise ValueError(f"table {table.table_name!r}: {sid!r} is not a Data Profiler source")


async def run_source(state: Any, source_id: str) -> list[dict]:
    """Profile every member of ``source_id`` in turn. Returns one outcome per member; a member's
    failure is recorded in its own history and in its outcome, and does not stop the others."""
    from provisa.profiler.run import ProfileError, profile_table
    from provisa.core.schema_org import sources

    async with state.model_db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(sources.c.type, sources.c.mapping).where(sources.c.id == source_id)
            )
        ).fetchone()
        if row is None or row[0] != PROFILER_SOURCE_TYPE:
            raise ValueError(f"{source_id!r} is not a Data Profiler source")
        settings = profiler_settings(source_id, dict(row[1]))
        tables = await members(conn, source_id)
    outcomes = []
    for table in tables:
        try:
            run = await profile_table(
                state,
                table_id=table["id"],
                table_name=table["table_name"],
                settings=settings,
            )
            outcomes.append({"table": table["table_name"], "run_id": run.run_id, "error": None})
        except ProfileError as exc:
            outcomes.append({"table": table["table_name"], "run_id": None, "error": str(exc)})
    return outcomes


def profiler_job_id(source_id: str, org_id: str) -> str:
    return f"profiler:{source_id}:org_{org_id}"


async def _scheduled_run(source_id: str, org_id: str) -> None:
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        outcomes = await run_source(state, source_id)
    finally:
        reset_current_org(token)
    failed = [o for o in outcomes if o["error"] is not None]
    if failed:
        # Each failure is already in its table's run history; the job reports them as one error so
        # the scheduler records the fire as failed rather than as a quiet success.
        raise RuntimeError(
            f"profiler {source_id!r}: {len(failed)} of {len(outcomes)} profiles failed: "
            + "; ".join(o["error"] for o in failed)
        )


async def register_org_profilers(scheduler: Any, org_id: str) -> int:
    """Schedule every profiler source of ``org_id`` on its cron, dropping jobs of profilers it no
    longer holds. The caller has bound ``org_id``. Returns how many are scheduled."""
    from provisa.api.app import state

    assert state.model_db is not None, "the org's model store is bound with its runtime"
    from sqlalchemy.schema import CreateTable

    from provisa.profiler.schema import RESULT_KINDS, result_sa_table

    async with state.model_db.acquire() as conn:
        profilers = await profiler_sources(conn)
        for p in profilers:
            # Every member's result relations exist from the moment it joins, empty until its first
            # run, so a result table registered over one compiles and reads as an empty history.
            for member in await members(conn, p["id"]):
                for kind in RESULT_KINDS:
                    table = result_sa_table(member["table_name"], member["id"], kind)
                    await conn.execute_core(CreateTable(table, if_not_exists=True))
    wanted = {profiler_job_id(p["id"], org_id) for p in profilers}
    suffix = f":org_{org_id}"
    for job in scheduler.get_jobs():
        if job.id.startswith("profiler:") and job.id.endswith(suffix) and job.id not in wanted:
            scheduler.remove_job(job.id)
    for p in profilers:
        scheduler.add_job(
            _scheduled_run,
            trigger=CronTrigger.from_crontab(p["settings"].cron),
            args=[p["id"], org_id],
            id=profiler_job_id(p["id"], org_id),
            name=f"profiler:{p['id']}",
            replace_existing=True,
        )
    return len(profilers)
