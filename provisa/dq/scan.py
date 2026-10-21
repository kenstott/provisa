# Copyright (c) 2026 Kenneth Stott
# Canary: 5a1e8c37-9d42-4b6f-8e03-c7b2a9f4d618
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A checker table's scans, and the schedule they run on (REQ-1443, amended 2026-10-12).

A scan runs the table's contract through its checker and APPENDS the per-check rows, stamped with
the scan's id and time, to the table's result relation in the org's control-plane schema -- the
path a Data Profiler's runs take (REQ-1934). The relation is read there in place on every engine and
never replicated, so every earlier scan's rows stay beside the new ones.

Each checker table scans every ``cache_ttl`` seconds on the org's scheduler; Run now
(``runDqCheckNow``) fires the same job at once. The dry run (``dryRunDqContract``) appends nothing.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select
from sqlalchemy.schema import CreateTable

from provisa.dq.contract import CHECKERS
from provisa.dq.results import results_sa_table

log = logging.getLogger(__name__)

_JOB_PREFIX = "checker:"


def checker_job_id(table_id: int, org_id: str) -> str:
    return f"{_JOB_PREFIX}{table_id}:org_{org_id}"


def checker_connection(mapping: dict) -> dict:
    """The pgwire endpoint a checker connects back through, from its source's ``mapping``
    (REQ-251), every reference it holds resolved here, at use."""
    from provisa.core.secrets import resolve_secrets_in_dict

    resolved = resolve_secrets_in_dict(dict(mapping))
    return {k: resolved[k] for k in ("host", "port", "database", "user", "password")}


async def ensure_relation(conn: Any, table_name: str) -> None:
    """Create ``table_name``'s result relation, empty, if it does not exist, so the registered
    table compiles and reads as an empty history before its first scan."""
    await conn.execute_core(CreateTable(results_sa_table(table_name), if_not_exists=True))


async def checker_tables(conn: Any) -> list[dict]:
    """``[{id, table_name, source_id, cache_ttl}]`` of every checker table of the org."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import sources

    result = await conn.execute_core(
        select(rt.c.id, rt.c.table_name, rt.c.source_id, rt.c.cache_ttl)
        .join(sources, sources.c.id == rt.c.source_id)
        .where(sources.c.type.in_(sorted(CHECKERS)))
        .order_by(rt.c.id)
    )
    return [
        {"id": tid, "table_name": name, "source_id": sid, "cache_ttl": ttl}
        for tid, name, sid, ttl in result.fetchall()
    ]


async def scan_table(state: Any, table_id: int) -> dict:
    """Scan checker table ``table_id`` once and append its rows. Returns ``{scan_id, rows}``.

    Raises :class:`ValueError` for a table that is not a checker table, and the checker's own
    errors (:class:`~provisa.dq.runner.CheckerError`, :class:`~provisa.dq.contract.ContractError`)
    as they come."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import sources
    from provisa.core.secrets_store import bound_to_request_org
    from provisa.dq.contract import contract_dataset, resolve_contract_target
    from provisa.dq.runner import run_contract

    async with state.model_db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(rt.c.table_name, rt.c.dq_contract, sources.c.type, sources.c.mapping)
                .join(sources, sources.c.id == rt.c.source_id)
                .where(rt.c.id == table_id)
            )
        ).fetchone()
    if row is None:
        raise ValueError(f"no table {table_id}")
    table_name, contract, checker, mapping = row
    if checker not in CHECKERS or not contract:
        raise ValueError(f"table {table_name!r} is not a data-quality checker table")
    dataset = contract_dataset(contract, checker)
    target = resolve_contract_target(dataset, state.contexts)
    async with bound_to_request_org():
        connection = checker_connection(mapping)
    scan_id = str(uuid.uuid4())
    scan_time = datetime.now(UTC)
    rows = await run_contract(
        checker=checker,
        contract_text=contract,
        connection=connection,
        data_source_name=dataset.split("/")[0],
        scan_id=scan_id,
        scan_time=scan_time,
        target_table=f"{target.schema_name}.{target.table_name}",
    )
    async with state.model_db.acquire() as conn:
        await ensure_relation(conn, table_name)
        # The scan's rows and its record as the table's latest run land together or not at all.
        async with conn.transaction():
            if rows:
                await conn.execute_core_many(results_sa_table(table_name).insert(), rows)
            await record_latest_run(conn, table_id, scan_id, scan_time, rows)
    return {"scan_id": scan_id, "rows": len(rows)}


def scan_outcome(rows: list[dict]) -> str:
    """A scan's outcome as REQ-2132 shows it: ``pass`` when every check passed, ``fail``
    otherwise -- a warning, an error or a skipped check is not a pass."""
    return "pass" if all(r["outcome"] == "pass" for r in rows) else "fail"


async def record_latest_run(
    conn: Any, table_id: int, scan_id: str, scan_time: datetime, rows: list[dict]
) -> None:
    """Record a scan as checker table ``table_id``'s latest run (checker_latest_runs), the one
    answer the meta domain and the dq service read for it."""
    from provisa.core.schema_org import checker_latest_runs

    await conn.upsert(
        checker_latest_runs,
        {
            "table_id": table_id,
            "scan_id": scan_id,
            "scan_time": scan_time,
            "outcome": scan_outcome(rows),
        },
        index_elements=["table_id"],
        update_columns=["scan_id", "scan_time", "outcome"],
    )


async def _scheduled_scan(table_id: int, org_id: str) -> None:
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        await scan_table(state, table_id)
    finally:
        reset_current_org(token)


async def register_org_checkers(scheduler: Any, org_id: str) -> int:
    """Schedule every checker table of ``org_id`` to scan every ``cache_ttl`` seconds, dropping
    the jobs of tables it no longer holds. A job whose interval is unchanged is kept, so its
    clock is not restarted by an unrelated edit. The caller has bound ``org_id``. Returns how
    many are scheduled."""
    from provisa.api.app import state

    assert state.model_db is not None, "the org's model store is bound with its runtime"
    async with state.model_db.acquire() as conn:
        tables = await checker_tables(conn)
        for t in tables:
            await ensure_relation(conn, t["table_name"])
    wanted = {checker_job_id(t["id"], org_id): t for t in tables}
    suffix = f":org_{org_id}"
    for job in scheduler.get_jobs():
        if job.id.startswith(_JOB_PREFIX) and job.id.endswith(suffix) and job.id not in wanted:
            scheduler.remove_job(job.id)
    for job_id, t in wanted.items():
        seconds = t["cache_ttl"]
        # A stored checker table states its interval: registration refuses one without it.
        assert isinstance(seconds, int) and seconds > 0, f"{t['table_name']} has no scan interval"
        held = scheduler.get_job(job_id)
        if held is not None and held.trigger.interval.total_seconds() == seconds:
            continue
        scheduler.add_job(
            _scheduled_scan,
            trigger=IntervalTrigger(seconds=seconds),
            args=[t["id"], org_id],
            id=job_id,
            name=f"checker:{t['table_name']}",
            replace_existing=True,
        )
    return len(tables)


async def reschedule_org_checkers() -> None:
    """Reschedule the bound org's checker tables, if this process runs a scheduler. Only prod's
    are scheduled, as a profiler's are (``register_org_triggers``)."""
    from provisa.api.app import state
    from provisa.core.environments import PROD
    from provisa.core.request_context import current_env, require_current_org

    scheduler = state._scheduler
    env = current_env.get()
    if scheduler is None or (env is not None and env != PROD):
        return
    await register_org_checkers(scheduler, require_current_org())
