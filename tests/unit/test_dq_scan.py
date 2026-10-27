# Copyright (c) 2026 Kenneth Stott
# Canary: 8b7c2e14-3f9a-4d06-b5e1-92a4c7d0f3b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A checker table's scans accumulate (REQ-1443, amended 2026-10-12).

Each scan appends its rows, stamped with its scan id and time, to the table's result relation in the
org's control plane, so a second scan leaves the first scan's rows in place. The org's scheduler
holds one job per checker table, firing every cache_ttl seconds; Run now fires that job at once.
The checker subprocess is replaced here by what it returns: its rows.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.core.database import Database
from provisa.core.schema_org import domains, registered_tables, sources
from provisa.dq.results import RESULT_FIELDS, results_sa_table
from tests.helpers import dq_contexts

pytestmark = pytest.mark.asyncio

RELATION = "orders_quality_dq_dq"
CONTRACT = "dataset: provisa/sales/orders\nchecks:\n  - row_count:\n      must_be_greater_than: 0\n"
MAPPING = {
    "host": "127.0.0.1",
    "port": 5439,
    "database": "provisa",
    "user": "checker",
    "password": "secret",
}


@pytest.fixture
def plane(control_plane, monkeypatch):
    """A control plane holding one checker source and its table, bound as the org's model store."""
    import provisa.api.app as appmod
    import provisa.core.secrets_store as secrets_store

    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(sources).values(id="dq", type="soda", mapping=MAPPING))
        c.execute(insert(sources).values(id="warehouse", type="postgresql"))
        c.execute(
            insert(registered_tables).values(
                id=7,
                source_id="dq",
                domain_id="sales",
                schema_name="main",
                table_name=RELATION,
                alias="orders_quality",
                dq_contract=CONTRACT,
                cache_ttl=600,
            )
        )
    db = control_plane.org()
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(
        appmod.state,
        "view_context",
        dq_contexts((1, "warehouse", "sales", "sales", "orders"))["analyst"],
        raising=False,
    )

    @asynccontextmanager
    async def _no_vault():
        yield

    # The mapping holds plain values: there is no reference for the org's vault to resolve.
    monkeypatch.setattr(secrets_store, "bound_to_request_org", _no_vault)
    yield SimpleNamespace(db=db, state=appmod.state)


@pytest.fixture
def checker(monkeypatch):
    """The checker subprocess, as the rows it reports: one row per check, stamped with the scan."""
    calls: list[dict] = []

    async def _run_contract(**kw):
        calls.append(kw)
        row: dict[str, Any] = {field: None for field in RESULT_FIELDS}
        row.update(
            scan_id=kw["scan_id"],
            scan_time=kw["scan_time"],
            checker=kw["checker"],
            target_table=kw["target_table"],
            check_name="row_count",
            outcome="pass",
            diagnostics={"dataset_rows_tested": 3},
        )
        return [row]

    monkeypatch.setattr("provisa.dq.runner.run_contract", _run_contract)
    return calls


async def _history(db: Database) -> list[tuple]:
    relation = results_sa_table(RELATION)
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(relation.c.scan_id, relation.c.check_name).order_by(relation.c.scan_time)
        )
        return [tuple(r) for r in result.fetchall()]


async def test_two_scans_leave_both_scans_rows(plane, checker):
    from provisa.dq.scan import scan_table

    first = await scan_table(plane.state, 7)
    second = await scan_table(plane.state, 7)

    assert first["scan_id"] != second["scan_id"]
    assert await _history(plane.db) == [
        (first["scan_id"], "row_count"),
        (second["scan_id"], "row_count"),
    ]


async def test_a_scan_runs_the_tables_contract_against_its_governed_target(plane, checker):
    from provisa.dq.scan import scan_table

    await scan_table(plane.state, 7)

    [call] = checker
    assert call["checker"] == "soda"
    assert call["contract_text"] == CONTRACT
    assert call["connection"] == MAPPING
    assert call["target_table"] == "sales.orders"


async def test_a_table_that_is_not_a_checker_table_is_refused(plane, checker):
    from provisa.dq.scan import scan_table

    with pytest.raises(ValueError, match="no table 99"):
        await scan_table(plane.state, 99)
    assert checker == []


class _Job:
    def __init__(self, func, trigger, args, id, name):
        self.func, self.trigger, self.args, self.id, self.name = func, trigger, args, id, name


class _Scheduler:
    """The part of APScheduler's registry the org's checker jobs use."""

    def __init__(self):
        self.jobs: dict[str, _Job] = {}
        self.added: list[str] = []

    def get_jobs(self):
        return list(self.jobs.values())

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def remove_job(self, job_id):
        del self.jobs[job_id]

    def add_job(self, func, *, trigger, args, id, name, replace_existing):
        assert replace_existing
        self.jobs[id] = _Job(func, trigger, args, id, name)
        self.added.append(id)


async def test_each_checker_table_scans_every_cache_ttl_seconds(plane, checker):
    from provisa.dq.scan import register_org_checkers

    scheduler = _Scheduler()
    scheduler.jobs["checker:3:org_acme"] = _Job(None, None, [], "checker:3:org_acme", "gone")
    scheduler.jobs["checker:3:org_other"] = _Job(None, None, [], "checker:3:org_other", "kept")

    assert await register_org_checkers(scheduler, "acme") == 1

    job = scheduler.jobs["checker:7:org_acme"]
    assert job.trigger.interval.total_seconds() == 600
    assert job.args == [7, "acme"]
    # A table the org no longer holds stops scanning; another org's job is not this org's to drop.
    assert "checker:3:org_acme" not in scheduler.jobs
    assert "checker:3:org_other" in scheduler.jobs
    # The relation exists before the first scan, so the table reads as an empty history.
    assert await _history(plane.db) == []


async def test_an_unchanged_interval_keeps_its_job_and_its_clock(plane, checker):
    from provisa.dq.scan import register_org_checkers

    scheduler = _Scheduler()
    await register_org_checkers(scheduler, "acme")
    await register_org_checkers(scheduler, "acme")
    assert scheduler.added == ["checker:7:org_acme"]


async def test_run_now_fires_the_tables_job_and_appends_a_scan(plane, checker):
    """Run now (runDqCheckNow) appends a scan beside the earlier ones; the dry run appends none."""
    from provisa.api.admin._dq_resolvers import run_dq_check_now
    from provisa.dq.scan import register_org_checkers, scan_table

    # The job binds its org when it fires: the org this test's work is bound to.
    org = plane.state.org_id
    scheduler = _Scheduler()
    await register_org_checkers(scheduler, org)
    earlier = await scan_table(plane.state, 7)

    async with plane.db.acquire() as conn:
        result = await run_dq_check_now(conn, scheduler=scheduler, org_id=org, table_id=7)

    assert result == {"success": True, "message": f"scanned main.{RELATION} now"}
    history = await _history(plane.db)
    assert len(history) == 2
    assert history[0] == (earlier["scan_id"], "row_count")
    assert history[1][0] != earlier["scan_id"]


async def _latest_runs(db: Database) -> list[tuple]:
    from provisa.core.schema_org import checker_latest_runs

    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(
                checker_latest_runs.c.table_id,
                checker_latest_runs.c.scan_id,
                checker_latest_runs.c.outcome,
            )
        )
        return [tuple(r) for r in result.fetchall()]


async def test_each_scan_is_recorded_as_the_tables_latest_run(plane, checker):
    """REQ-2132: the latest scan, with its outcome, is what the meta domain and the dq service
    read for a checker table; the next scan replaces it."""
    from provisa.dq.scan import scan_table

    assert await _latest_runs(plane.db) == []  # never run
    await scan_table(plane.state, 7)
    second = await scan_table(plane.state, 7)
    assert await _latest_runs(plane.db) == [(7, second["scan_id"], "pass")]


@pytest.mark.parametrize(
    ("outcomes", "expected"),
    [
        (["pass", "pass"], "pass"),
        (["pass", "fail"], "fail"),
        (["pass", "warn"], "fail"),
        (["error"], "fail"),
        (["skipped", "pass"], "fail"),
    ],
)
async def test_a_scan_passes_only_when_every_check_passed(outcomes, expected):
    """REQ-2132 knows passing, failing and not yet run: anything but a pass is a failing scan."""
    from provisa.dq.scan import scan_outcome

    assert scan_outcome([{"outcome": o} for o in outcomes]) == expected


async def test_the_dry_run_records_no_run(plane, checker):
    """The dry run lands nothing, its latest-run record included."""
    from provisa.api.admin._dq_resolvers import dry_run_contract

    async with plane.db.acquire() as conn:
        result = await dry_run_contract(conn, source_id="dq", contract_text=CONTRACT)
    assert result["success"] is True, result
    assert len(checker) == 1  # the checker did run
    assert await _latest_runs(plane.db) == []
