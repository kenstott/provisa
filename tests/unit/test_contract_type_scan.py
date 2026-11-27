# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e1f02-6c4d-4a97-9e25-d1f7a0c4b386
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2158 (amended 2026-11-27): the logical-type promise check runs for every product member on
the org's data-quality scheduler beside the checker jobs, every member cache_ttl; its latest result
per dataset is kept (contract_type_checks) and shown among the promises as Declared logical types."""

# Requirements: REQ-2158

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select, update

from provisa.api.offer_derivation import rederive_all
from provisa.core.models import Column, DataProduct, Table
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import data_product_offer as offers
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import contract_type_checks, domains, registered_tables, sources
from tests.unit.offer_build import installed

pytestmark = pytest.mark.asyncio


@dataclass(frozen=True)
class _Listed:
    name: str
    data_type: str


async def _table_id(conn, name: str = "orders") -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == name)
        )
    ).scalar_one()


@pytest.fixture
async def plane(control_plane, monkeypatch):
    """Product p with member orders (id integer, placed_on date served as placed) of source pg,
    whose cache_ttl is 900; and customers, a table of pg in no product. Bound as the org's model
    store."""
    import provisa.api.app as appmod

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", cache_ttl=900))
        await conn.execute_core(insert(domains).values(id="sales"))
        await data_product_repo.upsert(conn, DataProduct(id="p", domain_id="sales", name="P"))
        for name in ("orders", "customers"):
            await table_repo.upsert(
                conn,
                Table(
                    source_id="pg",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                    columns=[
                        Column(name="id", visible_to=["viewer"], data_type="integer"),
                        Column(
                            name="placed_on",
                            alias="placed",
                            visible_to=["viewer"],
                            data_type="date",
                        ),
                    ],
                ),
            )
        await data_product_repo.set_members(
            conn, "p", table_ids=[await _table_id(conn)], commands=[]
        )
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    return SimpleNamespace(db=db, state=appmod.state)


def _source(monkeypatch, *columns: tuple[str, str]) -> list[tuple[str, str, str]]:
    """The source's columns now, as resolve_available_columns_metadata lists them."""
    asked: list[tuple[str, str, str]] = []

    async def read(source_id: str, schema_name: str, table_name: str) -> list[_Listed]:
        asked.append((source_id, schema_name, table_name))
        return [_Listed(n, t) for n, t in columns]

    monkeypatch.setattr("provisa.api.admin.schema_query.resolve_available_columns_metadata", read)
    return asked


class _Job:
    def __init__(self, func, trigger, args, id, name):
        self.func, self.trigger, self.args, self.id, self.name = func, trigger, args, id, name


class _Scheduler:
    """The part of APScheduler's registry the org's jobs use."""

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


async def _latest(db) -> list[tuple]:
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(
                    contract_type_checks.c.table_id,
                    contract_type_checks.c.outcome,
                    contract_type_checks.c.failing,
                )
            )
        ).fetchall()
    return [tuple(r) for r in rows]


async def test_each_member_is_scheduled_every_its_sources_cache_ttl(plane):
    from provisa.dq.scan import _scheduled_type_check, register_org_type_checks

    scheduler = _Scheduler()
    scheduler.jobs["contract-types:99:org_acme"] = _Job(
        None, None, [], "contract-types:99:org_acme", "gone"
    )
    scheduler.jobs["contract-types:99:org_other"] = _Job(
        None, None, [], "contract-types:99:org_other", "kept"
    )

    assert await register_org_type_checks(scheduler, "acme") == 1

    async with plane.db.acquire() as conn:
        member = await _table_id(conn)
    job = scheduler.jobs[f"contract-types:{member}:org_acme"]
    assert job.func is _scheduled_type_check
    assert job.trigger.interval.total_seconds() == 900  # the source's: the member states none
    assert job.args == [member, "acme"]
    # customers is in no product; a member the org no longer holds stops; another org's stays.
    assert "contract-types:99:org_acme" not in scheduler.jobs
    assert "contract-types:99:org_other" in scheduler.jobs


async def test_a_members_own_cache_ttl_is_its_interval_and_an_unchanged_one_keeps_its_clock(plane):
    from provisa.dq.scan import register_org_type_checks

    scheduler = _Scheduler()
    await register_org_type_checks(scheduler, "acme")
    await register_org_type_checks(scheduler, "acme")
    assert len(scheduler.added) == 1

    async with plane.db.acquire() as conn:
        member = await _table_id(conn)
        await conn.execute_core(
            update(registered_tables).where(registered_tables.c.id == member).values(cache_ttl=60)
        )
    await register_org_type_checks(scheduler, "acme")
    assert len(scheduler.added) == 2
    assert (
        scheduler.jobs[f"contract-types:{member}:org_acme"].trigger.interval.total_seconds() == 60
    )


@pytest.mark.parametrize("ttl", [None, 0, -1])
async def test_a_member_with_no_interval_is_not_scheduled(plane, ttl):
    from provisa.dq.scan import register_org_type_checks

    async with plane.db.acquire() as conn:
        await conn.execute_core(update(sources).values(cache_ttl=ttl))
    scheduler = _Scheduler()
    assert await register_org_type_checks(scheduler, "acme") == 0
    assert scheduler.jobs == {}


async def test_each_run_records_the_members_latest_result_with_its_failing_columns(
    plane, monkeypatch
):
    from provisa.dq.scan import check_member_types

    built = installed(monkeypatch)
    async with plane.db.acquire() as conn:
        await rederive_all(conn, await built.state(conn))
        member = await _table_id(conn)
        (offer,) = await offers.list_offers(conn, "p")

    asked = _source(monkeypatch, ("id", "bigint"), ("placed_on", "date"))
    assert await check_member_types(plane.state, member) == {"outcome": "pass", "failing": []}
    assert asked == [("pg", "public", "orders")]
    assert await _latest(plane.db) == [(member, "pass", [])]

    _source(monkeypatch, ("id", "bigint"), ("placed_on", "varchar(10)"))
    await check_member_types(plane.state, member)
    failing = [
        {"offer_id": offer["id"], "column": "placed", "declared": "date", "live": "varchar(10)"}
    ]
    # The next run replaces the earlier one: one row per dataset.
    assert await _latest(plane.db) == [(member, "fail", failing)]


async def test_a_member_holding_no_contract_keeps_no_result(plane, monkeypatch):
    from provisa.dq.scan import check_member_types, record_type_check
    from provisa.dq.contract_types import TypeCheck

    async with plane.db.acquire() as conn:
        member = await _table_id(conn)
        await record_type_check(conn, member, {1: TypeCheck("pass")}, datetime.now(UTC))
    assert len(await _latest(plane.db)) == 1

    _source(monkeypatch, ("id", "bigint"))
    assert await check_member_types(plane.state, member) == {"outcome": None, "failing": []}
    assert await _latest(plane.db) == []


async def test_the_product_reads_each_members_latest_type_check(plane):
    from provisa.api.admin.lineage_router import _fetch_type_checks
    from provisa.dq.contract_types import TypeCheck, TypeMismatch
    from provisa.dq.scan import record_type_check

    at = datetime(2026, 11, 27, 10, 0, tzinfo=UTC)
    async with plane.db.acquire() as conn:
        member = await _table_id(conn)
        other = await _table_id(conn, "customers")
        gone = TypeMismatch(column="placed", declared="date", live=None)
        await record_type_check(conn, member, {5: TypeCheck("fail", [gone])}, at)
        await record_type_check(conn, other, {6: TypeCheck("pass")}, at)

    assert await _fetch_type_checks(plane.state, {member}) == {
        member: {
            "outcome": "fail",
            "checked_at": at.isoformat(),
            "failing": [{"offer_id": 5, "column": "placed", "declared": "date", "live": None}],
        }
    }
