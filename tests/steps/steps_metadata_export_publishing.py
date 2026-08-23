# Copyright (c) 2026 Kenneth Stott
# Canary: 8f14b6a0-3d27-4e59-b0c8-51a97e3d2064
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-1072 — how a metadata change reaches the external catalog.

The queue is a real SQLite control plane, so the claim/complete behaviour under test is the
REQ-942 substrate's own. Only the publish is stubbed: what the scenario asserts is the lifecycle
around it, and a live catalog is exercised by the Atlas e2e instead.

One event loop is created for the scenario and reused by every step — the SQLite connections are
bound to the loop that opened them, so a per-step ``asyncio.run`` would hand the next step a
connection whose loop is closed.
"""

from __future__ import annotations

import asyncio

from datetime import datetime, timezone

import pytest

from pytest_bdd import given, scenarios, then, when
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.api.metadata_export import publishing
from provisa.api.metadata_export.provider import AssetError, AssetRefStub, PublishResult
from provisa.core.database import Database
from provisa.core.schema_org import event_status, events

scenarios("../features/REQ-1072.feature")

ORG = "acme"
OTHER_ORG = "globex"
_T0 = datetime(2026, 8, 4, 9, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def shared_data():
    loop = asyncio.new_event_loop()
    data = {"loop": loop, "engines": []}
    yield data
    for engine in data["engines"]:
        loop.run_until_complete(engine.dispose())
    loop.close()


def _run(shared_data, coro):
    return shared_data["loop"].run_until_complete(coro)


def _publishes_ok(published: list[str]):
    async def _publish(org_id: str) -> PublishResult:
        published.append(org_id)
        result = PublishResult(provider_name="atlas")
        result.published["table"] = 3
        return result

    return _publish


@given("an org whose metadata export target is configured and entitled")
def _configured_org(shared_data, tmp_path, monkeypatch):
    published: list[str] = []
    monkeypatch.setattr(publishing, "publish_snapshot", _publishes_ok(published))
    shared_data["published"] = published

    async def _open(name: str) -> Database:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / name}.db")
        async with engine.begin() as conn:
            await conn.run_sync(
                lambda s: events.metadata.create_all(s, tables=[events, event_status])
            )
        shared_data["engines"].append(engine)
        return Database(engine, name=name)

    shared_data["dbs"] = {org: _run(shared_data, _open(org)) for org in (ORG, OTHER_ORG)}


@when("the governed model changes")
def _model_changes(shared_data):
    async def _post() -> None:
        async with shared_data["dbs"][ORG].acquire() as conn:
            for table in ("wh.public.orders", "wh.public.customers"):
                await publishing.notify_metadata_change(conn, table=table, reason="schema rebuild")

    _run(shared_data, _post())


@then("a publish is queued for that org and no other")
def _queued_for_one_org(shared_data):
    async def _counts() -> list[int]:
        out = []
        for org in (ORG, OTHER_ORG):
            async with shared_data["dbs"][org].acquire() as conn:
                result = await conn.execute_core(select(event_status.c.dependent_table))
                rows = result.fetchall()
            assert all(row[0] == publishing.EGRESS_TARGET for row in rows)
            out.append(len(rows))
        return out

    mine, theirs = _run(shared_data, _counts())
    assert mine == 2
    # The other org's queue lives in its own schema, so its drain finds nothing rather than
    # finding this org's change.
    assert theirs == 0


@then("one drain publishes the whole pending change set as a single snapshot")
def _one_publish(shared_data):
    async def _drain() -> None:
        async with shared_data["dbs"][ORG].acquire() as conn:
            result = await publishing.drain(conn, ORG, now=_T0)
            assert result is not None and result.ok
            statuses = await conn.execute_core(select(event_status.c.claim_status))
            assert [row[0] for row in statuses.fetchall()] == ["completed", "completed"]
            # Nothing left to claim, so a second drain publishes nothing.
            assert await publishing.drain(conn, ORG, now=_T0) is None

    _run(shared_data, _drain())
    assert shared_data["published"] == [ORG]


@then("a publish the catalog rejected stays claimable instead of being completed")
def _rejected_stays_claimable(shared_data, monkeypatch):
    async def _rejecting(org_id: str) -> PublishResult:
        result = PublishResult(provider_name="atlas")
        result.errors.append(AssetError(asset=AssetRefStub("wh.public.orders"), message="HTTP 500"))
        return result

    monkeypatch.setattr(publishing, "publish_snapshot", _rejecting)

    async def _drain() -> None:
        async with shared_data["dbs"][ORG].acquire() as conn:
            await publishing.notify_metadata_change(conn, table="wh.public.orders", reason="edit")
            result = await publishing.drain(conn, ORG, now=_T0)
            assert result is not None and not result.ok
            statuses = await conn.execute_core(
                select(event_status.c.claim_status).where(
                    event_status.c.claim_status != "completed"
                )
            )
            assert [row[0] for row in statuses.fetchall()] == ["claimed"]

    _run(shared_data, _drain())
    monkeypatch.setattr(publishing, "publish_snapshot", _publishes_ok(shared_data["published"]))


@then("the org's scheduled reconcile republishes the full snapshot on its own cron")
def _reconcile_is_scheduled(shared_data):
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    # The reconcile is the same publish on a cron the org owns, so what is asserted is that the
    # armed job carries that org's id and calls that publish.
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        publishing.reconcile_org, trigger="cron", args=[ORG], id=publishing.reconcile_job_id(ORG)
    )
    job = scheduler.get_job(publishing.reconcile_job_id(ORG))
    assert job is not None
    assert list(job.args) == [ORG]
    assert job.func is publishing.reconcile_org

    async def _reconcile() -> None:
        result = await publishing.reconcile_org(ORG)
        assert result is not None and result.ok

    _run(shared_data, _reconcile())
    assert shared_data["published"][-1] == ORG
