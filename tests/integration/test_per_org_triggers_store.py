# Copyright (c) 2026 Kenneth Stott
# Canary: d408f155-8260-48df-aeab-6197dbf159a0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A tenant's scheduled trigger writes only its own org's data (REQ-1003, REQ-1266, REQ-1919).

Two orgs built by the real ``build_org_runtime`` against a live Postgres. Each registers its own
table under the same governed name, ``trigs.marks`` -- physically ``trig_a.marks`` for one org and
``trig_b.marks`` for the other. Each org creates a trigger of the same id through the admin
mutation; it is written to that org's model store only. Each job fires with nothing bound, through
the real pipeline, and every assertion is on the rows AT THE SOURCE, read directly from Postgres.
"""

# Requirements: REQ-1003, REQ-1266, REQ-1919

from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.core.request_context import current_org, reset_current_org, set_current_org

_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "per_org_triggers_config.yaml")
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# org id -> the physical schema its trigs.marks is
_ORGS = {"trga": "trig_a", "trgb": "trig_b"}


def _table(schema: str) -> dict:
    return {
        "source_id": "trig-pg",
        "domain_id": "trigs",
        "schema": schema,
        "table": "marks",
        "columns": [
            {
                "name": "id",
                "data_type": "integer",
                "is_primary_key": True,
                "visible_to": ["trig_writer"],
                "writable_by": ["trig_writer"],
            },
            {
                "name": "note",
                "data_type": "varchar",
                "visible_to": ["trig_writer"],
                "writable_by": ["trig_writer"],
            },
        ],
    }


async def _drop(state) -> None:
    async with state.tenant_db.acquire() as conn:
        for org, schema in _ORGS.items():
            await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            await conn.execute(f"DROP SCHEMA IF EXISTS org_{org} CASCADE")
            await conn.execute(f"DROP SCHEMA IF EXISTS org_{org}_mv_cache CASCADE")


@pytest.fixture(scope="module")
async def two_orgs():
    """Two orgs, each with its own trigs.marks, built and served by the running app."""
    from _pytest.monkeypatch import MonkeyPatch

    from provisa.api.app import _rebuild_schemas, build_org_runtime, create_app, state
    from provisa.core.config_loader import apply_config, parse_config_dict

    mp = MonkeyPatch()
    mp.setenv("PROVISA_CONFIG", _CONFIG)
    mp.setenv("PG_HOST", os.environ.get("PG_HOST", "localhost"))
    mp.setenv("PG_PORT", os.environ.get("PG_PORT", "5432"))
    mp.setenv("PG_PASSWORD", os.environ.get("PG_PASSWORD", "provisa"))
    try:
        app = create_app()
        async with app.router.lifespan_context(app):
            assert state.tenant_db is not None
            await _drop(state)
            async with state.tenant_db.acquire() as conn:
                for schema in _ORGS.values():
                    await conn.execute(f"CREATE SCHEMA {schema}")
                    await conn.execute(
                        f"CREATE TABLE {schema}.marks (id integer PRIMARY KEY, note varchar)"
                    )
            for org, schema in _ORGS.items():
                rt = await build_org_runtime(org, include_demo=True)
                token = set_current_org(org)
                try:
                    assert rt.model_db is not None
                    async with rt.model_db.acquire() as conn:
                        await apply_config(
                            parse_config_dict(
                                {
                                    "sources": [],
                                    "domains": [],
                                    "roles": [],
                                    "tables": [_table(schema)],
                                }
                            ),
                            conn,
                            state.federation_engine,
                        )
                    await _rebuild_schemas()
                finally:
                    reset_current_org(token)
            try:
                yield state
            finally:
                await _drop(state)
    finally:
        mp.undo()


async def _rows(state, schema: str) -> list[tuple[int, str]]:
    async with state.tenant_db.acquire() as conn:
        rows = await conn.fetch(f"SELECT id, note FROM {schema}.marks ORDER BY id")
        return [(r["id"], r["note"]) for r in rows]


async def _in(org: str, work):
    """``work`` as a request to ``org``: bound to it, in the model change the request opens."""
    from provisa.core import model_change

    token = set_current_org(org)
    try:
        async with model_change.scope(f"test request to {org}"):
            return await work()
    finally:
        reset_current_org(token)


async def test_each_orgs_trigger_writes_only_its_own_orgs_table(two_orgs):
    from provisa.api.admin import schema_mutation_ops as ops
    from provisa.core.repositories import scheduled_trigger as trigger_repo
    from provisa.scheduler import jobs
    from provisa.scheduler.executor import background_scheduler

    state = two_orgs
    # A deployment with no auth provider: the trigger's role is taken as the caller's.
    request = cast("Any", SimpleNamespace(state=SimpleNamespace(identity=None)))

    for org in _ORGS:

        async def _create(org=org):
            return await ops.create_scheduled_task_op(
                request,
                "nightly",
                "Nightly",
                "0 2 * * *",
                "sql",
                None,
                None,
                f"INSERT INTO trigs.marks (id, note) VALUES (1, '{org}')",
                "trig_writer",
            )

        created = await _in(org, _create)
        assert created.success is True, created.message

    # Each org's model holds its own trigger of that id, and only its own.
    for org in _ORGS:

        async def _held(org=org):
            async with state.model_db.acquire() as conn:
                return [(r["id"], r["sql"]) for r in await trigger_repo.list_all(conn)]

        assert await _in(org, _held) == [
            ("nightly", f"INSERT INTO trigs.marks (id, note) VALUES (1, '{org}')")
        ]

    scheduler = background_scheduler()
    for org in _ORGS:
        assert await jobs.register_org_triggers(scheduler, org, None) == 1
    assert {j.id for j in scheduler.get_jobs()} == {f"nightly:org_{org}" for org in _ORGS}

    # Fire org trga's job alone, with nothing bound, as the scheduler fires it.
    job = scheduler.get_job("nightly:org_trga")
    assert job is not None
    unbound = current_org.set(None)
    try:
        await job.func(*job.args)
    finally:
        current_org.reset(unbound)
    assert await _rows(state, "trig_a") == [(1, "trga")]
    assert await _rows(state, "trig_b") == []

    # Then org trgb's: its write lands in its own table, and trga's table is unchanged.
    job = scheduler.get_job("nightly:org_trgb")
    assert job is not None
    unbound = current_org.set(None)
    try:
        await job.func(*job.args)
    finally:
        current_org.reset(unbound)
    assert await _rows(state, "trig_a") == [(1, "trga")]
    assert await _rows(state, "trig_b") == [(1, "trgb")]


async def test_an_orgs_delete_leaves_the_other_orgs_trigger_of_that_id(two_orgs):
    from provisa.api.admin import schema_mutation_ops as ops
    from provisa.core.repositories import scheduled_trigger as trigger_repo

    state = two_orgs
    removed = await _in("trgb", lambda: ops.delete_scheduled_task_op("nightly"))
    assert removed.success is True

    async def _ids():
        async with state.model_db.acquire() as conn:
            return [r["id"] for r in await trigger_repo.list_all(conn)]

    assert await _in("trga", _ids) == ["nightly"]
    assert await _in("trgb", _ids) == []
