# Copyright (c) 2026 Kenneth Stott
# Canary: 4bff9b52-0c96-4f8b-a53e-2aaa918b91f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every model change commits, once, and a path that commits for itself is not committed again
(REQ-1524, REQ-1543).

Real planes and a real repository: a model plane in PostgreSQL whose writes are recorded by the
connection, the platform registry that holds each environment's position, and the org's bare
repository under the test's own directory. The undo and redo are the environments router's own
``_move`` -- its collaborators that reach the app (the caller's rights, the audit trail, the
runtime refresh) are stood in for; the deploy, the cursor and the repository are real.
Lands on the TEST instance's PostgreSQL only.
"""

# Requirements: REQ-1524, REQ-1543

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import MetaData, select

from provisa.api.admin import environments_router as er
from provisa.core import env_repo, model_change
from provisa.core.environments import PROD, org_schema
from provisa.core.model_change import ModelPlane
from provisa.core.schema_admin import environments
from provisa.core.schema_org import domains

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DEV = "dev"
ACTOR = "uid-ada"


@pytest.fixture
async def planes(docker_postgres, tmp_path, monkeypatch):
    """An org with prod and dev schemas, a registry row for dev, its own repository, and dev's
    model plane attached for commits."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.schema_admin import init_registry_schema

    monkeypatch.setenv("PROVISA_REPO_DIR", str(tmp_path / "repos"))
    org_id = f"mc{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+psycopg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    engine = create_engine_from_url(url, pool_size=4)
    admin_db = Database(engine, name="admin")
    prod_db = Database(
        engine, name="org", search_path=org_schema(org_id), model=ModelPlane(org_id, PROD)
    )
    dev_db = Database(
        engine, name="org", search_path=org_schema(org_id, DEV), model=ModelPlane(org_id, DEV)
    )
    await init_registry_schema(admin_db, org_id)
    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    await init_schema(prod_db, schema_sql, org_id=org_id)
    await init_schema(prod_db, schema_sql, org_id=org_id, env=DEV)
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            # REQ-1942: every environment but prod records the one it was created from.
            environments.insert().values(
                org_id=org_id, name=DEV, created_by=ACTOR, parent="prod", data_mode="unbound"
            )
        )
    from provisa.core.env_deploy import PROJECTED

    model_change.attach(admin_db, commit=env_repo.write_through, projected=PROJECTED)
    yield type("Planes", (), {"org_id": org_id, "admin": admin_db, "prod": prod_db, "dev": dev_db})
    async with admin_db.acquire() as conn:
        for env in (PROD, DEV):
            await conn.execute(f"DROP SCHEMA IF EXISTS {org_schema(org_id, env)} CASCADE")
    model_change.detach()
    engine.dispose()


async def _add_domain(db, domain_id: str) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(domains.insert().values(id=domain_id))


async def _position(planes) -> tuple[str | None, str | None]:
    async with planes.admin.acquire() as conn:
        row = (
            await conn.execute_core(
                select(environments.c.deployed_sha, environments.c.redo_sha).where(
                    environments.c.org_id == planes.org_id, environments.c.name == DEV
                )
            )
        ).one()
    return row.deployed_sha, row.redo_sha


def _commits(planes) -> list[dict]:
    return env_repo.history(planes.org_id, DEV)


async def _dev_domains(planes) -> set[str]:
    scoped = domains.to_metadata(MetaData(), schema=org_schema(planes.org_id, DEV))
    async with planes.admin.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(scoped.c.id))).fetchall()}


async def test_a_scoped_change_is_one_commit_by_its_author_and_moves_the_position(planes):
    async with model_change.scope("POST /admin/graphql", actor=lambda: ACTOR):
        model_change.label("addDomains")
        await _add_domain(planes.dev, "sales")
        await _add_domain(planes.dev, "finance")
    commits = _commits(planes)
    assert len(commits) == 1
    assert commits[0]["message"].startswith("addDomains (domains)")
    assert ACTOR in commits[0]["author"]
    assert await _position(planes) == (commits[0]["sha"], None)


async def test_a_scope_that_changed_nothing_writes_no_commit(planes):
    async with model_change.scope("first"):
        await _add_domain(planes.dev, "sales")
    async with model_change.scope("again"):
        async with planes.dev.acquire() as conn:
            # Rewrites the row with what it already holds: recorded, projected, unchanged.
            await conn.execute("UPDATE domains SET description = description WHERE id = 'sales'")
    assert len(_commits(planes)) == 1


@pytest.fixture
def move(planes, monkeypatch):
    """The router's undo/redo with the app-facing collaborators stood in for."""

    async def _guard_within(request, org_id, name):
        return ACTOR

    async def _audit(*_args, **_kwargs):
        return None

    async def _refresh(*_args, **_kwargs):
        return "none"

    async def _org_model_db(org_id):
        return planes.prod

    monkeypatch.setattr(er, "_guard_within", _guard_within)
    monkeypatch.setattr(er, "_audit", _audit)
    monkeypatch.setattr(er, "_refresh", _refresh)
    monkeypatch.setattr(er, "_admin_pool", lambda: planes.admin)
    monkeypatch.setattr("provisa.api.admin.orgs_router._org_model_db", _org_model_db)

    async def _go(forward: bool) -> dict:
        # Under a request's own change scope, as the undo and redo endpoints run.
        async with model_change.scope(f"POST /admin/environments/{DEV}/undo", actor=lambda: ACTOR):
            return await er._move(None, planes.org_id, DEV, forward)

    return _go


async def test_undo_and_redo_move_the_cursor_and_add_no_commit(planes, move):
    async with model_change.scope("first", actor=lambda: ACTOR):
        await _add_domain(planes.dev, "sales")
    async with model_change.scope("second", actor=lambda: ACTOR):
        await _add_domain(planes.dev, "finance")
    second, first = (c["sha"] for c in _commits(planes))
    assert await _position(planes) == (second, None)

    undone = await move(forward=False)
    assert (undone["deployed_sha"], undone["redo_sha"]) == (first, second)
    assert await _position(planes) == (first, second)
    assert [c["sha"] for c in _commits(planes)] == [second, first]
    assert "sales" in await _dev_domains(planes)
    assert "finance" not in await _dev_domains(planes)

    redone = await move(forward=True)
    assert (redone["deployed_sha"], redone["redo_sha"]) == (second, None)
    assert await _position(planes) == (second, None)
    assert [c["sha"] for c in _commits(planes)] == [second, first]
    assert "finance" in await _dev_domains(planes)


@pytest.mark.parametrize(
    "path",
    [
        "provisa.core.env_create.create_environment",
        "provisa.api.admin.environments_router.merge_into_environment",
        "provisa.api.admin.environments_router.decide_merge_request",
        "provisa.api.admin.environments_router.deploy_into_environment",
        "provisa.api.admin.environments_router._move",
        "provisa.api.admin.environments_router.pull_environment",
        "provisa.api.admin.orgs_router._provision_org_task",
        "provisa.api.app._ensure_environment_baselines",
    ],
)
async def test_every_path_that_commits_itself_is_not_committed_again(path):
    import importlib
    import inspect

    module_name, _, attr = path.rpartition(".")
    fn = getattr(importlib.import_module(module_name), attr)
    # The marker wraps the body in committed_by_caller; the wrapped function is kept beside it.
    decorators = inspect.getsource(fn).split("\ndef ")[0].split("async def ")[0]
    assert "@model_change.commits_itself" in decorators
    assert hasattr(fn, "__wrapped__")
