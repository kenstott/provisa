# Copyright (c) 2026 Kenneth Stott
# Canary: 5c8e1a37-04bd-49f2-a6e1-93f70d5b28c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A model change reaching the environment's branch, and the drift a failure leaves (REQ-1524).

Both planes are real because the behaviour spans them: the commit is produced by projecting a real
org schema, and the failure it must survive is recorded on the platform registry's ``drifted``
column. A double for the tenant plane could not produce a tree to commit, and a double for the
admin plane would make the drift flag -- the whole point of a projection that never fails a change
-- unobservable.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import MetaData, select

from provisa.core import env_repo
from provisa.core.environments import PROD, org_schema
from provisa.core.schema_admin import environments
from provisa.core.schema_org import domains

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DEV = "dev"
ACTOR = "uid-ada"


@pytest.fixture
def repos(tmp_path, monkeypatch):
    """The org repositories land under the test's own directory, never the maintainer's."""
    monkeypatch.setenv("PROVISA_REPO_DIR", str(tmp_path / "repos"))
    return tmp_path / "repos"


@pytest.fixture
async def planes(docker_postgres, repos):
    """An org with a registry row, a prod schema, a dev schema, and its own bare repository."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.schema_admin import init_registry_schema

    org_id = f"envrepo{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    admin_engine = create_engine_from_url(url, pool_size=2)
    tenant_engine = create_engine_from_url(url, pool_size=2)
    admin_db = Database(admin_engine, name="admin")
    tenant_db = Database(tenant_engine, name="org", search_path=org_schema(org_id))

    await init_registry_schema(admin_db, org_id)
    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    await init_schema(tenant_db, schema_sql, org_id=org_id)
    await init_schema(tenant_db, schema_sql, org_id=org_id, env=DEV)
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            environments.insert().values(org_id=org_id, name=DEV, created_by=ACTOR)
        )

    async def add_domain(domain_id, env=PROD):
        scoped = domains.to_metadata(MetaData(), schema=org_schema(org_id, env))
        async with tenant_db.acquire() as conn:
            await conn.execute_core(scoped.insert().values(id=domain_id, description=domain_id))

    async def commit(env=PROD, message="change", actor=ACTOR):
        async with tenant_db.acquire() as conn:
            return await env_repo.write_through(
                conn, admin_db, org_id, env, org_schema(org_id, env), message, actor
            )

    async def drifted(env=PROD):
        async with admin_db.acquire() as conn:
            result = await conn.execute_core(
                select(environments.c.drifted).where(
                    environments.c.org_id == org_id, environments.c.name == env
                )
            )
            row = result.fetchone()
        assert row is not None
        return row[0]

    yield type(
        "Planes",
        (),
        {
            "org_id": org_id,
            "admin": admin_db,
            "tenant": tenant_db,
            "add_domain": staticmethod(add_domain),
            "commit": staticmethod(commit),
            "drifted": staticmethod(drifted),
        },
    )
    await admin_engine.dispose()
    await tenant_engine.dispose()


class TestTheChangeReachesTheBranch:
    async def test_the_model_is_readable_from_the_commit_it_produced(self, planes):
        await planes.add_domain("sales")
        sha = await planes.commit(message="adds the sales domain")
        assert sha is not None
        files = env_repo.files_at(planes.org_id, PROD)
        assert "sales/domain.yaml" in files, sorted(files)

    async def test_each_environment_writes_its_own_branch(self, planes):
        await planes.add_domain("sales", env=PROD)
        await planes.add_domain("marketing", env=DEV)
        await planes.commit(env=PROD)
        await planes.commit(env=DEV)
        assert env_repo.branches(planes.org_id) == [DEV, PROD]
        assert "sales/domain.yaml" in env_repo.files_at(planes.org_id, PROD)
        assert "marketing/domain.yaml" in env_repo.files_at(planes.org_id, DEV)
        assert "marketing/domain.yaml" not in env_repo.files_at(planes.org_id, PROD)

    async def test_a_second_change_is_a_second_commit_by_its_author(self, planes):
        await planes.add_domain("sales")
        await planes.commit(message="adds sales")
        await planes.add_domain("finance")
        await planes.commit(message="adds finance", actor="uid-grace")
        history = env_repo.history(planes.org_id, PROD)
        assert [h["message"] for h in history] == ["adds finance", "adds sales"]
        assert history[0]["author"].startswith("uid-grace")

    async def test_a_mutation_that_changed_nothing_writes_no_commit(self, planes):
        await planes.add_domain("sales")
        first = await planes.commit()
        assert await planes.commit() is None
        assert [h["sha"] for h in env_repo.history(planes.org_id, PROD)] == [first]


class TestAFailedProjectionNeverFailsTheChange:
    async def test_a_schema_that_cannot_be_read_marks_the_environment_drifted(self, planes):
        await planes.add_domain("sales")
        async with planes.tenant.acquire() as conn:
            sha = await env_repo.write_through(
                conn, planes.admin, planes.org_id, PROD, "org_does_not_exist", "change", ACTOR
            )
        assert sha is None
        assert await planes.drifted() is True

    async def test_rebuilding_commits_the_whole_model_and_clears_the_drift(self, planes):
        await planes.add_domain("sales")
        async with planes.tenant.acquire() as conn:
            await env_repo.write_through(
                conn, planes.admin, planes.org_id, PROD, "org_does_not_exist", "change", ACTOR
            )
        assert await planes.drifted() is True
        await planes.add_domain("finance")
        async with planes.tenant.acquire() as conn:
            sha = await env_repo.rebuild(
                conn, planes.admin, planes.org_id, PROD, org_schema(planes.org_id), ACTOR
            )
        assert sha is not None
        assert await planes.drifted() is False
        files = env_repo.files_at(planes.org_id, PROD)
        assert "sales/domain.yaml" in files and "finance/domain.yaml" in files

    async def test_a_landed_commit_clears_a_drift_that_preceded_it(self, planes):
        async with planes.tenant.acquire() as conn:
            await env_repo.write_through(
                conn, planes.admin, planes.org_id, PROD, "org_does_not_exist", "change", ACTOR
            )
        assert await planes.drifted() is True
        await planes.add_domain("sales")
        assert await planes.commit() is not None
        assert await planes.drifted() is False
