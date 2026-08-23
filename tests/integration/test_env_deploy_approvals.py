# Copyright (c) 2026 Kenneth Stott
# Canary: c4e17b93-6a20-4f8d-91b5-0e3ca7d2f861
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DEPLOY into a protected environment, held open until a second person reads it (REQ-1496).

Three real things, because the request spans all three: the row on the platform registry, the
report produced by planning against a real org schema, and the tree read back out of the org's real
repository at the sha the request pinned. What the pinning is for -- an approval that names one
commit and stays on it while the branch moves -- is only observable with an actual commit history.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from provisa.core import env_approvals as ea
from provisa.core.env_files import dump
from provisa.core.env_project import project
from provisa.core.environments import PROD, org_schema
from provisa.core.schema_admin import environments
from provisa.core.schema_org import domains

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DEV = "dev"
DEVELOPER = "uid-developer"
REVIEWER = "uid-reviewer"


@pytest.fixture
async def planes(docker_postgres, tmp_path, monkeypatch):
    """An org with both schemas, a registry row, and its own bare repository."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.env_repo import commit_files, ensure_repo
    from provisa.core.schema_admin import init_registry_schema

    monkeypatch.setenv("PROVISA_REPO_DIR", str(tmp_path / "repos"))
    org_id = f"envldap{uuid.uuid4().hex[:8]}"
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
            environments.insert().values(org_id=org_id, name=DEV, created_by=DEVELOPER)
        )

    async def add_domain(domain_id, env=DEV):
        from sqlalchemy import MetaData

        scoped = domains.to_metadata(MetaData(), schema=org_schema(org_id, env))
        async with tenant_db.acquire() as conn:
            await conn.execute_core(scoped.insert().values(id=domain_id, description=domain_id))

    async def tree(env=DEV):
        async with tenant_db.acquire() as conn:
            return await project(conn, org_schema(org_id, env))

    async def push(env=DEV, message="build"):
        """Commit what ``env`` currently holds, and hand back the sha and the parsed tree."""
        model = await tree(env)
        sha = commit_files(ensure_repo(org_id), env, dump(model), message, DEVELOPER)
        assert sha is not None  # every push here changes the tree
        return sha, model

    async def rows(env, table=domains):
        from sqlalchemy import MetaData, select

        scoped = table.to_metadata(MetaData(), schema=org_schema(org_id, env))
        async with tenant_db.acquire() as conn:
            result = await conn.execute_core(select(scoped.c.id).order_by(scoped.c.id))
            return [r[0] for r in result.fetchall()]

    yield type(
        "Planes",
        (),
        {
            "org_id": org_id,
            "admin": admin_db,
            "tenant": tenant_db,
            "add_domain": staticmethod(add_domain),
            "tree": staticmethod(tree),
            "push": staticmethod(push),
            "rows": staticmethod(rows),
        },
    )
    await admin_engine.dispose()
    await tenant_engine.dispose()


@pytest.fixture
async def proposed(planes):
    """A build in dev, committed, and proposed for deploying into prod."""
    await planes.add_domain("sales")
    sha, model = await planes.push()
    request = await ea.request_deploy(
        planes.admin,
        planes.tenant,
        planes.org_id,
        ref=DEV,
        sha=sha,
        tree=model,
        target_env=PROD,
        requested_by=DEVELOPER,
        message="release 4",
    )
    return request


class TestTheRequestNamesACommit:
    async def test_it_pins_the_sha_beside_the_ref_it_was_asked_for(self, proposed):
        assert proposed["source_ref"] == DEV
        assert len(proposed["source_sha"]) == 40
        # A deploy has no source ENVIRONMENT: what it applies is a tree, which may have come from a
        # branch no environment is currently writing.
        assert proposed["source_env"] is None

    async def test_it_carries_the_report_of_that_tree(self, proposed):
        assert "sales/domain.yaml" in proposed["report"]["added"]
        assert proposed["report"]["ref"] == proposed["source_sha"]

    async def test_requesting_changes_nothing_in_the_target(self, planes, proposed):
        assert "sales" not in await planes.rows(None)

    async def test_it_reads_back_by_id_and_in_the_list(self, planes, proposed):
        fetched = await ea.get_request(planes.admin, planes.org_id, proposed["id"])
        assert fetched is not None  # the id came from the row this fixture just wrote
        assert fetched["source_sha"] == proposed["source_sha"]
        assert [r["id"] for r in await ea.list_requests(planes.admin, planes.org_id)] == [
            proposed["id"]
        ]


class TestDecidingALoad:
    async def test_approval_applies_the_tree_that_was_read(self, planes, proposed):
        decided = await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=True,
            decided_by=REVIEWER,
        )
        assert decided["state"] == "applied"
        assert "sales" in await planes.rows(None)

    async def test_the_requester_cannot_approve_their_own_load(self, planes, proposed):
        with pytest.raises(ea.MergeRequestError, match="someone other than"):
            await ea.decide(
                planes.admin,
                planes.tenant,
                planes.org_id,
                proposed["id"],
                approve=True,
                decided_by=DEVELOPER,
            )
        assert "sales" not in await planes.rows(None)

    async def test_rejection_applies_nothing(self, planes, proposed):
        decided = await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=False,
            decided_by=REVIEWER,
            note="not this quarter",
        )
        assert decided["state"] == "rejected"
        assert "sales" not in await planes.rows(None)


class TestTheBranchMayMoveAndTheRequestMayNot:
    async def test_a_commit_after_the_request_does_not_change_what_would_be_applied(
        self, planes, proposed
    ):
        # The whole point of pinning: an approver read one tree, and the developer pushing again is
        # not a way to get the second one applied under the first one's approval.
        await planes.add_domain("finance")
        await planes.push(message="a second build")
        assert await ea.effective_state(planes.tenant, planes.org_id, proposed) == "requested"
        await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=True,
            decided_by=REVIEWER,
        )
        loaded = await planes.rows(None)
        assert "sales" in loaded and "finance" not in loaded


class TestStaleness:
    async def test_the_target_moving_on_makes_the_request_stale(self, planes, proposed):
        # A deploy is a REPLACE of what the tree addresses, so what prod already holds decides what
        # the deploy would do -- the report an approver read no longer describes it.
        await planes.add_domain("billing", env=None)
        assert await ea.effective_state(planes.tenant, planes.org_id, proposed) == "stale"

    async def test_a_stale_load_is_not_approvable(self, planes, proposed):
        await planes.add_domain("billing", env=None)
        with pytest.raises(ea.MergeRequestError, match="no longer describes"):
            await ea.decide(
                planes.admin,
                planes.tenant,
                planes.org_id,
                proposed["id"],
                approve=True,
                decided_by=REVIEWER,
            )

    async def test_a_stale_load_can_still_be_rejected(self, planes, proposed):
        await planes.add_domain("billing", env=None)
        decided = await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=False,
            decided_by=REVIEWER,
        )
        assert decided["state"] == "rejected"

    async def test_an_applied_load_never_goes_stale(self, planes, proposed):
        applied = await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=True,
            decided_by=REVIEWER,
        )
        await planes.add_domain("billing", env=None)
        assert await ea.effective_state(planes.tenant, planes.org_id, applied) == "applied"
