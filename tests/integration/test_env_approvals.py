# Copyright (c) 2026 Kenneth Stott
# Canary: 91a9ac24-b9c3-45c9-b2f1-918bb7eb4bc8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A merge held open until a second person reads it (REQ-1504), against two real planes.

Both planes are real because the request spans them: the row lives on the platform registry beside
the environments it names, and the report it carries is produced by planning a copy between two org
schemas on the tenant plane. A double for either would make the interesting cases -- the report
stored as it was produced, and the staleness that follows from the source moving on afterwards --
unobservable.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from provisa.core import env_approvals as ea
from provisa.core.environments import PROD, org_schema
from provisa.core.schema_admin import environments
from provisa.core.schema_org import domains

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DEV = "dev"
DEVELOPER = "uid-developer"
REVIEWER = "uid-reviewer"


@pytest.fixture
async def planes(docker_postgres):
    """An org with a registry row, a prod schema and a dev schema its developer owns."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.schema_admin import init_registry_schema

    org_id = f"envappr{uuid.uuid4().hex[:8]}"
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
    # prod is already there: init_registry_schema writes it with the org (REQ-1487), and it has no
    # creator, which is what makes prod's rights the org's own rather than one member's.
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            environments.insert().values(org_id=org_id, name=DEV, created_by=DEVELOPER)
        )

    async def add_domain(domain_id, env=DEV):
        from sqlalchemy import MetaData

        scoped = domains.to_metadata(MetaData(), schema=org_schema(org_id, env))
        async with tenant_db.acquire() as conn:
            await conn.execute_core(scoped.insert().values(id=domain_id, description=domain_id))

    yield type(
        "Planes",
        (),
        {
            "org_id": org_id,
            "admin": admin_db,
            "tenant": tenant_db,
            "add_domain": staticmethod(add_domain),
        },
    )
    await admin_engine.dispose()
    await tenant_engine.dispose()


@pytest.fixture
async def proposed(planes):
    """The developer has built something in dev and proposed it to prod."""
    await planes.add_domain("sales")
    return await ea.request_merge(
        planes.admin,
        planes.tenant,
        planes.org_id,
        source_env=DEV,
        target_env=PROD,
        requested_by=DEVELOPER,
        message="adds the sales domain",
    )


class TestProtection:
    """REQ-1504: what waits for an approval, and what the rule would break if it applied."""

    async def test_prod_waits_once_there_is_somebody_else_to_ask(self, planes):
        assert await ea.is_protected(planes.admin, planes.org_id, PROD, member_count=2)

    async def test_a_single_member_org_is_not_locked_out_of_its_own_prod(self, planes):
        # Nobody exists who could approve, so applying the rule would not protect prod — it would
        # make prod unmergeable.
        assert not await ea.is_protected(planes.admin, planes.org_id, PROD, member_count=1)

    async def test_any_environment_waits_once_an_org_admin_says_so(self, planes):
        from provisa.core.env_store import set_protected

        assert not await ea.is_protected(planes.admin, planes.org_id, DEV, member_count=5)
        await set_protected(planes.admin, planes.org_id, DEV, True)
        assert await ea.is_protected(planes.admin, planes.org_id, DEV, member_count=5)

    async def test_asking_about_an_environment_that_does_not_exist_is_refused(self, planes):
        with pytest.raises(ea.MergeRequestError, match="no environment"):
            await ea.is_protected(planes.admin, planes.org_id, "ghost", member_count=2)


class TestRequesting:
    async def test_the_request_stores_the_report_as_it_was_produced(self, proposed):
        assert proposed["state"] == ea.REQUESTED
        assert proposed["requested_by"] == DEVELOPER
        assert proposed["message"] == "adds the sales domain"
        assert "sales" in proposed["report"]["tables"][0]["added"]

    async def test_the_request_is_readable_by_id_and_in_the_list(self, planes, proposed):
        assert await ea.get_request(planes.admin, planes.org_id, proposed["id"]) == proposed
        listed = await ea.list_requests(planes.admin, planes.org_id, open_only=True)
        assert [r["id"] for r in listed] == [proposed["id"]]

    async def test_proposing_an_environment_to_itself_is_refused(self, planes):
        with pytest.raises(ea.MergeRequestError, match="onto itself|into itself"):
            await ea.request_merge(
                planes.admin,
                planes.tenant,
                planes.org_id,
                source_env=DEV,
                target_env=DEV,
                requested_by=DEVELOPER,
            )

    async def test_requesting_changes_nothing_in_the_target(self, planes, proposed):
        # Proposing is not writing: prod does not have the domain until somebody approves.
        plan = await ea.plan_copy(
            planes.tenant, planes.org_id, DEV, PROD, mode=ea.MERGE, removals=False
        )
        assert plan.as_dict() == proposed["report"]


class TestDeciding:
    async def test_the_requester_cannot_approve_their_own_request(self, planes, proposed):
        with pytest.raises(ea.MergeRequestError, match="someone other than"):
            await ea.decide(
                planes.admin,
                planes.tenant,
                planes.org_id,
                proposed["id"],
                approve=True,
                decided_by=DEVELOPER,
            )

    async def test_approval_applies_exactly_what_was_reviewed(self, planes, proposed):
        decided = await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=True,
            decided_by=REVIEWER,
        )
        assert decided["state"] == ea.APPLIED
        assert decided["decided_by"] == REVIEWER
        assert decided["applied_at"] is not None
        landed = await ea.plan_copy(
            planes.tenant, planes.org_id, DEV, PROD, mode=ea.MERGE, removals=False
        )
        assert landed.as_dict()["added"] == 0  # nothing left to carry: the merge happened

    async def test_rejection_is_recorded_with_its_reason_and_applies_nothing(
        self, planes, proposed
    ):
        decided = await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=False,
            decided_by=REVIEWER,
            note="name the domain sales_us",
        )
        assert decided["state"] == ea.REJECTED
        assert decided["decision_note"] == "name the domain sales_us"
        still = await ea.plan_copy(
            planes.tenant, planes.org_id, DEV, PROD, mode=ea.MERGE, removals=False
        )
        assert still.as_dict() == proposed["report"]  # prod is untouched

    async def test_a_decided_request_cannot_be_decided_again(self, planes, proposed):
        await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=False,
            decided_by=REVIEWER,
        )
        with pytest.raises(ea.MergeRequestError, match="only a requested merge"):
            await ea.decide(
                planes.admin,
                planes.tenant,
                planes.org_id,
                proposed["id"],
                approve=True,
                decided_by=REVIEWER,
            )

    async def test_deciding_a_request_the_org_does_not_have_is_refused(self, planes):
        with pytest.raises(ea.MergeRequestError, match="no merge request"):
            await ea.decide(
                planes.admin,
                planes.tenant,
                planes.org_id,
                999999,
                approve=True,
                decided_by=REVIEWER,
            )


class TestStaleness:
    """The state nobody writes: the source moved on after the report was produced."""

    async def test_a_waiting_request_is_current_until_its_source_changes(self, planes, proposed):
        assert await ea.effective_state(planes.tenant, planes.org_id, proposed) == ea.REQUESTED

    async def test_the_source_moving_on_makes_the_request_stale(self, planes, proposed):
        await planes.add_domain("marketing")
        assert await ea.effective_state(planes.tenant, planes.org_id, proposed) == ea.STALE

    async def test_a_stale_request_is_not_approvable(self, planes, proposed):
        await planes.add_domain("marketing")
        with pytest.raises(ea.MergeRequestError, match="no longer describes"):
            await ea.decide(
                planes.admin,
                planes.tenant,
                planes.org_id,
                proposed["id"],
                approve=True,
                decided_by=REVIEWER,
            )

    async def test_a_stale_request_can_still_be_rejected(self, planes, proposed):
        # Rejection does not depend on the report being current: the reviewer is refusing the
        # proposal, not applying it.
        await planes.add_domain("marketing")
        decided = await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=False,
            decided_by=REVIEWER,
        )
        assert decided["state"] == ea.REJECTED

    async def test_a_decided_request_never_goes_stale(self, planes, proposed):
        decided = await ea.decide(
            planes.admin,
            planes.tenant,
            planes.org_id,
            proposed["id"],
            approve=True,
            decided_by=REVIEWER,
        )
        await planes.add_domain("marketing")
        assert await ea.effective_state(planes.tenant, planes.org_id, decided) == ea.APPLIED
