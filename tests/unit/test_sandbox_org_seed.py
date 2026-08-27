# Copyright (c) 2026 Kenneth Stott
# Canary: 4d81c6f2-9a05-47be-b3ce-0e7f21a9d648
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1598: the sandbox org is seeded by startup, not by an operator.

The public "Try it Out" invite names an org, so the org has to be there before the first redemption
is. These tests pin the three things that makes true: the boot builds it when it is absent, the boot
leaves a built one alone, and the boot does not stop at the checkout gate a sold org waits behind.
"""

# Requirements: REQ-1598

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool

from provisa.api import sandbox_org as sandbox_mod
from provisa.api.sandbox_org import SANDBOX_ORG_ID, ensure_sandbox_org
from provisa.core.database import Database
from provisa.core.schema_admin import AWAITING_CHECKOUT, metadata, orgs


@pytest.fixture
async def admin_db():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield Database(engine, "test")
    finally:
        await engine.dispose()


@pytest.fixture
def spawned(monkeypatch):
    """Capture the provisioning the seed spawns; the build itself is the create path's, tested there."""
    calls: list[tuple] = []
    entitled: list[str] = []

    monkeypatch.setattr(
        "provisa.api.admin.orgs_router._spawn_provisioning",
        lambda *args: calls.append(args),
    )

    async def _entitle(_pool, org_id):
        entitled.append(org_id)

    monkeypatch.setattr("provisa.core.commerce.entitle_starter", _entitle)
    return SimpleNamespace(calls=calls, entitled=entitled)


async def _row(pool):
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(
                orgs.c.name, orgs.c.provisioning_state, orgs.c.seeded_demo, orgs.c.created_by
            ).where(orgs.c.id == SANDBOX_ORG_ID)
        )
        return result.fetchone()


class TestSeedsTheOrg:
    async def test_an_absent_sandbox_org_is_created_and_built(self, admin_db, spawned):
        assert await ensure_sandbox_org(admin_db) == "building"
        row = await _row(admin_db)
        assert row is not None
        assert row.provisioning_state == "provisioning"
        # Demo-seeded, because an empty sandbox is nothing to try; ownerless, because its members
        # arrive by redeeming the open invite rather than by creating it.
        assert bool(row.seeded_demo) is True
        assert row.created_by is None
        assert spawned.calls == [(SANDBOX_ORG_ID, True, None, False)]

    async def test_it_is_put_on_starter_before_it_is_built(self, admin_db, spawned):
        await ensure_sandbox_org(admin_db)
        assert spawned.entitled == [SANDBOX_ORG_ID]

    async def test_a_built_sandbox_org_is_left_alone(self, admin_db, spawned):
        await ensure_sandbox_org(admin_db)
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                orgs.update().where(orgs.c.id == SANDBOX_ORG_ID).values(provisioning_state="ready")
            )
        spawned.calls.clear()
        spawned.entitled.clear()

        assert await ensure_sandbox_org(admin_db) == "ready"
        assert spawned.calls == []
        assert spawned.entitled == []

    @pytest.mark.parametrize("state", [AWAITING_CHECKOUT, "failed", "provisioning"])
    async def test_any_unfinished_state_is_rebuilt(self, admin_db, spawned, state):
        # awaiting_checkout is the gate a SOLD org waits behind (REQ-1476) and nobody buys the
        # sandbox; failed and provisioning are a build that died. All three are an org the platform
        # still owes its visitors, so the next boot takes them back to provisioning.
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                orgs.insert().values(
                    id=SANDBOX_ORG_ID,
                    name="Sandbox",
                    provisioning_state=state,
                    provisioning_error="whatever went wrong",
                )
            )

        assert await ensure_sandbox_org(admin_db) == "building"
        row = await _row(admin_db)
        assert row is not None and row.provisioning_state == "provisioning"
        async with admin_db.acquire() as conn:
            result = await conn.execute_core(
                select(orgs.c.provisioning_error).where(orgs.c.id == SANDBOX_ORG_ID)
            )
            assert result.scalar() is None
        assert spawned.calls == [(SANDBOX_ORG_ID, True, None, False)]


class TestStartupHook:
    async def test_a_self_hosted_deployment_seeds_no_sandbox(self, monkeypatch):
        # The switch is the commerce seam: the sandbox exists to show what the platform sells.
        import logging

        from provisa.api.app_startup import _seed_sandbox_org

        monkeypatch.setattr("provisa.core.commerce.enabled", lambda: False)
        called: list[int] = []
        monkeypatch.setattr(sandbox_mod, "ensure_sandbox_org", lambda pool: called.append(1))
        await _seed_sandbox_org(logging.getLogger("test"))
        assert called == []

    async def test_a_commercial_deployment_with_no_registry_fails_loud(self, monkeypatch):
        import logging

        from provisa.api.app_startup import _seed_sandbox_org

        monkeypatch.setattr("provisa.core.commerce.enabled", lambda: True)
        monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(admin_db=None), raising=False)
        with pytest.raises(RuntimeError, match="REQ-1598"):
            await _seed_sandbox_org(logging.getLogger("test"))
