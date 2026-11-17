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

REQ-1599 adds who may use it. platform_admin confers nothing inside a tenant org, so the
deployment's administrators are seated in the sandbox the ordinary way -- a membership and a role
granted in its own schema.
"""

# Requirements: REQ-1598, REQ-1599

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from provisa.api import sandbox_org as sandbox_mod
from provisa.api.sandbox_org import SANDBOX_ORG_ID, ensure_sandbox_org
from provisa.core.schema_admin import AWAITING_CHECKOUT, orgs
from tests.pg_control_plane import SCHEMA_SQL


@pytest.fixture
def admin_db(control_plane):
    return control_plane.platform


@pytest.fixture
def spawned(monkeypatch):
    """Capture the provisioning the seed spawns; the build itself is the create path's, tested there."""
    calls: list[tuple] = []
    entitled: list[str] = []

    async def _provision(*args):
        calls.append(args)

    monkeypatch.setattr("provisa.api.admin.orgs_router._provision_org_task", _provision)

    seated: list[int] = []

    async def _seat(_pool):
        seated.append(1)
        return 0

    monkeypatch.setattr(sandbox_mod, "seat_platform_admins", _seat)

    async def _entitle(_pool, org_id):
        entitled.append(org_id)

    monkeypatch.setattr("provisa.core.commerce.entitle_starter", _entitle)
    return SimpleNamespace(calls=calls, entitled=entitled, seated=seated)


async def _drain():
    """Await the background build the seed spawns, so what it did is there to assert on."""
    # REQ-1882: the build runs on a background worker; its future is awaited from this loop.
    while sandbox_mod._build_tasks:
        await asyncio.gather(*(asyncio.wrap_future(f) for f in list(sandbox_mod._build_tasks)))


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
        await _drain()
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
        await _drain()
        assert spawned.entitled == [SANDBOX_ORG_ID]

    async def test_a_built_sandbox_org_is_left_alone(self, admin_db, spawned):
        await ensure_sandbox_org(admin_db)
        await _drain()
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                orgs.update().where(orgs.c.id == SANDBOX_ORG_ID).values(provisioning_state="ready")
            )
        spawned.calls.clear()
        spawned.entitled.clear()
        spawned.seated.clear()

        assert await ensure_sandbox_org(admin_db) == "ready"
        assert spawned.calls == []
        assert spawned.entitled == []
        # REQ-1599: nothing to build, but the boot still reconciles who may use it -- an
        # administrator conferred before this org existed is seated on this pass.
        assert spawned.seated == [1]

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
        await _drain()
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
        with pytest.raises(RuntimeError, match="The sandbox org needs the control-plane registry"):
            await _seed_sandbox_org(logging.getLogger("test"))


@pytest.fixture
async def tenant_dbs(monkeypatch, control_plane):
    """The root org's schema and the sandbox org's, as two separate planes.

    Two engines rather than one: the platform_admin assignments the seating READS live in the root
    org's schema, and the org_admin assignment it WRITES lands in the sandbox's. Sharing one would
    let the read see what the write put there.
    """
    from provisa.core.db import init_schema

    root = control_plane.org()
    sandbox = control_plane.org(SANDBOX_ORG_ID)
    await init_schema(sandbox, SCHEMA_SQL, org_id=SANDBOX_ORG_ID)

    monkeypatch.setattr(
        "provisa.api.app.state",
        # The deployment org's store, as the app names it for a request bound to no org (#187).
        SimpleNamespace(platform_model_db=root, tenant_db=root, admin_db=None),
        raising=False,
    )

    async def _ensure(org_id):
        assert org_id == SANDBOX_ORG_ID
        return SimpleNamespace(model_db=sandbox, tenant_db=sandbox)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _ensure, raising=False)
    return SimpleNamespace(root=root, sandbox=sandbox)


async def _assign(db, user_id, role_id):
    from provisa.core.schema_org import user_role_assignments

    async with db.acquire() as conn:
        await conn.execute_core(
            user_role_assignments.insert().values(user_id=user_id, role_id=role_id, domain_id="*")
        )


async def _sandbox_roles(db, user_id):
    from provisa.core.schema_org import user_role_assignments

    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(user_role_assignments.c.role_id).where(
                user_role_assignments.c.user_id == user_id
            )
        )
        return sorted(r[0] for r in result.fetchall())


async def _ready(pool):
    async with pool.acquire() as conn:
        await conn.execute_core(
            orgs.insert().values(
                id=SANDBOX_ORG_ID, name="Sandbox", provisioning_state="ready", seeded_demo=True
            )
        )


class TestSeatingTheDeploymentsAdministrators:
    """REQ-1599: platform_admin confers nothing inside a tenant org, so an operator is seated in
    the sandbox the ordinary way -- a membership and a role granted in its own schema."""

    async def test_every_platform_admin_gets_a_membership_and_org_admin(self, admin_db, tenant_dbs):
        from provisa.core.schema_admin import user_org_memberships

        await _ready(admin_db)
        await _assign(tenant_dbs.root, "alice", "platform_admin")
        await _assign(tenant_dbs.root, "bob", "platform_admin")
        await _assign(tenant_dbs.root, "carol", "org_admin")

        assert await sandbox_mod.seat_platform_admins(admin_db) == 2

        async with admin_db.acquire() as conn:
            result = await conn.execute_core(
                select(user_org_memberships.c.user_id, user_org_memberships.c.env_name).where(
                    user_org_memberships.c.org_id == SANDBOX_ORG_ID
                )
            )
            rows = sorted(result.fetchall())
        # Unpinned: the env pin (REQ-1596) is what confines a visitor to the environment minted for
        # them, and an operator is not a visitor.
        assert rows == [("alice", None), ("bob", None)]
        # org_admin, not `sandbox` -- an operator has to reach the environments, members and
        # settings a visitor's role deliberately withholds (REQ-1597).
        assert await _sandbox_roles(tenant_dbs.sandbox, "alice") == ["org_admin"]
        assert await _sandbox_roles(tenant_dbs.sandbox, "carol") == []

    async def test_seating_twice_seats_once(self, admin_db, tenant_dbs):
        await _ready(admin_db)
        await _assign(tenant_dbs.root, "alice", "platform_admin")
        assert await sandbox_mod.seat_platform_admins(admin_db) == 1
        assert await sandbox_mod.seat_platform_admins(admin_db) == 1
        assert await _sandbox_roles(tenant_dbs.sandbox, "alice") == ["org_admin"]

    async def test_an_unbuilt_sandbox_seats_nobody(self, admin_db, tenant_dbs):
        # Nothing to grant a role in yet; the build itself seats them the moment its schema exists.
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                orgs.insert().values(
                    id=SANDBOX_ORG_ID, name="Sandbox", provisioning_state="provisioning"
                )
            )
        await _assign(tenant_dbs.root, "alice", "platform_admin")
        assert await sandbox_mod.seat_platform_admins(admin_db) == 0

    async def test_a_deployment_with_no_sandbox_org_seats_nobody(self, admin_db, tenant_dbs):
        await _assign(tenant_dbs.root, "alice", "platform_admin")
        assert await sandbox_mod.seat_platform_admins(admin_db) == 0

    async def test_a_conferral_reconciles_from_the_assignments_not_the_role_it_granted(
        self, admin_db, tenant_dbs
    ):
        # The hook takes no role id: asking which role was just granted would be a gate on a role
        # name (REQ-1337), and the answer is already in the assignments the seating reads.
        await _ready(admin_db)
        await sandbox_mod.reseat_after_conferral(admin_db)
        assert await _sandbox_roles(tenant_dbs.sandbox, "alice") == []
        await _assign(tenant_dbs.root, "alice", "platform_admin")
        await sandbox_mod.reseat_after_conferral(admin_db)
        assert await _sandbox_roles(tenant_dbs.sandbox, "alice") == ["org_admin"]


async def _public_invite(pool):
    from provisa.core.schema_admin import org_invites

    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(org_invites).where(
                org_invites.c.token == sandbox_mod.SANDBOX_PUBLIC_INVITE_TOKEN
            )
        )
        row = result.fetchone()
    return dict(row._mapping) if row is not None else None


class TestThePublicInvite:
    """The public sandbox invite is part of the deployment's configuration, like the org it names.

    The marketing site links one fixed token. On 2026-11-16 a deploy wiped ``org_invites`` and the
    link answered "Invite not found" until somebody minted a new one by hand -- with a new token the
    site did not carry. So startup seeds it with the site's token on every boot, exactly as it seeds
    the org: a reset that deletes it gets it back on the restart that follows.
    """

    def test_the_token_is_the_one_the_site_links(self):
        from pathlib import Path

        site = Path(__file__).resolve().parents[2] / "site" / "index.html"
        assert (
            f"sandbox.provisa.dev/register?invite={sandbox_mod.SANDBOX_PUBLIC_INVITE_TOKEN}"
            in site.read_text()
        )

    async def test_a_first_boot_seeds_it(self, admin_db, spawned):
        from provisa.core.environments import PROD
        from provisa.core.org_invite import ENV_POLICY_PER_VISITOR, SANDBOX_ROLE

        await ensure_sandbox_org(admin_db)
        await _drain()
        invite = await _public_invite(admin_db)
        assert invite is not None
        assert invite["org_id"] == SANDBOX_ORG_ID
        # The visitor's role, conferrable only by a per-visitor invitation (REQ-1595, REQ-2155):
        # each redeemer gets a fresh environment deployed from prod, reaped after a day of disuse.
        assert invite["role_id"] == SANDBOX_ROLE
        assert invite["env_policy"] == ENV_POLICY_PER_VISITOR
        assert invite["env_name"] == PROD
        assert invite["env_ttl_seconds"] == sandbox_mod.SANDBOX_INVITE_IDLE_TTL_SECONDS
        # Open: addressed to nobody, redeemed by everybody (REQ-1594).
        assert invite["max_uses"] is None
        assert invite["email"] is None

    async def test_a_ready_org_still_has_it_seeded(self, admin_db, spawned):
        await _ready(admin_db)
        assert await ensure_sandbox_org(admin_db) == "ready"
        assert await _public_invite(admin_db) is not None

    async def test_a_reset_that_deleted_it_gets_it_back_on_the_next_boot(self, admin_db, spawned):
        # What scripts/deploy-cloud.sh reset_state does to org_invites, then the restart.
        from provisa.core.schema_admin import org_invites

        await _ready(admin_db)
        await ensure_sandbox_org(admin_db)
        async with admin_db.acquire() as conn:
            await conn.execute_core(org_invites.delete().where(org_invites.c.org_id.isnot(None)))
        assert await _public_invite(admin_db) is None

        await ensure_sandbox_org(admin_db)
        assert await _public_invite(admin_db) is not None

    async def test_a_boot_restores_its_terms_and_keeps_its_count(self, admin_db, spawned):
        # Its terms are configuration, so an edit or an expiry does not survive a boot; how many
        # people redeemed it is history, and is kept.
        import datetime

        from provisa.core.schema_admin import org_invites

        await _ready(admin_db)
        await ensure_sandbox_org(admin_db)
        past = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
        async with admin_db.acquire() as conn:
            await conn.execute_core(
                org_invites.update()
                .where(org_invites.c.token == sandbox_mod.SANDBOX_PUBLIC_INVITE_TOKEN)
                .values(expires_at=past, max_uses=3, uses=3, role_id="analyst")
            )
        await ensure_sandbox_org(admin_db)
        invite = await _public_invite(admin_db)
        assert invite is not None
        assert invite["max_uses"] is None
        assert invite["role_id"] == "sandbox"
        assert invite["expires_at"] > datetime.datetime.now(tz=datetime.timezone.utc)
        assert invite["uses"] == 3

    async def test_a_visitor_is_admitted_by_it_and_so_is_the_next(
        self, admin_db, spawned, monkeypatch
    ):
        # The preview the sign-in page reads, then the redemption's own read and claim -- twice,
        # because an open link is not burnt by its first visitor.
        import datetime

        from provisa.api.auth_router import get_invite
        from provisa.core.org_invite import INVITE_REDEMPTION_COLUMNS, is_spent, spend
        from provisa.core.schema_admin import org_invites

        await _ready(admin_db)
        await ensure_sandbox_org(admin_db)
        token = sandbox_mod.SANDBOX_PUBLIC_INVITE_TOKEN
        monkeypatch.setattr("provisa.api.app.state.admin_db", admin_db, raising=False)
        info = await get_invite(token)
        assert info["valid"] is True and info["org_id"] == SANDBOX_ORG_ID

        for visitor in ("visitor-1", "visitor-2"):
            async with admin_db.acquire() as conn:
                result = await conn.execute_core(
                    select(*INVITE_REDEMPTION_COLUMNS).where(org_invites.c.token == token)
                )
                invite = dict(result.fetchone()._mapping)
                assert not is_spent(invite)
                assert invite["expires_at"] > datetime.datetime.now(tz=datetime.timezone.utc)
                # REQ-1308 refuses a user redeeming an invite they issued; the deployment issued
                # this one, and no account is the deployment.
                assert invite["created_by"] != visitor
                assert (await conn.execute_core(spend(token, visitor))).fetchone() is not None
