# Copyright (c) 2026 Kenneth Stott
# Canary: 3d5a91c4-0b62-4f18-8a77-c6e4b1d90f23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1594/REQ-1595/REQ-1596: an invitation admits a bounded number of people, hands each one an
environment or none, and confines the ones it seats in a sandbox to it.

The ceiling, the policy and the pin are asserted as three separate things because they are three
separate decisions an inviter makes -- an invitation may be open and hand out nothing, or single-use
and mint a sandbox -- and a test that only exercised the "Try it Out" link would let the ordinary
addressed invitation break unnoticed.
"""

# Requirements: REQ-1594, REQ-1595, REQ-1596, REQ-1615

from __future__ import annotations

import types
from datetime import datetime, timezone

import pytest

from provisa.core.org_invite import (
    ENV_POLICY_NONE,
    ENV_POLICY_PER_VISITOR,
    ENV_POLICY_SHARED,
    SANDBOX_ENV_PREFIX,
    SANDBOX_ROLE,
    is_spent,
    sandbox_env_name,
    spend,
    unspent,
)
from provisa.api.invite_env import RedeemedEnv
from provisa.api.sandbox_org import SANDBOX_ORG_ID
from provisa.core.org_membership import JOINED_VIA_INVITE, membership_values


def _invite(**overrides) -> dict:
    row = {
        "org_id": "acme",
        "role_id": "analyst",
        "uses": 0,
        "max_uses": 1,
        "env_policy": ENV_POLICY_NONE,
        "env_ttl_seconds": None,
        "env_name": None,
    }
    row.update(overrides)
    return row


async def _no_such_env(db, org_id, name):
    """REQ-1615: the name is free, so the redemption is the one that mints it."""
    return None


class TestTheCeiling:
    """REQ-1594: how many redemptions the link admits is a number, not a boolean."""

    def test_the_addressed_invitation_is_a_ceiling_of_one(self):
        assert not is_spent(_invite(uses=0, max_uses=1))
        assert is_spent(_invite(uses=1, max_uses=1))

    def test_null_max_uses_is_unlimited(self):
        # The only answer a link on a public page can give: nobody knows how many people will click.
        assert not is_spent(_invite(uses=0, max_uses=None))
        assert not is_spent(_invite(uses=10_000, max_uses=None))

    def test_a_bounded_open_invite_closes_at_its_bound(self):
        assert not is_spent(_invite(uses=49, max_uses=50))
        assert is_spent(_invite(uses=50, max_uses=50))

    def test_a_row_past_its_ceiling_is_still_spent(self):
        # Not an equality test: a ceiling lowered after the fact must not reopen the link.
        assert is_spent(_invite(uses=7, max_uses=3))

    def test_the_sql_clause_says_the_same_thing_as_the_python_one(self):
        # Both gates have to agree, or the WHERE admits someone the fetched row would have refused.
        clause = str(unspent())
        assert "max_uses IS NULL" in clause
        assert "uses < org_invites.max_uses" in clause


class TestClaimingARedemption:
    """REQ-1594: two people clicking at once must not both get through the last redemption."""

    def test_the_increment_happens_in_sql(self):
        # A read-modify-write would let both readers compute the same next value.
        text = str(spend("tok-1", "bob"))
        assert "uses=(org_invites.uses + " in text.replace(" = ", "=")

    def test_the_ceiling_is_re_checked_in_the_where(self):
        text = str(spend("tok-1", "bob"))
        where = text.split("WHERE", 1)[1]
        assert "max_uses IS NULL" in where
        assert "uses < org_invites.max_uses" in where

    def test_it_returns_the_row_it_claimed(self):
        # The caller distinguishes "claimed" from "someone else took the last one" by this row.
        assert "RETURNING org_invites.token" in str(spend("tok-1", "bob"))

    def test_the_last_redemption_is_still_recorded(self):
        text = str(spend("tok-1", "bob"))
        assert "used_at" in text and "used_by" in text


class TestSandboxNames:
    def test_it_is_a_legal_environment_name(self):
        from provisa.core.environments import is_env_name

        assert is_env_name(sandbox_env_name())

    def test_two_visitors_never_share_one(self):
        assert len({sandbox_env_name() for _ in range(100)}) == 100

    def test_it_carries_no_identity(self):
        # The name becomes a schema name; deriving it from the redeemer would put a person into the
        # database's own namespace.
        name = sandbox_env_name()
        assert name.startswith(SANDBOX_ENV_PREFIX)
        assert len(name) <= 32


class TestTheMembershipPin:
    """REQ-1596: the pin is written with the membership, never as a second statement."""

    def test_an_ordinary_membership_is_unpinned(self):
        assert "env_name" not in membership_values("bob", "acme", JOINED_VIA_INVITE)

    def test_a_seated_membership_carries_its_environment(self):
        row = membership_values("bob", "acme", JOINED_VIA_INVITE, env_name="sandbox_ab12")
        assert row["env_name"] == "sandbox_ab12"

    def test_the_pin_rides_in_the_same_row_as_the_membership(self):
        # Written together so a pinned member never exists unpinned for an instant -- that instant
        # would serve a sandbox visitor the org's production data.
        row = membership_values("bob", "acme", JOINED_VIA_INVITE, env_name="sandbox_ab12")
        assert row["user_id"] == "bob" and row["org_id"] == "acme"


class TestWhatTheRedeemerIsGiven:
    """REQ-1595: env_policy decides, and only per_visitor mints anything."""

    @pytest.mark.asyncio
    async def test_an_ordinary_invitation_seats_nobody(self):
        from provisa.api.invite_env import redeem_env

        assert await redeem_env(_invite(), "bob") == RedeemedEnv(None, minted=False)

    @pytest.mark.asyncio
    async def test_a_shared_portal_seats_everyone_in_the_environment_it_names(self):
        from provisa.api.invite_env import redeem_env

        invite = _invite(env_policy=ENV_POLICY_SHARED, env_name="portal")
        assert await redeem_env(invite, "bob") == RedeemedEnv("portal", minted=False)
        assert await redeem_env(invite, "carol") == RedeemedEnv("portal", minted=False)

    @pytest.mark.asyncio
    async def test_a_visitor_gets_a_fresh_environment_deployed_from_the_env_the_invite_names(
        self, monkeypatch
    ):
        created: list[dict] = []

        async def _create(state, admin_db, tenant_pool, tenant_db, org_id, name, **kw):
            created.append({"org_id": org_id, "name": name, **kw})

        async def _org_tenant_db(org_id):
            return object()

        monkeypatch.setattr("provisa.core.env_create.create_environment", _create)
        monkeypatch.setattr("provisa.api.admin.orgs_router._org_tenant_db", _org_tenant_db)
        monkeypatch.setattr("provisa.core.env_store.get_env", _no_such_env)
        monkeypatch.setattr(
            "provisa.api.app.state",
            types.SimpleNamespace(admin_db=object(), tenant_db=object()),
            raising=False,
        )
        from provisa.api.invite_env import redeem_env

        # REQ-1602: create_invite captures env_name from the inviter's own active_env at creation
        # time, so a per_visitor invite always names its source -- here, the inviter was on "qa".
        invite = _invite(env_policy=ENV_POLICY_PER_VISITOR, env_ttl_seconds=3600, env_name="qa")
        redeemed = await redeem_env(invite, "bob")

        name = redeemed.name
        assert name is not None and name.startswith(SANDBOX_ENV_PREFIX)
        assert redeemed.minted
        assert created[0]["name"] == name
        assert created[0]["from_env"] == "qa"
        assert created[0]["created_by"] == "bob"

    @pytest.mark.asyncio
    async def test_the_environment_is_given_the_hour_the_invite_names(self, monkeypatch):
        from datetime import datetime, timezone

        created: list[dict] = []

        async def _create(state, admin_db, tenant_pool, tenant_db, org_id, name, **kw):
            created.append(kw)

        async def _org_tenant_db(org_id):
            return object()

        monkeypatch.setattr("provisa.core.env_create.create_environment", _create)
        monkeypatch.setattr("provisa.api.admin.orgs_router._org_tenant_db", _org_tenant_db)
        monkeypatch.setattr("provisa.core.env_store.get_env", _no_such_env)
        monkeypatch.setattr(
            "provisa.api.app.state",
            types.SimpleNamespace(admin_db=object(), tenant_db=object()),
            raising=False,
        )
        from provisa.api.invite_env import redeem_env

        await redeem_env(
            _invite(env_policy=ENV_POLICY_PER_VISITOR, env_ttl_seconds=3600, env_name="prod"),
            "bob",
        )

        left = created[0]["expires_at"] - datetime.now(tz=timezone.utc)
        assert 3500 < left.total_seconds() <= 3600
        # REQ-1600: and the hour is an hour of DISUSE -- the same span rides along as the idle
        # allowance, so a visitor still working when it elapses keeps the environment.
        assert created[0]["idle_ttl_seconds"] == 3600

    @pytest.mark.asyncio
    async def test_an_unknown_policy_is_refused_rather_than_treated_as_none(self):
        from provisa.api.invite_env import redeem_env

        with pytest.raises(ValueError):
            await redeem_env(_invite(env_policy="whatever"), "bob")


class TestAVisitorWhoComesBack:
    """REQ-1615: a sandbox environment is named after its visitor, so a second visit finds it."""

    @staticmethod
    def _sandbox_invite():
        return _invite(
            org_id=SANDBOX_ORG_ID,
            role_id=SANDBOX_ROLE,
            env_policy=ENV_POLICY_PER_VISITOR,
            env_ttl_seconds=3600,
            env_name="prod",
        )

    @pytest.mark.asyncio
    async def test_the_second_redemption_seats_them_in_the_one_they_already_have(self, monkeypatch):
        async def _create(*a, **kw):
            raise AssertionError("the visitor's environment exists; minting it again is the 500")

        async def _get_env(db, org_id, name):
            return {"org_id": org_id, "name": name}

        renewed: list[tuple] = []

        async def _set_expiry(db, org_id, name, expires_at):
            renewed.append((org_id, name, expires_at))

        monkeypatch.setattr("provisa.core.env_create.create_environment", _create)
        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        monkeypatch.setattr("provisa.core.env_store.set_expiry", _set_expiry)
        monkeypatch.setattr(
            "provisa.api.app.state",
            types.SimpleNamespace(admin_db=object(), tenant_db=object()),
            raising=False,
        )
        from provisa.api.invite_env import redeem_env

        redeemed = await redeem_env(self._sandbox_invite(), "bob")

        # Nothing was minted -- so nothing here is this redemption's to take back.
        assert redeemed.minted is False
        assert redeemed.name is not None and redeemed.name.startswith("ephemeral_")
        # REQ-1600: arriving is use, so the idle deadline moves out by the invite's hour.
        left = renewed[0][2] - datetime.now(tz=timezone.utc)
        assert 3500 < left.total_seconds() <= 3600

    @pytest.mark.asyncio
    async def test_two_visits_resolve_to_the_same_name(self, monkeypatch):
        """REQ-1602: the name is a function of the visitor, which is why it collided at all."""
        seen: list[str] = []

        async def _create(state, admin_db, tenant_pool, tenant_db, org_id, name, **kw):
            seen.append(name)

        async def _org_tenant_db(org_id):
            return object()

        monkeypatch.setattr("provisa.core.env_create.create_environment", _create)
        monkeypatch.setattr("provisa.api.admin.orgs_router._org_tenant_db", _org_tenant_db)
        monkeypatch.setattr("provisa.core.env_store.get_env", _no_such_env)
        monkeypatch.setattr(
            "provisa.api.app.state",
            types.SimpleNamespace(admin_db=object(), tenant_db=object()),
            raising=False,
        )
        from provisa.api.invite_env import redeem_env

        first = await redeem_env(self._sandbox_invite(), "bob")
        second = await redeem_env(self._sandbox_invite(), "bob")

        assert first.name == second.name and seen == [first.name, first.name]

    @pytest.mark.asyncio
    async def test_a_reused_environment_survives_a_failed_redemption(self, monkeypatch):
        """What the visitor did last session is not this redemption's to delete."""

        async def _retire(*a, **kw):
            raise AssertionError("this environment predates the redemption that failed")

        monkeypatch.setattr("provisa.core.env_retire.retire_environment", _retire)
        from provisa.api.invite_env import release_env

        await release_env(self._sandbox_invite(), RedeemedEnv("ephemeral_0c57a70a", minted=False))


class TestReleasingWhatWasMinted:
    """REQ-1595: the loser of a race for the last redemption has already provisioned a schema."""

    @pytest.mark.asyncio
    async def test_a_visitor_environment_is_retired_with_its_branch(self, monkeypatch):
        retired: list[dict] = []

        async def _retire(tenant_db, admin_db, org_id, name, *, drop_branch):
            retired.append({"org_id": org_id, "name": name, "drop_branch": drop_branch})
            return {}

        monkeypatch.setattr("provisa.core.env_retire.retire_environment", _retire)
        monkeypatch.setattr(
            "provisa.api.app.state",
            types.SimpleNamespace(admin_db=object(), tenant_db=object()),
            raising=False,
        )
        from provisa.api.invite_env import release_env

        await release_env(
            _invite(env_policy=ENV_POLICY_PER_VISITOR, env_ttl_seconds=60),
            RedeemedEnv("sbx", minted=True),
        )

        assert retired == [{"org_id": "acme", "name": "sbx", "drop_branch": True}]

    @pytest.mark.asyncio
    async def test_a_shared_portal_is_never_taken_down_by_a_failed_redemption(self, monkeypatch):
        async def _retire(*a, **kw):
            raise AssertionError("the org published this environment; other people are in it")

        monkeypatch.setattr("provisa.core.env_retire.retire_environment", _retire)
        from provisa.api.invite_env import release_env

        await release_env(
            _invite(env_policy=ENV_POLICY_SHARED, env_name="portal"),
            RedeemedEnv("portal", minted=False),
        )

    @pytest.mark.asyncio
    async def test_nothing_minted_is_nothing_to_release(self, monkeypatch):
        async def _retire(*a, **kw):
            raise AssertionError("nothing was minted")

        monkeypatch.setattr("provisa.core.env_retire.retire_environment", _retire)
        from provisa.api.invite_env import release_env

        await release_env(_invite(), RedeemedEnv(None, minted=False))


class TestTheInviterIsToldWhy:
    """REQ-1595: the CheckConstraint decides; this is so nobody has to meet it as a 500."""

    def _body(self, **kw):
        from provisa.api.admin.invites_router import CreateInviteBody

        return CreateInviteBody(org_id="acme", email="bob@rival.com", role_id="analyst", **kw)

    def test_the_default_body_is_the_invitation_that_already_existed(self):
        body = self._body()
        assert body.max_uses == 1
        assert body.env_policy == ENV_POLICY_NONE

    @pytest.mark.parametrize(
        "kwargs,code",
        [
            ({"env_policy": "whatever"}, "invites.invalid_env_policy"),
            ({"env_policy": ENV_POLICY_PER_VISITOR}, "invites.env_ttl_required"),
            ({"env_policy": ENV_POLICY_SHARED}, "invites.env_name_required"),
            ({"max_uses": 0}, "invites.invalid_max_uses"),
        ],
        ids=["unknown-policy", "sandbox-without-a-deadline", "portal-without-a-name", "zero-uses"],
    )
    def test_an_unusable_invite_is_a_400_naming_the_missing_field(self, kwargs, code):
        from provisa.api.admin.invites_router import _check_env_policy
        from provisa.api.errors import ApiError

        with pytest.raises(ApiError) as exc:
            _check_env_policy(self._body(**kwargs))
        assert exc.value.code == code

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"max_uses": None},
            {"env_policy": ENV_POLICY_PER_VISITOR, "env_ttl_seconds": 3600},
            {"env_policy": ENV_POLICY_SHARED, "env_name": "portal"},
        ],
        ids=["addressed", "unlimited", "sandbox", "portal"],
    )
    def test_every_shape_the_product_offers_passes(self, kwargs):
        from provisa.api.admin.invites_router import _check_env_policy

        _check_env_policy(self._body(**kwargs))


class TestTheSandboxRoleGoesNowhereElse:
    """REQ-1597: the role is defined by a subtraction performed inside the environment redemption
    mints, so an invitation that mints no environment confers a name nothing narrowed."""

    def _body(self, **kw):
        from provisa.api.admin.invites_router import CreateInviteBody

        return CreateInviteBody(org_id="acme", role_id=SANDBOX_ROLE, **kw)

    @pytest.mark.parametrize(
        "kwargs",
        [{}, {"env_policy": ENV_POLICY_SHARED, "env_name": "portal"}],
        ids=["ordinary-invitation", "published-portal"],
    )
    def test_a_sandbox_role_without_an_environment_to_define_it_is_refused(self, kwargs):
        from provisa.api.admin.invites_router import _check_env_policy
        from provisa.api.errors import ApiError

        with pytest.raises(ApiError) as exc:
            _check_env_policy(self._body(**kwargs))
        assert exc.value.code == "invites.sandbox_role_needs_per_visitor"

    def test_the_per_visitor_invitation_is_the_one_that_may_confer_it(self):
        from provisa.api.admin.invites_router import _check_env_policy

        _check_env_policy(self._body(env_policy=ENV_POLICY_PER_VISITOR, env_ttl_seconds=3600))


class TestTheVisitorAlsoHoldsTheNameTheGrantsCarry:
    """REQ-1597: `sandbox` is named by no authored model's `table_columns.visible_to`, so on its own
    it is shown no column and `schema_gen` drops every table left with none. The second assignment
    is what makes the copied model visible; `define_role_from` is what keeps it from widening."""

    class _Recorder:
        def __init__(self):
            self.granted = []

    async def _seat(self, role_id):
        import provisa.core.org_membership as om
        from provisa.api.invite_env import seat_redeemed_roles

        rec = self._Recorder()
        original = om.grant_org_role

        async def fake(tenant_db, user_id, rid):
            rec.granted.append((user_id, rid))

        om.grant_org_role = fake
        try:
            await seat_redeemed_roles(object(), "visitor", role_id)
        finally:
            om.grant_org_role = original
        return rec.granted

    @pytest.mark.asyncio
    async def test_a_sandbox_redemption_is_seated_under_both_names(self):
        assert await self._seat(SANDBOX_ROLE) == [
            ("visitor", SANDBOX_ROLE),
            ("visitor", "org_admin"),
        ]

    @pytest.mark.asyncio
    async def test_every_other_invitation_confers_exactly_what_it_named(self):
        assert await self._seat("analyst") == [("visitor", "analyst")]
