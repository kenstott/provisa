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

# Requirements: REQ-1594, REQ-1595, REQ-1596

from __future__ import annotations

import types

import pytest

from provisa.core.org_invite import (
    ENV_POLICY_NONE,
    ENV_POLICY_PER_VISITOR,
    ENV_POLICY_SHARED,
    SANDBOX_ENV_PREFIX,
    is_spent,
    sandbox_env_name,
    spend,
    unspent,
)
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

        assert await redeem_env(_invite(), "bob") is None

    @pytest.mark.asyncio
    async def test_a_shared_portal_seats_everyone_in_the_environment_it_names(self):
        from provisa.api.invite_env import redeem_env

        invite = _invite(env_policy=ENV_POLICY_SHARED, env_name="portal")
        assert await redeem_env(invite, "bob") == "portal"
        assert await redeem_env(invite, "carol") == "portal"

    @pytest.mark.asyncio
    async def test_a_visitor_gets_a_fresh_environment_deployed_from_prod(self, monkeypatch):
        from provisa.core.environments import PROD

        created: list[dict] = []

        async def _create(state, admin_db, tenant_pool, tenant_db, org_id, name, **kw):
            created.append({"org_id": org_id, "name": name, **kw})

        async def _org_tenant_db(org_id):
            return object()

        monkeypatch.setattr("provisa.core.env_create.create_environment", _create)
        monkeypatch.setattr("provisa.api.admin.orgs_router._org_tenant_db", _org_tenant_db)
        monkeypatch.setattr(
            "provisa.api.app.state",
            types.SimpleNamespace(admin_db=object(), tenant_db=object()),
            raising=False,
        )
        from provisa.api.invite_env import redeem_env

        invite = _invite(env_policy=ENV_POLICY_PER_VISITOR, env_ttl_seconds=3600)
        name = await redeem_env(invite, "bob")

        assert name is not None and name.startswith(SANDBOX_ENV_PREFIX)
        assert created[0]["name"] == name
        # A sandbox with nothing in it demonstrates nothing.
        assert created[0]["from_env"] == PROD
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
        monkeypatch.setattr(
            "provisa.api.app.state",
            types.SimpleNamespace(admin_db=object(), tenant_db=object()),
            raising=False,
        )
        from provisa.api.invite_env import redeem_env

        await redeem_env(_invite(env_policy=ENV_POLICY_PER_VISITOR, env_ttl_seconds=3600), "bob")

        left = created[0]["expires_at"] - datetime.now(tz=timezone.utc)
        assert 3500 < left.total_seconds() <= 3600

    @pytest.mark.asyncio
    async def test_an_unknown_policy_is_refused_rather_than_treated_as_none(self):
        from provisa.api.invite_env import redeem_env

        with pytest.raises(ValueError):
            await redeem_env(_invite(env_policy="whatever"), "bob")


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

        await release_env(_invite(env_policy=ENV_POLICY_PER_VISITOR, env_ttl_seconds=60), "sbx")

        assert retired == [{"org_id": "acme", "name": "sbx", "drop_branch": True}]

    @pytest.mark.asyncio
    async def test_a_shared_portal_is_never_taken_down_by_a_failed_redemption(self, monkeypatch):
        async def _retire(*a, **kw):
            raise AssertionError("the org published this environment; other people are in it")

        monkeypatch.setattr("provisa.core.env_retire.retire_environment", _retire)
        from provisa.api.invite_env import release_env

        await release_env(_invite(env_policy=ENV_POLICY_SHARED, env_name="portal"), "portal")

    @pytest.mark.asyncio
    async def test_nothing_minted_is_nothing_to_release(self, monkeypatch):
        async def _retire(*a, **kw):
            raise AssertionError("nothing was minted")

        monkeypatch.setattr("provisa.core.env_retire.retire_environment", _retire)
        from provisa.api.invite_env import release_env

        await release_env(_invite(), None)


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
