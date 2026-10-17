# Copyright (c) 2026 Kenneth Stott
# Canary: 87a295e6-f205-4710-955d-44a85eb49db7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A user's attributes: the provider's claims and the org's record, resolved in one place, the
org's record winning (REQ-1382, REQ-1383)."""

# Requirements: REQ-1382, REQ-1383

from __future__ import annotations

import types

import pytest

import provisa.api.app as appmod
from provisa.api.admin import local_users_router as router
from provisa.api.errors import ApiError
from provisa.auth.attributes import (
    IDP,
    ORG,
    ResolvedAttribute,
    attribute_name_problem,
    effective,
    idp_attributes,
    normalize_name,
    recorded_idp_attributes,
    resolve,
)
from provisa.auth.middleware import request_session_vars
from provisa.core import env_classes
from provisa.core.config_stamp import MODEL, TENANT_TABLES
from provisa.core.env_deploy import PROJECTED
from provisa.core.repositories import user_attribute as attr_repo


class TestResolution:
    def test_the_orgs_record_wins_and_the_providers_value_is_reported_overridden(self):
        resolved = resolve({"region": "us", "dept": "sales"}, {"region": "eu", "team": "a"})
        assert resolved == [
            ResolvedAttribute("dept", "sales", IDP),
            ResolvedAttribute("region", "eu", ORG),
            ResolvedAttribute("region", "us", IDP, overridden=True),
            ResolvedAttribute("team", "a", ORG),
        ]
        assert effective({"region": "us", "dept": "sales"}, {"region": "eu"}) == {
            "region": "eu",
            "dept": "sales",
        }

    def test_a_providers_attributes_are_its_scalar_claims_under_one_spelling(self):
        claims = {"X-Hasura-Org-Id": "acme", "Cost-Center": 42, "admin": True, "groups": ["a"]}
        assert idp_attributes(claims) == {
            "hasura_org_id": "acme",
            "cost_center": "42",
            "admin": "true",
        }

    def test_hasura_session_variables_are_read_from_the_hasura_claims_object(self):
        """Hasura reads x-hasura-* from the claims object its JWT names; each is its own
        hasura_* attribute, never user_id or role."""
        claims = {
            "sub": "auth0|abc",
            "https://hasura.io/jwt/claims": {"x-hasura-user-id": 7, "x-hasura-org-id": "acme"},
        }
        assert idp_attributes(claims) == {
            "sub": "auth0|abc",
            "hasura_user_id": "7",
            "hasura_org_id": "acme",
        }

    def test_what_is_recorded_of_a_provider_leaves_the_token_out(self):
        assert recorded_idp_attributes({"region": "eu", "iat": 1, "exp": 2, "sid": "s"}) == {
            "region": "eu"
        }

    @pytest.mark.parametrize("name", ["user_id", "Role", "User-Id"])
    def test_the_identity_the_request_binds_cannot_be_recorded(self, name):
        problem = attribute_name_problem(name)
        assert problem is not None and problem[0] == "name_reserved"

    @pytest.mark.parametrize("name", ["9lives", "a.b", "a b"])
    def test_a_name_a_predicate_cannot_spell_is_refused(self, name):
        problem = attribute_name_problem(name)
        assert problem is not None and problem[0] == "name_invalid"

    def test_a_good_name_has_no_problem(self):
        assert attribute_name_problem("Cost-Center") is None
        assert normalize_name("Cost-Center") == "cost_center"

    def test_a_hasura_session_variable_is_an_attribute_of_its_own(self):
        """The app's user key an imported rule reads (X-Hasura-User-Id) may be recorded for a
        member: it is hasura_user_id, not the identity's user_id."""
        assert normalize_name("X-Hasura-User-Id") == "hasura_user_id"
        assert attribute_name_problem("X-Hasura-User-Id") is None


class TestSessionVariables:
    def test_the_orgs_record_overlays_the_claims(self):
        identity = types.SimpleNamespace(
            user_id="u1", raw_claims={"region": "us", "dept": "x"}, org_attributes={"region": "eu"}
        )
        out = request_session_vars(identity, "analyst", {}, honor_session_headers=False)
        assert out == {"region": "eu", "dept": "x", "user_id": "u1", "role": "analyst"}

    def test_an_identity_nothing_bound_is_refused_by_name(self):
        identity = types.SimpleNamespace(user_id="u1", raw_claims={}, org_attributes=None)
        with pytest.raises(RuntimeError, match="'u1' has no org attributes"):
            request_session_vars(identity, None, {}, honor_session_headers=False)


class TestWhereTheTablesLive:
    def test_the_orgs_record_travels_into_a_new_environment_and_reloads_the_model(self):
        assert "user_attributes" in env_classes.CARRIED
        assert "user_attributes" in env_classes.SEEDED_AT_CREATION
        assert TENANT_TABLES["user_attributes"] == MODEL

    def test_a_deploy_makes_no_statement_about_a_members_record(self):
        assert "user_attributes" not in PROJECTED

    def test_what_a_provider_said_stays_in_the_environment_it_said_it_in(self):
        assert "user_idp_attributes" in env_classes.NEVER_SENSITIVE


@pytest.fixture
async def conn(control_plane):
    async with control_plane.org().acquire() as connection:
        yield connection


class TestStore:
    async def test_an_attribute_is_stored_under_its_normalized_name_and_replaced(self, conn):
        assert await attr_repo.upsert(conn, "u1", "Cost-Center", "42") == "cost_center"
        await attr_repo.upsert(conn, "u1", "cost_center", "43")
        await attr_repo.upsert(conn, "u2", "region", "eu")
        assert await attr_repo.org_for_user(conn, "u1") == {"cost_center": "43"}
        assert await attr_repo.org_by_user(conn) == {
            "u1": {"cost_center": "43"},
            "u2": {"region": "eu"},
        }
        assert await attr_repo.delete(conn, "u1", "Cost-Center")
        assert not await attr_repo.delete(conn, "u1", "cost_center")

    async def test_a_reserved_name_is_never_stored(self, conn):
        with pytest.raises(ValueError, match="acting identity"):
            await attr_repo.upsert(conn, "u1", "role", "x")

    async def test_the_providers_record_is_rewritten_only_when_it_changed(self, conn):
        assert await attr_repo.record_idp(conn, "u1", {"region": "us", "dept": "x"})
        assert not await attr_repo.record_idp(conn, "u1", {"dept": "x", "region": "us"})
        assert await attr_repo.record_idp(conn, "u1", {"region": "us"})
        assert await attr_repo.idp_for_user(conn, "u1") == {"region": "us"}
        assert await attr_repo.idp_names_by_user(conn) == {"u1": {"region"}}

    def test_two_first_requests_at_once_record_the_providers_attributes_once(
        self, control_plane, monkeypatch
    ):
        """A new sandbox's visitor sends its first requests together (the tour warm-up and the
        page's own): each reads that nothing is recorded yet, and each writes it. The second
        write is not a unique-key violation answered 500; the attributes are recorded once."""
        import asyncio
        import threading

        db = control_plane.org()
        both_read = threading.Barrier(2)
        read = attr_repo.idp_for_user

        async def _read_together(conn, user_id):
            found = await read(conn, user_id)
            try:
                both_read.wait(timeout=2)
            except threading.BrokenBarrierError:
                pass  # the other request is not reading at the same time
            return found

        monkeypatch.setattr(attr_repo, "idp_for_user", _read_together)
        errors: list[BaseException] = []

        def _first_request():
            async def _record():
                async with db.acquire() as conn:
                    await attr_repo.record_idp(conn, "visitor", {"pat": "true"})

            try:
                asyncio.run(_record())
            except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
                errors.append(exc)

        requests = [threading.Thread(target=_first_request) for _ in range(2)]
        for request in requests:
            request.start()
        for request in requests:
            request.join()
        assert errors == []
        monkeypatch.setattr(attr_repo, "idp_for_user", read)

        async def _recorded():
            async with db.acquire() as conn:
                return await attr_repo.idp_for_user(conn, "visitor")

        assert asyncio.run(_recorded()) == {"pat": "true"}


_ROLES = {"people_admin": {"capabilities": ["user_management"], "domain_access": ["*"]}}
ADMIN = types.SimpleNamespace(
    state=types.SimpleNamespace(
        identity=types.SimpleNamespace(
            user_id="admin-1", roles=["people_admin"], acting_roles=["people_admin"]
        )
    )
)


class TestAdminRoutes:
    @pytest.fixture
    def plane(self, control_plane, monkeypatch):
        db = control_plane.org()
        monkeypatch.setattr(appmod.state, "roles", _ROLES, raising=False)
        monkeypatch.setattr(router, "_pool", lambda _request: db)
        return db

    async def test_an_attribute_is_recorded_listed_with_its_source_and_removed(self, plane):
        async with plane.acquire() as c:
            await attr_repo.record_idp(c, "u1", {"region": "us", "dept": "x"})
        put = await router.set_attribute("u1", "Region", router.AttributeBody(value="eu"), ADMIN)
        assert put == {"name": "region", "value": "eu", "source": ORG, "overridden": False}
        # This worker resolves requests against the write at once.
        assert appmod.state.user_attributes == {"u1": {"region": "eu"}}
        assert await router.list_attributes("u1", ADMIN) == [
            {"name": "dept", "value": "x", "source": IDP, "overridden": False},
            {"name": "region", "value": "eu", "source": ORG, "overridden": False},
            {"name": "region", "value": "us", "source": IDP, "overridden": True},
        ]
        assert await router.delete_attribute("u1", "region", ADMIN) == {"deleted": "region"}
        assert appmod.state.user_attributes == {}

    async def test_a_reserved_name_is_refused_naming_it_as_it_is_bound(self, plane):
        with pytest.raises(ApiError) as refused:
            await router.set_attribute("u1", "User-Id", router.AttributeBody(value="x"), ADMIN)
        assert refused.value.status_code == 422
        assert refused.value.code == "users.attribute_name_reserved"
        assert refused.value.params == {"name": "user_id"}

    async def test_removing_an_attribute_the_user_lacks_is_a_404(self, plane):
        with pytest.raises(ApiError) as refused:
            await router.delete_attribute("u1", "region", ADMIN)
        assert refused.value.status_code == 404
        assert refused.value.code == "users.attribute_not_found"
