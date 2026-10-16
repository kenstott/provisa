# Copyright (c) 2026 Kenneth Stott
# Canary: 552a1671-b263-4001-bf96-fca387215fc7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A credential addresses exactly one environment (REQ-1503).

A personal access token is issued INTO an environment and carries it. The environment a request is
served by then comes from the credential, not from a header: a header naming another environment is
refused by name rather than resolved in favour of either, and retiring an environment revokes the
tokens issued into it and no others.
"""

# Requirements: REQ-1503, REQ-2053

from __future__ import annotations

import pytest
from sqlalchemy import insert

from provisa.api.env_routing import EnvironmentRightError, resolve_selected_env
from provisa.auth.models import AuthIdentity
from provisa.auth.pat import PersonalAccessTokenStore
from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import environments, orgs

ORG = "acme"
ALICE = "uid-alice"


@pytest.fixture
async def admin(tmp_path) -> Database:
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    with engine.begin() as conn:
        schema_admin.metadata.create_all(conn)
    plane = Database(engine, name="admin")
    async with plane.acquire() as conn:
        await conn.execute_core(insert(orgs).values(id=ORG, name=ORG))
        await conn.execute_core(insert(environments).values(org_id=ORG, name="prod"))
        for name in ("dev", "staging"):
            # Every environment but prod is made from one, with its data chosen (REQ-1942).
            await conn.execute_core(
                insert(environments).values(
                    org_id=ORG, name=name, parent="prod", data_mode="unbound"
                )
            )
    return plane


async def _token(admin: Database, env: str, name: str = "ci") -> str:
    secret, _row = await PersonalAccessTokenStore(admin).issue(
        user_id=ALICE, org_id=ORG, env_name=env, name=name
    )
    return secret


class TestTheTokenCarriesItsEnvironment:
    async def test_the_identity_names_the_environment_the_token_was_issued_into(self, admin):
        identity = await PersonalAccessTokenStore(admin).validate(await _token(admin, "dev"))
        assert identity.credential_env == "dev"

    async def test_the_owner_sees_which_environment_each_token_addresses(self, admin):
        await _token(admin, "dev", "dev-ci")
        await _token(admin, "prod", "prod-ci")
        listed = await PersonalAccessTokenStore(admin).list_for_user(ALICE, ORG)
        assert {row["name"]: row["env_name"] for row in listed} == {
            "dev-ci": "dev",
            "prod-ci": "prod",
        }

    async def test_an_interactive_session_carries_none(self):
        identity = AuthIdentity(user_id=ALICE, email=None, display_name=None, roles=[])
        assert identity.credential_env is None


class TestTheEnvironmentComesFromTheCredential:
    @staticmethod
    def _presenting(env: str) -> AuthIdentity:
        return AuthIdentity(
            user_id=ALICE, email=None, display_name=None, roles=[], credential_env=env
        )

    async def test_a_request_naming_no_environment_is_served_by_the_tokens(self, admin):
        selected = await resolve_selected_env(admin, ORG, self._presenting("dev"), None, set())
        assert selected == "dev"

    async def test_no_switch_right_is_needed_to_be_served_by_it(self, admin):
        # The token does not switch: it was issued into the environment it addresses.
        selected = await resolve_selected_env(admin, ORG, self._presenting("dev"), "dev", set())
        assert selected == "dev"

    @pytest.mark.parametrize("header", ["prod", "staging"])
    async def test_a_header_naming_another_environment_is_refused_by_name(self, admin, header):
        with pytest.raises(EnvironmentRightError) as refused:
            await resolve_selected_env(
                admin, ORG, self._presenting("dev"), header, {"environment_switch"}
            )
        assert "'dev'" in str(refused.value) and repr(header) in str(refused.value)

    async def test_a_prod_token_cannot_be_pointed_at_dev(self, admin):
        with pytest.raises(EnvironmentRightError):
            await resolve_selected_env(
                admin, ORG, self._presenting("prod"), "dev", {"environment_switch"}
            )

    async def test_a_token_whose_environment_is_gone_is_refused_not_served_prod(self, admin):
        from provisa.api.env_routing import EnvironmentSelectionError

        with pytest.raises(EnvironmentSelectionError):
            await resolve_selected_env(admin, ORG, self._presenting("deleted"), None, set())


class TestADemoSandboxTokenAddressesItsWholeOrg:
    """REQ-2053, REQ-1503's one exception: a demo sandbox is its person's alone and thrown away, so
    its token is served by whichever of its environments it names."""

    SANDBOX = "sbx0123abcd"

    async def _sandbox_token(self, admin: Database) -> str:
        async with admin.acquire() as conn:
            await conn.execute_core(insert(orgs).values(id=self.SANDBOX, name="sandbox"))
            await conn.execute_core(insert(environments).values(org_id=self.SANDBOX, name="prod"))
            await conn.execute_core(
                insert(environments).values(
                    org_id=self.SANDBOX, name="dev", parent="prod", data_mode="test_fake"
                )
            )
        secret, _row = await PersonalAccessTokenStore(admin).issue(
            user_id=ALICE, org_id=self.SANDBOX, env_name="prod", name="Demo sandbox"
        )
        return secret

    async def test_it_carries_no_one_environment(self, admin):
        identity = await PersonalAccessTokenStore(admin).validate(await self._sandbox_token(admin))
        assert identity.credential_env is None

    async def test_it_is_served_by_each_environment_it_names(self, admin):
        identity = await PersonalAccessTokenStore(admin).validate(await self._sandbox_token(admin))
        held = {"environment_switch"}
        for env in ("prod", "dev"):
            assert await resolve_selected_env(admin, self.SANDBOX, identity, env, held) == env
        assert await resolve_selected_env(admin, self.SANDBOX, identity, None, held) == "prod"

    async def test_an_ordinary_orgs_token_still_addresses_one(self, admin):
        identity = await PersonalAccessTokenStore(admin).validate(await _token(admin, "dev"))
        assert identity.credential_env == "dev"


class TestRetiringAnEnvironmentRevokesItsTokens:
    async def test_its_tokens_stop_working_and_the_others_keep_working(self, admin):
        store = PersonalAccessTokenStore(admin)
        dev, prod = await _token(admin, "dev", "dev-ci"), await _token(admin, "prod", "prod-ci")
        assert await store.revoke_all_for_environment(org_id=ORG, env_name="dev") == 1
        with pytest.raises(ValueError, match="Invalid token"):
            await store.validate(dev)
        assert (await store.validate(prod)).credential_env == "prod"

    async def test_retirement_does_it(self, admin, monkeypatch):
        from provisa.core import env_retire

        async def _deprovision(pool, org_id, redis_url=None, env=None):
            return None

        async def _drop_store(org_id, name):
            return None

        async def _purge(pool, org_id, envs):
            return None

        monkeypatch.setattr("provisa.core.org_provisioning.deprovision_org", _deprovision)
        monkeypatch.setattr(env_retire, "discard_file_sources", lambda org_id, name: 0)
        monkeypatch.setattr(env_retire, "_drop_store_schema", _drop_store)
        monkeypatch.setattr("provisa.core.region_purge.purge_org_regions", _purge)
        monkeypatch.setattr(env_retire, "delete_branch", lambda org_id, name: True)
        monkeypatch.setattr("provisa.core.redis_location.redis_url", lambda: None)

        dev, prod = await _token(admin, "dev", "dev-ci"), await _token(admin, "prod", "prod-ci")
        await env_retire.retire_environment(admin, admin, ORG, "dev", drop_branch=False)

        store = PersonalAccessTokenStore(admin)
        with pytest.raises(ValueError, match="Invalid token"):
            await store.validate(dev)
        assert (await store.validate(prod)).credential_env == "prod"

    async def test_a_token_never_holds_its_environment_up(self, admin):
        from provisa.core.env_retire import env_dependents

        await _token(admin, "dev")
        assert await env_dependents(admin, ORG, "dev") == []


class TestATokenIsIssuedIntoAnEnvironmentTheCallerCanReach:
    """REQ-1503: issuing a token is not a way into an environment the caller cannot be served."""

    @staticmethod
    def _request():
        from types import SimpleNamespace

        identity = AuthIdentity(user_id=ALICE, email=None, display_name=None, roles=[])
        return SimpleNamespace(state=SimpleNamespace(identity=identity))

    async def test_naming_none_issues_into_the_environment_being_read(self, monkeypatch):
        from provisa.api import pat_router

        monkeypatch.setattr("provisa.core.request_context.active_env", lambda: "dev")
        assert await pat_router._issued_into(self._request(), ORG, None) == "dev"

    async def test_an_environment_the_caller_cannot_be_served_is_refused(self, monkeypatch):
        from provisa.api import pat_router
        from provisa.api.errors import ApiError

        async def _refuse(*args, **kwargs):
            raise EnvironmentRightError("no environment_switch")

        async def _caps(identity, org_id):
            return set()

        monkeypatch.setattr("provisa.core.request_context.active_env", lambda: "prod")
        monkeypatch.setattr("provisa.api.env_routing.resolve_selected_env", _refuse)
        monkeypatch.setattr("provisa.auth.middleware.org_env_capabilities", _caps)
        with pytest.raises(ApiError) as refused:
            await pat_router._issued_into(self._request(), ORG, "staging")
        assert refused.value.status_code == 403
        assert refused.value.code == "env.switch_forbidden"
