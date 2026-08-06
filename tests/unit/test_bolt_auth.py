# Copyright (c) 2026 Kenneth Stott
# Canary: 01617c80-1898-400a-8e73-d5b56c213226
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bolt authenticates under every provider (REQ-124, REQ-273, REQ-1263).

Bolt used to accept the ``simple`` provider and nothing else: every other deployment got a blanket
refusal, so a Neo4j driver could not connect to a server secured with OIDC or basic auth at all.
These tests pin the replacement — the driver's declared ``scheme`` selects the provider's validator,
a personal access token works here as on every other surface, and the selectable roles come from
the validated identity rather than from a user record read behind the provider's back.
"""

from __future__ import annotations

import base64
from types import SimpleNamespace

import bcrypt
import pytest

from provisa.auth.models import AuthIdentity
from provisa.bolt.session import BoltSession, _scheme_of

_PASSWORD = "s3cret"
_HASH = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()


def _app_state(**overrides):
    state = SimpleNamespace(
        auth_config={"provider": "simple", "default_role": "analyst", "role_mapping": []},
        auth_middleware_active=True,
        contexts={"analyst": object(), "steward": object()},
        admin_db=None,
    )
    for key, value in overrides.items():
        setattr(state, key, value)
    return state


@pytest.fixture()
def app_state(monkeypatch):
    """Point the module-level app state the Bolt session reads at a stub."""
    import provisa.api.app as app_module

    state = _app_state()
    monkeypatch.setattr(app_module, "state", state)
    return state


@pytest.fixture()
def simple_provider(monkeypatch):
    """Wire a real SimpleAuthProvider in, so the scheme dispatch runs against real validators."""
    import provisa.auth.wiring as wiring
    from provisa.auth.providers.simple import SimpleAuthProvider

    provider = SimpleAuthProvider(
        users=[{"username": "alice", "password_hash": _HASH, "roles": ["steward"]}],
        jwt_secret="unit-test-secret",
    )
    monkeypatch.setattr(wiring, "build_auth_provider", lambda config, admin_pool=None: provider)
    return provider


def _session() -> BoltSession:
    return BoltSession(SimpleNamespace(), (5, 4))  # type: ignore[arg-type]


class TestScheme:
    """The driver declares its credential presentation; the server does not guess."""

    def test_a_declared_scheme_is_taken_as_given(self):
        assert _scheme_of({"scheme": "Bearer"}) == "bearer"

    def test_an_absent_scheme_is_basic(self):
        """Neo4j's own default, and what every driver that omits the field is actually sending."""
        assert _scheme_of({}) == "basic"

    def test_an_empty_scheme_is_basic(self):
        assert _scheme_of({"scheme": ""}) == "basic"


@pytest.mark.asyncio
class TestBasicScheme:
    async def test_a_password_authenticates_under_the_simple_provider(
        self, app_state, simple_provider
    ):
        resolved = await _session()._resolve_user("basic", "alice", _PASSWORD)
        assert resolved == ("alice", ["analyst", "steward"])

    async def test_a_wrong_password_is_refused(self, app_state, simple_provider):
        assert await _session()._resolve_user("basic", "alice", "wrong") is None

    async def test_an_unknown_principal_is_refused(self, app_state, simple_provider):
        assert await _session()._resolve_user("basic", "mallory", _PASSWORD) is None


@pytest.mark.asyncio
class TestBearerScheme:
    async def test_a_token_authenticates(self, app_state, simple_provider):
        token = simple_provider.login("alice", _PASSWORD)
        resolved = await _session()._resolve_user("bearer", "", token)
        assert resolved == ("alice", ["analyst", "steward"])

    async def test_a_personal_access_token_is_just_a_bearer_credential(
        self, app_state, simple_provider, monkeypatch
    ):
        """REQ-1263: the PAT store sits on the provider, so Bolt needs no notion of a PAT."""

        class _Store:
            async def validate(self, token):
                assert token == "provisa_pat_abc"
                return AuthIdentity(
                    user_id="u-1",
                    email=None,
                    display_name="Alice",
                    roles=["steward"],
                    raw_claims={},
                )

        monkeypatch.setattr(simple_provider, "pat_store", _Store())
        monkeypatch.setattr(
            "provisa.auth.pat.is_personal_access_token", lambda t: t == "provisa_pat_abc"
        )

        resolved = await _session()._resolve_user("bearer", "", "provisa_pat_abc")
        assert resolved == ("u-1", ["analyst", "steward"])

    async def test_a_forged_token_is_refused(self, app_state, simple_provider):
        assert await _session()._resolve_user("bearer", "", "not-a-jwt") is None


@pytest.mark.asyncio
class TestSchemeSelectsTheValidator:
    async def test_a_presentation_the_provider_does_not_accept_is_refused(
        self, app_state, simple_provider
    ):
        """A refused presentation stops there; the credential is not retried elsewhere."""
        assert await _session()._resolve_user("kerberos", "alice", _PASSWORD) is None

    async def test_a_basic_credential_presented_as_bearer_is_refused(
        self, app_state, simple_provider
    ):
        packed = base64.b64encode(f"alice:{_PASSWORD}".encode()).decode()
        assert await _session()._resolve_user("bearer", "", packed) is None


@pytest.mark.asyncio
class TestRolesComeFromTheIdentity:
    async def test_a_role_with_no_compiled_context_is_not_selectable(
        self, app_state, simple_provider
    ):
        app_state.contexts = {"analyst": object()}
        resolved = await _session()._resolve_user("basic", "alice", _PASSWORD)
        assert resolved == ("alice", ["analyst"])

    async def test_an_identity_with_no_selectable_role_is_refused(self, app_state, simple_provider):
        app_state.contexts = {"auditor": object()}
        assert await _session()._resolve_user("basic", "alice", _PASSWORD) is None

    async def test_a_mapping_rule_selects_the_first_role(self, app_state, simple_provider):
        app_state.auth_config = {
            "provider": "simple",
            "default_role": "analyst",
            "role_mapping": [
                {"claim": "roles", "type": "contains", "value": "steward", "role": "steward"}
            ],
        }
        resolved = await _session()._resolve_user("basic", "alice", _PASSWORD)
        assert resolved == ("alice", ["steward"])

    async def test_no_default_role_is_a_misconfiguration(self, app_state, simple_provider):
        """An identity matching no rule must be refused, never escalated to some standing role."""
        app_state.auth_config = {"provider": "simple", "role_mapping": []}
        with pytest.raises(RuntimeError, match="default_role"):
            await _session()._resolve_user("basic", "alice", _PASSWORD)


@pytest.mark.asyncio
class TestUnsecuredDeployment:
    async def test_no_auth_makes_every_role_selectable(self, monkeypatch):
        import provisa.api.app as app_module

        monkeypatch.setattr(
            app_module, "state", _app_state(auth_config=None, auth_middleware_active=False)
        )
        user_id, roles = await _session()._resolve_user("basic", "alice", "")  # type: ignore[misc]
        assert user_id == "alice"
        assert sorted(roles) == ["analyst", "steward"]

    async def test_a_live_middleware_without_config_fails_closed(self, monkeypatch):
        import provisa.api.app as app_module

        monkeypatch.setattr(
            app_module, "state", _app_state(auth_config=None, auth_middleware_active=True)
        )
        with pytest.raises(RuntimeError, match="auth_config not configured"):
            await _session()._resolve_user("basic", "alice", "")
