# Copyright (c) 2026 Kenneth Stott
# Canary: 49634deb-23ed-4ea4-8c95-45f3346eda01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for auth providers — SimpleAuthProvider with JWT round-trip."""

from __future__ import annotations

import bcrypt
import jwt
import pytest

from provisa.auth.models import AuthIdentity
from provisa.auth.providers.simple import SimpleAuthProvider
from provisa.auth.role_mapping import resolve_role

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

JWT_SECRET = "integration-test-secret"


def _hash_pw(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


@pytest.fixture
def provider():
    users = [
        {
            "username": "alice",
            "password_hash": _hash_pw("alice-pass"),
            "roles": ["analyst"],
        },
        {
            "username": "bob",
            "password_hash": _hash_pw("bob-pass"),
            "roles": ["admin", "analyst"],
        }]
    return SimpleAuthProvider(users=users, jwt_secret=JWT_SECRET)


class TestSimpleAuthValidCredentials:
    async def test_login_returns_jwt(self, provider):
        token = provider.login("alice", "alice-pass")
        assert isinstance(token, str)
        decoded = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        assert decoded["sub"] == "alice"
        assert decoded["roles"] == ["analyst"]
        assert "exp" in decoded
        assert "iat" in decoded

    async def test_login_bob_returns_jwt_with_multiple_roles(self, provider):
        token = provider.login("bob", "bob-pass")
        decoded = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        assert decoded["sub"] == "bob"
        assert set(decoded["roles"]) == {"admin", "analyst"}


class TestSimpleAuthInvalidCredentials:
    async def test_wrong_password_rejected(self, provider):
        with pytest.raises(ValueError, match="Invalid credentials"):
            provider.login("alice", "wrong-password")

    async def test_unknown_user_rejected(self, provider):
        with pytest.raises(ValueError, match="Invalid credentials"):
            provider.login("nobody", "any-pass")

    async def test_empty_password_rejected(self, provider):
        with pytest.raises(ValueError, match="Invalid credentials"):
            provider.login("alice", "")


class TestJWTValidationRoundTrip:
    async def test_create_and_verify_token(self, provider):
        token = provider.login("alice", "alice-pass")
        identity = await provider.validate_token(token)
        assert isinstance(identity, AuthIdentity)
        assert identity.user_id == "alice"
        assert identity.display_name == "alice"
        assert identity.roles == ["analyst"]

    async def test_tampered_token_rejected(self, provider):
        token = provider.login("alice", "alice-pass")
        tampered = token + "x"
        with pytest.raises(Exception):
            await provider.validate_token(tampered)

    async def test_wrong_secret_rejected(self):
        provider_a = SimpleAuthProvider(users=[], jwt_secret="secret-a")
        provider_b = SimpleAuthProvider(users=[], jwt_secret="secret-b")
        token = jwt.encode(
            {"sub": "x", "roles": []}, "secret-a", algorithm="HS256"
        )
        with pytest.raises(jwt.InvalidSignatureError):
            await provider_b.validate_token(token)


class TestAllowSimpleAuthGuard:
    """REQ-124: simple auth must be explicitly opted into via allow_simple_auth."""

    def _simple_cfg(self, **extra) -> dict:
        return {
            "provider": "simple",
            "jwt_secret": JWT_SECRET,
            "simple": {"users": []},
            **extra,
        }

    async def test_simple_without_flag_refused(self):
        from provisa.auth.wiring import build_auth_provider

        with pytest.raises(ValueError, match="allow_simple_auth"):
            build_auth_provider(self._simple_cfg())

    async def test_simple_with_flag_false_refused(self):
        from provisa.auth.wiring import build_auth_provider

        with pytest.raises(ValueError, match="allow_simple_auth"):
            build_auth_provider(self._simple_cfg(allow_simple_auth=False))

    async def test_simple_with_flag_true_builds(self):
        from provisa.auth.wiring import build_auth_provider

        provider = build_auth_provider(self._simple_cfg(allow_simple_auth=True))
        assert isinstance(provider, SimpleAuthProvider)

    async def test_other_providers_unaffected_by_flag(self):
        # A non-simple provider never consults allow_simple_auth.
        from provisa.auth.wiring import build_auth_provider

        provider = build_auth_provider(
            {
                "provider": "oauth",
                "oauth": {"discovery_url": "https://idp.example/.well-known", "client_id": "c"},
            }
        )
        from provisa.auth.providers.oauth import OAuthProvider

        assert isinstance(provider, OAuthProvider)


class TestAuthConfigDefault:
    async def test_allow_simple_auth_defaults_false(self):
        from provisa.core.models import AuthConfig

        assert AuthConfig().allow_simple_auth is False


class TestRoleMappingFromJWT:
    async def test_role_from_claims_contains_rule(self, provider):
        token = provider.login("bob", "bob-pass")
        identity = await provider.validate_token(token)
        rules = [
            {"type": "contains", "claim": "roles", "value": "admin", "role": "admin"}]
        role = resolve_role(identity, rules, default_role="viewer")
        assert role == "admin"

    async def test_role_from_claims_exact_rule(self, provider):
        token = provider.login("alice", "alice-pass")
        identity = await provider.validate_token(token)
        rules = [
            {"type": "exact", "claim": "sub", "value": "alice", "role": "power-user"}]
        role = resolve_role(identity, rules, default_role="viewer")
        assert role == "power-user"

    async def test_default_role_when_no_rule_matches(self, provider):
        token = provider.login("alice", "alice-pass")
        identity = await provider.validate_token(token)
        rules = [
            {"type": "exact", "claim": "sub", "value": "charlie", "role": "admin"}]
        role = resolve_role(identity, rules, default_role="viewer")
        assert role == "viewer"

    async def test_empty_rules_returns_default(self, provider):
        token = provider.login("alice", "alice-pass")
        identity = await provider.validate_token(token)
        role = resolve_role(identity, [], default_role="analyst")
        assert role == "analyst"
