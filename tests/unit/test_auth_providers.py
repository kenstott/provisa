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

import base64

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


class TestKeycloakRoles:
    """REQ-122: Keycloak maps realm roles AND this client's roles."""

    def _provider(self, monkeypatch, decoded: dict):
        from provisa.auth.providers import keycloak as kc_mod
        from provisa.auth.providers.keycloak import KeycloakAuthProvider

        provider = KeycloakAuthProvider(
            server_url="https://kc.example", realm="r", client_id="provisa-api"
        )

        class _Key:
            key = "k"

        class _JwksClient:
            def get_signing_key_from_jwt(self, token):
                return _Key()

        monkeypatch.setattr(provider, "_get_jwks_client", lambda: _JwksClient())
        monkeypatch.setattr(kc_mod.jwt, "decode", lambda *a, **k: decoded)
        return provider

    async def test_realm_roles_only(self, monkeypatch):
        provider = self._provider(
            monkeypatch,
            {"sub": "u1", "realm_access": {"roles": ["analyst", "viewer"]}},
        )
        identity = await provider.validate_token("t")
        assert identity.roles == ["analyst", "viewer"]

    async def test_client_roles_only(self, monkeypatch):
        provider = self._provider(
            monkeypatch,
            {"sub": "u1", "resource_access": {"provisa-api": {"roles": ["editor"]}}},
        )
        identity = await provider.validate_token("t")
        assert identity.roles == ["editor"]

    async def test_realm_and_client_roles_merged(self, monkeypatch):
        provider = self._provider(
            monkeypatch,
            {
                "sub": "u1",
                "realm_access": {"roles": ["analyst"]},
                "resource_access": {"provisa-api": {"roles": ["editor"]}},
            },
        )
        identity = await provider.validate_token("t")
        assert identity.roles == ["analyst", "editor"]  # realm first, then client

    async def test_overlap_deduplicated_order_preserved(self, monkeypatch):
        provider = self._provider(
            monkeypatch,
            {
                "sub": "u1",
                "realm_access": {"roles": ["analyst", "shared"]},
                "resource_access": {"provisa-api": {"roles": ["shared", "editor"]}},
            },
        )
        identity = await provider.validate_token("t")
        assert identity.roles == ["analyst", "shared", "editor"]

    async def test_other_clients_roles_ignored(self, monkeypatch):
        # Only the configured client's roles are mapped, not other clients'.
        provider = self._provider(
            monkeypatch,
            {
                "sub": "u1",
                "resource_access": {
                    "provisa-api": {"roles": ["editor"]},
                    "some-other-client": {"roles": ["should-not-appear"]},
                },
            },
        )
        identity = await provider.validate_token("t")
        assert identity.roles == ["editor"]

    async def test_no_roles(self, monkeypatch):
        provider = self._provider(monkeypatch, {"sub": "u1"})
        identity = await provider.validate_token("t")
        assert identity.roles == []


class TestFirebaseProvider:
    """REQ-121: Firebase ID token validation maps uid/email/name/roles."""

    def _provider(self, monkeypatch, decoded: dict):
        from provisa.auth.providers import firebase as fb_mod
        from provisa.auth.providers.firebase import FirebaseAuthProvider

        class _Auth:
            def verify_id_token(self, token):
                return decoded

        # Bypass __init__ (which needs firebase-admin); validate_token only uses
        # the module-level firebase_auth handle.
        provider = FirebaseAuthProvider.__new__(FirebaseAuthProvider)
        monkeypatch.setattr(fb_mod, "firebase_auth", _Auth())
        return provider

    async def test_validate_token_maps_identity(self, monkeypatch):
        provider = self._provider(
            monkeypatch,
            {"uid": "u-123", "email": "a@b.com", "name": "Ada", "roles": ["analyst"]},
        )
        identity = await provider.validate_token("t")
        assert identity.user_id == "u-123"
        assert identity.email == "a@b.com"
        assert identity.display_name == "Ada"
        assert identity.roles == ["analyst"]

    async def test_validate_token_defaults_when_claims_absent(self, monkeypatch):
        provider = self._provider(monkeypatch, {"uid": "anon-1"})
        identity = await provider.validate_token("t")
        assert identity.user_id == "anon-1"
        assert identity.email is None
        assert identity.roles == []


class TestOAuthProvider:
    """REQ-123: generic OIDC — discovery → JWKS → JWT; configurable role claim."""

    def _provider(self, monkeypatch, decoded: dict, role_claim: str = "roles"):
        from provisa.auth.providers import oauth as oa_mod
        from provisa.auth.providers.oauth import OAuthProvider

        provider = OAuthProvider(
            discovery_url="https://idp.example/.well-known/openid-configuration",
            client_id="provisa",
            role_claim=role_claim,
        )

        class _Key:
            key = "k"

        class _JwksClient:
            def get_signing_key_from_jwt(self, token):
                return _Key()

        monkeypatch.setattr(provider, "_get_jwks_client", lambda: _JwksClient())
        monkeypatch.setattr(oa_mod.jwt, "decode", lambda *a, **k: decoded)
        return provider

    async def test_roles_as_list(self, monkeypatch):
        provider = self._provider(
            monkeypatch, {"sub": "u", "email": "u@x.io", "name": "U", "roles": ["a", "b"]}
        )
        identity = await provider.validate_token("t")
        assert identity.user_id == "u"
        assert identity.email == "u@x.io"
        assert identity.roles == ["a", "b"]

    async def test_role_as_string_coerced_to_list(self, monkeypatch):
        provider = self._provider(monkeypatch, {"sub": "u", "roles": "solo"})
        identity = await provider.validate_token("t")
        assert identity.roles == ["solo"]

    async def test_custom_role_claim(self, monkeypatch):
        provider = self._provider(
            monkeypatch, {"sub": "u", "groups": ["g1"]}, role_claim="groups"
        )
        identity = await provider.validate_token("t")
        assert identity.roles == ["g1"]

    async def test_no_roles(self, monkeypatch):
        provider = self._provider(monkeypatch, {"sub": "u"})
        identity = await provider.validate_token("t")
        assert identity.roles == []


class TestBasicProvider:
    """REQ-120: Basic provider validates against local_users with bcrypt."""

    def _provider(self, row):
        from provisa.auth.providers.basic import BasicAuthProvider

        class _Conn:
            async def fetchrow(self, query, *args):
                return row

        class _Ctx:
            async def __aenter__(self):
                return _Conn()

            async def __aexit__(self, *a):
                return False

        class _Pool:
            def acquire(self):
                return _Ctx()

        return BasicAuthProvider(db_pool=_Pool())

    def _creds(self, username: str, password: str) -> str:
        return base64.b64encode(f"{username}:{password}".encode()).decode()

    async def test_valid_credentials(self):
        row = {
            "id": "user-1",
            "username": "alice",
            "password_hash": _hash_pw("pw"),
            "email": "alice@x.io",
            "display_name": "Alice",
            "attributes": {"team": "data"},
        }
        provider = self._provider(row)
        identity = await provider.validate_token(self._creds("alice", "pw"))
        assert identity.user_id == "user-1"
        assert identity.email == "alice@x.io"
        assert identity.raw_claims["username"] == "alice"
        assert identity.raw_claims["team"] == "data"

    async def test_wrong_password_rejected(self):
        row = {
            "id": "user-1",
            "username": "alice",
            "password_hash": _hash_pw("pw"),
            "email": None,
            "display_name": None,
            "attributes": None,
        }
        provider = self._provider(row)
        with pytest.raises(ValueError, match="Invalid credentials"):
            await provider.validate_token(self._creds("alice", "wrong"))

    async def test_unknown_user_rejected(self):
        provider = self._provider(None)
        with pytest.raises(ValueError, match="Invalid credentials"):
            await provider.validate_token(self._creds("nobody", "pw"))

    async def test_malformed_token_rejected(self):
        provider = self._provider(None)
        with pytest.raises(ValueError, match="Invalid credentials"):
            await provider.validate_token("not-base64-without-colon")


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
