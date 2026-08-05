# Copyright (c) 2026 Kenneth Stott
# Canary: 24916eec-da0e-4286-8eda-3f18a26a7e7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for AuthMiddleware."""

from __future__ import annotations

import base64

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider


class MockProvider(AuthProvider):
    """Test provider that accepts 'valid-token' and rejects everything else."""

    async def validate_token(self, token: str) -> AuthIdentity:
        if token == "valid-token":
            return AuthIdentity(
                user_id="user1",
                email="user1@example.com",
                display_name="User One",
                roles=["editor"],
                raw_claims={"department": "engineering"},
            )
        raise ValueError("Invalid token")


def _make_app(provider=None, mapping_rules=None, default_role="analyst", superuser=None):
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=mapping_rules,
        default_role=default_role,
        superuser=superuser,
    )

    @app.get("/health")
    async def _health():
        return {"status": "ok"}

    @app.get("/auth/provider-type")
    async def _provider_type():
        return {"provider": "firebase"}

    @app.get("/auth/bootstrap-status")
    async def _bootstrap_status():
        return {"unclaimed": True}

    @app.get("/setup/status")
    async def _setup_status():
        return {"needs_setup": False}

    @app.get("/test")
    async def _test_route(request: Request):
        return {
            "user_id": request.state.identity.user_id,
            "role": request.state.role,
        }

    return app


def test_no_auth_configured_anonymous_org_admin():
    app = _make_app(provider=None)
    client = TestClient(app)
    resp = client.get("/test")
    assert resp.status_code == 200
    data = resp.json()
    # Unsecured: the ANONYMOUS dev principal (engages the documented dev-mode enforcement
    # skip) with the DATA-plane default role. REQ-1327: platform_admin is control-plane
    # only — an unsecured demo need not even define it, so it can never be the default.
    assert data["user_id"] == "anonymous"
    assert data["role"] == "org_admin"


def test_no_auth_configured_header_selects_role():
    """Unsecured + explicit X-Provisa-Role → that role, still the anonymous principal."""
    app = _make_app(provider=None)
    client = TestClient(app)
    resp = client.get("/test", headers={"x-provisa-role": "analyst"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "anonymous"
    assert data["role"] == "analyst"


def test_valid_token():
    rules = [
        {"type": "exact", "claim": "department", "value": "engineering", "role": "engineer"},
    ]
    app = _make_app(provider=MockProvider(), mapping_rules=rules)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Bearer valid-token"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "user1"
    assert data["role"] == "engineer"


def test_missing_auth_header():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test")
    assert resp.status_code == 401
    assert "Missing" in resp.json()["detail"]


def test_invalid_token():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Bearer bad-token"})
    assert resp.status_code == 401
    assert "Invalid" in resp.json()["detail"]


def test_health_skips_auth():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_provider_type_skips_auth():
    # REQ-1267: the login page fetches /auth/provider-type BEFORE the user has a token,
    # to choose the sign-in UI. It must bypass the bearer gate even when auth is enforced.
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/auth/provider-type")
    assert resp.status_code == 200
    assert resp.json()["provider"] == "firebase"


def test_bootstrap_status_skips_auth():
    # REQ-1288: the login page asks whether the platform-admin slot is unclaimed BEFORE the user
    # picks a provider — the whole point is to warn them before they hold a credential.
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/auth/bootstrap-status")
    assert resp.status_code == 200
    assert resp.json()["unclaimed"] is True


def test_setup_status_skips_auth():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/setup/status")
    assert resp.status_code == 200


def test_malformed_auth_header():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Basic abc"})
    assert resp.status_code == 401
    assert "Missing" in resp.json()["detail"] or "invalid" in resp.json()["detail"].lower()


# --- REQ-125: superuser bootstrap short-circuit -----------------------------

_SU = {"username": "root", "password": "s3cr3t"}


def _basic(username: str, password: str) -> str:
    raw = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {raw}"


def test_superuser_basic_grants_platform_admin_with_bearer_provider():
    # Works even though a bearer IdP is configured.
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "s3cr3t")})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "root"
    assert data["role"] == "platform_admin"  # REQ-1297


def test_superuser_wrong_password_falls_through_to_provider():
    # Non-matching Basic creds are not superuser; the bearer provider rejects Basic.
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "wrong")})
    assert resp.status_code == 401
    # The Basic header doesn't match the Bearer scheme expected by MockProvider,
    # so the middleware returns a missing/invalid header error.
    detail = resp.json()["detail"]
    assert "Missing" in detail or "invalid" in detail.lower() or "Invalid" in detail


def test_bearer_token_still_works_when_superuser_configured():
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Bearer valid-token"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "user1"


def test_superuser_not_configured_no_short_circuit():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "s3cr3t")})
    assert resp.status_code == 401
    # Without superuser configured, Basic header is not a special case;
    # MockProvider expects Bearer so the middleware rejects as missing/invalid.
    detail = resp.json()["detail"]
    assert "Missing" in detail or "invalid" in detail.lower() or "Invalid" in detail


def test_superuser_password_from_env_secret(monkeypatch):
    # Secrets are resolved once at wiring time via resolve_superuser_config.
    from provisa.auth.superuser import resolve_superuser_config

    monkeypatch.setenv("SU_PASS", "env-pass")
    su = resolve_superuser_config({"username": "root", "password": "${env:SU_PASS}"})
    app = _make_app(provider=MockProvider(), superuser=su)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "env-pass")})
    assert resp.status_code == 200
    assert resp.json()["role"] == "platform_admin"  # REQ-1297


def test_resolve_superuser_config_fails_fast_on_unset_secret(monkeypatch):
    from provisa.auth.superuser import resolve_superuser_config

    monkeypatch.delenv("SU_PASS", raising=False)
    # An unset secret raises at startup rather than silently disabling the superuser.
    with pytest.raises(KeyError) as exc_info:
        resolve_superuser_config({"username": "root", "password": "${env:SU_PASS}"})
    assert "SU_PASS" in str(exc_info.value)


def test_resolve_superuser_config_none_when_unconfigured():
    from provisa.auth.superuser import resolve_superuser_config

    assert resolve_superuser_config(None) is None
    assert resolve_superuser_config({}) is None
    assert resolve_superuser_config({"username": "root"}) is None  # missing password


# --- REQ-1267: lazy resolver re-resolves when auth is reconfigured at runtime -----


def _resolver_settings(provider):
    return {
        "provider": provider,
        "mapping_rules": [],
        "default_role": "analyst",
        "db_pool": None,
        "admin_pool": None,
        "assignments_source": "claims",
        "default_assignments": [],
        "multitenancy": False,
        "default_org_id": "root",
        "superuser": None,
        "bootstrap_superadmin": False,
    }


def _make_resolver_app(holder, gen, monkeypatch):
    monkeypatch.setattr(AuthMiddleware, "_current_generation", lambda self: gen["v"])
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware, config_resolver=lambda: _resolver_settings(holder["provider"])
    )

    @app.get("/test")
    async def _test_route(request: Request):
        return {"user_id": request.state.identity.user_id, "role": request.state.role}

    return app


def test_resolver_reresolves_on_generation_bump(monkeypatch):
    """A server that boots unsecured and is later switched to a real provider enforces
    auth on the next request — the cached resolution invalidates when the generation
    advances (REQ-1267 runtime reconfigure / PROVISA_IDP boot deferral)."""
    holder: dict[str, AuthProvider | None] = {"provider": None}
    gen = {"v": 0}
    client = TestClient(_make_resolver_app(holder, gen, monkeypatch))

    # Boots unsecured: the anonymous dev principal (REQ-1327 — org_admin default, not
    # platform_admin; the control-plane role never defaults onto the data plane).
    assert client.get("/test").json()["user_id"] == "anonymous"

    # Reconfigure to a real provider WITHOUT bumping the generation: still cached (unsecured).
    holder["provider"] = MockProvider()
    assert client.get("/test").status_code == 200
    assert client.get("/test").json()["user_id"] == "anonymous"  # still the dev principal

    # Bump the generation: the middleware re-resolves and now enforces the provider.
    gen["v"] += 1
    assert client.get("/test").status_code == 401  # missing token now rejected
    resp = client.get("/test", headers={"Authorization": "Bearer valid-token"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "user1"


def test_check_superuser_blank_config_never_matches():
    from provisa.auth.superuser import check_superuser

    # A resolved-but-empty password cannot authenticate, even with empty input.
    assert check_superuser("root", "", {"username": "root", "password": ""}) is None
    assert check_superuser("", "", {"username": "", "password": ""}) is None


class _TwoSchemeProvider(AuthProvider):
    """REQ-124: the shape the basic provider presents — one validator per credential form."""

    @property
    def auth_scheme(self) -> str:
        return "basic"

    @property
    def token_validators(self):
        return {"basic": self.validate_token, "bearer": self.validate_session_token}

    async def validate_token(self, token: str) -> AuthIdentity:
        if token != base64.b64encode(b"alice:pw").decode():
            raise ValueError("Invalid credentials")
        return AuthIdentity(user_id="alice-basic", email=None, display_name=None, roles=[])

    async def validate_session_token(self, token: str) -> AuthIdentity:
        if token != "session-jwt":
            raise ValueError("Invalid credentials")
        return AuthIdentity(user_id="alice-session", email=None, display_name=None, roles=[])


def _two_scheme_client():
    return TestClient(_make_app(provider=_TwoSchemeProvider(), default_role="analyst"))


def test_basic_credential_still_accepted():
    creds = base64.b64encode(b"alice:pw").decode()
    resp = _two_scheme_client().get("/test", headers={"Authorization": f"Basic {creds}"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "alice-basic"


def test_session_jwt_accepted_under_bearer():
    resp = _two_scheme_client().get("/test", headers={"Authorization": "Bearer session-jwt"})
    assert resp.status_code == 200
    assert resp.json()["user_id"] == "alice-session"


def test_scheme_selects_exactly_one_validator():
    """A Basic credential presented as Bearer is rejected outright — a failed validation is
    never retried under the other scheme, which would make the header a second guess."""
    creds = base64.b64encode(b"alice:pw").decode()
    resp = _two_scheme_client().get("/test", headers={"Authorization": f"Bearer {creds}"})
    assert resp.status_code == 401


def test_unoffered_scheme_rejected():
    resp = _two_scheme_client().get("/test", headers={"Authorization": "Digest whatever"})
    assert resp.status_code == 401


def test_single_scheme_provider_unchanged():
    """A provider without token_validators still enforces its one declared scheme."""
    client = TestClient(_make_app(provider=MockProvider()))
    assert client.get("/test", headers={"Authorization": "Bearer valid-token"}).status_code == 200
    assert client.get("/test", headers={"Authorization": "Basic valid-token"}).status_code == 401
