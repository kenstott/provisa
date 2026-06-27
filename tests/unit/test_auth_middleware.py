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

    @app.get("/test")
    async def _test_route(request: Request):
        return {
            "user_id": request.state.identity.user_id,
            "role": request.state.role,
        }

    return app


def test_no_auth_configured_backward_compat():
    app = _make_app(provider=None)
    client = TestClient(app)
    resp = client.get("/test")
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "anonymous"
    assert data["role"] == "admin"


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


def test_malformed_auth_header():
    app = _make_app(provider=MockProvider())
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": "Basic abc"})
    assert resp.status_code == 401
    assert "Missing" in resp.json()["detail"] or "invalid" in resp.json()["detail"].lower()


# --- REQ-125: superuser bootstrap short-circuit -----------------------------

import base64

_SU = {"username": "root", "password": "s3cr3t"}


def _basic(username: str, password: str) -> str:
    raw = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {raw}"


def test_superuser_basic_grants_admin_with_bearer_provider():
    # Works even though a bearer IdP is configured.
    app = _make_app(provider=MockProvider(), superuser=_SU)
    client = TestClient(app)
    resp = client.get("/test", headers={"Authorization": _basic("root", "s3cr3t")})
    assert resp.status_code == 200
    data = resp.json()
    assert data["user_id"] == "root"
    assert data["role"] == "admin"


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
    assert resp.json()["role"] == "admin"


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


def test_check_superuser_blank_config_never_matches():
    from provisa.auth.superuser import check_superuser

    # A resolved-but-empty password cannot authenticate, even with empty input.
    assert check_superuser("root", "", {"username": "root", "password": ""}) is None
    assert check_superuser("", "", {"username": "", "password": ""}) is None
