# Copyright (c) 2026 Kenneth Stott
# Canary: ef33c156-4128-4a77-ab77-3e8dcf7f2a66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1472: the break-glass browser sign-in, mounted for every provider."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.api.auth_router import router
from provisa.auth.superuser import validate_superuser_session

_SECRET = "unit-test-superuser-session-secret-padded!"
_AUTH_CONFIG = {
    "provider": "firebase",
    "jwt_secret": _SECRET,
    "superuser": {"username": "root", "password": "s3cr3t"},
}


@pytest.fixture
def client(monkeypatch):
    """The router alone — the endpoint reads its credentials from state, not from a provider."""
    from provisa.api import app as app_mod

    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr(app_mod.state, "auth_config", _AUTH_CONFIG, raising=False)
    return TestClient(app)


def test_superuser_login_issues_a_session_the_middleware_accepts(client):
    resp = client.post("/auth/superuser-login", json={"username": "root", "password": "s3cr3t"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["token_type"] == "bearer"
    identity = validate_superuser_session(body["access_token"], _AUTH_CONFIG["superuser"], _SECRET)
    assert identity is not None
    assert identity.user_id == "root"


def test_wrong_password_is_401(client):
    resp = client.post("/auth/superuser-login", json={"username": "root", "password": "nope"})
    assert resp.status_code == 401


def test_unknown_username_is_401(client):
    resp = client.post("/auth/superuser-login", json={"username": "someone", "password": "s3cr3t"})
    assert resp.status_code == 401


def test_no_superuser_configured_is_404(monkeypatch):
    from provisa.api import app as app_mod

    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr(
        app_mod.state, "auth_config", {"provider": "firebase", "jwt_secret": _SECRET}, raising=False
    )
    resp = TestClient(app).post(
        "/auth/superuser-login", json={"username": "root", "password": "s3cr3t"}
    )
    assert resp.status_code == 404


def test_missing_jwt_secret_is_503_not_an_unsigned_token(monkeypatch):
    """No signing key means no session — never a token signed with a guessable default."""
    from provisa.api import app as app_mod

    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr(
        app_mod.state,
        "auth_config",
        {"provider": "firebase", "superuser": {"username": "root", "password": "s3cr3t"}},
        raising=False,
    )
    resp = TestClient(app).post(
        "/auth/superuser-login", json={"username": "root", "password": "s3cr3t"}
    )
    assert resp.status_code == 503
    assert "jwt_secret" in resp.json()["detail"]
