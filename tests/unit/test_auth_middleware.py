# Copyright (c) 2025 Kenneth Stott
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


def _make_app(provider=None, mapping_rules=None, default_role="analyst"):
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=mapping_rules,
        default_role=default_role,
    )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/test")
    async def test_route(request: Request):
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
