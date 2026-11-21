# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4f2c61-7e3b-4d8a-b05c-3f6e1d92a7b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``/debug/logs`` is the platform plane's (REQ-1327).

The buffer is the process's, shared by every organization, so no organization's caller reads it:
not one without a credential, not a member or administrator of an organization (a single-tenant
org administrator, who holds ``platform_settings``, included). A platform administrator
(``platform_settings`` and ``cross_org``) does."""

# Requirements: REQ-1327, REQ-1913

from __future__ import annotations

import logging
import types

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.api.debug_logs import DebugLogBufferHandler, router
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider

ACME_RECORD = "provisa.api.org_runtime - WARNING - org acme: source pets refused"


@pytest.fixture
def buffered():
    """A buffer on the root logger holding one org's record, as the lifespan installs it."""
    handler = DebugLogBufferHandler(capacity=10)
    handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    logging.getLogger("provisa.api.org_runtime").warning("org acme: source pets refused")
    assert list(handler.buffer) == [ACME_RECORD]
    yield handler
    root.removeHandler(handler)


@pytest.fixture
def caller(monkeypatch):
    """An app serving the route to a signed-in caller holding the given rights."""
    import provisa.api.admin.capabilities as capmod

    caps: set[str] = set()
    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: caps)

    def _as(user_id: str, *rights: str) -> TestClient:
        caps.clear()
        caps.update(rights)
        app = FastAPI()

        @app.middleware("http")
        async def _identity(request: Request, call_next):
            request.state.identity = types.SimpleNamespace(user_id=user_id, roles=[])
            return await call_next(request)

        app.include_router(router)
        return TestClient(app)

    return _as


def test_a_member_of_another_org_reads_nothing(buffered, caller):
    resp = caller("bob@beta", "usage", "query_development").get("/debug/logs")
    assert resp.status_code == 403
    assert "acme" not in resp.text


def test_another_orgs_administrator_reads_nothing(buffered, caller):
    resp = caller("carol@beta", "user_management", "org_settings").get("/debug/logs")
    assert resp.status_code == 403
    assert "acme" not in resp.text


def test_a_single_tenant_org_administrator_reads_nothing(buffered, caller):
    """``platform_settings`` without ``cross_org`` is an org administrator's in a single-tenant
    deployment; the buffer is still not theirs."""
    resp = caller("dave", "user_management", "org_settings", "platform_settings").get("/debug/logs")
    assert resp.status_code == 403
    assert "acme" not in resp.text


def test_a_platform_administrator_reads_the_records(buffered, caller):
    resp = caller("root-admin", "platform_settings", "cross_org").get("/debug/logs")
    assert resp.status_code == 200
    assert resp.json() == {"count": 1, "logs": [ACME_RECORD]}


class _RejectingProvider(AuthProvider):
    provider_name = "firebase"

    async def validate_token(self, token: str) -> AuthIdentity:
        raise ValueError("Invalid token")


def test_a_caller_with_no_credential_reads_nothing(buffered, serve_deployment_org):
    """On a deployment with an auth provider, the route is behind the bearer gate."""
    serve_deployment_org("root")
    app = FastAPI()
    app.add_middleware(AuthMiddleware, provider=_RejectingProvider())
    app.include_router(router)
    resp = TestClient(app).get("/debug/logs")
    assert resp.status_code == 401
    assert "acme" not in resp.text
