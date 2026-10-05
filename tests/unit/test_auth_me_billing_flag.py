# Copyright (c) 2026 Kenneth Stott
# Canary: 5e91b3d7-2c48-4a06-8f52-b0d19c7a63e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1469: ``/auth/me`` reports whether this deployment has a billing surface at all.

``/billing`` is mounted by the commercial plugin, so a self-hosted install serves none of it. The
UI needs that deployment fact to decide whether to show a Billing nav entry; without it the link
renders everywhere and leads to a 404 on every self-hosted deployment.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


class _Result:
    @staticmethod
    def fetchall():
        return [("analyst",)]


class _Conn:
    async def execute_core(self, _stmt):
        return _Result()


class _Db:
    def acquire(self):
        return self

    async def __aenter__(self):
        return _Conn()

    async def __aexit__(self, *_exc):
        return False


class _BindsTheOrg:
    """Stands in for the org-routing middleware: the request is acme's, bound (REQ-1266)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        from provisa.core.request_context import reset_current_org, set_current_org

        token = set_current_org("acme")
        try:
            await self.app(scope, receive, send)
        finally:
            reset_current_org(token)


@pytest.fixture
def client(monkeypatch, serve_deployment_org):
    from provisa.api import app as app_module
    from provisa.api.auth_router import router

    # Unsecured branch: auth_config is None, so /me answers without an identity or the control
    # plane, and the billing flag is the only thing under test.
    serve_deployment_org("acme")
    monkeypatch.setattr(app_module.state, "auth_config", None, raising=False)
    monkeypatch.setattr(app_module.state, "tenant_db", _Db(), raising=False)
    monkeypatch.setattr(app_module.state, "model_db", app_module.state.tenant_db, raising=False)

    api = FastAPI()
    api.include_router(router)
    api.add_middleware(_BindsTheOrg)
    return TestClient(api, raise_server_exceptions=False)


def _set_plugin(monkeypatch, module):
    """Force the commerce seam's memoized answer, which is what /me reads."""
    import provisa.core.commerce as commerce

    monkeypatch.setattr(commerce, "_PLUGIN", module, raising=False)
    monkeypatch.setattr(commerce, "_LOADED", True, raising=False)


def test_billing_is_true_where_the_commercial_plugin_is_installed(client, monkeypatch):
    _set_plugin(monkeypatch, SimpleNamespace())
    assert client.get("/auth/me").json()["billing"] is True


def test_billing_is_false_on_a_self_hosted_deployment(client, monkeypatch):
    _set_plugin(monkeypatch, None)
    assert client.get("/auth/me").json()["billing"] is False
