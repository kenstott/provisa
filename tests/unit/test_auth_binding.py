# Copyright (c) 2026 Kenneth Stott
# Canary: f657cfb0-4f0f-4066-baf7-bb4d0b955681
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A configured provider is the deployment's auth on every surface, from the moment it is bound.

``create_app`` installs the auth middleware before any config is loaded, so nothing it decides
at construction can say whether a provider is configured. ``bind_auth_config`` is where the
provider becomes known, and it is what pgwire, Bolt, Flight and gRPC read; ``/auth/login`` reads
the same state per request.
"""

from __future__ import annotations

import bcrypt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.auth.wiring import bind_auth_config
from tests.platform_plane import platform_db


class _State:
    auth_config = None
    auth_middleware_active = False
    auth_reconfig_generation = 0
    admin_db = None


class TestBindAuthConfig:
    def test_a_configured_provider_turns_auth_on(self):
        state = _State()
        bind_auth_config(state, {"provider": "basic", "jwt_secret": "s" * 48})
        assert state.auth_middleware_active is True
        assert state.auth_config["provider"] == "basic"
        assert state.auth_reconfig_generation == 1

    @pytest.mark.parametrize("raw", [None, {"provider": "none"}])
    def test_no_provider_leaves_it_off(self, raw):
        state = _State()
        state.auth_middleware_active = True
        bind_auth_config(state, raw)
        assert state.auth_middleware_active is False
        assert state.auth_config is None
        assert state.auth_reconfig_generation == 1

    def test_the_app_binds_it_where_it_loads_the_config(self):
        """The lifespan's config load is the one place the auth section is read."""
        import inspect

        import provisa.api.app as app_module

        source = inspect.getsource(app_module)
        assert 'bind_auth_config(state, raw_config.get("auth"))' in source
        assert "state.auth_config = (" not in source


_PASSWORD = "the-right-password"


def _simple_config() -> dict:
    return {
        "provider": "simple",
        "allow_simple_auth": True,
        "jwt_secret": "s" * 48,
        "simple": {
            "users": [
                {
                    "username": "ana",
                    "password_hash": bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode(),
                    "roles": ["analyst"],
                }
            ]
        },
    }


@pytest.fixture()
def client(monkeypatch):
    from provisa.api.app import state
    from provisa.auth.login_router import router

    monkeypatch.setattr(state, "admin_db", platform_db())
    app = FastAPI()
    app.include_router(router)
    return TestClient(app), state


class TestLoginRoute:
    def test_it_answers_for_the_provider_bound_now(self, client, monkeypatch):
        http, state = client
        monkeypatch.setattr(state, "auth_config", _simple_config())
        ok = http.post("/auth/login", json={"username": "ana", "password": _PASSWORD})
        assert ok.status_code == 200, ok.text
        assert ok.json()["token_type"] == "bearer"
        bad = http.post("/auth/login", json={"username": "ana", "password": "wrong"})
        assert bad.status_code == 401

    def test_an_unsecured_deployment_has_no_sign_in(self, client, monkeypatch):
        http, state = client
        monkeypatch.setattr(state, "auth_config", None)
        resp = http.post("/auth/login", json={"username": "ana", "password": _PASSWORD})
        assert resp.status_code == 404

    def test_a_provider_without_passwords_has_no_sign_in(self, client, monkeypatch):
        http, state = client
        monkeypatch.setattr(
            state,
            "auth_config",
            {
                "provider": "oidc",
                "oidc": {"discovery_url": "https://idp.example/.well-known/x", "client_id": "c"},
            },
        )
        resp = http.post("/auth/login", json={"username": "ana", "password": _PASSWORD})
        assert resp.status_code == 404

    def test_the_app_mounts_it_whatever_the_config(self):
        import inspect

        import provisa.api.app as app_module

        assert "app.include_router(login_router)" in inspect.getsource(app_module.create_app)


class TestRegistration:
    """REQ-1265: under the chart's auth.provider: local the break-glass account is the only
    sign-in, so /auth/register creates nothing."""

    async def test_registration_turned_off_is_refused_before_any_account_is_written(
        self, monkeypatch
    ):
        from types import SimpleNamespace
        from typing import Any, cast

        from provisa.api.app import state
        from provisa.api.auth_router import RegisterRequest, register
        from provisa.api.errors import ApiError

        monkeypatch.setattr(
            state,
            "config",
            SimpleNamespace(auth={"provider": "basic", "allow_registration": False}),
        )
        monkeypatch.setattr(state, "admin_db", None)  # a write would fail on this, loudly
        with pytest.raises(ApiError) as refused:
            # A request with no identity: the one the middleware lets through to this handler.
            no_identity = cast("Any", SimpleNamespace(state=SimpleNamespace()))
            await register(RegisterRequest(username="mallory", password="pw"), no_identity)
        assert refused.value.status_code == 403
        assert refused.value.code == "auth.registration_disabled"
