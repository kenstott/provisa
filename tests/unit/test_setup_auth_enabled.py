# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1259: /setup/status exposes a runtime auth_enabled flag so the SPA's login
gate keys off the server's configured provider, not a build-time VITE_AUTH_ENABLED."""

from __future__ import annotations

import pytest

from provisa.api import setup_router
from provisa.api.setup_router import _auth_enabled, setup_status


def test_auth_enabled_helper():
    assert _auth_enabled({"provider": "firebase"}) is True
    assert _auth_enabled({"provider": "basic"}) is True
    assert _auth_enabled({"provider": "none"}) is False
    assert _auth_enabled({"provider": None}) is False
    assert _auth_enabled({}) is False
    assert _auth_enabled(None) is False


@pytest.mark.asyncio
async def test_status_demo_no_auth_is_unsecured(monkeypatch):
    monkeypatch.setattr(setup_router, "_is_demo", lambda: True)
    monkeypatch.setattr(setup_router, "_idp_override", lambda: None)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {})
    result = await setup_status()
    assert result == {"needs_setup": True, "demo_mode": True, "auth_enabled": False}


@pytest.mark.asyncio
async def test_status_demo_firebase_config_is_enforced(monkeypatch):
    monkeypatch.setattr(setup_router, "_is_demo", lambda: True)
    monkeypatch.setattr(setup_router, "_idp_override", lambda: None)
    monkeypatch.setattr(
        "provisa.api.admin._config_io.read_config",
        lambda: {"auth": {"provider": "firebase"}},
    )
    result = await setup_status()
    assert result == {"needs_setup": False, "demo_mode": True, "auth_enabled": True}


@pytest.mark.asyncio
async def test_status_non_demo_no_auth_needs_setup(monkeypatch):
    monkeypatch.setattr(setup_router, "_is_demo", lambda: False)
    monkeypatch.setattr(setup_router, "_idp_override", lambda: None)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {})
    result = await setup_status()
    assert result == {"needs_setup": True, "demo_mode": False, "auth_enabled": False}
