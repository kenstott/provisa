# Copyright (c) 2026 Kenneth Stott
# Canary: 39a85e27-4389-46c5-b7e1-650a73aa2479
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1267: /setup/status exposes a runtime auth_enabled flag so the SPA's login
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
    monkeypatch.delenv("PROVISA_NAMED_DEMO_DIR", raising=False)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {})
    result = await setup_status(_request())
    assert result == {
        "needs_setup": True,
        "demo_mode": True,
        "auth_enabled": False,
        "multitenancy": False,
        "named_demo": None,  # REQ-2010
        "example_queries": [],  # REQ-2009: no named demo was started
        "demo_tour": [],  # REQ-2010
        "demo_landing": None,  # REQ-2010: no tour, no landing
        "demo_notebook": [],  # REQ-2084: no notebook
        "demo_org_files": [],  # REQ-2101: no org files
    }


@pytest.mark.asyncio
async def test_status_demo_firebase_config_is_enforced(monkeypatch):
    monkeypatch.setattr(setup_router, "_is_demo", lambda: True)
    monkeypatch.setattr(setup_router, "_idp_override", lambda: None)
    monkeypatch.delenv("PROVISA_NAMED_DEMO_DIR", raising=False)
    monkeypatch.setattr(
        "provisa.api.admin._config_io.read_config",
        lambda: {"auth": {"provider": "firebase"}},
    )
    result = await setup_status(_request())
    assert result == {
        "needs_setup": False,
        "demo_mode": True,
        "auth_enabled": True,
        "multitenancy": False,
        "named_demo": None,  # REQ-2010
        "example_queries": [],  # REQ-2009: no named demo was started
        "demo_tour": [],  # REQ-2010
        "demo_landing": None,  # REQ-2010: no tour, no landing
        "demo_notebook": [],  # REQ-2084: no notebook
        "demo_org_files": [],  # REQ-2101: no org files
    }


@pytest.mark.asyncio
async def test_status_non_demo_no_auth_needs_setup(monkeypatch):
    monkeypatch.setattr(setup_router, "_is_demo", lambda: False)
    monkeypatch.setattr(setup_router, "_idp_override", lambda: None)
    monkeypatch.delenv("PROVISA_NAMED_DEMO_DIR", raising=False)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {})
    result = await setup_status(_request())
    assert result == {
        "needs_setup": True,
        "demo_mode": False,
        "auth_enabled": False,
        "multitenancy": False,
        "named_demo": None,  # REQ-2010
        "example_queries": [],  # REQ-2009: no named demo was started
        "demo_tour": [],  # REQ-2010
        "demo_landing": None,  # REQ-2010: no tour, no landing
        "demo_notebook": [],  # REQ-2084: no notebook
        "demo_org_files": [],  # REQ-2101: no org files
    }


def _request(headers: dict[str, str] | None = None):
    """A request to /setup/status, with ``headers``."""
    from starlette.requests import Request

    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request({"type": "http", "method": "GET", "path": "/setup/status", "headers": raw})
