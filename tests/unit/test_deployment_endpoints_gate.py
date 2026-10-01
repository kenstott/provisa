# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3a9e52-d184-4b60-a2f7-59e0c8b61d34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The deployment-wide admin endpoints are the platform administrator's in every deployment
(REQ-1913).

The config file, the federation engine, cache storage, encryption, the secrets service, the auth
provider and the query-engine lifecycle are deployment settings as much as the settings catalog
is. ``platform_settings`` alone — what a single-tenant org administrator holds — does not open
them; a platform administrator (``platform_settings`` and ``cross_org``, or the platform bypass)
does."""

# Requirements: REQ-1913, REQ-1337

from __future__ import annotations

import types

import pytest

from provisa.api.admin import settings_router
from provisa.api.errors import ApiError

DEPLOYMENT_ENDPOINTS = [
    "download_config",
    "download_live_config",
    "config_diff",
    "config_patch",
    "upload_config",
    "set_federation_engine",
    "get_cache_storage",
    "set_cache_storage",
    "get_federation_engine",
    "get_encryption",
    "set_encryption",
    "get_secrets_service",
    "set_secrets_service",
    "generate_encryption_key",
    "get_auth",
    "set_auth",
    "reload_query_engine_catalog",
    "restart_query_engine",
]


@pytest.fixture
def caller(monkeypatch):
    import provisa.api.admin.capabilities as capmod

    caps: set[str] = set()
    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: caps)

    def _as(*rights: str):
        caps.clear()
        caps.update(rights)

        async def _json():
            raise AssertionError("the body was read before the caller's right was checked")

        return types.SimpleNamespace(
            state=types.SimpleNamespace(identity=types.SimpleNamespace(user_id="alice", roles=[])),
            json=_json,
            body=_json,
        )

    return _as


@pytest.mark.parametrize("endpoint", DEPLOYMENT_ENDPOINTS)
async def test_a_single_tenant_org_administrator_is_refused(caller, endpoint):
    request = caller("platform_settings", "org_settings")
    with pytest.raises(ApiError) as err:
        await getattr(settings_router, endpoint)(request)
    assert (err.value.status_code, err.value.code) == (403, "platform.control_plane_role_required")


@pytest.mark.parametrize("endpoint", DEPLOYMENT_ENDPOINTS)
async def test_a_platform_administrator_passes_the_gate(caller, endpoint):
    """Past the gate the handler goes on to its work — here that ends at the stand-in request's
    body, or at state this test does not provide; what it must not end at is the gate's 403."""
    request = caller("platform_settings", "cross_org")
    try:
        await getattr(settings_router, endpoint)(request)
    except ApiError as err:
        assert err.code != "platform.control_plane_role_required", endpoint
    except Exception:  # noqa: BLE001 - anything else is the handler's own work, past the gate
        pass


def test_every_gated_handler_in_the_router_is_covered():
    """A handler added to this router with a platform gate has to be listed above."""
    import inspect

    gated = {
        name
        for name, fn in vars(settings_router).items()
        if inspect.iscoroutinefunction(fn)
        and fn.__module__ == settings_router.__name__
        and "require_deployment_settings(request)" in inspect.getsource(fn)
        and name != "update_settings"  # gated per block; covered by test_settings_scope_split
    }
    assert gated == set(DEPLOYMENT_ENDPOINTS)
    for name, fn in vars(settings_router).items():
        if inspect.iscoroutinefunction(fn) and fn.__module__ == settings_router.__name__:
            assert "require_platform_settings(request)" not in inspect.getsource(fn), name
