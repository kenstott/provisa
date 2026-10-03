# Copyright (c) 2026 Kenneth Stott
# Canary: 5c8a1e07-9b23-4f6a-8d41-a7e2b0c94f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1349/REQ-1266: the settings surfaces split by WHOSE setting each block is.

The failures worth pinning are the quiet ones: a deployment-wide block travelling back to an org
administrator who cannot write it, an org writing a deployment setting through the org door, and a
per-org value resolving from a process-global scalar so the org that saved last governs the rest.
"""

# Requirements: REQ-1266, REQ-1349

from __future__ import annotations

import types

import pytest

from provisa.api.errors import ApiError

ORG_ID = "acme"


def _request(caps: set[str]):
    return types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[]), active_org_id=ORG_ID
        )
    ), caps


@pytest.fixture
def caller(monkeypatch):
    """Drive the handlers as an identity holding exactly the rights a case names."""
    import provisa.api.admin.capabilities as capmod

    caps: set[str] = set()
    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: caps)

    def _as(*rights: str):
        caps.clear()
        caps.update(rights)
        return types.SimpleNamespace(
            state=types.SimpleNamespace(
                identity=types.SimpleNamespace(user_id="alice", roles=[]), active_org_id=ORG_ID
            )
        )

    return _as


class _FakeState:
    """Enough of AppState for the settings handlers, with the routed TTL property's real body."""

    def __init__(self):
        self.settings_overrides: dict = {}
        self.deployment_cache_default_ttl = 300
        self.config_live_export = False
        self.tenant_db = object()
        self.model_db = self.tenant_db

    @property
    def response_cache_default_ttl(self) -> int:
        override = self.settings_overrides.get("cache") or {}
        ttl = override.get("default_ttl")
        return int(ttl) if ttl is not None else self.deployment_cache_default_ttl


@pytest.fixture
def app_state(monkeypatch):
    st = _FakeState()
    monkeypatch.setattr("provisa.api.app.state", st, raising=False)
    return st


class TestOrgOverrideDoor:
    async def test_naming_conventions_are_not_org_overridable(self):
        """The org door owns the DOMAIN MODE alone — the conventions configure a process-global
        module, so reaching them through an org write would rename every other org's schema."""
        from provisa.core.org_settings import write_org_overrides

        with pytest.raises(ValueError, match="naming.convention"):
            await write_org_overrides(
                object(), {"naming": {"convention": "camel"}}, updated_by="alice"
            )

    async def test_domain_mode_is_org_overridable(self, monkeypatch):
        from provisa.core import org_settings

        seen: dict = {}

        class _Conn:
            async def upsert(self, table, values, **kw):
                seen[values["key"]] = values["value"]

            async def execute_core(self, *a, **kw):
                raise AssertionError("no delete expected")

        class _Db:
            def acquire(self):
                from contextlib import asynccontextmanager

                @asynccontextmanager
                async def _cm():
                    yield _Conn()

                return _cm()

        written = await org_settings.write_org_overrides(
            _Db(), {"naming": {"use_domains": False, "default_domain": "core"}}, updated_by="alice"
        )
        assert written == ["naming"]
        assert seen["naming"] == {"use_domains": False, "default_domain": "core"}


class TestGetSettingsScope:
    async def test_platform_caller_sees_every_block(self, caller, app_state):
        from provisa.api.admin.settings_router import _PLATFORM_BLOCKS, get_settings

        # REQ-1913: the platform administrator — `platform_settings` alone is what a single-tenant
        # org administrator holds, and no longer reads the deployment blocks.
        body = await get_settings(caller("platform_settings", "cross_org"))
        for block in _PLATFORM_BLOCKS:
            assert block in body
        assert body["features"]["platform_settings"] is True
        assert body["naming"]["convention"]

    async def test_org_caller_sees_only_org_scope(self, caller, app_state):
        from provisa.api.admin.settings_router import _PLATFORM_BLOCKS, get_settings

        body = await get_settings(caller("org_settings"))
        for block in _PLATFORM_BLOCKS:
            assert block not in body
        assert body["features"]["platform_settings"] is False
        # The domain mode stays — ordinary pages read it to decide whether domain UI exists.
        assert set(body["naming"]) == {"use_domains", "default_domain"}
        assert "redirect" in body and "cache" in body

    async def test_org_overrides_resolve_on_the_read(self, caller, app_state):
        from provisa.api.admin.settings_router import get_settings

        app_state.settings_overrides = {"cache": {"default_ttl": 42}}
        body = await get_settings(caller("org_settings"))
        assert body["cache"]["default_ttl"] == 42


class TestPutSettingsScope:
    async def test_org_blocks_need_only_the_org_right(self, caller, app_state, monkeypatch):
        import provisa.api.admin.settings_router as router

        written: dict = {}

        async def _read(_db):
            return dict(app_state.settings_overrides)

        async def _write(_db, updates, *, updated_by):
            written.update(updates)
            app_state.settings_overrides.update({k: v for k, v in updates.items() if v is not None})
            return list(updates)

        monkeypatch.setattr("provisa.core.org_settings.read_org_overrides", _read)
        monkeypatch.setattr("provisa.core.org_settings.write_org_overrides", _write)

        request = caller("org_settings")
        request.json = lambda: _coro({"cache": {"default_ttl": 60}, "redirect": {"threshold": 5}})
        body = await router.update_settings(request)
        assert body["success"] is True
        assert written["cache"] == {"default_ttl": 60}
        assert written["redirect"] == {"threshold": 5}
        # The runtime copy the query path reads was refreshed, not left on the pre-save value.
        assert app_state.settings_overrides["cache"]["default_ttl"] == 60
        assert app_state.response_cache_default_ttl == 60

    async def test_platform_block_still_needs_the_platform_right(self, caller, app_state):
        import provisa.api.admin.settings_router as router

        request = caller("org_settings")
        request.json = lambda: _coro({"sampling": {"default_sample_size": 5}})
        with pytest.raises(ApiError) as exc:
            await router.update_settings(request)
        assert exc.value.status_code == 403

    async def test_a_refused_body_writes_none_of_it(self, caller, app_state, monkeypatch):
        """A body carrying an org block and a deployment block from a caller holding only the org
        right is refused WHOLE. The org block used to be written before the platform right was
        checked, so the 403 left half the body applied."""
        import provisa.api.admin.settings_router as router

        written: dict = {}

        async def _read(_db):
            return {}

        async def _write(_db, updates, *, updated_by):
            written.update(updates)
            return list(updates)

        monkeypatch.setattr("provisa.core.org_settings.read_org_overrides", _read)
        monkeypatch.setattr("provisa.core.org_settings.write_org_overrides", _write)

        request = caller("org_settings")
        request.json = lambda: _coro(
            {"cache": {"default_ttl": 60}, "sampling": {"default_sample_size": 5}}
        )
        with pytest.raises(ApiError) as exc:
            await router.update_settings(request)
        assert exc.value.status_code == 403
        assert written == {}


# The rights each administrator holds. A single-tenant deployment grants org_admin
# ``platform_settings`` (apply_tenancy_role_grants); ``cross_org`` is platform_admin's alone.
PLATFORM_ADMIN = ("platform_settings", "cross_org")
SINGLE_TENANT_ORG_ADMIN = ("platform_settings", "org_settings")


class TestDeploymentSettingsArePlatformAdminsEverywhere:
    """REQ-1913: the deployment-wide blocks of ``/admin/settings`` are the platform
    administrator's in every deployment — the same rule as the settings catalog, which writes the
    same stored keys. A single-tenant org administrator keeps the org's own blocks."""

    async def test_single_tenant_org_admin_does_not_read_the_deployment_blocks(
        self, caller, app_state
    ):
        from provisa.api.admin.settings_router import _PLATFORM_BLOCKS, get_settings

        body = await get_settings(caller(*SINGLE_TENANT_ORG_ADMIN))
        for block in _PLATFORM_BLOCKS:
            assert block not in body
        assert body["features"]["deployment_settings"] is False
        # The right itself is unchanged: it still opens the other platform surfaces.
        assert body["features"]["platform_settings"] is True
        assert set(body["naming"]) == {"use_domains", "default_domain"}
        assert "redirect" in body and "cache" in body

    async def test_platform_admin_reads_every_block(self, caller, app_state):
        from provisa.api.admin.settings_router import _PLATFORM_BLOCKS, get_settings

        body = await get_settings(caller(*PLATFORM_ADMIN))
        for block in _PLATFORM_BLOCKS:
            assert block in body
        assert body["features"]["deployment_settings"] is True

    async def test_single_tenant_org_admin_cannot_write_a_deployment_block(self, caller, app_state):
        import provisa.api.admin.settings_router as router

        request = caller(*SINGLE_TENANT_ORG_ADMIN)
        request.json = lambda: _coro({"limits": {"default_row_limit": 5}})
        with pytest.raises(ApiError) as exc:
            await router.update_settings(request)
        assert (exc.value.status_code, exc.value.code) == (
            403,
            "platform.control_plane_role_required",
        )

    async def test_single_tenant_org_admin_still_writes_the_orgs_own_blocks(
        self, caller, app_state, monkeypatch
    ):
        import provisa.api.admin.settings_router as router

        written: dict = {}

        async def _read(_db):
            return dict(app_state.settings_overrides)

        async def _write(_db, updates, *, updated_by):
            written.update(updates)
            return list(updates)

        monkeypatch.setattr("provisa.core.org_settings.read_org_overrides", _read)
        monkeypatch.setattr("provisa.core.org_settings.write_org_overrides", _write)
        request = caller(*SINGLE_TENANT_ORG_ADMIN)
        request.json = lambda: _coro({"cache": {"default_ttl": 60}, "redirect": {"threshold": 5}})
        body = await router.update_settings(request)
        assert body["success"] is True
        assert written == {"cache": {"default_ttl": 60}, "redirect": {"threshold": 5}}

    async def test_a_mixed_body_from_a_single_tenant_org_admin_stores_nothing(
        self, caller, app_state, monkeypatch
    ):
        import provisa.api.admin.settings_router as router

        written: dict = {}

        async def _read(_db):
            return {}

        async def _write(_db, updates, *, updated_by):
            written.update(updates)
            return list(updates)

        monkeypatch.setattr("provisa.core.org_settings.read_org_overrides", _read)
        monkeypatch.setattr("provisa.core.org_settings.write_org_overrides", _write)
        request = caller(*SINGLE_TENANT_ORG_ADMIN)
        request.json = lambda: _coro(
            {"cache": {"default_ttl": 60}, "sampling": {"default_sample_size": 5}}
        )
        with pytest.raises(ApiError) as exc:
            await router.update_settings(request)
        assert exc.value.status_code == 403
        assert written == {}


async def _coro(value):
    return value
