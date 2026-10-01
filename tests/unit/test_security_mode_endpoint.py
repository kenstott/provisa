# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2e6a40-7b13-4c85-a0f9-3e8b5d71c264
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The security posture is a deployment setting, and its endpoint is gated like one (REQ-1913).

``/admin/security`` reads and sets ``security.mode`` — standard, or high, where data endpoints
refuse every client that cannot decrypt results itself. It carried no authorization check: any
signed-in caller could read it and change it. It is the platform administrator's, the mode is
stored in the control plane, and the anonymous caller of a deployment with no auth provider cannot
change it."""

# Requirements: REQ-1913, REQ-693, REQ-1337

from __future__ import annotations

import types

import pytest

from provisa.core import config_stamp
import provisa.api.app  # noqa: F401 - imported before any test narrows process state
from provisa.api.admin import security_router
from provisa.api.errors import ApiError
from provisa.core import deployment_settings, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata


@pytest.fixture
def caller(tmp_path, monkeypatch):
    import provisa.api.admin.capabilities as capmod

    monkeypatch.delenv("PROVISA_SECURITY_MODE", raising=False)
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
    cfg = tmp_path / "provisa.yaml"
    cfg.write_text("sources: []\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(cfg))
    monkeypatch.setattr(settings_registry, "_config", {})
    monkeypatch.setattr(settings_registry, "_frozen", None)
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[settings_table, metadata.tables["config_stamp"]])
        # REQ-1914: the settings snapshot is loaded with the plane's `settings` stamp.
        config_stamp.install(conn, config_stamp.PLATFORM_TABLES)
    db = Database(engine, name="platform")
    monkeypatch.setattr(deployment_settings, "_held", None)
    monkeypatch.setattr(deployment_settings, "_db", db)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(admin_db=db, roles={}), raising=False
    )
    caps: set[str] = set()
    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: caps)

    def _as(*rights: str, body: dict | None = None, user: str = "alice"):
        caps.clear()
        caps.update(rights)

        async def _json():
            return body

        return types.SimpleNamespace(
            state=types.SimpleNamespace(identity=types.SimpleNamespace(user_id=user, roles=[])),
            json=_json,
        )

    yield types.SimpleNamespace(as_=_as, config=cfg)
    engine.dispose()


PLATFORM_ADMIN = ("platform_settings", "cross_org")


@pytest.mark.parametrize(
    "rights",
    [("query_development",), ("org_settings",), ("platform_settings", "org_settings")],
    ids=["analyst", "multitenant-org-admin", "single-tenant-org-admin"],
)
async def test_only_a_platform_administrator_changes_the_security_mode(caller, rights):
    with pytest.raises(ApiError) as err:
        await security_router.set_security(caller.as_(*rights, body={"mode": "high"}))
    assert (err.value.status_code, err.value.code) == (403, "platform.control_plane_role_required")
    assert settings_registry.resolve("security.mode") == ("standard", "default")


async def test_only_a_platform_administrator_reads_the_security_mode(caller):
    with pytest.raises(ApiError) as err:
        await security_router.get_security(caller.as_("query_development"))
    assert err.value.status_code == 403
    assert (await security_router.get_security(caller.as_(*PLATFORM_ADMIN)))["mode"] == "standard"


async def test_the_mode_is_stored_in_the_control_plane(caller):
    before = caller.config.read_text()
    result = await security_router.set_security(caller.as_(*PLATFORM_ADMIN, body={"mode": "high"}))
    assert result == {"success": True, "restart_required": True}
    assert settings_registry.resolve("security.mode") == ("high", "stored")
    assert (await security_router.get_security(caller.as_(*PLATFORM_ADMIN)))["mode"] == "high"
    assert caller.config.read_text() == before


async def test_an_unknown_mode_is_refused(caller):
    with pytest.raises(ApiError) as err:
        await security_router.set_security(caller.as_(*PLATFORM_ADMIN, body={"mode": "paranoid"}))
    assert (err.value.status_code, err.value.code) == (400, "security.unknown_mode")


async def test_the_anonymous_caller_reads_but_cannot_change_the_mode(caller):
    """No auth provider: the anonymous caller is admitted to the admin pages, and a guarded
    setting — one that can cut clients off — is still not its to change."""
    anonymous = caller.as_(user="anonymous", body={"mode": "high"})
    assert (await security_router.get_security(anonymous))["mode"] == "standard"
    with pytest.raises(ApiError) as err:
        await security_router.set_security(anonymous)
    assert (err.value.status_code, err.value.code) == (403, "settings.auth_required")
    assert err.value.params == {"field": "security.mode", "reason": "auth_required"}
