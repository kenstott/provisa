# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6f3b8e-2c47-4a91-b5e3-7f18c9a4d620
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The settings catalog: every operator setting, readable and editable through one API (REQ-1913).

``GET /admin/settings/catalog`` lists every declared setting with its value, where the value comes
from, and whether a change waits for a restart. ``PUT`` validates every value before storing any,
and refuses a bad one with an error naming the field. Deployment settings are the platform
administrator's in every deployment; a secret is never returned."""

# Requirements: REQ-1913, REQ-1337

from __future__ import annotations

import types

import pytest

from provisa.core import config_stamp, config_watch
import provisa.api.app  # noqa: F401 - imported before a test narrows the registry to its own settings
from provisa.api.admin import settings_catalog_router as catalog
from provisa.api.errors import ApiError
from provisa.core import deployment_settings, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata
from provisa.core.settings_registry import Setting

LIMIT = Setting(
    key="test.limit",
    card="limits",
    type="int",
    effect="live",
    req="REQ-1913",
    env="PROVISA_TEST_LIMIT",
    default=120,
    min=1,
    max=1000,
    unit="seconds",
)
POOL = Setting(
    key="test.pool",
    card="concurrency",
    type="int",
    effect="restart",
    req="REQ-1913",
    default=16,
    min=1,
)
MODE = Setting(
    key="test.mode",
    card="security",
    type="enum",
    effect="restart",
    req="REQ-1913",
    default="standard",
    choices=("standard", "high"),
    guard="confirm",
)
PASSWORD = Setting(
    key="test.password",
    card="security",
    type="str",
    effect="restart",
    req="REQ-1913",
    default=None,
    nullable=True,
    secret=True,
    guard="confirm",
)
LOCATOR = Setting(
    key="test.locator",
    card="bootstrap",
    type="str",
    effect="restart",
    req="REQ-1913",
    default="here",
    editable=False,
    readonly_reason="locates_control_plane",
)
ALL = (LIMIT, POOL, MODE, PASSWORD, LOCATOR)
# REQ-1914: the catalog also reports how long a saved setting takes to reach every other worker,
# which is the config reload interval — a setting of the real catalog, registered beside the
# test's own so the report resolves.
RELOAD_INTERVAL = Setting(
    key=config_watch.INTERVAL_SETTING,
    card="concurrency",
    type="float",
    effect="live",
    req="REQ-1914",
    default=2.0,
    min=0.5,
)

PLATFORM_ADMIN = ("platform_settings", "cross_org")
SINGLE_TENANT_ORG_ADMIN = ("platform_settings", "org_settings")


@pytest.fixture
def control_plane(tmp_path, monkeypatch):
    monkeypatch.setattr(settings_registry, "_settings", {s.key: s for s in (*ALL, RELOAD_INTERVAL)})
    monkeypatch.setattr(settings_registry, "_loaded", True)
    monkeypatch.setattr(settings_registry, "_config", {})
    monkeypatch.setattr(settings_registry, "_frozen", None)
    monkeypatch.setattr(settings_registry, "_appliers", [])
    monkeypatch.delenv("PROVISA_TEST_LIMIT", raising=False)
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
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
    yield db
    engine.dispose()


@pytest.fixture
def caller(monkeypatch, control_plane):
    """A request from an identity holding exactly the rights a case names; no rights named is the
    anonymous caller of a deployment with no auth provider."""
    import provisa.api.admin.capabilities as capmod

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

    return _as


def _by_key(payload: dict) -> dict[str, dict]:
    return {s["key"]: s for card in payload["cards"] for s in card["settings"]}


# --- who may read and edit -----------------------------------------------------------------------


async def test_the_platform_administrator_reads_the_catalog(caller):
    payload = await catalog.get_catalog(caller(*PLATFORM_ADMIN))
    assert "test.limit" in _by_key(payload)


@pytest.mark.parametrize("rights", [("admin",), ("superadmin",), ("admin", "superadmin")])
async def test_a_retired_wildcard_string_does_not_read_the_catalog(caller, rights):
    # REQ-1327: the catalog needs platform_settings AND cross_org; no string stands in for them.
    with pytest.raises(ApiError) as err:
        await catalog.get_catalog(caller(*rights))
    assert (err.value.status_code, err.value.code) == (403, "platform.control_plane_role_required")


async def test_a_single_tenant_org_administrator_is_refused_the_catalog(caller):
    """org_admin holds ``platform_settings`` in a single-tenant deployment; deployment settings
    are the platform administrator's all the same."""
    with pytest.raises(ApiError) as err:
        await catalog.get_catalog(caller(*SINGLE_TENANT_ORG_ADMIN))
    assert (err.value.status_code, err.value.code) == (403, "platform.control_plane_role_required")


async def test_a_single_tenant_org_administrator_cannot_store_a_setting(caller):
    request = caller(*SINGLE_TENANT_ORG_ADMIN, body={"values": {"test.limit": 5}})
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(request)
    assert err.value.status_code == 403
    assert settings_registry.resolve("test.limit") == (120, "default")


async def test_a_caller_with_no_platform_right_is_refused(caller):
    with pytest.raises(ApiError) as err:
        await catalog.get_catalog(caller("org_settings"))
    assert err.value.status_code == 403


async def test_a_deployment_with_no_auth_provider_reads_the_catalog(caller):
    assert await catalog.get_catalog(caller(user="anonymous"))


# --- what the catalog says -----------------------------------------------------------------------


async def test_the_catalog_groups_every_setting_into_its_card_in_display_order(caller):
    payload = await catalog.get_catalog(caller(*PLATFORM_ADMIN))
    assert [c["id"] for c in payload["cards"]] == ["limits", "concurrency", "security", "bootstrap"]
    assert set(_by_key(payload)) == {s.key for s in (*ALL, RELOAD_INTERVAL)}


async def test_the_catalog_says_how_long_a_save_takes_to_reach_every_worker(caller):
    """REQ-1914: that is the config reload interval, the operator's own setting."""
    payload = await catalog.get_catalog(caller(*PLATFORM_ADMIN))
    assert payload["snapshot_ttl_seconds"] == 2.0
    await catalog.put_catalog(
        caller(*PLATFORM_ADMIN, body={"values": {config_watch.INTERVAL_SETTING: 0.5}})
    )
    payload = await catalog.get_catalog(caller(*PLATFORM_ADMIN))
    assert payload["snapshot_ttl_seconds"] == 0.5


async def test_a_setting_carries_value_source_range_and_who_changed_it(caller):
    await catalog.put_catalog(caller(*PLATFORM_ADMIN, body={"values": {"test.limit": 300}}))
    entry = _by_key(await catalog.get_catalog(caller(*PLATFORM_ADMIN)))["test.limit"]
    assert (entry["value"], entry["source"], entry["stored"]) == (300, "stored", 300)
    assert (entry["min"], entry["max"], entry["default"], entry["unit"]) == (
        1,
        1000,
        120,
        "seconds",
    )
    assert entry["restart_required"] is False and entry["pending_restart"] is False
    assert entry["updated_by"] == "alice" and entry["updated_at"]


async def test_a_saved_restart_setting_is_reported_pending(caller):
    settings_registry.freeze()
    saved = await catalog.put_catalog(caller(*PLATFORM_ADMIN, body={"values": {"test.pool": 32}}))
    assert saved == {"updated": ["test.pool"], "pending_restart": ["test.pool"]}
    payload = await catalog.get_catalog(caller(*PLATFORM_ADMIN))
    assert payload["pending_restart"] == ["test.pool"]
    entry = _by_key(payload)["test.pool"]
    assert (entry["value"], entry["stored"], entry["pending_restart"]) == (16, 32, True)


async def test_a_secret_is_never_returned(caller):
    body = {"values": {"test.password": "hunter2"}, "confirm": ["test.password"]}
    saved = await catalog.put_catalog(caller(*PLATFORM_ADMIN, body=body))
    payload = await catalog.get_catalog(caller(*PLATFORM_ADMIN))
    entry = _by_key(payload)["test.password"]
    assert entry["set"] is True and entry["type"] == "secret"
    assert "hunter2" not in repr(payload) and "hunter2" not in repr(saved)


async def test_a_read_only_setting_says_why(caller):
    entry = _by_key(await catalog.get_catalog(caller(*PLATFORM_ADMIN)))["test.locator"]
    assert entry["editable"] is False and entry["readonly_reason"] == "locates_control_plane"


async def test_a_stored_value_that_cannot_be_used_is_listed_with_its_error(caller, control_plane):
    deployment_settings.write(control_plane, {"test.limit": 99999}, updated_by="x")
    entry = _by_key(await catalog.get_catalog(caller(*PLATFORM_ADMIN)))["test.limit"]
    assert entry["error"] == {"field": "test.limit", "source": "stored", "reason": "above_max"}


# --- storing: validated first, refused with the field named --------------------------------------


async def test_a_value_out_of_range_is_refused_naming_the_field(caller):
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(caller(*PLATFORM_ADMIN, body={"values": {"test.limit": 0}}))
    assert (err.value.status_code, err.value.code) == (400, "settings.invalid_value")
    assert err.value.params == {"field": "test.limit", "reason": "below_min", "min": 1, "max": 1000}


async def test_a_value_outside_the_choices_is_refused_with_the_choices(caller):
    body = {"values": {"test.mode": "paranoid"}, "confirm": ["test.mode"]}
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(caller(*PLATFORM_ADMIN, body=body))
    assert err.value.params == {
        "field": "test.mode",
        "reason": "not_in_choices",
        "choices": ["standard", "high"],
    }


async def test_an_unknown_setting_is_refused_naming_it(caller):
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(caller(*PLATFORM_ADMIN, body={"values": {"test.nope": 1}}))
    assert err.value.status_code == 400
    assert err.value.params == {"field": "test.nope", "reason": "unknown_setting"}


async def test_a_read_only_setting_is_refused(caller):
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(caller(*PLATFORM_ADMIN, body={"values": {"test.locator": "x"}}))
    assert err.value.params == {"field": "test.locator", "reason": "not_editable"}


async def test_one_bad_value_stores_none_of_the_body(caller):
    body = {"values": {"test.limit": 300, "test.pool": 0}}
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(caller(*PLATFORM_ADMIN, body=body))
    assert err.value.params["field"] == "test.pool"
    assert settings_registry.resolve("test.limit") == (120, "default")


async def test_null_clears_the_stored_value(caller, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_LIMIT", "250")
    await catalog.put_catalog(caller(*PLATFORM_ADMIN, body={"values": {"test.limit": 300}}))
    await catalog.put_catalog(caller(*PLATFORM_ADMIN, body={"values": {"test.limit": None}}))
    entry = _by_key(await catalog.get_catalog(caller(*PLATFORM_ADMIN)))["test.limit"]
    assert (entry["value"], entry["source"], entry["stored"]) == (250, "env", None)


@pytest.mark.parametrize("body", [None, [], {"values": [1]}, {"values": {}}, {"confirm": []}])
async def test_a_body_that_names_no_values_is_refused(caller, body):
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(caller(*PLATFORM_ADMIN, body=body))
    assert (err.value.status_code, err.value.code) == (400, "settings.invalid_body")


# --- guarded settings ----------------------------------------------------------------------------


async def test_a_guarded_setting_needs_explicit_confirmation(caller):
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(caller(*PLATFORM_ADMIN, body={"values": {"test.mode": "high"}}))
    assert err.value.status_code == 400
    assert err.value.params == {"field": "test.mode", "reason": "confirmation_required"}
    assert settings_registry.resolve("test.mode") == ("standard", "default")


async def test_a_confirmed_guarded_setting_is_stored(caller):
    body = {"values": {"test.mode": "high"}, "confirm": ["test.mode"]}
    await catalog.put_catalog(caller(*PLATFORM_ADMIN, body=body))
    assert settings_registry.resolve("test.mode") == ("high", "stored")


async def test_the_anonymous_caller_is_refused_a_guarded_setting(caller):
    body = {"values": {"test.mode": "high"}, "confirm": ["test.mode"]}
    with pytest.raises(ApiError) as err:
        await catalog.put_catalog(caller(user="anonymous", body=body))
    assert (err.value.status_code, err.value.code) == (403, "settings.auth_required")
    assert err.value.params == {"field": "test.mode", "reason": "auth_required"}
    assert settings_registry.resolve("test.mode") == ("standard", "default")


async def test_the_anonymous_caller_may_store_an_unguarded_setting(caller):
    await catalog.put_catalog(caller(user="anonymous", body={"values": {"test.limit": 300}}))
    assert settings_registry.resolve("test.limit") == (300, "stored")


async def test_a_guarded_setting_in_the_body_stores_none_of_it_unconfirmed(caller):
    body = {"values": {"test.limit": 300, "test.mode": "high"}}
    with pytest.raises(ApiError):
        await catalog.put_catalog(caller(*PLATFORM_ADMIN, body=body))
    assert settings_registry.resolve("test.limit") == (120, "default")


def test_the_catalog_routes_are_served_by_the_app():
    import inspect

    from provisa.api import app as app_module

    assert "settings_catalog_router" in inspect.getsource(app_module)
    assert {(r.path, m) for r in catalog.router.routes for m in r.methods} == {
        ("/admin/settings/catalog", "GET"),
        ("/admin/settings/catalog", "PUT"),
        (catalog.UI_SERVER_SETTINGS_PATH, "GET"),
    }


# --- what the UI server asks the API for ---------------------------------------------------------


async def test_the_ui_server_is_told_its_proxy_timeout_and_where_it_comes_from(
    control_plane, monkeypatch
):
    """The UI server has no control plane of its own; it carries no identity when it asks."""
    from provisa.auth.middleware import _SKIP_PATHS

    timeout = Setting(
        key="ui.proxy_timeout", card="limits", type="float", effect="live", req="REQ-1913",
        default=480.0, min=1,
    )  # fmt: skip
    monkeypatch.setitem(settings_registry._settings, timeout.key, timeout)
    assert await catalog.ui_server_settings() == {
        "proxy_timeout": {"value": 480.0, "source": "default"}
    }
    settings_registry.store(control_plane, {"ui.proxy_timeout": 30}, updated_by="a")
    assert await catalog.ui_server_settings() == {
        "proxy_timeout": {"value": 30.0, "source": "stored"}
    }
    assert catalog.UI_SERVER_SETTINGS_PATH in _SKIP_PATHS


def test_the_proxy_timeout_is_declared_for_the_settings_page():
    from provisa.core import settings_registry as real

    declared = {s.key: s for s in real.all_settings()}["ui.proxy_timeout"]
    assert (declared.type, declared.effect, declared.default, declared.min) == (
        "float", "live", 480.0, 1,
    )  # fmt: skip
    # The variable is the UI server's, read in its own process; the API does not read it.
    assert declared.env is None
