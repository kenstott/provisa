# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e7a3c-41d9-4c6e-9f27-8a1d3e6b2c94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Operator settings are declared once and resolved in one order (REQ-1913).

Stored value, then environment, then config, then the declared default. A value that cannot be
parsed is an error naming the setting and its source. A setting that takes effect only after a
restart keeps the value the process booted with and reports that a restart is pending."""

# Requirements: REQ-1913, REQ-1900

from __future__ import annotations

import logging

import pytest

from provisa.core import deployment_settings, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata
from provisa.core.settings_registry import Setting, SettingInvalid, UnknownSetting

LIVE = Setting(
    key="test.live_limit",
    card="limits",
    type="int",
    effect="live",
    req="REQ-1913",
    env="PROVISA_TEST_LIVE_LIMIT",
    config_path=("server", "limits", "live_limit"),
    default=120,
    min=1,
    max=1000,
)
BOOT = Setting(
    key="test.boot_pool",
    card="concurrency",
    type="int",
    effect="restart",
    req="REQ-1913",
    env="PROVISA_TEST_BOOT_POOL",
    config_path=("server", "boot_pool"),
    default=16,
    min=1,
)
FLAG = Setting(
    key="test.flag",
    card="security",
    type="bool",
    effect="live",
    req="REQ-1913",
    env="PROVISA_TEST_FLAG",
    default=False,
)
MODE = Setting(
    key="test.mode",
    card="security",
    type="enum",
    effect="restart",
    req="REQ-1913",
    env="PROVISA_TEST_MODE",
    default="standard",
    choices=("standard", "high"),
)
ORIGINS = Setting(
    key="test.origins",
    card="security",
    type="list",
    effect="live",
    req="REQ-1913",
    env="PROVISA_TEST_ORIGINS",
    default=(),
)
RATE = Setting(
    key="test.rate",
    card="telemetry",
    type="float",
    effect="live",
    req="REQ-1913",
    default=1.0,
    min=0.0,
    max=1.0,
)
OPTIONAL = Setting(
    key="test.optional_path",
    card="network",
    type="str",
    effect="restart",
    req="REQ-1913",
    env="PROVISA_TEST_OPTIONAL_PATH",
    default=None,
    nullable=True,
)
PER_TRANSPORT = Setting(
    key="test.timeouts",
    card="limits",
    type="map",
    effect="live",
    req="REQ-1905",
    config_path=("server", "limits", "timeouts"),
    map_keys=("graphql", "flight", "pgwire"),
    default={"flight": 3600.0, "pgwire": 300.0},
    min=0.001,
)
COMPUTED = Setting(
    key="test.computed",
    card="concurrency",
    type="int",
    effect="restart",
    req="REQ-1913",
    default_fn=lambda: 7,
    min=1,
)
ALL = (LIVE, BOOT, FLAG, MODE, ORIGINS, RATE, OPTIONAL, PER_TRANSPORT, COMPUTED)


@pytest.fixture
def registry(monkeypatch):
    """The registry holding only this module's settings, on a process with nothing bound."""
    monkeypatch.setattr(settings_registry, "_settings", {s.key: s for s in ALL})
    monkeypatch.setattr(settings_registry, "_loaded", True)
    monkeypatch.setattr(settings_registry, "_config", {})
    monkeypatch.setattr(settings_registry, "_frozen", None)
    monkeypatch.setattr(settings_registry, "_appliers", [])
    monkeypatch.setattr(deployment_settings, "_db", None)
    monkeypatch.setattr(deployment_settings, "_held", None)
    for s in ALL:
        if s.env:
            monkeypatch.delenv(s.env, raising=False)
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)


@pytest.fixture
def control_plane(tmp_path, registry):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[settings_table])
    db = Database(engine, name="platform")
    deployment_settings.bind(db)
    yield db
    engine.dispose()


# --- precedence ----------------------------------------------------------------------------------


def test_the_declared_default_is_the_value_when_nothing_sets_it(registry):
    assert settings_registry.resolve("test.live_limit") == (120, "default")
    assert settings_registry.value("test.live_limit") == 120


def test_config_overrides_the_default(registry):
    settings_registry.bind_config({"server": {"limits": {"live_limit": 200}}})
    assert settings_registry.resolve("test.live_limit") == (200, "config")


def test_the_environment_overrides_config(registry, monkeypatch):
    settings_registry.bind_config({"server": {"limits": {"live_limit": 200}}})
    monkeypatch.setenv("PROVISA_TEST_LIVE_LIMIT", "300")
    assert settings_registry.resolve("test.live_limit") == (300, "env")


def test_the_stored_value_overrides_the_environment(control_plane, monkeypatch):
    settings_registry.bind_config({"server": {"limits": {"live_limit": 200}}})
    monkeypatch.setenv("PROVISA_TEST_LIVE_LIMIT", "300")
    settings_registry.store(control_plane, {"test.live_limit": 400}, updated_by="alice")
    assert settings_registry.resolve("test.live_limit") == (400, "stored")


def test_clearing_the_stored_value_returns_to_the_environment(control_plane, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_LIVE_LIMIT", "300")
    settings_registry.store(control_plane, {"test.live_limit": 400}, updated_by="alice")
    settings_registry.store(control_plane, {"test.live_limit": None}, updated_by="alice")
    assert settings_registry.resolve("test.live_limit") == (300, "env")


def test_clearing_removes_the_row_rather_than_storing_a_null(control_plane):
    settings_registry.store(
        control_plane, {"test.live_limit": 400, "test.rate": 0.5}, updated_by="alice"
    )
    settings_registry.store(control_plane, {"test.live_limit": None}, updated_by="alice")
    with control_plane.engine.connect() as conn:
        rows = {r.key: r.value for r in conn.execute(settings_table.select())}
    assert rows == {"test.rate": "0.5"}
    # Clearing a setting with nothing stored is not an error, and a map emptied key by key goes too.
    settings_registry.store(control_plane, {"test.live_limit": None}, updated_by="alice")
    settings_registry.store(control_plane, {"test.timeouts": {"flight": 7200}}, updated_by="a")
    settings_registry.store(control_plane, {"test.timeouts": {"flight": None}}, updated_by="a")
    with control_plane.engine.connect() as conn:
        keys = {r.key for r in conn.execute(settings_table.select())}
    assert keys == {"test.rate"}


def test_an_empty_environment_variable_is_not_a_value(registry, monkeypatch):
    # Compose files pass `${VAR:-}` for a variable the operator did not set.
    monkeypatch.setenv("PROVISA_TEST_LIVE_LIMIT", "")
    assert settings_registry.resolve("test.live_limit") == (120, "default")


def test_a_default_may_be_computed(registry):
    assert settings_registry.resolve("test.computed") == (7, "default")


def test_a_nullable_setting_with_nothing_set_is_none(registry):
    assert settings_registry.resolve("test.optional_path") == (None, "default")


def test_an_unknown_setting_is_an_error_naming_it(registry):
    with pytest.raises(UnknownSetting, match="test.nope"):
        settings_registry.resolve("test.nope")


# --- parsing: a bad value is an error naming the setting and its source --------------------------


def test_an_unparsable_environment_value_names_the_setting_and_the_source(registry, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_LIVE_LIMIT", "lots")
    with pytest.raises(SettingInvalid) as err:
        settings_registry.resolve("test.live_limit")
    assert err.value.key == "test.live_limit"
    assert err.value.source == "env"
    assert err.value.reason == "not_a_number"
    assert "test.live_limit" in str(err.value) and "PROVISA_TEST_LIVE_LIMIT" in str(err.value)


def test_an_out_of_range_config_value_names_the_setting_and_the_source(registry):
    settings_registry.bind_config({"server": {"limits": {"live_limit": 0}}})
    with pytest.raises(SettingInvalid) as err:
        settings_registry.resolve("test.live_limit")
    assert (err.value.key, err.value.source, err.value.reason) == (
        "test.live_limit",
        "config",
        "below_min",
    )
    assert err.value.min == 1


def test_a_bad_stored_value_is_an_error_not_the_next_source(control_plane, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_LIVE_LIMIT", "300")
    # Written past the validating door, as a hand edit of the control plane would be.
    deployment_settings.write(control_plane, {"test.live_limit": 5000}, updated_by="x")
    with pytest.raises(SettingInvalid) as err:
        settings_registry.resolve("test.live_limit")
    assert (err.value.source, err.value.reason, err.value.max) == ("stored", "above_max", 1000)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", True),
        ("true", True),
        ("YES", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("No", False),
        ("off", False),
    ],
)
def test_a_boolean_reads_the_usual_spellings(registry, monkeypatch, raw, expected):
    monkeypatch.setenv("PROVISA_TEST_FLAG", raw)
    assert settings_registry.resolve("test.flag") == (expected, "env")


def test_a_boolean_refuses_anything_else(registry, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_FLAG", "maybe")
    with pytest.raises(SettingInvalid) as err:
        settings_registry.resolve("test.flag")
    assert err.value.reason == "not_a_boolean"


def test_a_boolean_does_not_pass_for_a_number(registry):
    with pytest.raises(SettingInvalid) as err:
        settings_registry.parse(LIVE, True, "stored")
    assert err.value.reason == "not_a_number"


def test_an_enum_refuses_a_value_outside_its_choices(registry, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_MODE", "paranoid")
    with pytest.raises(SettingInvalid) as err:
        settings_registry.resolve("test.mode")
    assert err.value.reason == "not_in_choices"
    assert err.value.choices == ("standard", "high")


def test_a_list_is_comma_separated_in_the_environment(registry, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_ORIGINS", "https://a.example, https://b.example ,")
    assert settings_registry.value("test.origins") == ["https://a.example", "https://b.example"]


def test_a_float_is_range_checked(registry):
    with pytest.raises(SettingInvalid) as err:
        settings_registry.parse(RATE, 1.5, "stored")
    assert err.value.reason == "above_max"
    assert settings_registry.parse(RATE, "0.25", "env") == 0.25


# --- map settings: one value per named key, each resolved on its own -----------------------------


def test_a_map_resolves_every_key_and_carries_its_declared_defaults(registry):
    assert settings_registry.value("test.timeouts") == {
        "graphql": None,
        "flight": 3600.0,
        "pgwire": 300.0,
    }


def test_a_map_is_merged_key_by_key_across_the_sources(control_plane):
    settings_registry.bind_config({"server": {"limits": {"timeouts": {"graphql": 30}}}})
    settings_registry.store(control_plane, {"test.timeouts": {"flight": 7200}}, updated_by="a")
    assert settings_registry.value("test.timeouts") == {
        "graphql": 30.0,
        "flight": 7200.0,
        "pgwire": 300.0,
    }
    assert settings_registry.map_sources("test.timeouts") == {
        "graphql": "config",
        "flight": "stored",
        "pgwire": "default",
    }


def test_storing_a_map_is_partial_and_null_clears_one_key(control_plane):
    settings_registry.store(
        control_plane, {"test.timeouts": {"flight": 7200, "graphql": 10}}, updated_by="a"
    )
    settings_registry.store(control_plane, {"test.timeouts": {"flight": None}}, updated_by="a")
    assert settings_registry.value("test.timeouts") == {
        "graphql": 10.0,
        "flight": 3600.0,
        "pgwire": 300.0,
    }


def test_a_map_refuses_a_key_it_does_not_declare(control_plane):
    with pytest.raises(SettingInvalid) as err:
        settings_registry.store(control_plane, {"test.timeouts": {"smtp": 5}}, updated_by="a")
    assert (err.value.key, err.value.reason) == ("test.timeouts.smtp", "unknown_key")


def test_a_map_value_out_of_range_names_the_key(control_plane):
    with pytest.raises(SettingInvalid) as err:
        settings_registry.store(control_plane, {"test.timeouts": {"flight": 0}}, updated_by="a")
    assert (err.value.key, err.value.reason) == ("test.timeouts.flight", "below_min")


# --- storing validates everything first, then writes once ----------------------------------------


def test_one_bad_value_stores_nothing(control_plane):
    with pytest.raises(SettingInvalid) as err:
        settings_registry.store(
            control_plane, {"test.live_limit": 50, "test.boot_pool": 0}, updated_by="a"
        )
    assert err.value.key == "test.boot_pool" and err.value.source == "stored"
    assert settings_registry.resolve("test.live_limit") == (120, "default")


def test_storing_an_unknown_setting_is_refused(control_plane):
    with pytest.raises(UnknownSetting):
        settings_registry.store(control_plane, {"test.nope": 1}, updated_by="a")


def test_a_setting_that_is_not_editable_cannot_be_stored(control_plane, monkeypatch):
    locator = Setting(
        key="test.locator",
        card="network",
        type="str",
        effect="restart",
        req="REQ-1913",
        default="x",
        editable=False,
        readonly_reason="locates the control plane",
    )
    monkeypatch.setitem(settings_registry._settings, locator.key, locator)
    with pytest.raises(SettingInvalid) as err:
        settings_registry.store(control_plane, {"test.locator": "y"}, updated_by="a")
    assert err.value.reason == "not_editable"


# --- live or restart-required --------------------------------------------------------------------


def test_a_live_setting_follows_the_stored_value_without_a_restart(control_plane):
    settings_registry.freeze()
    settings_registry.store(control_plane, {"test.live_limit": 400}, updated_by="a")
    assert settings_registry.value("test.live_limit") == 400
    assert settings_registry.pending_restart("test.live_limit") is False


def test_a_restart_setting_keeps_the_value_the_process_booted_with(control_plane):
    settings_registry.freeze()
    settings_registry.store(control_plane, {"test.boot_pool": 32}, updated_by="a")
    assert settings_registry.value("test.boot_pool") == 16
    assert settings_registry.resolve("test.boot_pool") == (32, "stored")
    assert settings_registry.pending_restart("test.boot_pool") is True
    assert settings_registry.pending() == ["test.boot_pool"]


def test_the_next_boot_runs_on_the_stored_value_and_nothing_is_pending(control_plane):
    settings_registry.freeze()
    settings_registry.store(control_plane, {"test.boot_pool": 32}, updated_by="a")
    settings_registry.freeze()  # the restarted process
    assert settings_registry.value("test.boot_pool") == 32
    assert settings_registry.pending() == []


def test_a_config_reload_is_not_a_restart(control_plane):
    settings_registry.freeze_at_boot()
    settings_registry.store(control_plane, {"test.boot_pool": 32}, updated_by="a")
    settings_registry.bind_config({"server": {"boot_pool": 64}})
    settings_registry.freeze_at_boot()  # the reload applies the server config again
    assert settings_registry.value("test.boot_pool") == 16
    assert settings_registry.pending() == ["test.boot_pool"]


def test_storing_the_value_already_running_is_not_pending(control_plane):
    settings_registry.freeze()
    settings_registry.store(control_plane, {"test.boot_pool": 16}, updated_by="a")
    assert settings_registry.pending_restart("test.boot_pool") is False


def test_a_process_that_never_booted_reads_what_it_was_started_with(registry, monkeypatch):
    # A script or a unit test compiling a query: no boot, nothing frozen, nothing pending.
    monkeypatch.setenv("PROVISA_TEST_BOOT_POOL", "8")
    assert settings_registry.value("test.boot_pool") == 8
    assert settings_registry.pending_restart("test.boot_pool") is False


def test_a_boot_refuses_to_start_on_a_bad_restart_setting(registry, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_BOOT_POOL", "0")
    with pytest.raises(SettingInvalid) as err:
        settings_registry.freeze()
    assert err.value.key == "test.boot_pool"


# --- the operator override that ignores stored settings (REQ-1913) -------------------------------


def test_the_override_flag_ignores_stored_values_and_logs_each_one(
    control_plane, monkeypatch, caplog
):
    monkeypatch.setenv("PROVISA_TEST_BOOT_POOL", "8")
    settings_registry.store(
        control_plane, {"test.boot_pool": 32, "test.live_limit": 400}, updated_by="a"
    )
    monkeypatch.setenv(settings_registry.IGNORE_STORED_ENV, "1")
    with caplog.at_level(logging.WARNING, logger="provisa.core.settings_registry"):
        settings_registry.freeze()
    assert settings_registry.value("test.boot_pool") == 8
    assert settings_registry.resolve("test.live_limit") == (120, "default")
    ignored = [r.getMessage() for r in caplog.records if "ignored" in r.getMessage()]
    assert any("test.boot_pool" in m for m in ignored)
    assert any("test.live_limit" in m for m in ignored)


def test_without_the_flag_stored_values_are_never_ignored(control_plane, monkeypatch):
    monkeypatch.setenv(settings_registry.IGNORE_STORED_ENV, "0")
    settings_registry.store(control_plane, {"test.live_limit": 400}, updated_by="a")
    assert settings_registry.resolve("test.live_limit") == (400, "stored")


# --- secrets: encrypted where they are stored, never shown --------------------------------------

SECRET = Setting(
    key="test.store_password",
    card="cache",
    type="str",
    effect="restart",
    req="REQ-1913",
    env="PROVISA_TEST_STORE_PASSWORD",
    config_path=("cache", "password"),
    default=None,
    nullable=True,
    secret=True,
)


class _Reversing:
    """A stand-in provider whose ciphertext is visibly not the plaintext."""

    def encrypt(self, plaintext: bytes) -> bytes:
        return b"enc:" + plaintext[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        assert blob.startswith(b"enc:"), "decrypt was handed something it did not encrypt"
        return blob[4:][::-1]


@pytest.fixture
def secret_store(control_plane, monkeypatch):
    from provisa.encryption import runtime

    monkeypatch.setattr(runtime, "_service", _Reversing())
    monkeypatch.setitem(settings_registry._settings, SECRET.key, SECRET)
    monkeypatch.delenv("PROVISA_TEST_STORE_PASSWORD", raising=False)
    return control_plane


def _stored_text(db, key: str) -> str:
    with db.engine.connect() as conn:
        return conn.execute(settings_table.select().where(settings_table.c.key == key)).one().value


def test_a_secret_is_encrypted_in_the_control_plane(secret_store):
    settings_registry.store(secret_store, {"test.store_password": "hunter2"}, updated_by="a")
    assert "hunter2" not in _stored_text(secret_store, "test.store_password")


def test_a_stored_secret_is_read_back_by_the_reader(secret_store):
    settings_registry.store(secret_store, {"test.store_password": "hunter2"}, updated_by="a")
    assert settings_registry.resolve("test.store_password") == ("hunter2", "stored")


def test_a_secret_reference_is_stored_as_given_and_resolved_when_read(secret_store, monkeypatch):
    monkeypatch.setenv("TEST_VAULTED_PASSWORD", "from-the-vault")
    settings_registry.store(
        secret_store, {"test.store_password": "${env:TEST_VAULTED_PASSWORD}"}, updated_by="a"
    )
    assert settings_registry.value("test.store_password") == "from-the-vault"
    # Rotating what the reference names rotates what the reader is handed.
    monkeypatch.setenv("TEST_VAULTED_PASSWORD", "rotated")
    assert settings_registry.resolve("test.store_password").value == "rotated"


def test_a_secret_stored_as_plain_text_is_refused_not_used(secret_store):
    # Written past the validating door: the registry does not treat it as a secret it stored.
    deployment_settings.write(secret_store, {"test.store_password": "plain"}, updated_by="x")
    with pytest.raises(SettingInvalid) as err:
        settings_registry.resolve("test.store_password")
    assert (err.value.source, err.value.reason) == ("stored", "not_encrypted")
    assert "plain" not in str(err.value)


def test_a_secret_this_provider_cannot_decrypt_is_an_error_naming_the_setting(
    secret_store, monkeypatch
):
    """Sealed under another key or provider (a changed encryption block): not a crash with a
    cryptography traceback, and not the ciphertext passed off as the value."""
    from provisa.encryption import runtime

    settings_registry.store(secret_store, {"test.store_password": "hunter2"}, updated_by="a")

    class _OtherKey:
        def decrypt(self, blob: bytes) -> bytes:
            raise ValueError("authentication tag mismatch")

    monkeypatch.setattr(runtime, "_service", _OtherKey())
    with pytest.raises(SettingInvalid) as err:
        settings_registry.resolve("test.store_password")
    assert (err.value.key, err.value.source, err.value.reason) == (
        "test.store_password",
        "stored",
        "cannot_decrypt",
    )
    described = settings_registry.describe("test.store_password")
    assert described["error"]["reason"] == "cannot_decrypt" and described["set"] is False


def test_an_error_about_a_secret_never_carries_its_value(secret_store):
    with pytest.raises(SettingInvalid) as err:
        settings_registry.store(secret_store, {"test.store_password": 12345}, updated_by="a")
    assert "12345" not in str(err.value)


def test_the_description_of_a_secret_says_only_whether_it_is_set(secret_store, monkeypatch):
    assert settings_registry.describe("test.store_password")["set"] is False
    settings_registry.store(secret_store, {"test.store_password": "hunter2"}, updated_by="a")
    described = settings_registry.describe("test.store_password")
    assert described["set"] is True and described["source"] == "stored"
    assert described["type"] == "secret" and described["secret"] is True
    assert "value" not in described and "stored" not in described and "default" not in described
    assert "hunter2" not in repr(described)
    # A secret that comes from the environment is not shown either.
    settings_registry.store(secret_store, {"test.store_password": None}, updated_by="a")
    monkeypatch.setenv("PROVISA_TEST_STORE_PASSWORD", "from-env")
    described = settings_registry.describe("test.store_password")
    assert described["set"] is True and described["source"] == "env"
    assert "from-env" not in repr(described)


def test_a_changed_secret_is_pending_until_the_restart(secret_store):
    settings_registry.freeze()
    settings_registry.store(secret_store, {"test.store_password": "hunter2"}, updated_by="a")
    assert settings_registry.pending_restart("test.store_password") is True
    assert settings_registry.value("test.store_password") is None


# --- describing a setting for the catalog --------------------------------------------------------


def test_the_description_carries_value_source_range_and_effect(control_plane, monkeypatch):
    monkeypatch.setenv("PROVISA_TEST_LIVE_LIMIT", "300")
    assert settings_registry.describe("test.live_limit") == {
        "key": "test.live_limit",
        "type": "int",
        "unit": None,
        "min": 1,
        "max": 1000,
        "value": 300,
        "source": "env",
        "default": 120,
        "stored": None,
        "env": "PROVISA_TEST_LIVE_LIMIT",
        "config_key": "server.limits.live_limit",
        "restart_required": False,
        "pending_restart": False,
        "secret": False,
        "editable": True,
        "guard": None,
    }


def test_a_restart_setting_is_described_with_the_running_and_the_stored_value(control_plane):
    settings_registry.freeze()
    settings_registry.store(control_plane, {"test.boot_pool": 32}, updated_by="a")
    described = settings_registry.describe("test.boot_pool")
    assert described["value"] == 16 and described["stored"] == 32
    assert described["source"] == "stored"
    assert described["restart_required"] is True and described["pending_restart"] is True


def test_an_enum_a_map_and_a_read_only_setting_describe_their_shape(control_plane, monkeypatch):
    assert settings_registry.describe("test.mode")["choices"] == ["standard", "high"]
    settings_registry.store(control_plane, {"test.timeouts": {"flight": 7200}}, updated_by="a")
    described = settings_registry.describe("test.timeouts")
    assert described["map_keys"] == ["graphql", "flight", "pgwire"]
    assert described["stored"] == {"flight": 7200.0}
    assert described["sources"] == {"graphql": "default", "flight": "stored", "pgwire": "default"}
    locator = Setting(
        key="test.locator",
        card="bootstrap",
        type="str",
        effect="restart",
        req="REQ-1913",
        default="x",
        editable=False,
        readonly_reason="locates_control_plane",
    )
    monkeypatch.setitem(settings_registry._settings, locator.key, locator)
    described = settings_registry.describe("test.locator")
    assert described["editable"] is False
    assert described["readonly_reason"] == "locates_control_plane"


# --- a live setting a worker has to APPLY (a pool size, not a value read where it is used) --------


@pytest.fixture
def appliers(monkeypatch):
    monkeypatch.setattr(settings_registry, "_appliers", [])


def test_a_changed_setting_is_applied_and_an_unchanged_one_is_not(control_plane, appliers):
    applied: list[tuple] = []
    settings_registry.on_change(("test.live_limit", "test.rate"), lambda *v: applied.append(v))
    settings_registry.apply_changes()
    assert applied == [(120, 1.0)]  # what the process runs on is applied once
    settings_registry.apply_changes()
    assert applied == [(120, 1.0)]
    # Another worker stores a change: this one applies it when its snapshot shows it.
    deployment_settings.write(control_plane, {"test.live_limit": 300}, updated_by="other-worker")
    settings_registry.apply_changes()
    assert applied == [(120, 1.0), (300, 1.0)]
    assert settings_registry.applied("test.live_limit") == 300


def test_the_worker_that_stores_a_change_applies_it_at_once(control_plane, appliers):
    applied: list[tuple] = []
    settings_registry.on_change(("test.live_limit",), lambda *v: applied.append(v))
    settings_registry.apply_changes()
    settings_registry.store(control_plane, {"test.live_limit": 300}, updated_by="a")
    assert applied == [(120,), (300,)]


def test_a_value_that_cannot_be_applied_leaves_the_last_one_in_force(
    control_plane, appliers, caplog
):
    applied: list[tuple] = []
    settings_registry.on_change(("test.live_limit",), lambda *v: applied.append(v))
    settings_registry.apply_changes()
    deployment_settings.write(control_plane, {"test.live_limit": 99999}, updated_by="hand-edit")
    with caplog.at_level(logging.ERROR, logger="provisa.core.settings_registry"):
        settings_registry.apply_changes()
    assert applied == [(120,)]
    assert settings_registry.applied("test.live_limit") == 120
    assert any("test.live_limit" in r.getMessage() for r in caplog.records)


def test_a_setting_nothing_applies_reports_no_applied_value(control_plane, appliers):
    assert settings_registry.applied("test.live_limit") is None
    assert "applied" not in settings_registry.describe("test.live_limit")
    settings_registry.on_change(("test.live_limit",), lambda *_v: None)
    settings_registry.apply_changes()
    assert settings_registry.describe("test.live_limit")["applied"] == 120


# --- the declarations ----------------------------------------------------------------------------


def test_every_declared_setting_is_well_formed():
    settings = settings_registry.all_settings()
    assert settings, "the registry declares the product's operator settings"
    keys = [s.key for s in settings]
    assert len(keys) == len(set(keys))
    envs = [s.env for s in settings if s.env]
    assert len(envs) == len(set(envs)), "one environment variable names one setting"
    for s in settings:
        assert s.effect in ("live", "restart"), s.key
        assert s.card in settings_registry.CARDS, s.key
        assert s.req.startswith("REQ-"), s.key
        assert s.editable or s.readonly_reason, s.key
        if s.type == "enum":
            assert s.choices, s.key
        if s.type == "map":
            assert s.map_keys, s.key
        # The declared default is itself a valid value.
        settings_registry.parse_default(s)
