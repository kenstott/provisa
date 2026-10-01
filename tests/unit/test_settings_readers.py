# Copyright (c) 2026 Kenneth Stott
# Canary: e3a17c5d-80b4-4f29-9c6e-25d8b1f04a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every reader of an operator setting resolves it through the registry (REQ-1913).

A reader that reads the environment itself, or a value copied into app state at boot, does not see
a value stored through the admin UI. Each case here stores a value in the control plane and asks
the product's own reader for it; and asks what the reader does when the value cannot be used —
an error naming the setting, never a quiet default."""

# Requirements: REQ-1913, REQ-1900

from __future__ import annotations

import types

import pytest

from provisa.core import deployment_settings, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata
from provisa.core.settings_registry import SettingInvalid


@pytest.fixture
def control_plane(tmp_path, monkeypatch):
    """The real declarations on a fresh control plane, with no environment or config stating
    anything."""
    for s in settings_registry.all_settings():
        if s.env:
            monkeypatch.delenv(s.env, raising=False)
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
    monkeypatch.setattr(settings_registry, "_config", {})
    monkeypatch.setattr(settings_registry, "_frozen", None)
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[settings_table])
    db = Database(engine, name="platform")
    monkeypatch.setattr(deployment_settings, "_held", None)
    monkeypatch.setattr(deployment_settings, "_db", db)
    yield db
    engine.dispose()


def _store(db, **values) -> None:
    settings_registry.store(
        db, {k.replace("__", "."): v for k, v in values.items()}, updated_by="t"
    )


class _Unreachable:
    """A control plane that cannot be read."""

    class engine:  # noqa: N801 - mirrors Database.engine
        @staticmethod
        def connect():
            raise ConnectionError("control plane unreachable")


# --- server limits -------------------------------------------------------------------------------


def test_the_engine_query_timeout_follows_stored_then_env_then_config(control_plane, monkeypatch):
    from provisa.executor import trino

    # What the app state copied at boot is not the value: a stored change must reach the reader.
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(server_limits={"engine_query_timeout": 999}),
        raising=False,
    )
    assert trino._trino_query_timeout() == 120
    settings_registry.bind_config({"server": {"limits": {"engine_query_timeout": 200}}})
    assert trino._trino_query_timeout() == 200
    monkeypatch.setenv("PROVISA_ENGINE_QUERY_TIMEOUT", "300")
    assert trino._trino_query_timeout() == 300
    _store(control_plane, limits__engine_query_timeout=45)
    assert trino._trino_query_timeout() == 45


def test_the_retry_budget_follows_the_stored_value_in_both_executors(control_plane, monkeypatch):
    from provisa.executor import direct, trino

    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(server_limits={"retry_budget_secs": 999.0}),
        raising=False,
    )
    assert trino._retry_budget() == 30.0 and direct._retry_budget() == 30.0
    monkeypatch.setenv("PROVISA_RETRY_BUDGET_SECS", "12.5")
    assert trino._retry_budget() == 12.5 and direct._retry_budget() == 12.5
    _store(control_plane, limits__retry_budget_secs=0)
    assert trino._retry_budget() == 0.0 and direct._retry_budget() == 0.0


@pytest.mark.parametrize(
    "reader", ["trino._trino_query_timeout", "trino._retry_budget", "direct._retry_budget"]
)
def test_an_executor_limit_that_cannot_be_read_is_an_error_not_a_default(
    control_plane, monkeypatch, reader
):
    """These readers used to catch every exception and answer with the environment default."""
    from provisa import executor  # noqa: F401
    from provisa.executor import direct, trino

    module, name = reader.split(".")
    read = getattr({"trino": trino, "direct": direct}[module], name)
    monkeypatch.setattr(deployment_settings, "_db", _Unreachable())
    monkeypatch.setattr(deployment_settings, "_held", None)
    with pytest.raises(ConnectionError, match="control plane unreachable"):
        read()


def test_an_executor_limit_that_cannot_be_parsed_names_the_setting(control_plane, monkeypatch):
    from provisa.executor import trino

    monkeypatch.setenv("PROVISA_ENGINE_QUERY_TIMEOUT", "two minutes")
    with pytest.raises(SettingInvalid) as err:
        trino._trino_query_timeout()
    assert (err.value.key, err.value.source) == ("limits.engine_query_timeout", "env")


def test_the_mcp_row_limit_follows_the_stored_value(control_plane, monkeypatch):
    from provisa.api.mcp import tools

    assert tools._max_rows() == 1000
    monkeypatch.setenv("PROVISA_MCP_MAX_ROWS", "50")
    assert tools._max_rows() == 50
    _store(control_plane, mcp__max_rows=7)
    assert tools._max_rows() == 7


def test_an_mcp_row_limit_of_zero_is_refused_naming_the_setting(control_plane, monkeypatch):
    from provisa.api.mcp import tools

    monkeypatch.setenv("PROVISA_MCP_MAX_ROWS", "0")
    with pytest.raises(SettingInvalid) as err:
        tools._max_rows()
    assert (err.value.key, err.value.reason) == ("mcp.max_rows", "below_min")


def test_the_mcp_status_reports_the_row_limit_in_force(control_plane, monkeypatch):
    from provisa.api.mcp.status import mcp_status

    monkeypatch.setenv("PROVISA_MCP_MAX_ROWS", "250")
    assert mcp_status()["max_rows"] == 250
    _store(control_plane, mcp__max_rows=7)
    assert mcp_status()["max_rows"] == 7


def test_the_row_limit_the_sample_size_and_fk_tracking_are_range_checked(control_plane):
    """Their readers used to convert whatever was stored; a stored 0 became a limit of 0."""
    from provisa.compiler import sampling
    from provisa.core import limits

    deployment_settings.write(
        control_plane,
        {"limits.default_row_limit": 0, "sampling.default_sample_size": -5},
        updated_by="hand-edit",
    )
    with pytest.raises(SettingInvalid) as err:
        limits.default_row_limit()
    assert (err.value.key, err.value.reason) == ("limits.default_row_limit", "below_min")
    with pytest.raises(SettingInvalid):
        sampling.get_sample_size()


def test_the_row_limit_reads_the_config_the_registry_was_given(control_plane, monkeypatch):
    from provisa.core import limits

    settings_registry.bind_config({"server": {"limits": {"default_row_limit": 250}}})
    assert limits.default_row_limit() == 250
    monkeypatch.setenv("PROVISA_DEFAULT_ROW_LIMIT", "500")
    assert limits.default_row_limit() == 500
    _store(control_plane, limits__default_row_limit=9)
    assert limits.default_row_limit() == 9


def test_fk_tracking_refuses_a_value_that_is_not_a_boolean(control_plane, monkeypatch):
    """Any spelling but 0/false/no used to read as on."""
    monkeypatch.setenv("PROVISA_AUTO_TRACK_FK", "sometimes")
    with pytest.raises(SettingInvalid) as err:
        deployment_settings.auto_track_fk()
    assert (err.value.key, err.value.reason) == ("relationships.auto_track_fk", "not_a_boolean")


# --- no reader states a default or reads the environment for a declared setting ------------------


def test_no_module_reads_the_environment_variable_of_a_declared_setting():
    """The registry is the one reader of a declared setting's environment variable. A second
    read elsewhere is a second precedence order, and it does not see the stored value."""
    import pathlib
    import re

    root = pathlib.Path(settings_registry.__file__).parents[1]
    # A read-only bootstrap setting has no stored value and is read before the control plane
    # exists, so its own reader and the catalog see the same environment.
    declared = {s.env for s in settings_registry.all_settings() if s.env and s.editable}
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if path.name in ("settings_catalog.py",):
            continue
        text = path.read_text()
        for env in declared:
            # A read: os.environ.get("X"…), os.environ["X"], os.getenv("X"…), spanning a newline.
            if re.search(rf"environ(?:\.get\(|\[)\s*[\"']{env}[\"']", text) or re.search(
                rf"getenv\(\s*[\"']{env}[\"']", text
            ):
                offenders.append(f"{path.relative_to(root)}: {env}")
    assert offenders == []


# --- concurrency: fixed at boot, pending until the restart ---------------------------------------


@pytest.fixture
def booted(control_plane, monkeypatch):
    """Apply the server config the way a boot does, without an engine to connect."""
    import provisa.api.app as app_module
    from provisa.api import app_loaders
    from provisa.core import connection_loop, limits

    sized: list[int] = []
    monkeypatch.setattr(connection_loop, "configure_background_workers", sized.append)
    for attr in ("server_cfg", "security_high", "hostname", "server_limits", "flight_global_cap"):
        monkeypatch.setattr(app_module.state, attr, getattr(app_module.state, attr))
    monkeypatch.setattr(limits, "_server_limits", limits.server_limits())
    # Applying the server config also installs the encryption provider and selects the secrets
    # service, process-wide: both are put back when the test ends.
    from provisa.core import secrets_runtime
    from provisa.encryption import runtime as encryption_runtime

    monkeypatch.setattr(encryption_runtime, "_service", encryption_runtime._service)
    monkeypatch.setattr(secrets_runtime, "_selected", secrets_runtime._selected)
    monkeypatch.setattr(secrets_runtime, "_backend", secrets_runtime._backend)

    def _boot(config: dict | None = None) -> types.SimpleNamespace:
        app_loaders._apply_server_and_engine_config(config or {}, connect_engine=False)
        return types.SimpleNamespace(state=app_module.state, background_workers=sized)

    return _boot


def test_the_background_pool_is_sized_from_the_stored_value(control_plane, booted):
    _store(control_plane, concurrency__background_workers=3)
    assert booted({"server": {"background_workers": 9}}).background_workers == [3]


def test_the_background_pool_default_and_config(control_plane, booted):
    from provisa.core.connection_loop import DEFAULT_BACKGROUND_WORKERS

    assert booted().background_workers == [DEFAULT_BACKGROUND_WORKERS]
    assert settings_registry.setting("concurrency.background_workers").effect == "restart"


def test_a_background_pool_size_stored_after_boot_waits_for_the_restart(control_plane, booted):
    boot = booted({"server": {"background_workers": 9}})
    _store(control_plane, concurrency__background_workers=3)
    assert boot.background_workers == [9]
    assert settings_registry.value("concurrency.background_workers") == 9
    assert settings_registry.pending() == ["concurrency.background_workers"]


def test_the_flight_stream_limit_is_the_stored_value_then_env_then_config(
    control_plane, booted, monkeypatch
):
    from provisa.core.limits import flight_stream_default_limit as default_limit

    assert settings_registry.resolve("concurrency.flight_max_concurrent_streams") == (
        default_limit(),
        "default",
    )
    # The config key used to be read before the config was loaded, so it never applied.
    settings_registry.bind_config({"server": {"flight_max_concurrent_streams": 11}})
    assert settings_registry.resolve("concurrency.flight_max_concurrent_streams").value == 11
    monkeypatch.setenv("FLIGHT_MAX_CONCURRENT_STREAMS", "64")
    assert settings_registry.resolve("concurrency.flight_max_concurrent_streams").value == 64
    _store(control_plane, concurrency__flight_max_concurrent_streams=5)
    assert booted().state.flight_global_cap == 5


def test_the_grpc_limits_are_the_stored_values_then_env_then_config(control_plane, monkeypatch):
    from provisa.grpc import server as grpc_server

    assert grpc_server._max_concurrent_rpcs() == 200
    assert grpc_server._max_message_bytes() == 32 * 1024 * 1024
    settings_registry.bind_config(
        {"server": {"grpc_max_concurrent_rpcs": 50, "grpc_max_message_bytes": 1024}}
    )
    assert (grpc_server._max_concurrent_rpcs(), grpc_server._max_message_bytes()) == (50, 1024)
    monkeypatch.setenv("GRPC_MAX_CONCURRENT_RPCS", "75")
    monkeypatch.setenv("GRPC_MAX_MESSAGE_BYTES", "2048")
    assert (grpc_server._max_concurrent_rpcs(), grpc_server._max_message_bytes()) == (75, 2048)
    _store(control_plane, concurrency__grpc_max_concurrent_rpcs=9, grpc__max_message_bytes=4096)
    assert (grpc_server._max_concurrent_rpcs(), grpc_server._max_message_bytes()) == (9, 4096)


def test_a_concurrency_limit_of_zero_is_refused(control_plane):
    for key in (
        "concurrency.background_workers",
        "concurrency.grpc_max_concurrent_rpcs",
        "concurrency.flight_max_concurrent_streams",
    ):
        with pytest.raises(SettingInvalid) as err:
            settings_registry.store(control_plane, {key: 0}, updated_by="t")
        assert (err.value.key, err.value.reason) == (key, "below_min")


# --- redirect: where large results are written (the deployment's object store) -------------------


def test_the_redirect_store_follows_the_stored_values(control_plane, monkeypatch):
    from provisa.encryption import runtime
    from provisa.executor.redirect import DEFAULT_THRESHOLD, DEFAULT_TTL, RedirectConfig

    monkeypatch.setattr(runtime, "_service", None)
    cfg = RedirectConfig.from_env()
    assert (cfg.enabled, cfg.threshold, cfg.ttl) == (False, DEFAULT_THRESHOLD, DEFAULT_TTL)
    assert (cfg.bucket, cfg.region, cfg.default_format, cfg.encrypt) == (
        "provisa-results",
        "us-east-1",
        "parquet",
        False,
    )
    # Unset is the empty string in RedirectConfig, as it always was.
    assert (cfg.endpoint_url, cfg.access_key, cfg.secret_key) == ("", "", "")
    assert cfg.local_dir.endswith("provisa-redirect-results")

    monkeypatch.setenv("PROVISA_REDIRECT_BUCKET", "from-env")
    monkeypatch.setenv("PROVISA_REDIRECT_SECRET_KEY", "env-secret")
    assert RedirectConfig.from_env().bucket == "from-env"
    _store(
        control_plane,
        redirect__enabled=True,
        redirect__threshold=5,
        redirect__ttl=60,
        redirect__bucket="stored-bucket",
        redirect__endpoint="http://store.example:9000",
        redirect__region="eu-west-1",
        redirect__default_format="orc",
        redirect__encrypt=True,
        redirect__local_dir="/var/tmp/redirect",
        redirect__access_key="stored-access",
        redirect__secret_key="stored-secret",
    )
    assert RedirectConfig.from_env() == RedirectConfig(
        enabled=True,
        threshold=5,
        bucket="stored-bucket",
        endpoint_url="http://store.example:9000",
        access_key="stored-access",
        secret_key="stored-secret",
        ttl=60,
        region="eu-west-1",
        default_format="orc",
        encrypt=True,
        local_dir="/var/tmp/redirect",
    )


def test_an_org_still_narrows_its_own_redirect_over_the_stored_deployment_value(
    control_plane, monkeypatch
):
    from provisa.executor import redirect

    _store(control_plane, redirect__enabled=True, redirect__threshold=500, redirect__ttl=600)
    monkeypatch.setattr(
        redirect, "_org_overrides_resolver", lambda: {"threshold": 7, "enabled": False}
    )
    cfg = redirect.RedirectConfig.from_env()
    assert (cfg.enabled, cfg.threshold, cfg.ttl) == (False, 7, 600)


def test_the_redirect_credentials_are_secrets_and_the_store_is_guarded(control_plane):
    for key in ("redirect.access_key", "redirect.secret_key"):
        s = settings_registry.setting(key)
        assert s.secret and s.guard == "confirm", key
    for key in ("redirect.endpoint", "redirect.bucket"):
        assert settings_registry.setting(key).guard == "confirm", key
    _store(control_plane, redirect__secret_key="hunter2")
    assert "hunter2" not in repr(settings_registry.describe("redirect.secret_key"))


# --- security and listeners: fixed at boot, guarded ----------------------------------------------


def test_the_security_mode_and_hostname_a_boot_runs_on_are_the_stored_ones(
    control_plane, booted, monkeypatch
):
    monkeypatch.setenv("PROVISA_SECURITY_MODE", "standard")
    monkeypatch.setenv("PROVISA_HOSTNAME", "from-env.example")
    _store(control_plane, security__mode="high", server__hostname="stored.example")
    state = booted(
        {"server": {"hostname": "config.example"}, "security": {"mode": "standard"}}
    ).state
    assert state.security_high is True
    assert state.hostname == "stored.example"


def test_the_security_mode_follows_env_then_config_and_refuses_an_unknown_mode(
    control_plane, booted, monkeypatch
):
    assert booted({"security": {"mode": "high"}}).state.security_high is True
    monkeypatch.setenv("PROVISA_SECURITY_MODE", "HIGH")  # the environment was case-insensitive
    assert settings_registry.resolve("security.mode") == ("high", "env")
    monkeypatch.setenv("PROVISA_SECURITY_MODE", "paranoid")
    with pytest.raises(SettingInvalid) as err:
        settings_registry.resolve("security.mode")
    assert err.value.reason == "not_in_choices"


def test_the_tls_pair_is_the_protocols_own_else_the_nodes(control_plane, monkeypatch):
    from provisa.api.app_startup import _resolve_tls

    assert _resolve_tls("PROVISA_GRPC_CERT", "PROVISA_GRPC_KEY") is None
    monkeypatch.setenv("PROVISA_TLS_CERT", "/node/cert.pem")
    monkeypatch.setenv("PROVISA_TLS_KEY", "/node/key.pem")
    assert _resolve_tls("PROVISA_GRPC_CERT", "PROVISA_GRPC_KEY") == (
        "/node/cert.pem",
        "/node/key.pem",
    )
    monkeypatch.setenv("PROVISA_GRPC_CERT", "/grpc/cert.pem")
    monkeypatch.setenv("PROVISA_GRPC_KEY", "/grpc/key.pem")
    assert _resolve_tls("PROVISA_GRPC_CERT", "PROVISA_GRPC_KEY") == (
        "/grpc/cert.pem",
        "/grpc/key.pem",
    )
    assert _resolve_tls("PROVISA_BOLT_CERT", "PROVISA_BOLT_KEY") == (
        "/node/cert.pem",
        "/node/key.pem",
    )
    _store(control_plane, tls__cert="/stored/cert.pem", tls__key="/stored/key.pem")
    assert _resolve_tls("PROVISA_PGWIRE_CERT", "PROVISA_PGWIRE_KEY") == (
        "/stored/cert.pem",
        "/stored/key.pem",
    )


def test_the_listener_ports_follow_the_stored_values(control_plane, monkeypatch):
    defaults = {
        "server.grpc_port": 50051,
        "server.flight_port": 8815,
        "server.pgwire_port": 0,
        "server.bolt_port": 0,
        "server.airport_port": 0,
        "mcp.port": 0,
    }
    for key, default in defaults.items():
        s = settings_registry.setting(key)
        assert settings_registry.resolve(key) == (default, "default"), key
        assert (s.effect, s.guard) == ("restart", "confirm"), key
    settings_registry.bind_config({"server": {"grpc_port": 6000, "flight_port": 6001}})
    assert settings_registry.value("server.grpc_port") == 6000
    monkeypatch.setenv("GRPC_PORT", "6100")
    monkeypatch.setenv("PROVISA_PGWIRE_PORT", "5439")
    assert settings_registry.value("server.grpc_port") == 6100
    assert settings_registry.value("server.pgwire_port") == 5439
    _store(control_plane, server__pgwire_port=6432, server__flight_port=6200)
    assert settings_registry.value("server.pgwire_port") == 6432
    assert settings_registry.value("server.flight_port") == 6200
    with pytest.raises(SettingInvalid) as err:
        settings_registry.store(control_plane, {"server.bolt_port": 70000}, updated_by="t")
    assert err.value.reason == "above_max"


def test_the_mcp_listener_follows_the_stored_values(control_plane, monkeypatch):
    from provisa.api.mcp.status import mcp_status

    assert settings_registry.value("mcp.host") == "0.0.0.0"  # noqa: S104 - REQ-1101 default
    monkeypatch.setenv("PROVISA_MCP_PORT", "9100")
    assert mcp_status()["port"] == 9100
    _store(control_plane, mcp__port=9200, mcp__host="127.0.0.1", mcp__tls=True)
    assert mcp_status()["port"] == 9200
    assert settings_registry.value("mcp.host") == "127.0.0.1"
    assert settings_registry.value("mcp.tls") is True


def test_a_browser_origin_is_admitted_to_bolt_only_when_listed(control_plane, monkeypatch):
    from provisa.bolt import websocket

    class _Writer:
        def write(self, _data):
            pass

    check = next(
        fn
        for name, fn in vars(websocket).items()
        if callable(fn) and "origin" in name.lower() and name.startswith("_")
    )
    check(None, _Writer())  # a driver sends no Origin
    with pytest.raises(ConnectionError):
        check("https://app.example", _Writer())
    monkeypatch.setenv("PROVISA_BOLT_ALLOWED_ORIGINS", "https://env.example")
    check("https://env.example", _Writer())
    _store(control_plane, bolt__allowed_origins=["https://app.example"])
    check("https://app.example", _Writer())
    with pytest.raises(ConnectionError):
        check("https://env.example", _Writer())
    assert settings_registry.setting("bolt.allowed_origins").guard == "confirm"


def test_redis_tls_is_required_when_the_stored_setting_says_so(control_plane, monkeypatch):
    from provisa.apq.cache import RedisAPQCache
    from provisa.cache.store import RedisCacheStore

    RedisCacheStore("redis://cache.example:6379")
    monkeypatch.setenv("PROVISA_REQUIRE_REDIS_TLS", "true")
    with pytest.raises(RuntimeError, match="rediss://"):
        RedisCacheStore("redis://cache.example:6379")
    monkeypatch.delenv("PROVISA_REQUIRE_REDIS_TLS")
    _store(control_plane, redis__require_tls=True)
    with pytest.raises(RuntimeError, match="rediss://"):
        RedisCacheStore("redis://cache.example:6379")
    with pytest.raises(RuntimeError, match="rediss://"):
        RedisAPQCache("redis://cache.example:6379")
    RedisCacheStore("rediss://cache.example:6379")


def test_unsecured_grpc_reflection_is_an_explicit_guarded_choice(control_plane, monkeypatch):
    from provisa.grpc import server as grpc_server

    assert grpc_server._allow_unsecured_reflection() is False
    # Any non-empty value used to read as yes, "0" and "false" included.
    monkeypatch.setenv("GRPC_ALLOW_UNSECURED_REFLECTION", "0")
    assert grpc_server._allow_unsecured_reflection() is False
    monkeypatch.setenv("GRPC_ALLOW_UNSECURED_REFLECTION", "1")
    assert grpc_server._allow_unsecured_reflection() is True
    _store(control_plane, grpc__allow_unsecured_reflection=False)
    assert grpc_server._allow_unsecured_reflection() is False
    assert settings_registry.setting("grpc.allow_unsecured_reflection").guard == "confirm"


# --- cache and Redis -----------------------------------------------------------------------------


def test_the_redis_url_follows_stored_then_env_then_config(control_plane, monkeypatch):
    from provisa.core.redis_location import redis_url
    from provisa.encryption import runtime

    monkeypatch.setattr(runtime, "_service", None)
    assert redis_url() is None
    monkeypatch.setenv("CACHE_HOST_FOR_TEST", "cfg.example")
    settings_registry.bind_config(
        {"cache": {"redis_url": "redis://${env:CACHE_HOST_FOR_TEST}:6379"}}
    )
    assert redis_url() == "redis://cfg.example:6379"
    monkeypatch.setenv("REDIS_URL", "redis://env.example:6379")
    assert redis_url() == "redis://env.example:6379"
    _store(control_plane, cache__redis_url="rediss://:pw@stored.example:6380")
    assert redis_url() == "rediss://:pw@stored.example:6380"
    described = settings_registry.describe("cache.redis_url")
    assert described["set"] is True and "stored.example" not in repr(described)
    assert settings_registry.setting("cache.redis_url").guard == "confirm"


def test_a_desktop_launch_uses_the_embedded_redis_whatever_url_is_set(control_plane, monkeypatch):
    from provisa.core.redis_location import redis_url

    monkeypatch.setenv("REDIS_URL", "redis://env.example:6379")
    monkeypatch.setenv("PROVISA_REDIS_EMBEDDED", "1")
    assert redis_url() is None
    # Set by the launcher for a desktop launch; not something the settings page changes.
    embedded = settings_registry.setting("cache.redis_embedded")
    assert embedded.editable is False and embedded.card == "bootstrap"
    with pytest.raises(SettingInvalid) as err:
        settings_registry.store(control_plane, {"cache.redis_embedded": False}, updated_by="t")
    assert err.value.reason == "not_editable"


def test_the_persisted_query_and_compiled_query_ttls_follow_the_stored_values(
    control_plane, monkeypatch
):
    from provisa.compiler.compiled_query_cache import CompiledQueryCache

    assert settings_registry.resolve("apq.ttl") == (86400, "default")
    settings_registry.bind_config({"apq": {"ttl": 600}})
    assert settings_registry.value("apq.ttl") == 600
    monkeypatch.setenv("PROVISA_APQ_TTL", "120")
    assert settings_registry.value("apq.ttl") == 120
    _store(control_plane, apq__ttl=30, compiler__compiled_query_cache_ttl=5)
    assert settings_registry.value("apq.ttl") == 30
    assert CompiledQueryCache()._ttl == 5


def test_the_response_cache_switch_and_default_ttl_are_settings(control_plane):
    assert settings_registry.resolve("cache.enabled") == (True, "default")
    assert settings_registry.resolve("cache.default_ttl") == (300, "default")
    settings_registry.bind_config({"cache": {"enabled": False, "default_ttl": 45}})
    assert settings_registry.value("cache.enabled") is False
    assert settings_registry.value("cache.default_ttl") == 45


# --- MCP, Bolt and trace switches ----------------------------------------------------------------


def test_the_mcp_stdio_role_external_url_and_chat_model_follow_the_stored_values(
    control_plane, monkeypatch
):
    from provisa.api.mcp import server as mcp_server
    from provisa.api.mcp.status import mcp_status

    with pytest.raises(ValueError, match="mcp.role"):
        mcp_server._pinned_stdio_role()
    monkeypatch.setenv("PROVISA_MCP_ROLE", " analyst ")
    assert mcp_server._pinned_stdio_role() == "analyst"
    _store(control_plane, mcp__role="developer", mcp__port=9200)
    assert mcp_server._pinned_stdio_role() == "developer"
    assert mcp_status()["stdio_role"] == "developer"
    _store(control_plane, mcp__external_url="https://mcp.example/mcp")
    assert mcp_status()["url"] == "https://mcp.example/mcp"
    assert settings_registry.setting("mcp.role").guard == "confirm"


async def test_the_mcp_chat_model_follows_the_stored_value(control_plane, monkeypatch):
    from provisa.api.mcp import chat

    monkeypatch.setenv("PROVISA_MCP_CHAT_MODEL", "env-model")
    assert await chat._resolve_model(object()) == "env-model"
    _store(control_plane, mcp__chat_model="stored-model")
    assert await chat._resolve_model(object()) == "stored-model"


def test_the_bolt_receive_timeout_hint_follows_the_stored_value(control_plane, monkeypatch):
    from provisa.bolt import session

    assert session._recv_timeout() == 120
    monkeypatch.setenv("PROVISA_BOLT_RECV_TIMEOUT", "300")
    assert session._recv_timeout() == 300
    _store(control_plane, bolt__recv_timeout=45)
    assert session._recv_timeout() == 45


def test_the_sql_trace_switches_follow_the_stored_values(control_plane, monkeypatch):
    from provisa.observability import stage_trace

    assert stage_trace._mode() == "off"
    monkeypatch.setenv("PROVISA_TRACE_SQL", "REDACTED")
    assert stage_trace._mode() == "redacted"
    _store(control_plane, otel__trace_sql="full", otel__trace_ast=True)
    assert stage_trace._mode() == "full"
    assert stage_trace._trace_ast() is True
    # An unknown mode used to read as off without a word.
    monkeypatch.setenv("PROVISA_TRACE_SQL", "everything")
    _store(control_plane, otel__trace_sql=None)
    with pytest.raises(SettingInvalid) as err:
        stage_trace._mode()
    assert (err.value.key, err.value.reason) == ("otel.trace_sql", "not_in_choices")


def test_the_trace_detail_and_metric_interval_are_settings(control_plane, monkeypatch):
    from provisa.api import otel_setup
    from provisa.otel_compat import TRACE_DETAILS

    detail = settings_registry.setting("otel.trace_detail")
    assert detail.choices == tuple(TRACE_DETAILS) and detail.effect == "restart"
    assert settings_registry.resolve("otel.trace_detail") == ("normal", "default")
    settings_registry.bind_config({"observability": {"trace_detail": TRACE_DETAILS[-1]}})
    assert settings_registry.value("otel.trace_detail") == TRACE_DETAILS[-1]
    assert otel_setup._metric_export_interval_millis() == 15000
    monkeypatch.setenv("OTEL_METRIC_EXPORT_INTERVAL", "5000")
    assert otel_setup._metric_export_interval_millis() == 5000
    _store(control_plane, otel__metric_export_interval=1000)
    assert otel_setup._metric_export_interval_millis() == 1000


def test_the_engine_ready_timeout_follows_the_stored_value(control_plane, monkeypatch):
    from provisa.compiler import introspect
    from provisa.core import catalog

    assert introspect._startup_timeout_secs() == 120.0 == catalog._ready_timeout_secs()
    monkeypatch.setenv("PROVISA_TRINO_READY_TIMEOUT", "300")
    assert introspect._startup_timeout_secs() == 300.0 == catalog._ready_timeout_secs()
    _store(control_plane, engine__ready_timeout=45)
    assert introspect._startup_timeout_secs() == 45.0 == catalog._ready_timeout_secs()


# --- bootstrap: shown, never editable ------------------------------------------------------------

_BOOTSTRAP = {
    "control_plane.tenant_url": "locates_control_plane",
    "control_plane.platform_url": "locates_control_plane",
    "control_plane.org_id": "locates_control_plane",
    "control_plane.pool_min": "locates_control_plane",
    "control_plane.pool_max": "locates_control_plane",
    "bootstrap.config_path": "read_before_control_plane",
    "bootstrap.data_dir": "read_before_control_plane",
    "bootstrap.home": "read_before_control_plane",
    "bootstrap.repo_dir": "read_before_control_plane",
    "bootstrap.demo": "set_by_launcher",
    "cache.redis_embedded": "set_by_desktop_launcher",
}


def test_the_bootstrap_settings_are_listed_read_only_with_the_reason(control_plane):
    declared = {
        s.key: s.readonly_reason for s in settings_registry.all_settings() if not s.editable
    }
    assert declared == _BOOTSTRAP
    for key in _BOOTSTRAP:
        s = settings_registry.setting(key)
        assert (s.card, s.effect) == ("bootstrap", "restart"), key
        described = settings_registry.describe(key)
        assert described["editable"] is False and described["readonly_reason"] == _BOOTSTRAP[key]
        with pytest.raises(SettingInvalid) as err:
            settings_registry.store(control_plane, {key: "x"}, updated_by="t")
        assert err.value.reason == "not_editable", key


def test_the_control_plane_addresses_are_never_shown(control_plane, monkeypatch):
    monkeypatch.setenv("TENANT_DATABASE_URL", "postgresql+psycopg://u:tenant-pw@db.example/t")
    monkeypatch.setenv("PLATFORM_DATABASE_URL", "postgresql+psycopg://u:platform-pw@db.example/p")
    for key in ("control_plane.tenant_url", "control_plane.platform_url"):
        described = settings_registry.describe(key)
        assert described["secret"] is True and described["set"] is True
        assert described["source"] == "env"
        assert "-pw" not in repr(described) and "db.example" not in repr(described)


def test_the_bootstrap_card_shows_what_the_launch_was_given(control_plane, monkeypatch):
    from provisa.core.models import ControlPlaneConfig

    monkeypatch.setenv("PROVISA_CONFIG", "/etc/provisa/provisa.yaml")
    monkeypatch.setenv("PROVISA_DATA_DIR", "/var/lib/provisa")
    assert (
        settings_registry.describe("bootstrap.config_path")["value"] == "/etc/provisa/provisa.yaml"
    )
    assert settings_registry.describe("bootstrap.data_dir")["value"] == "/var/lib/provisa"
    assert settings_registry.describe("bootstrap.home")["value"] is None  # not given
    assert settings_registry.value("control_plane.pool_max") == (
        ControlPlaneConfig.model_fields["pool_max"].default
    )
    settings_registry.bind_config({"control_plane": {"pool_min": 4, "pool_max": 20}})
    assert settings_registry.value("control_plane.pool_min") == 4
    assert settings_registry.value("control_plane.pool_max") == 20


# --- a stored secret is unsealed by the deployment's encryption provider at boot -----------------


class _Reversing:
    """A provider whose ciphertext is visibly not the plaintext, and that refuses anything it did
    not encrypt — as a real one does."""

    def encrypt(self, plaintext: bytes) -> bytes:
        return b"enc:" + plaintext[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        if not blob.startswith(b"enc:"):
            raise ValueError("not this provider's ciphertext")
        return blob[4:][::-1]


def test_a_boot_configures_encryption_before_it_reads_a_stored_secret(
    control_plane, booted, monkeypatch
):
    """The restart settings are fixed while the server config is applied; the encryption provider
    used to be configured later, by the schema rebuild. A sealed setting read in between was
    handed to the passthrough provider, which returns the ciphertext as the value."""
    from provisa.encryption import factory, runtime

    monkeypatch.setattr(
        factory,
        "build_encryption_service",
        lambda provider, **_kw: _Reversing() if provider == "reversing" else None,
    )
    # A running process on the provider stores the secret...
    monkeypatch.setattr(runtime, "_service", _Reversing())
    _store(control_plane, cache__redis_url="rediss://:pw@stored.example:6380")
    # ...and the next process boots with no provider configured yet.
    monkeypatch.setattr(runtime, "_service", None)
    booted({"encryption": {"provider": "reversing"}})
    assert settings_registry.value("cache.redis_url") == "rediss://:pw@stored.example:6380"


# --- the login throttle --------------------------------------------------------------------------


def test_the_login_throttle_follows_stored_then_the_auth_block_then_the_default(control_plane):
    from provisa.auth import throttle

    assert throttle._settings_from(None) == (5, 300, 900)
    assert throttle._settings_from({"login_throttle": {"max_attempts": 3}}) == (3, 300, 900)
    _store(
        control_plane,
        auth__login_throttle__max_attempts=10,
        auth__login_throttle__lockout_seconds=60,
    )
    assert throttle._settings_from({"login_throttle": {"max_attempts": 3}}) == (10, 300, 60)
    for field in ("max_attempts", "window_seconds", "lockout_seconds"):
        s = settings_registry.setting(f"auth.login_throttle.{field}")
        assert (s.card, s.guard, s.min) == ("security", "confirm", 1), field


def test_a_login_throttle_of_zero_attempts_is_refused(control_plane):
    from provisa.auth import throttle

    with pytest.raises(SettingInvalid) as err:
        throttle._settings_from({"login_throttle": {"max_attempts": 0}})
    assert (err.value.key, err.value.source, err.value.reason) == (
        "auth.login_throttle.max_attempts",
        "config",
        "below_min",
    )


# --- request threads: per-worker bounds applied live ----------------------------------------------


@pytest.fixture
def request_pools(control_plane):
    """This worker's request-thread pools, put back on their starting bounds afterwards."""
    from provisa.core import request_thread

    yield request_thread
    request_thread.configure(**request_thread.default_limits())


def test_the_request_thread_bounds_follow_stored_then_env(control_plane, monkeypatch):
    from provisa.core import request_thread

    assert request_thread.default_limits() == {
        "request_threads": 4,
        "stream_threads": 256,
        "control_request_threads": 4,
    }
    monkeypatch.setenv("PROVISA_REQUEST_THREADS", "9")
    assert request_thread.default_limits()["request_threads"] == 9
    _store(control_plane, concurrency__request_threads=6, concurrency__stream_threads=32)
    assert request_thread.default_limits() == {
        "request_threads": 6,
        "stream_threads": 32,
        "control_request_threads": 4,
    }
    for key in ("request_threads", "stream_threads", "control_request_threads"):
        s = settings_registry.setting(f"concurrency.{key}")
        assert (s.effect, s.min, s.card) == ("live", 1, "concurrency"), key


def test_a_stored_request_thread_bound_is_applied_to_this_workers_pools(
    control_plane, request_pools
):
    """Stored by another worker: this one applies it on its next settings check, no restart."""
    settings_registry.apply_changes()
    assert request_pools._pool._max == 4
    deployment_settings.write(
        control_plane,
        {
            "concurrency.request_threads": 7,
            "concurrency.stream_threads": 64,
            "concurrency.control_request_threads": 2,
        },
        updated_by="other-worker",
    )
    settings_registry.apply_changes()
    assert (request_pools._pool._max, request_pools._pool._max_streams) == (7, 64)
    assert request_pools._control_pool._max == 2
    assert settings_registry.describe("concurrency.request_threads")["applied"] == 7


def test_a_request_thread_bound_of_zero_is_refused(control_plane):
    with pytest.raises(SettingInvalid) as err:
        settings_registry.store(control_plane, {"concurrency.request_threads": 0}, updated_by="t")
    assert err.value.reason == "below_min"


# --- the rate limiter connects to the Redis the deployment is configured with ---------------------


def test_the_rate_limiter_is_built_on_the_configured_redis(control_plane, monkeypatch):
    """It used to be built while the app object was created — before the config was loaded and
    before the control plane was bound — so it always got no URL and counted in the embedded
    Redis of its own process, whatever Redis the deployment named."""
    import inspect

    from provisa.api import app as app_module
    from provisa.api import app_loaders, rate_limit
    from provisa.encryption import runtime

    monkeypatch.setattr(runtime, "_service", None)
    built: list[str | None] = []
    monkeypatch.setattr(
        rate_limit, "build_rate_limiter", lambda url: built.append(url) or f"limiter:{url}"
    )
    state = types.SimpleNamespace(redis_url=None, rate_limiter=None)
    monkeypatch.setenv("REDIS_URL", "redis://env.example:6379")
    _store(control_plane, cache__redis_url="rediss://stored.example:6380")
    app_loaders.apply_redis_settings(state)
    assert state.redis_url == "rediss://stored.example:6380"
    assert built == ["rediss://stored.example:6380"]
    assert state.rate_limiter == "limiter:rediss://stored.example:6380"
    # A config reload is not a restart: the limiter built at boot stays.
    app_loaders.apply_redis_settings(state)
    assert built == ["rediss://stored.example:6380"]
    assert "build_rate_limiter" not in inspect.getsource(app_module.create_app)


# --- the hosted-function egress allowlist (REQ-885) ----------------------------------------------


def test_the_egress_allowlist_is_env_added_to_config_until_a_stored_value_replaces_it(
    control_plane, monkeypatch
):
    key = "udf.egress_allowlist"
    assert settings_registry.resolve(key) == ([], "default")  # deny by default
    settings_registry.bind_config({"server": {"udf_egress_allowlist": ["svc-a:8443", "svc-b"]}})
    assert settings_registry.resolve(key) == (["svc-a:8443", "svc-b"], "config")
    # REQ-885: the environment ADDS to the config file's list; it does not replace it.
    monkeypatch.setenv("PROVISA_UDF_EGRESS_ALLOWLIST", "svc-c:443, svc-a:8443")
    assert settings_registry.resolve(key) == (["svc-a:8443", "svc-b", "svc-c:443"], "env")
    # A value stored through the settings page is the whole list.
    _store(control_plane, udf__egress_allowlist=["only.example:443"])
    assert settings_registry.resolve(key) == (["only.example:443"], "stored")
    s = settings_registry.setting(key)
    assert (s.guard, s.effect, s.card) == ("confirm", "restart", "security")
