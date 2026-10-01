# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9b6d14-e07a-4c35-8a61-d4b3907c5e8f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A setting that would stop the deployment being reachable is refused when it is saved (REQ-1913).

Every operator setting is editable, including the ones that can lock the operator out: listener
ports, TLS files, the Redis the deployment connects to. A restart setting is only tried when the
server next starts, so a value that cannot work is refused at the save, naming the field, rather
than found by a server that no longer comes up."""

# Requirements: REQ-1913

from __future__ import annotations

import subprocess
import types

import pytest

from provisa.core import config_stamp
from provisa.api.admin import settings_catalog_router as catalog
from provisa.api.errors import ApiError
from provisa.core import deployment_settings, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata

PLATFORM_ADMIN = {"platform_settings", "cross_org"}


@pytest.fixture
def save(tmp_path, monkeypatch):
    """Save values through the catalog route as a platform administrator, on the real
    declarations and a fresh control plane; every value is confirmed."""
    import provisa.api.admin.capabilities as capmod
    from provisa.encryption import runtime

    for s in settings_registry.all_settings():
        if s.env:
            monkeypatch.delenv(s.env, raising=False)
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
    monkeypatch.setattr(settings_registry, "_config", {})
    monkeypatch.setattr(settings_registry, "_frozen", None)
    monkeypatch.setattr(runtime, "_service", None)
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
    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: PLATFORM_ADMIN)

    async def _save(**values):
        values = {k.replace("__", "."): v for k, v in values.items()}

        async def _json():
            return {"values": values, "confirm": list(values)}

        request = types.SimpleNamespace(
            state=types.SimpleNamespace(identity=types.SimpleNamespace(user_id="alice", roles=[])),
            json=_json,
        )
        return await catalog.put_catalog(request)

    yield _save
    engine.dispose()


async def _refused(save, **values) -> dict:
    with pytest.raises(ApiError) as err:
        await save(**values)
    assert (err.value.status_code, err.value.code) == (400, "settings.invalid_value")
    return err.value.params


@pytest.fixture
def tls_pair(tmp_path):
    """A real self-signed certificate and its key, and a second key that does not match it."""
    cert, key, other = tmp_path / "cert.pem", tmp_path / "key.pem", tmp_path / "other.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=settings-guards.test", "-keyout", str(key), "-out", str(cert)],
        check=True,
        capture_output=True,
    )  # fmt: skip
    subprocess.run(
        ["openssl", "genrsa", "-out", str(other), "2048"], check=True, capture_output=True
    )
    return str(cert), str(key), str(other)


# --- listener ports ------------------------------------------------------------------------------


async def test_two_listeners_cannot_be_given_the_same_port(save):
    params = await _refused(save, server__pgwire_port=50051)  # the gRPC listener's port
    assert params == {
        "field": "server.pgwire_port",
        "reason": "port_in_use",
        "other": "server.grpc_port",
    }
    assert settings_registry.resolve("server.pgwire_port") == (0, "default")


async def test_two_ports_saved_together_are_checked_against_each_other(save):
    params = await _refused(save, server__pgwire_port=6000, server__bolt_port=6000)
    assert params["reason"] == "port_in_use"
    assert {params["field"], params["other"]} == {"server.pgwire_port", "server.bolt_port"}


async def test_a_port_freed_in_the_same_save_may_be_taken(save):
    await save(server__grpc_port=50052, server__pgwire_port=50051)
    assert settings_registry.resolve("server.pgwire_port").value == 50051


async def test_listeners_that_are_off_do_not_collide(save):
    await save(server__pgwire_port=0, server__bolt_port=0)
    await save(server__pgwire_port=5439, server__bolt_port=7687)


# --- TLS files -----------------------------------------------------------------------------------


async def test_a_tls_file_that_does_not_exist_is_refused(save, tmp_path):
    params = await _refused(save, tls__cert=str(tmp_path / "missing.pem"))
    assert params == {"field": "tls.cert", "reason": "file_not_found"}


async def test_a_certificate_and_key_that_do_not_match_are_refused(save, tls_pair):
    cert, _key, other = tls_pair
    params = await _refused(save, tls__cert=cert, tls__key=other)
    assert params == {"field": "tls.cert", "reason": "invalid_tls_pair", "other": "tls.key"}
    assert settings_registry.resolve("tls.cert") == (None, "default")


async def test_a_matching_pair_is_stored(save, tls_pair):
    cert, key, _other = tls_pair
    await save(tls__cert=cert, tls__key=key)
    assert settings_registry.resolve("tls.key") == (key, "stored")


async def test_a_key_is_checked_against_the_certificate_already_in_force(
    save, tls_pair, monkeypatch
):
    cert, _key, other = tls_pair
    monkeypatch.setenv("PROVISA_TLS_CERT", cert)
    params = await _refused(save, tls__key=other)
    assert params == {"field": "tls.key", "reason": "invalid_tls_pair", "other": "tls.cert"}


async def test_a_protocols_own_pair_is_checked_too(save, tls_pair):
    cert, _key, other = tls_pair
    params = await _refused(save, tls__pgwire_cert=cert, tls__pgwire_key=other)
    assert params["reason"] == "invalid_tls_pair" and params["field"] == "tls.pgwire_cert"


async def test_clearing_a_tls_file_needs_no_file(save, tls_pair):
    cert, key, _other = tls_pair
    await save(tls__cert=cert, tls__key=key)
    await save(tls__cert=None, tls__key=None)
    assert settings_registry.resolve("tls.cert") == (None, "default")


# --- Redis ---------------------------------------------------------------------------------------


async def test_a_plain_redis_url_is_refused_when_tls_is_required(save):
    await save(redis__require_tls=True)
    params = await _refused(save, cache__redis_url="redis://cache.example:6379")
    assert params == {"field": "cache.redis_url", "reason": "redis_tls_required"}
    await save(cache__redis_url="rediss://cache.example:6380")


async def test_requiring_tls_is_refused_while_the_redis_url_is_plain(save, monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://cache.example:6379")
    params = await _refused(save, redis__require_tls=True)
    assert params == {"field": "redis.require_tls", "reason": "redis_tls_required"}


async def test_the_refusal_about_a_redis_url_does_not_carry_it(save):
    await save(redis__require_tls=True)
    with pytest.raises(ApiError) as err:
        await save(cache__redis_url="redis://:hunter2@cache.example:6379")
    assert "hunter2" not in repr(err.value.params) and "hunter2" not in str(err.value.detail)


# --- the break-glass superuser -------------------------------------------------------------------


async def test_the_stored_superuser_replaces_the_configured_one(save):
    from provisa.auth.superuser import check_superuser, resolve_superuser_config

    configured = {"username": "root", "password": "from-config"}
    assert resolve_superuser_config(configured) == configured
    assert resolve_superuser_config(None) is None
    await save(security__superuser__username="breakglass", security__superuser__password="s3cret!")
    resolved = resolve_superuser_config(configured)
    assert resolved == {"username": "breakglass", "password": "s3cret!"}
    assert resolve_superuser_config(None) == resolved
    assert check_superuser("breakglass", "s3cret!", resolved) is not None
    assert check_superuser("root", "from-config", resolved) is None
    for key in ("security.superuser.username", "security.superuser.password"):
        s = settings_registry.setting(key)
        assert (s.secret, s.guard, s.effect) == (True, "confirm", "restart"), key
        assert "s3cret!" not in repr(settings_registry.describe(key))
        assert "breakglass" not in repr(settings_registry.describe(key))


async def test_a_superuser_name_without_a_password_is_refused(save):
    params = await _refused(save, security__superuser__username="breakglass")
    assert params == {
        "field": "security.superuser.username",
        "reason": "superuser_incomplete",
        "other": "security.superuser.password",
    }
    params = await _refused(save, security__superuser__password="s3cret!")
    assert params["field"] == "security.superuser.password"
    assert settings_registry.stored_value("security.superuser.username") is None


async def test_clearing_one_half_of_the_stored_superuser_is_refused(save):
    await save(security__superuser__username="breakglass", security__superuser__password="s3cret!")
    params = await _refused(save, security__superuser__password=None)
    assert params["reason"] == "superuser_incomplete"
    await save(security__superuser__username=None, security__superuser__password=None)
    assert settings_registry.stored_value("security.superuser.username") is None
