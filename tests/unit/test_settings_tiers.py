# Copyright (c) 2026 Kenneth Stott
# Canary: c8a43d19-6e07-4b52-9f8c-2d5e7a1b0f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The cache tiers' settings are stored in the control plane (REQ-1913).

``/admin/cache-storage`` used to write the hot-table, warm-table, materialized-view and Redis
settings into the config file of the node that served the save, and to hand the Redis address
back with its password in it. They are operator settings now: stored, validated, resolved in the
one order, and a credential is never returned."""

# Requirements: REQ-1913, REQ-230, REQ-231, REQ-240, REQ-543, REQ-917

from __future__ import annotations

import types

import pytest
import yaml

from provisa.core import config_stamp
import provisa.api.app  # noqa: F401 - imported before any test narrows process state
from provisa.api.errors import ApiError
from provisa.core import deployment_settings, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    import provisa.api.admin.capabilities as capmod
    from provisa.encryption import runtime

    for s in settings_registry.all_settings():
        if s.env:
            monkeypatch.delenv(s.env, raising=False)
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
    cfg = tmp_path / "provisa.yaml"
    cfg.write_text(yaml.safe_dump({"sources": [], "replication": {"hot_threshold": 150}}))
    monkeypatch.setenv("PROVISA_CONFIG", str(cfg))
    monkeypatch.setattr(settings_registry, "_config", yaml.safe_load(cfg.read_text()))
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
    state = types.SimpleNamespace(
        admin_db=db,
        roles={},
        federation_engine=types.SimpleNamespace(
            engine=types.SimpleNamespace(default_materialize_store=lambda: None)
        ),
    )
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(
        capmod, "_resolved_capabilities", lambda identity, state: {"platform_settings", "cross_org"}
    )

    def _request(body: dict | None = None):
        async def _json():
            return body

        return types.SimpleNamespace(
            state=types.SimpleNamespace(identity=types.SimpleNamespace(user_id="alice", roles=[])),
            json=_json,
        )

    yield types.SimpleNamespace(db=db, config=cfg, request=_request)
    engine.dispose()


def test_the_defaults_are_the_config_models(deployment):
    from provisa.core.models import (
        HotTablesConfig,
        MaterializedViewsConfig,
        ReplicationConfig,
        WarmTablesConfig,
    )

    for field in ("auto_threshold", "max_bytes"):
        assert settings_registry.setting(f"hot_tables.{field}").default == (
            HotTablesConfig.model_fields[field].default
        )
    # REQ-238: what is left of the warm block is the engine's filesystem read cache.
    assert set(WarmTablesConfig.model_fields) == {
        "fs_cache_enabled",
        "fs_cache_directories",
        "fs_cache_max_sizes",
    }
    for field, model_field in WarmTablesConfig.model_fields.items():
        assert settings_registry.resolve(f"warm_tables.{field}") == (
            model_field.default,
            "default",
        ), field
    # REQ-826: when a busy table is replicated. The fixture's config file states the threshold.
    for field in ("hot_interval", "hot_max_rows"):
        assert settings_registry.resolve(f"replication.{field}") == (
            ReplicationConfig.model_fields[field].default,
            "default",
        ), field
    assert settings_registry.resolve("replication.hot_threshold") == (150, "config")
    assert settings_registry.resolve("materialized_views.default_ttl") == (
        MaterializedViewsConfig.model_fields["default_ttl"].default,
        "default",
    )


async def test_saving_stores_the_tier_settings_and_leaves_the_config_file_alone(deployment):
    from provisa.api.admin.settings_router import set_cache_storage

    before = deployment.config.read_text()
    result = await set_cache_storage(
        deployment.request(
            {
                "cache": {"enabled": False, "default_ttl": 45},
                "hot_tables": {"auto_threshold": 500, "max_rows": "", "refresh_interval": 60},
                "replication": {
                    "hot_threshold": 250,
                    "hot_interval": 120,
                    "hot_max_rows": 5_000_000,
                },
                "warm_tables": {"fs_cache_enabled": True, "fs_cache_max_sizes": "20GB"},
                "materialized_views": {"default_ttl": 900},
            }
        )
    )
    assert result["success"] is True and result["restart_required"] is True
    stored = settings_registry.resolve
    assert stored("cache.enabled") == (False, "stored")
    assert stored("cache.default_ttl") == (45, "stored")
    assert stored("hot_tables.auto_threshold") == (500, "stored")
    assert stored("hot_tables.max_rows") == (None, "default")  # blank clears
    assert stored("hot_tables.refresh_interval") == (60, "stored")
    assert stored("replication.hot_threshold") == (250, "stored")
    assert stored("replication.hot_interval") == (120, "stored")
    assert stored("replication.hot_max_rows") == (5_000_000, "stored")
    assert stored("warm_tables.fs_cache_enabled") == (True, "stored")
    assert stored("warm_tables.fs_cache_max_sizes") == ("20GB", "stored")
    assert stored("materialized_views.default_ttl") == (900, "stored")
    assert "replication.hot_threshold" in result["updated"]
    assert deployment.config.read_text() == before


async def test_a_value_that_cannot_be_used_is_refused_naming_the_field(deployment):
    from provisa.api.admin.settings_router import set_cache_storage

    with pytest.raises(ApiError) as err:
        await set_cache_storage(
            deployment.request(
                {"hot_tables": {"auto_threshold": 500}, "replication": {"hot_max_rows": "many"}}
            )
        )
    assert (err.value.status_code, err.value.params["field"]) == (400, "replication.hot_max_rows")
    assert settings_registry.resolve("hot_tables.auto_threshold").source == "default"


async def test_the_page_reads_back_what_was_saved(deployment):
    from provisa.api.admin.settings_router import get_cache_storage, set_cache_storage

    first = await get_cache_storage(deployment.request())
    # The response cache is ON unless turned off; the page used to report it off when unset.
    assert first["cache"]["enabled"] is True
    assert first["replication"]["hot_threshold"] == 150
    # REQ-230: with no ceiling of its own, the hot tier's row ceiling is its auto threshold.
    assert first["hot_tables"]["max_rows"] == first["hot_tables"]["auto_threshold"]
    await set_cache_storage(
        deployment.request(
            {"hot_tables": {"max_rows": 2000}, "materialized_views": {"default_ttl": 120}}
        )
    )
    after = await get_cache_storage(deployment.request())
    assert after["hot_tables"]["max_rows"] == 2000
    assert after["materialized_views"]["default_ttl"] == 120


async def test_the_redis_address_is_never_returned_with_its_password(deployment):
    from provisa.api.admin.settings_router import get_cache_storage, set_cache_storage

    await set_cache_storage(
        deployment.request({"cache": {"redis_url": "rediss://user:hunter2@cache.example:6380/0"}})
    )
    page = await get_cache_storage(deployment.request())
    assert page["cache"]["redis_url"] == "rediss://user@cache.example:6380/0"
    assert "hunter2" not in repr(page)
    # Saving the page back unchanged posts the address without its password: the stored one stands.
    await set_cache_storage(
        deployment.request({"cache": {"redis_url": page["cache"]["redis_url"]}})
    )
    assert settings_registry.resolve("cache.redis_url") == (
        "rediss://user:hunter2@cache.example:6380/0",
        "stored",
    )
    # A blank address clears the stored one (back to the embedded Redis when nothing else sets it).
    await set_cache_storage(deployment.request({"cache": {"redis_url": ""}}))
    assert settings_registry.resolve("cache.redis_url") == (None, "default")


async def test_the_store_addresses_are_never_returned_with_their_passwords(deployment):
    from provisa.api.admin._config_io import read_config
    from provisa.api.admin.settings_router import get_cache_storage, set_cache_storage

    mat = "postgresql://svc:hunter2@pg.example:5432/mat"
    ops = "postgresql://ops:s3cret@pg.example:5432/ops"
    await set_cache_storage(
        deployment.request({"materialize": {"store_url": mat}, "ops": {"store_url": ops}})
    )
    page = await get_cache_storage(deployment.request())
    assert page["materialize"]["store_url"] == "postgresql://svc@pg.example:5432/mat"
    assert page["ops"]["store_url"] == "postgresql://ops@pg.example:5432/ops"
    assert "hunter2" not in repr(page) and "s3cret" not in repr(page)
    # Saving the page back unchanged keeps the stored credentials.
    await set_cache_storage(
        deployment.request(
            {
                "materialize": {"store_url": page["materialize"]["store_url"]},
                "ops": {"store_url": page["ops"]["store_url"]},
            }
        )
    )
    cfg = read_config()
    assert cfg["materialize_store_url"] == mat
    assert cfg["ops_store_url"] == ops


def test_the_hot_tier_refreshes_on_its_own_interval_else_the_view_ttl(deployment):
    """REQ-231: with no interval of its own the hot tier follows the materialized-view TTL."""
    from provisa.cache import hot_tables

    assert hot_tables.refresh_interval() == 300
    settings_registry.store(deployment.db, {"materialized_views.default_ttl": 120}, updated_by="a")
    assert hot_tables.refresh_interval() == 120
    settings_registry.store(deployment.db, {"hot_tables.refresh_interval": 30}, updated_by="a")
    assert hot_tables.refresh_interval() == 30
    assert hot_tables.max_rows() == settings_registry.value("hot_tables.auto_threshold")
    settings_registry.store(deployment.db, {"hot_tables.max_rows": 50}, updated_by="a")
    assert hot_tables.max_rows() == 50
