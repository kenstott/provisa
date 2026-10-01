# Copyright (c) 2026 Kenneth Stott
# Canary: 59e2c7a1-0d84-4f36-b9a5-e17c4d60832b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The federation engine's sizing is stored in the control plane (REQ-1913).

JVM heap, query memory limits and fault-tolerant execution used to be written to the config file of
the node that served the save. They are operator settings now: stored, validated, and rendered into
the engine's own config files from the stored value. They take effect when the ENGINE restarts."""

# Requirements: REQ-1913, REQ-055, REQ-250

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

ENGINE_KEYS = (
    "jvm_heap_gb",
    "query_max_memory",
    "query_max_memory_per_node",
    "query_max_total_memory",
    "fault_tolerant_execution",
    "fault_tolerant_task_memory",
    "exchange_spool_dir",
)


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    """A config file stating a heap of 4 GB, a fresh control plane, and an engine that records
    when its config files are regenerated."""
    cfg = tmp_path / "provisa.yaml"
    cfg.write_text(yaml.safe_dump({"sources": [], "jvm_heap_gb": 4, "federation_engine": "trino"}))
    monkeypatch.setenv("PROVISA_CONFIG", str(cfg))
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
    monkeypatch.setattr(settings_registry, "_config", yaml.safe_load(cfg.read_text()))
    monkeypatch.setattr(settings_registry, "_frozen", None)
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[settings_table, metadata.tables["config_stamp"]])
        # REQ-1914: the settings snapshot is loaded with the plane's `settings` stamp.
        config_stamp.install(conn, config_stamp.PLATFORM_TABLES)
    db = Database(engine, name="platform")
    monkeypatch.setattr(deployment_settings, "_held", None)
    monkeypatch.setattr(deployment_settings, "_db", db)
    regenerated: list[str] = []
    state = types.SimpleNamespace(
        admin_db=db,
        roles={},
        federation_engine=types.SimpleNamespace(write_config=regenerated.append),
        active_engine_url=None,  # no org-specific engine: the engine registry reads this
    )
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    yield types.SimpleNamespace(db=db, config=cfg, state=state, regenerated=regenerated)
    engine.dispose()


def test_the_sizing_keys_are_declared_as_engine_restart_settings(deployment):
    from provisa.core.models import ProvisaConfig

    for key in ENGINE_KEYS:
        s = settings_registry.setting(f"engine.{key}")
        assert (s.card, s.effect, s.restart_scope) == ("engine", "restart", "engine"), key
        assert s.default == ProvisaConfig.model_fields[key].default, key
        assert settings_registry.describe(f"engine.{key}")["restart_scope"] == "engine"
    assert settings_registry.resolve("engine.jvm_heap_gb") == (4, "config")


def test_saving_stores_the_sizing_and_regenerates_the_engine_config(deployment):
    from provisa.api.admin.settings_router import _apply_engine

    before = deployment.config.read_text()
    updated: list[str] = []
    restart = _apply_engine(
        {"jvm_heap_gb": 16, "query_max_memory": "8GB", "fault_tolerant_execution": False},
        deployment.state,
        updated,
        updated_by="alice",
    )
    assert restart is True
    assert settings_registry.resolve("engine.jvm_heap_gb") == (16, "stored")
    assert settings_registry.resolve("engine.query_max_memory") == ("8GB", "stored")
    assert settings_registry.resolve("engine.fault_tolerant_execution") == (False, "stored")
    assert updated == [
        "engine.jvm_heap_gb",
        "engine.query_max_memory",
        "engine.fault_tolerant_execution",
    ]
    # The engine's own config files are regenerated; the node's config file is not written.
    assert deployment.regenerated == [str(deployment.config)]
    assert deployment.config.read_text() == before


def test_a_save_with_no_sizing_key_changes_nothing(deployment):
    from provisa.api.admin.settings_router import _apply_engine

    assert _apply_engine({}, deployment.state, [], updated_by="a") is False
    assert deployment.regenerated == []


def test_a_heap_that_is_not_a_number_is_refused_naming_the_field(deployment):
    from provisa.api.admin.settings_router import _apply_engine

    with pytest.raises(ApiError) as err:
        _apply_engine({"jvm_heap_gb": "lots"}, deployment.state, [], updated_by="a")
    assert (err.value.status_code, err.value.params["field"]) == (400, "engine.jvm_heap_gb")
    assert deployment.regenerated == []


def test_the_engine_config_is_rendered_from_the_stored_sizing(deployment):
    """The value the engine will start on next — also while the server still runs on the value
    it booted with (a restart setting's ``value`` is frozen; the engine's files are not)."""
    from provisa.api import trino_setup

    cfg = yaml.safe_load(deployment.config.read_text())
    settings_registry.freeze()
    assert trino_setup._cfg(cfg, "jvm_heap_gb") == 4
    settings_registry.store(deployment.db, {"engine.jvm_heap_gb": 16}, updated_by="a")
    assert trino_setup._cfg(cfg, "jvm_heap_gb") == 16
    assert settings_registry.pending_restart("engine.jvm_heap_gb") is True
    # A key that is not an operator setting still comes from the config being rendered.
    assert trino_setup._cfg({"node_role": "worker"}, "node_role") == "worker"
    # A config handed in for rendering is what is rendered when nothing is stored.
    assert trino_setup._cfg({"query_max_memory": "6GB"}, "query_max_memory") == "6GB"


def test_the_settings_endpoint_reports_the_saved_sizing(deployment):
    from provisa.api.admin.settings_router import _apply_engine, _engine_block

    assert _engine_block({"jvm_heap_gb": 4})["jvm_heap_gb"] == 4
    _apply_engine({"jvm_heap_gb": 16}, deployment.state, [], updated_by="a")
    block = _engine_block({"jvm_heap_gb": 4})
    assert block["jvm_heap_gb"] == 16
    assert set(block) == set(ENGINE_KEYS)


async def test_the_engine_tab_stores_the_sizing_too(deployment, monkeypatch):
    """PUT /admin/federation-engine writes the same keys; it stores them where the settings
    page does, so one cannot shadow the other."""
    import provisa.api.admin.capabilities as capmod
    from provisa.api.admin import settings_router

    monkeypatch.setattr(
        capmod, "_resolved_capabilities", lambda identity, state: {"platform_settings", "cross_org"}
    )

    async def _json():
        return {"engine": "trino", "jvm_heap_gb": 12, "query_max_memory": ""}

    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=types.SimpleNamespace(user_id="alice", roles=[])),
        json=_json,
    )
    settings_registry.store(deployment.db, {"engine.query_max_memory": "8GB"}, updated_by="a")
    result = await settings_router.set_federation_engine(request)
    assert result["success"] is True
    assert settings_registry.resolve("engine.jvm_heap_gb") == (12, "stored")
    # A blank value resets the key: the stored value is cleared.
    assert settings_registry.resolve("engine.query_max_memory").source == "default"
    saved = yaml.safe_load(deployment.config.read_text())
    assert saved["federation_engine"] == "trino"
    assert saved["jvm_heap_gb"] == 4, "the sizing is not written to the config file any more"
