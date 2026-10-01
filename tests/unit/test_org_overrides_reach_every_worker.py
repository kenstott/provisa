# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8f5b60-9a17-4e2d-b4c3-7f1e0d6a2b95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org's own settings reach every worker (REQ-1349, REQ-1900, REQ-1914).

`redirect` and `cache.default_ttl` are rows in the org's ``org_settings`` table, but the query
path reads a copy cached on the org's runtime, and only the worker that served the change
refreshed its copy. A write to ``org_settings`` advances the ``settings`` config stamp of the
org's plane; every worker's config watcher compares that stamp with the one its copy was loaded
at and reloads the copy when they differ. A request reads the copy and never the control plane."""

# Requirements: REQ-1349, REQ-1900, REQ-1914

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import insert

from provisa.api import model_reload
from provisa.api.app import AppState
from provisa.api.org_runtime import OrgRuntime
from provisa.core import config_stamp, config_watch
from provisa.core import org_settings as org_settings_mod
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import config_stamp as stamp_table
from provisa.core.schema_org import metadata, org_settings


@pytest.fixture
def tenant_db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[org_settings, stamp_table])
        config_stamp.install(conn, {"org_settings": config_stamp.SETTINGS})
    yield Database(engine, name="org")
    engine.dispose()


def _another_worker_writes(db: Database, key: str, value: dict) -> None:
    with db.engine.begin() as conn:
        conn.execute(insert(org_settings).values(key=key, value=value))


@pytest.fixture
def served(tenant_db, monkeypatch):
    """A worker serving org ``acme``: its runtime, loaded, in the process's registry."""
    runtime = OrgRuntime(org_id="acme")
    runtime.tenant_db = tenant_db
    state = AppState()
    state.org_registry.set("acme", runtime)
    monkeypatch.setattr(state, "_active_runtime", lambda: runtime)
    monkeypatch.setattr("provisa.api.app.state", state)
    asyncio.run(model_reload.load_org_settings(runtime))
    return state, runtime


def _watcher_checks(runtime: OrgRuntime) -> list[str]:
    assert runtime.tenant_db is not None
    target = config_watch.Target(
        name="org acme: settings",
        db=runtime.tenant_db,
        kind=config_stamp.SETTINGS,
        loaded=lambda: runtime.settings_stamp,
        reload=lambda: model_reload.reload_org_settings(runtime),
    )
    return asyncio.run(config_watch.check([target]))


def test_the_rows_are_read_with_the_stamp_they_were_read_at(tenant_db):
    _another_worker_writes(tenant_db, "cache", {"default_ttl": 7})
    stamp, rows = asyncio.run(org_settings_mod.read_org_overrides_stamped(tenant_db))
    assert rows == {"cache": {"default_ttl": 7}}
    assert stamp == asyncio.run(config_stamp.read(tenant_db))[config_stamp.SETTINGS] == 1


def test_another_workers_change_is_in_force_when_the_watcher_finds_the_stamp_changed(served):
    state, runtime = served
    state.deployment_cache_default_ttl = 300
    assert state.settings_overrides == {}
    assert state.response_cache_default_ttl == 300
    assert _watcher_checks(runtime) == []  # nothing stored has changed: nothing reloads

    _another_worker_writes(runtime.tenant_db, "cache", {"default_ttl": 7})
    # A request reads this worker's copy, never the control plane.
    assert state.response_cache_default_ttl == 300

    assert _watcher_checks(runtime) == ["org acme: settings"]
    assert state.response_cache_default_ttl == 7
    assert state.settings_overrides == {"cache": {"default_ttl": 7}}
    assert _watcher_checks(runtime) == []


def test_the_writing_worker_sees_its_own_change_at_once(served):
    state, _runtime = served
    state.settings_overrides = {"redirect": {"threshold": 5}}  # what the settings router does
    assert state.settings_overrides == {"redirect": {"threshold": 5}}


def test_a_runtime_dropped_from_the_registry_is_not_reloaded(served):
    state, runtime = served
    _another_worker_writes(runtime.tenant_db, "cache", {"default_ttl": 7})
    state.org_registry.invalidate("acme")
    asyncio.run(model_reload.reload_org_settings(runtime))
    assert runtime.settings_overrides == {}


def test_a_runtime_with_no_tenant_database_keeps_its_copy(monkeypatch):
    runtime = OrgRuntime(org_id="acme")
    runtime.settings_overrides = {"cache": {"default_ttl": 9}}
    state = AppState()
    monkeypatch.setattr(state, "_active_runtime", lambda: runtime)
    assert state.settings_overrides == {"cache": {"default_ttl": 9}}
    state.org_registry.set("acme", runtime)
    monkeypatch.setattr("provisa.api.app.state", state)
    assert [t.name for t in model_reload.targets() if t.name.startswith("org acme")] == []
