# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8f5b60-9a17-4e2d-b4c3-7f1e0d6a2b95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An org's own settings reach every worker (REQ-1349, REQ-1900).

`redirect` and `cache.default_ttl` are rows in the org's ``org_settings`` table, but the query
path reads a copy cached on the org's runtime, and only the worker that served the change
refreshed its copy. Every worker now re-reads the rows when its copy is older than the snapshot
TTL, so a change made through one worker is in force on the others within that many seconds."""

# Requirements: REQ-1349, REQ-1900

from __future__ import annotations

import time

import pytest
from sqlalchemy import insert

from provisa.api.app import AppState
from provisa.api.org_runtime import OrgRuntime
from provisa.core import org_settings as org_settings_mod
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import metadata, org_settings


@pytest.fixture
def tenant_db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[org_settings])
    yield Database(engine, name="org")
    engine.dispose()


def _another_worker_writes(db: Database, key: str, value: dict) -> None:
    with db.engine.begin() as conn:
        conn.execute(insert(org_settings).values(key=key, value=value))


def _state_bound_to(runtime: OrgRuntime, monkeypatch) -> AppState:
    state = AppState()
    monkeypatch.setattr(state, "_active_runtime", lambda: runtime)
    return state


def test_the_sync_reader_returns_the_orgs_rows(tenant_db):
    _another_worker_writes(tenant_db, "cache", {"default_ttl": 7})
    assert org_settings_mod.read_org_overrides_sync(tenant_db) == {"cache": {"default_ttl": 7}}


def test_another_workers_change_is_in_force_once_the_copy_is_stale(tenant_db, monkeypatch):
    runtime = OrgRuntime(org_id="acme")
    runtime.tenant_db = tenant_db
    state = _state_bound_to(runtime, monkeypatch)
    state.deployment_cache_default_ttl = 300
    assert state.settings_overrides == {}
    assert state.response_cache_default_ttl == 300

    _another_worker_writes(tenant_db, "cache", {"default_ttl": 7})
    assert state.response_cache_default_ttl == 300  # this worker's copy is still fresh

    runtime.settings_overrides_read_at = time.monotonic() - org_settings_mod.SNAPSHOT_TTL_SECONDS
    assert state.response_cache_default_ttl == 7
    assert state.settings_overrides == {"cache": {"default_ttl": 7}}


def test_the_writing_worker_sees_its_own_change_at_once(tenant_db, monkeypatch):
    runtime = OrgRuntime(org_id="acme")
    runtime.tenant_db = tenant_db
    state = _state_bound_to(runtime, monkeypatch)
    state.settings_overrides = {"redirect": {"threshold": 5}}  # what the settings router does
    assert state.settings_overrides == {"redirect": {"threshold": 5}}
    assert time.monotonic() - runtime.settings_overrides_read_at < 1.0


def test_a_runtime_with_no_tenant_database_keeps_its_copy(monkeypatch):
    runtime = OrgRuntime(org_id="acme")
    runtime.settings_overrides = {"cache": {"default_ttl": 9}}
    runtime.settings_overrides_read_at = 0.0
    state = _state_bound_to(runtime, monkeypatch)
    assert state.settings_overrides == {"cache": {"default_ttl": 9}}
