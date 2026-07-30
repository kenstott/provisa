# Copyright (c) 2026 Kenneth Stott
# Canary: 7b1e4c92-8d05-4f6a-9312-e5a8c0d47f61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1043/REQ-1067/REQ-1244: per-org isolated federation engine.

An org created with ``isolated_engine`` runs on its OWN EngineRuntime; every other org resolves
the shared (default-org) engine. The AppState shims (``federation_engine`` / ``engine_conn`` /
``engine_conn_kwargs``) route by the ``current_org`` ContextVar, and a Trino dedicated
coordinator resolves through ``isolated_engine_endpoint``.
"""

from __future__ import annotations

import pytest

from provisa.api.app import state
from provisa.api.org_runtime import OrgRuntime, reset_current_org, set_current_org
from provisa.federation.engine import build_duckdb_engine, isolated_engine_endpoint
from provisa.federation.runtime import EngineRuntime


@pytest.fixture()
def iso_org():
    """Register a fake isolated-engine org runtime; tear it down after."""
    org_id = "isotest"
    rt = OrgRuntime(org_id=org_id)
    rt.isolated_engine = True
    rt.federation_engine = EngineRuntime(build_duckdb_engine(), state)
    state.org_registry.set(org_id, rt)
    yield org_id, rt
    state.org_registry.invalidate(org_id)


@pytest.fixture()
def pooled_org():
    """Register a shared-lane org runtime (no dedicated engine); tear it down after."""
    org_id = "pooledtest"
    rt = OrgRuntime(org_id=org_id)
    state.org_registry.set(org_id, rt)
    yield org_id, rt
    state.org_registry.invalidate(org_id)


# ---- AppState routing (REQ-1244) --------------------------------------------


def test_unbound_context_resolves_shared_engine():
    shared = state.federation_engine
    assert shared is not None
    default_rt = state.org_registry.get(state.org_id)
    assert default_rt is not None
    assert default_rt.federation_engine is shared


def test_isolated_org_resolves_its_own_engine(iso_org):
    org_id, rt = iso_org
    shared = state.federation_engine
    token = set_current_org(org_id)
    try:
        assert state.federation_engine is rt.federation_engine
        assert state.federation_engine is not shared
    finally:
        reset_current_org(token)
    # Outside the org's context the shared engine is back.
    assert state.federation_engine is shared


def test_pooled_org_resolves_shared_engine(pooled_org):
    org_id, rt = pooled_org
    assert rt.federation_engine is None
    shared = state.federation_engine
    token = set_current_org(org_id)
    try:
        assert state.federation_engine is shared
    finally:
        reset_current_org(token)


def test_engine_conn_routes_to_isolated_org(iso_org):
    org_id, rt = iso_org
    sentinel = object()
    token = set_current_org(org_id)
    try:
        state.engine_conn = sentinel
        state.engine_conn_kwargs = {"host": "trino-isotest", "port": 8080}
    finally:
        reset_current_org(token)
    # The write landed on the ORG's runtime, not the shared one.
    assert rt.engine_conn is sentinel
    assert rt.engine_conn_kwargs["host"] == "trino-isotest"
    default_rt = state.org_registry.get(state.org_id)
    assert default_rt is not None
    assert default_rt.engine_conn is not sentinel


def test_engine_conn_of_pooled_org_is_the_shared_one(pooled_org):
    org_id, _rt = pooled_org
    default_rt = state.org_registry.get(state.org_id)
    assert default_rt is not None
    token = set_current_org(org_id)
    try:
        assert state.engine_conn is default_rt.engine_conn
        assert state.engine_conn_kwargs is default_rt.engine_conn_kwargs
    finally:
        reset_current_org(token)


def test_active_isolated_org_seam(iso_org, pooled_org):
    assert state.active_isolated_org is None  # default org: shared lane
    token = set_current_org(iso_org[0])
    try:
        assert state.active_isolated_org == iso_org[0]
        assert state.active_org_id == iso_org[0]
    finally:
        reset_current_org(token)
    token = set_current_org(pooled_org[0])
    try:
        assert state.active_isolated_org is None
        assert state.active_org_id == pooled_org[0]
    finally:
        reset_current_org(token)


# ---- dedicated coordinator endpoint (REQ-1043) -------------------------------


def test_isolated_endpoint_requires_template(monkeypatch):
    monkeypatch.delenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", raising=False)
    with pytest.raises(RuntimeError, match="isolated federation engine"):
        isolated_engine_endpoint("acme")


def test_isolated_endpoint_templates_org_id(monkeypatch):
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_PORT", "8443")
    assert isolated_engine_endpoint("acme") == ("trino-acme.internal", 8443)


# ---- sleep/wake terminal (REQ-1043/REQ-1244) ---------------------------------


def test_bind_terminal_stores_kwargs_without_connecting(iso_org, monkeypatch):
    """A dedicated cluster sleeps between sessions; org provisioning must not wake it.
    bind_terminal resolves + stores conn kwargs and leaves the terminal unconnected — the
    first real query's lazy connect does the wake."""
    from provisa.federation.engine import build_trino_engine

    org_id, rt = iso_org
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    rt.federation_engine = EngineRuntime(build_trino_engine(), state)
    token = set_current_org(org_id)
    try:
        rt.federation_engine.bind_terminal()
    finally:
        reset_current_org(token)
    assert rt.engine_conn is None  # never connected — the cluster stays asleep
    assert rt.engine_conn_kwargs["host"] == f"trino-{org_id}.internal"
    assert rt.engine_conn_kwargs["schema"] == f"org_{org_id}"


def test_kwargs_only_terminal_passes_the_execute_guard(iso_org, monkeypatch):
    """TrinoBackend.execute must NOT reject a bound-but-unconnected terminal: execute_trino
    lazily connects from state.engine_conn_kwargs (the wake)."""
    from provisa.federation.engine import build_trino_engine

    org_id, rt = iso_org
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    rt.federation_engine = EngineRuntime(build_trino_engine(), state)
    woke = {}

    def _fake_execute_trino(conn, sql, **kw):
        woke["conn"] = conn
        woke["sql"] = sql
        return "ok"

    import provisa.executor.trino as trino_exec

    monkeypatch.setattr(trino_exec, "execute_trino", _fake_execute_trino)
    token = set_current_org(org_id)
    try:
        rt.federation_engine.bind_terminal()
        result = rt.federation_engine.execute_engine_sync("SELECT 1")
    finally:
        reset_current_org(token)
    assert result == "ok"
    assert woke["conn"] is None  # handed the unconnected terminal; execute_trino wakes it


# ---- admin-plane persistence (REQ-1043) --------------------------------------


def test_orgs_table_has_isolated_engine_column():
    from provisa.core.schema_admin import orgs

    col = orgs.c.isolated_engine
    assert col.nullable is False


def test_create_org_body_defaults_to_shared_lane():
    from provisa.api.admin.orgs_router import CreateOrgBody

    body = CreateOrgBody(id="acme", name="Acme")
    assert body.isolated_engine is False
    assert CreateOrgBody(id="acme", name="Acme", isolated_engine=True).isolated_engine is True
