# Copyright (c) 2026 Kenneth Stott
# Canary: 736aa734-184c-4d51-a0aa-4c5eeca89317
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1412: the per-org federation-engine lane, as CORE resolves it.

The admin surface that moves an org between lanes is commercial (``provisa_commercial``); what is
here is what core must do once an org is on one — resolve its terminal, its endpoint, and its DSN.

An org administrator moves the org between three lanes. The mode is DERIVED from the ``orgs``
row (``external_engine_host`` set = external; else ``isolated_engine`` decides isolated vs
shared), and an external lane's coordinator outranks both the deployment's shared endpoint and
the SaaS isolated-host template — nothing else resolves a cluster the org itself operates.
"""

from __future__ import annotations

import pytest

from provisa.api.app import state
from provisa.api.org_runtime import OrgRuntime, reset_current_org, set_current_org
from provisa.federation.trino_lifecycle import terminal_conn_kwargs


@pytest.fixture()
def external_org():
    """An org whose runtime points at a coordinator the ORG operates."""
    org_id = "extorg"
    rt = OrgRuntime(org_id=org_id)
    rt.isolated_engine = True
    rt.engine_endpoint = ("trino.acme.example.com", 8443)
    state.org_registry.set(org_id, rt)
    yield org_id, rt
    state.org_registry.invalidate(org_id)


@pytest.fixture()
def saas_isolated_org():
    org_id = "isolane"
    rt = OrgRuntime(org_id=org_id)
    rt.isolated_engine = True
    state.org_registry.set(org_id, rt)
    yield org_id, rt
    state.org_registry.invalidate(org_id)


def test_orgs_table_carries_the_external_endpoint():
    from provisa.core.schema_admin import orgs

    assert orgs.c.external_engine_host.nullable is True
    assert orgs.c.external_engine_port.nullable is True


# ---- endpoint resolution (the lane that actually answers queries) ------------


def test_external_org_terminal_targets_its_own_coordinator(external_org, monkeypatch):
    # The SaaS template is configured and the org is flagged isolated, and still neither resolves
    # the endpoint — the org's own coordinator outranks both.
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    org_id, _rt = external_org
    token = set_current_org(org_id)
    try:
        kwargs = terminal_conn_kwargs(state)
    finally:
        reset_current_org(token)
    assert kwargs["host"] == "trino.acme.example.com"
    assert kwargs["port"] == 8443
    assert kwargs["schema"] == f"org_{org_id}"


def test_saas_isolated_org_still_uses_the_host_template(saas_isolated_org, monkeypatch):
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_PORT", "8080")
    org_id, rt = saas_isolated_org
    assert rt.engine_endpoint is None
    token = set_current_org(org_id)
    try:
        kwargs = terminal_conn_kwargs(state)
    finally:
        reset_current_org(token)
    assert kwargs["host"] == f"trino-{org_id}.internal"


def test_active_engine_endpoint_is_none_off_the_external_lane(saas_isolated_org):
    assert state.active_engine_endpoint is None
    token = set_current_org(saas_isolated_org[0])
    try:
        assert state.active_engine_endpoint is None
    finally:
        reset_current_org(token)


def test_active_engine_endpoint_exposes_the_external_coordinator(external_org):
    org_id, _rt = external_org
    token = set_current_org(org_id)
    try:
        assert state.active_engine_endpoint == ("trino.acme.example.com", 8443)
    finally:
        reset_current_org(token)


# ---- per-org engine kind + DSN (REQ-1418) ------------------------------------


@pytest.fixture()
def databricks_org():
    """An org running its OWN Databricks warehouse while the deployment runs something else."""
    org_id = "dbxorg"
    rt = OrgRuntime(org_id=org_id)
    rt.isolated_engine = True
    rt.engine_kind = "databricks"
    rt.engine_url = "databricks://token:SECRET@dbx.example.com?http_path=/sql/1.0/warehouses/abc"
    state.org_registry.set(org_id, rt)
    yield org_id, rt
    state.org_registry.invalidate(org_id)


def test_orgs_table_carries_the_org_engine_kind_and_encrypted_dsn():
    from provisa.core.schema_admin import orgs

    assert orgs.c.engine_kind.nullable is True
    # The DSN carries the org's warehouse token, so it is stored as ciphertext, never as text.
    assert orgs.c.engine_url_enc.nullable is True
    assert orgs.c.engine_url_enc.type.__class__.__name__ == "LargeBinary"


def test_engine_addressing_comes_from_the_registry_not_from_the_value():
    from provisa.federation.engine import _ENGINE_BUILDERS, engine_addressing

    assert engine_addressing("databricks") == "url"
    assert engine_addressing("snowflake") == "url"
    assert engine_addressing("bigquery") == "url"
    assert engine_addressing("sqlalchemy") == "url"
    assert engine_addressing("trino-byo") == "endpoint"
    assert engine_addressing("duckdb") == "none"
    assert engine_addressing("trino") == "none"
    # Every builder key is answerable — the admin surface validates against this set.
    for key in _ENGINE_BUILDERS:
        assert engine_addressing(key) in {"url", "endpoint", "none"}


def test_unknown_engine_kind_raises_rather_than_guessing():
    from provisa.federation.engine import engine_addressing

    with pytest.raises(ValueError, match="unknown engine kind"):
        engine_addressing("not-an-engine")


def test_active_engine_url_is_the_orgs_own_dsn(databricks_org):
    org_id, rt = databricks_org
    assert state.active_engine_url is None  # no org bound → the deployment's URL answers
    token = set_current_org(org_id)
    try:
        assert state.active_engine_url == rt.engine_url
    finally:
        reset_current_org(token)


def test_configured_engine_url_prefers_the_orgs_dsn_over_the_deployment(
    databricks_org, monkeypatch
):
    """The org's DSN is the more specific statement: it says which warehouse ITS queries run on,
    which is what lets one org run Databricks while the deployment runs Trino."""
    from provisa.federation.engine import configured_engine_url

    monkeypatch.setenv("PROVISA_ENGINE_URL", "snowflake://deployment/db/schema")
    assert configured_engine_url() == "snowflake://deployment/db/schema"
    org_id, rt = databricks_org
    token = set_current_org(org_id)
    try:
        assert configured_engine_url() == rt.engine_url
    finally:
        reset_current_org(token)


def test_an_org_with_its_own_engine_runtime_is_not_blocked_by_the_native_single_org_guard():
    """REQ-1266's guard exists because native engines SHARE one process-wide runtime with bare
    attach aliases. An org on the isolated/external lane holds its own EngineRuntime, so there is
    nothing to collide with — rejecting it would make a per-org Databricks engine unusable."""
    import inspect

    from provisa.federation import native_backend

    src = inspect.getsource(native_backend.NativeEngineBackend._attach_registered)
    assert "active_isolated_org" in src
    assert "_owns_engine" in src


# ---- availability gate -------------------------------------------------------


def test_isolated_lane_is_unavailable_without_a_host_template(monkeypatch):
    """Offering isolation a deployment cannot resolve would sell a guarantee it cannot keep —
    isolated_engine_endpoint raises rather than falling back to the shared coordinator."""
    from provisa.federation.engine import isolated_engine_available

    monkeypatch.delenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", raising=False)
    assert isolated_engine_available() is False
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    assert isolated_engine_available() is True


# ---- the surface itself is not here ------------------------------------------


def test_core_mounts_no_org_engine_surface():
    """REQ-1412: moving one tenant between coordinators the PLATFORM offers is a hosted-platform
    question, so the surface lives in the commercial plugin. An installed Provisa points at the
    engine it operates from /admin/federation-engine and mounts nothing per-org."""
    import importlib

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("provisa.api.admin.org_engine_router")


# ---- the entitlement gate ----------------------------------------------------


async def test_a_deployment_without_the_commercial_plugin_gates_no_lane():
    """A self-hosted deployment has no subscription, so every lane is open to it."""
    from provisa.core import commerce

    commerce.reset_for_tests()
    assert commerce.load() is None
    assert await commerce.lane_entitled(state, "acme", "isolated") is True
    await commerce.require_lane_entitlement(state, "acme", "isolated")
