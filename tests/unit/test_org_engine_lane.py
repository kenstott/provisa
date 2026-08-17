# Copyright (c) 2026 Kenneth Stott
# Canary: 3c5f8ab1-72d6-4e09-b4a3-9f61d27e05cc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1412: the org's federation-engine lane — shared, isolated, or external.

An org administrator moves the org between three lanes. The mode is DERIVED from the ``orgs``
row (``external_engine_host`` set = external; else ``isolated_engine`` decides isolated vs
shared), and an external lane's coordinator outranks both the deployment's shared endpoint and
the SaaS isolated-host template — nothing else resolves a cluster the org itself operates.
"""

from __future__ import annotations

import pytest

from provisa.api.admin.org_engine_router import EXTERNAL, ISOLATED, MODES, SHARED, _mode_of
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


# ---- mode derivation ---------------------------------------------------------


def test_modes_are_the_three_lanes():
    assert MODES == (SHARED, ISOLATED, EXTERNAL)


def test_mode_is_derived_never_stored_twice():
    assert _mode_of(None, False, False) == SHARED
    assert _mode_of(None, False, True) == ISOLATED
    assert _mode_of("trino.acme.example.com", False, True) == EXTERNAL
    # An external host wins over the isolated flag: the org runs the cluster either way, and the
    # host is what says whose it is.
    assert _mode_of("trino.acme.example.com", False, False) == EXTERNAL


def test_a_dsn_addressed_org_is_external_without_a_host():  # REQ-1418
    """A Databricks/Snowflake/BigQuery engine has no coordinator host — reading only the host would
    file an org running its own warehouse as shared, and route its queries to the pooled engine."""
    assert _mode_of(None, True, False) == EXTERNAL
    assert _mode_of(None, True, True) == EXTERNAL


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


def test_external_kinds_exclude_engines_an_org_cannot_operate():
    from provisa.api.admin.org_engine_router import _external_kinds

    keys = {k["key"] for k in _external_kinds()}
    # Bundled Trino is the deployment's to run and DuckDB is in-process — neither is reachable
    # from outside, so neither is offered as an engine the ORG operates.
    assert "trino" not in keys and "duckdb" not in keys
    # Embedded ClickHouse (chdb) links into the Provisa process; its "URL" is a local data
    # directory, not an address an org can operate. clickhouse-server is its external counterpart.
    assert "clickhouse" not in keys and "clickhouse-server" in keys
    assert {"databricks", "snowflake", "bigquery", "trino-byo"} <= keys
    assert all(k["addressing"] in {"url", "endpoint"} for k in _external_kinds())


def test_a_deployment_managed_trino_is_endpoint_addressed_when_the_org_runs_it():
    """The bundled kind carries no address in the registry because the DEPLOYMENT manages its
    coordinator; an org that operates one reaches it exactly as trino-byo does."""
    from provisa.api.admin.org_engine_router import _external_addressing

    assert _external_addressing("trino") == "endpoint"
    assert _external_addressing("databricks") == "url"


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
    from provisa.api.admin.org_engine_router import _isolated_available

    monkeypatch.delenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", raising=False)
    assert _isolated_available() is False
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    assert _isolated_available() is True


# ---- the surface is org-scoped, not deployment-wide --------------------------


def test_endpoints_are_gated_on_org_settings():
    """The engine KIND is platform_settings; which lane an org runs on is the org's own setting,
    so an org_admin holds it in either tenancy mode (REQ-1349)."""
    import inspect

    from provisa.api.admin import org_engine_router as mod

    for fn in (mod.get_org_engine, mod.set_org_engine):
        src = inspect.getsource(fn)
        assert "require_org_settings(request)" in src
        assert "require_platform_settings" not in src


# ---- the entitlement gate ----------------------------------------------------


def test_the_lane_is_entitlement_checked_before_anything_is_written():
    """REQ-1412: the isolated lane is platform-run compute, so on a hosted deployment it belongs to
    a plan. The check has to precede the write and the provisioning, or a refused org still gets a
    coordinator stood up for it."""
    import inspect

    from provisa.api.admin import org_engine_router as mod

    src = inspect.getsource(mod.set_org_engine)
    gate = src.index("require_lane_entitlement")
    assert gate < src.index("update(orgs)")
    assert gate < src.index("state.org_registry.rebuild")
    assert "lane_entitled(state, org_id, ISOLATED)" in inspect.getsource(mod.get_org_engine)


async def test_a_deployment_without_the_commercial_plugin_gates_no_lane():
    """A self-hosted deployment has no subscription, so every lane is open to it."""
    from provisa.core import commerce

    commerce.reset_for_tests()
    assert commerce.load() is None
    assert await commerce.lane_entitled(state, "acme", "isolated") is True
    await commerce.require_lane_entitlement(state, "acme", "isolated")
