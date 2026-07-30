# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4c1e2b-6d90-4f37-b1c2-9e5f7a0d3c48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1266: per-request multi-org data-plane primitives (no DB, no engine).

Covers the routing invariants that keep two orgs isolated in one process:
the registry builds each org's runtime exactly once even under concurrency,
``require_current_org`` refuses to default when no org is bound, and the
engine catalog names are org-prefixed for org-scoped sources but bare for the
default/bootstrap org (so single-org deployments stay byte-identical)."""

from __future__ import annotations

import asyncio

import pytest

from provisa.api.org_runtime import (
    OrgRegistry,
    OrgRuntime,
    current_org,
    require_current_org,
    reset_current_org,
    set_current_org,
)
from provisa.compiler.naming import org_prefixed_catalog


def test_org_prefixed_catalog_default_org_is_bare():
    # The bootstrap/default org keeps bare names so single-org deployments and every existing
    # source_to_catalog call site stay byte-identical.
    assert org_prefixed_catalog("root", "pg_sales", default_org="root") == "pg_sales"


def test_org_prefixed_catalog_other_org_is_namespaced():
    assert org_prefixed_catalog("acme", "pg_sales", default_org="root") == "org_acme__pg_sales"


def test_two_orgs_same_source_id_get_distinct_catalogs():
    # The collision guard: identical demo source ids across orgs must map to different physical
    # catalog names in the one global engine namespace.
    a = org_prefixed_catalog("acme", "demo_pg", default_org="root")
    b = org_prefixed_catalog("beta", "demo_pg", default_org="root")
    assert a != b
    assert a == "org_acme__demo_pg"
    assert b == "org_beta__demo_pg"


def test_require_current_org_raises_when_unset():
    # A tenant-data path that reaches this with no org bound is a routing defect — it must raise,
    # never silently default (no-fallback rule).
    assert current_org.get() is None
    with pytest.raises(RuntimeError, match="No active org bound"):
        require_current_org()


def test_set_and_reset_current_org_round_trips():
    token = set_current_org("acme")
    try:
        assert require_current_org() == "acme"
        assert current_org.get() == "acme"
    finally:
        reset_current_org(token)
    assert current_org.get() is None


@pytest.mark.asyncio
async def test_get_or_build_builds_once_under_concurrency():
    # 20 concurrent callers racing on the same unbuilt org must invoke the builder exactly once
    # (per-org lock + double-check), and all receive the same runtime instance.
    registry = OrgRegistry()
    calls = 0

    async def builder(org_id: str) -> OrgRuntime:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)  # widen the race window
        return OrgRuntime(org_id=org_id)

    results = await asyncio.gather(
        *[registry.get_or_build("acme", builder) for _ in range(20)]
    )
    assert calls == 1
    first = results[0]
    assert all(r is first for r in results)
    assert registry.get("acme") is first


def test_appstate_catalog_for_routes_to_contextvar_org():
    # The AppState shim must resolve the ContextVar-selected org's runtime, not the default org's.
    # Two orgs share source id "demo_pg" but under distinct org-prefixed catalogs; catalog_for must
    # return the ACTIVE org's name — a wrong resolution here is a cross-org data leak.
    from provisa.api.app import state

    acme = OrgRuntime(org_id="acme")
    acme.source_catalogs["demo_pg"] = "org_acme__demo_pg"
    beta = OrgRuntime(org_id="beta")
    beta.source_catalogs["demo_pg"] = "org_beta__demo_pg"
    state.org_registry.set("acme", acme)
    state.org_registry.set("beta", beta)
    try:
        tok = set_current_org("acme")
        try:
            assert state.catalog_for("demo_pg") == "org_acme__demo_pg"
        finally:
            reset_current_org(tok)
        tok = set_current_org("beta")
        try:
            assert state.catalog_for("demo_pg") == "org_beta__demo_pg"
        finally:
            reset_current_org(tok)
    finally:
        state.org_registry.invalidate("acme")
        state.org_registry.invalidate("beta")


def test_appstate_schema_build_cache_routes_to_contextvar_org():
    # Domains are per-org. As a process-global this cache held whatever the last org to rebuild put
    # there, so /data/domains handed one org's domain list — meta and ops included — to every other.
    # Both the read and the write must land on the active org's runtime.
    from provisa.api.app import state

    acme = OrgRuntime(org_id="acme")
    beta = OrgRuntime(org_id="beta")
    state.org_registry.set("acme", acme)
    state.org_registry.set("beta", beta)
    try:
        tok = set_current_org("acme")
        try:
            state.schema_build_cache = {"domains": [{"id": "pet-store"}]}
        finally:
            reset_current_org(tok)

        tok = set_current_org("beta")
        try:
            assert state.schema_build_cache == {}, "acme's rebuild must not reach beta"
            state.schema_build_cache = {"domains": [{"id": "meta"}, {"id": "ops"}]}
        finally:
            reset_current_org(tok)

        tok = set_current_org("acme")
        try:
            assert state.schema_build_cache["domains"] == [{"id": "pet-store"}]
        finally:
            reset_current_org(tok)
        assert beta.schema_build_cache["domains"] == [{"id": "meta"}, {"id": "ops"}]
    finally:
        state.org_registry.invalidate("acme")
        state.org_registry.invalidate("beta")


def test_appstate_catalog_for_raises_for_unknown_source():
    # No bare source_to_catalog fallback: an unregistered source under the active org must raise,
    # never silently resolve to the default org's physical catalog.
    from provisa.api.app import state

    acme = OrgRuntime(org_id="acme")
    state.org_registry.set("acme", acme)
    try:
        tok = set_current_org("acme")
        try:
            with pytest.raises(KeyError, match="no catalog in org"):
                state.catalog_for("never_registered")
        finally:
            reset_current_org(tok)
    finally:
        state.org_registry.invalidate("acme")


@pytest.mark.asyncio
async def test_get_or_build_distinct_orgs_build_separately():
    registry = OrgRegistry()

    async def builder(org_id: str) -> OrgRuntime:
        return OrgRuntime(org_id=org_id)

    a = await registry.get_or_build("acme", builder)
    b = await registry.get_or_build("beta", builder)
    assert a is not b
    assert a.org_id == "acme"
    assert b.org_id == "beta"
    assert set(registry.all_org_ids()) == {"acme", "beta"}
