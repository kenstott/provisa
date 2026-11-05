# Copyright (c) 2026 Kenneth Stott
# Canary: a7a26aa8-3d80-4125-b584-9d62c7c597a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Destroying an org forgets what this process held for it (REQ-1300, REQ-2006): every live-demo
visitor is an org of its own, deleted at its idle expiry, so a map keyed by an org, its schemas or
its users that is never told grows by one visitor's entries per visitor, for the process's life.
Another org's entries stay."""

# Requirements: REQ-1300, REQ-2006

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from sqlalchemy import MetaData

from provisa.api.admin import metadata_export_router, orgs_router
from provisa.api_source import engine_cache, fill_cache
from provisa.auth import middleware
from provisa.codesource import remote_sources
from provisa.core import env_copy
from provisa.pgwire import catalog_populate


class _Conn:
    async def execute_core(self, stmt):
        return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [])


class _Pool:
    @asynccontextmanager
    async def acquire(self):
        yield _Conn()


def _destroy(monkeypatch, org_id: str) -> None:
    from provisa.api import app
    from provisa.core import env_store, org_provisioning, region_purge
    from provisa.federation import pgwire_replica
    from provisa.scheduler import jobs

    async def _envs(pool, org):
        return [{"name": "prod"}, {"name": "dev"}]

    async def _nothing(*args, **kwargs):
        return None

    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: _Pool())
    monkeypatch.setattr(orgs_router, "_pool", lambda: _Pool())
    monkeypatch.setattr(orgs_router, "_redis_url", lambda: None)
    monkeypatch.setattr(orgs_router, "_refresh_root_registry_view", _nothing)
    monkeypatch.setattr(env_store, "list_envs", _envs)
    monkeypatch.setattr(region_purge, "purge_org_regions", _nothing)
    monkeypatch.setattr(org_provisioning, "deprovision_org", _nothing)
    monkeypatch.setattr(jobs, "unschedule_org", lambda scheduler, org: None)
    monkeypatch.setattr(app.state, "org_registry", SimpleNamespace(invalidate_org=lambda o: None))
    monkeypatch.setattr(pgwire_replica, "_ENDPOINTS", {})
    asyncio.run(orgs_router.destroy_org(org_id))


def test_destroying_an_org_forgets_its_entries_and_keeps_another_orgs(monkeypatch):
    gone, kept = "sbxgone", "sbxkept"
    monkeypatch.setattr(
        middleware, "_recorded_idp", {(gone, "prod", "u1"): {}, (kept, "prod", "u2"): {}}
    )
    monkeypatch.setattr(remote_sources, "_identities", {gone: object(), kept: object()})
    monkeypatch.setattr(metadata_export_router, "_LAST_PUBLISH", {gone: {}, kept: {}})
    monkeypatch.setattr(
        env_copy,
        "_scoped_metadata",
        {f"org_{gone}": MetaData(), f"org_{gone}_env_dev": MetaData(), f"org_{kept}": MetaData()},
    )
    monkeypatch.setattr(
        fill_cache,
        "_shapes",
        {("cat", f"org_{gone}_cache", "t"): "s", ("cat", f"org_{kept}_cache", "t"): "s"},
    )
    monkeypatch.setattr(
        fill_cache,
        "_mem_fresh",
        {(f"org_{gone}_cache", "t", "h"): 1.0, (f"org_{kept}_cache", "t", "h"): 1.0},
    )
    monkeypatch.setattr(
        engine_cache,
        "_SCHEMA_EXISTS_CACHE",
        {("cat", f"org_{gone}_cache"), ("cat", f"org_{kept}_cache")},
    )
    monkeypatch.setattr(
        engine_cache,
        "_TABLE_EXISTS_CACHE",
        {("cat", f"org_{gone}_cache", "t"): 1.0, ("cat", f"org_{kept}_cache", "t"): 1.0},
    )
    monkeypatch.setattr(
        catalog_populate,
        "_row_count_cache",
        {(gone, "org_admin"): (0.0, {}), (kept, "org_admin"): (0.0, {})},
    )

    from provisa.sparql import reasoning

    monkeypatch.setattr(
        reasoning, "_classified", {(gone, "kyc"): object(), (kept, "kyc"): object()}
    )
    monkeypatch.setattr(reasoning, "_locks", {(gone, "kyc"): object(), (kept, "kyc"): object()})

    _destroy(monkeypatch, gone)

    assert list(reasoning._classified) == [(kept, "kyc")]
    assert list(reasoning._locks) == [(kept, "kyc")]

    assert list(middleware._recorded_idp) == [(kept, "prod", "u2")]
    assert list(remote_sources._identities) == [kept]
    assert list(metadata_export_router._LAST_PUBLISH) == [kept]
    assert list(env_copy._scoped_metadata) == [f"org_{kept}"]
    assert list(fill_cache._shapes) == [("cat", f"org_{kept}_cache", "t")]
    assert list(fill_cache._mem_fresh) == [(f"org_{kept}_cache", "t", "h")]
    assert engine_cache._SCHEMA_EXISTS_CACHE == {("cat", f"org_{kept}_cache")}
    assert list(engine_cache._TABLE_EXISTS_CACHE) == [("cat", f"org_{kept}_cache", "t")]
    assert list(catalog_populate._row_count_cache) == [(kept, "org_admin")]


def test_an_org_whose_id_another_extends_forgets_only_what_is_its_own_or_a_cache_to_refill():
    """``org_a`` is a prefix of ``org_ab``: the schema rule names ``org_a`` and ``org_a_...``, so
    ``org_ab``'s entries stay; an ``org_a_x`` org's would be refilled, never wrong."""
    from provisa.core.org_forget import schema_of_org

    assert schema_of_org("a", "org_a") and schema_of_org("a", "org_a_env_dev")
    assert schema_of_org("a", "org_a_cache")
    assert not schema_of_org("a", "org_ab") and not schema_of_org("a", "org_ab_cache")
