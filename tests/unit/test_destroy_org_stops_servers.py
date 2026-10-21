# Copyright (c) 2026 Kenneth Stott
# Canary: 9be8110e-7f60-44e5-8a54-33886e82528d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Destroying an org stops the connector servers started for its sources (REQ-1300, REQ-2006): a
reaped demo sandbox's files connector, a JVM of ~570 MB, kept running until the process restarted,
and every visitor of the files demo left one behind."""

# Requirements: REQ-1300, REQ-2006

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from provisa.api.admin import orgs_router
from provisa.federation import pgwire_replica


class _Conn:
    async def execute_core(self, stmt):
        return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [])


class _Pool:
    @asynccontextmanager
    async def acquire(self):
        yield _Conn()


class _Server:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_destroying_an_org_stops_its_connector_servers_and_no_other(monkeypatch):
    from provisa.api import app
    from provisa.core import env_store, org_provisioning, region_purge
    from provisa.scheduler import jobs

    async def _envs(pool, org_id):
        return [{"name": "prod"}]

    async def _nothing(*args, **kwargs):
        return None

    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: _Pool())
    monkeypatch.setattr(orgs_router, "_pool", lambda: _Pool())
    monkeypatch.setattr(orgs_router, "_redis_url", lambda: None)
    monkeypatch.setattr(orgs_router, "_refresh_root_registry_view", _nothing)
    monkeypatch.setattr(env_store, "list_envs", _envs)
    monkeypatch.setattr(region_purge, "purge_org_regions", _nothing)
    monkeypatch.setattr(org_provisioning, "deprovision_org", _nothing)
    monkeypatch.setattr(jobs, "unschedule_org", lambda scheduler, org_id: None)
    monkeypatch.setattr(
        app.state, "org_registry", SimpleNamespace(invalidate_org=lambda org_id: None)
    )
    gone, kept = _Server(), _Server()
    monkeypatch.setattr(
        pgwire_replica,
        "_ENDPOINTS",
        {("sbxgone", "drop"): gone, ("sbxkept", "drop"): kept},
    )

    asyncio.run(orgs_router.destroy_org("sbxgone"))

    assert gone.closed and not kept.closed
    assert list(pgwire_replica._ENDPOINTS) == [("sbxkept", "drop")]


def test_an_orgs_jobs_stop_before_anything_of_it_is_dropped(monkeypatch):
    """REQ-2006: the org's scheduled jobs (its replica build pass, every 2 s) go first. Dropped
    after its stores, the pass fired for minutes against schemas already gone ("relation
    replica_state does not exist", cloud-dev's sandbox tier after each close)."""
    from provisa.api import app
    from provisa.core import env_store, org_provisioning, region_purge
    from provisa.scheduler import jobs

    order: list[str] = []

    async def _envs(pool, org_id):
        return [{"name": "prod"}]

    async def _nothing(*args, **kwargs):
        return None

    def _step(name):
        async def _record(*args, **kwargs):
            order.append(name)

        return _record

    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: _Pool())
    monkeypatch.setattr(orgs_router, "_pool", lambda: _Pool())
    monkeypatch.setattr(orgs_router, "_redis_url", lambda: None)
    monkeypatch.setattr(orgs_router, "_refresh_root_registry_view", _nothing)
    monkeypatch.setattr(env_store, "list_envs", _envs)
    monkeypatch.setattr(region_purge, "purge_org_regions", _step("purge regions"))
    monkeypatch.setattr(org_provisioning, "deprovision_org", _step("deprovision"))
    monkeypatch.setattr(jobs, "unschedule_org", lambda scheduler, org_id: order.append("jobs"))
    monkeypatch.setattr(
        app.state, "org_registry", SimpleNamespace(invalidate_org=lambda org_id: None)
    )
    monkeypatch.setattr(pgwire_replica, "_ENDPOINTS", {})

    asyncio.run(orgs_router.destroy_org("sbxgone"))

    assert order[0] == "jobs", order
