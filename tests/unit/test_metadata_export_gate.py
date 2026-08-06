# Copyright (c) 2026 Kenneth Stott
# Canary: 41b7e9c5-08d3-4a26-9f71-b3c05e6a2148
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1073: metadata export is a premium entitlement, on every path into it.

The admin endpoints are gated in ``tests/unit/test_metadata_export_admin_surface.py``. What is
pinned here is the path with no request behind it — the REQ-1072 scheduled reconcile — because a gate
that holds on the API but not on the cron lets an org stage a config while entitled and keep
publishing after the plan lapses.
"""

# Requirements: REQ-1072, REQ-1073

from __future__ import annotations

import types

import pytest

from provisa.api.metadata_export import publishing
from provisa.control_plane.models import Org
from provisa.control_plane.store import control_plane_store

ORG_ID = "acme"

_CONFIGURED = {
    "metadata_export": {
        "enabled": True,
        "provider": "atlas",
        "endpoint": "http://atlas:21000",
        "reconcile_cron": "0 * * * *",
    }
}


@pytest.fixture
def org(monkeypatch):
    """An org with a complete export config, whose tier the test sets."""
    import provisa.core.org_settings as org_settings_mod

    async def _resolve(_db):
        return _CONFIGURED

    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(tenant_db=object(), config=object()),
        raising=False,
    )

    def _at(tier: str):
        control_plane_store().register_org(
            Org(id=ORG_ID, name=ORG_ID, data_plane_id="dp", created_at="2026-01-01", tier=tier)
        )

    return _at


def _scheduler():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    return AsyncIOScheduler()


@pytest.mark.asyncio
async def test_an_entitled_org_gets_both_sync_jobs_on_its_own_schedule(org):
    org("premium")
    scheduler = _scheduler()

    await publishing.register_org_jobs(scheduler, ORG_ID)

    drain = scheduler.get_job(publishing.drain_job_id(ORG_ID))
    reconcile = scheduler.get_job(publishing.reconcile_job_id(ORG_ID))
    assert drain is not None and list(drain.args) == [ORG_ID]
    assert reconcile is not None and list(reconcile.args) == [ORG_ID]
    # The org id is bound to the job rather than read from ambient state when it fires, which is
    # what keeps one org's cron from publishing whatever org happened to be current.
    assert drain.func is publishing.drain_org
    assert reconcile.func is publishing.reconcile_org


@pytest.mark.asyncio
async def test_an_unentitled_org_has_its_sync_jobs_disarmed(org):
    """Armed while entitled, the plan then lapses: the jobs must go, not fire-and-skip."""
    org("premium")
    scheduler = _scheduler()
    await publishing.register_org_jobs(scheduler, ORG_ID)

    org("standard")
    await publishing.register_org_jobs(scheduler, ORG_ID)

    assert scheduler.get_job(publishing.drain_job_id(ORG_ID)) is None
    assert scheduler.get_job(publishing.reconcile_job_id(ORG_ID)) is None


@pytest.mark.asyncio
async def test_an_unentitled_org_cannot_publish_even_on_the_scheduled_path(org):
    org("standard")
    with pytest.raises(publishing.ExportNotAllowed, match="not entitled"):
        await publishing.publish_snapshot(ORG_ID)


@pytest.mark.asyncio
async def test_a_scheduled_reconcile_for_an_unentitled_org_is_skipped_not_raised(org):
    """The job fires on a schedule nobody re-armed for the lapse, so it reports nothing to do
    rather than raising into the scheduler's error log every hour."""
    org("standard")
    assert await publishing.reconcile_org(ORG_ID) is None


@pytest.mark.asyncio
async def test_an_unentitled_org_queues_no_work_when_the_model_changes(org):
    """A work item nobody may ever claim is a queue that only grows."""
    org("standard")
    assert await publishing.notify_model_changed(ORG_ID, reason="schema rebuild") is None
