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

ORG_ID = "acme"

_ATLAS = {
    "id": "atlas",
    "type": "data_catalog",
    "direction": "both",
    "provider": "atlas",
    "endpoint": "http://atlas:21000",
    "domains": ["sales"],
    "role": "steward",
}
# REQ-2247: what a publish reaches is each Syncing entry whose direction includes Export.
_CONFIGURED: dict = {
    "catalogs": [_ATLAS],
    "metadata_export": {"reconcile_cron": "0 * * * *"},
}


@pytest.fixture
def org(monkeypatch):
    """An org with a complete export config, whose tier the test sets."""
    import provisa.core.org_settings as org_settings_mod

    configured = {
        key: list(value) if isinstance(value, list) else value for key, value in _CONFIGURED.items()
    }

    async def _resolve(_db):
        return configured

    monkeypatch.setattr(org_settings_mod, "resolve_org_config", _resolve)
    monkeypatch.setattr(
        "provisa.api.app.state",
        types.SimpleNamespace(model_db=(_one_db := object()), tenant_db=_one_db, config=object()),
        raising=False,
    )

    # The tier stored with the org (REQ-1066), as provisa.core.org_tier reads it.
    tiers: dict[str, str] = {}

    async def _tier(org_id: str) -> str | None:
        return tiers.get(org_id)

    monkeypatch.setattr("provisa.core.org_tier.read_org_tier", _tier)
    # A commercial deployment, where the stored tier decides (REQ-1066).
    monkeypatch.setattr("provisa.core.commerce.enabled", lambda: True)

    def _at(tier: str):
        tiers[ORG_ID] = tier

    _at.configured = configured  # type: ignore[attr-defined]
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


# --- the entries publishing reads (REQ-2247) ----------------------------------------------------


@pytest.mark.asyncio
async def test_publishing_reads_the_entries_that_export(org):
    """Each data catalog entry whose direction includes Export, with its export settings."""
    org("premium")
    org.configured["catalogs"] = [
        _ATLAS,
        {**_ATLAS, "id": "atlas-in", "direction": "import"},
        {"id": "terms", "type": "ontology", "direction": "both"},
    ]
    _, targets = await publishing._export_config(ORG_ID)

    assert [(t.id, t.role, t.domains) for t in targets] == [("atlas", "steward", ["sales"])]


@pytest.mark.asyncio
async def test_an_org_with_no_entry_that_exports_has_its_sync_jobs_disarmed(org):
    org("premium")
    scheduler = _scheduler()
    await publishing.register_org_jobs(scheduler, ORG_ID)

    org.configured["catalogs"] = [{**_ATLAS, "direction": "import"}]
    await publishing.register_org_jobs(scheduler, ORG_ID)

    assert scheduler.get_job(publishing.drain_job_id(ORG_ID)) is None
    assert scheduler.get_job(publishing.reconcile_job_id(ORG_ID)) is None


@pytest.mark.asyncio
async def test_a_run_to_an_entry_whose_export_is_off_is_refused_before_a_snapshot(org, monkeypatch):
    """A direction that is off never runs: the publish refuses the entry by name and builds no
    snapshot for it."""
    from provisa.core.models import SyncDirectionOff

    async def _no_snapshot(org_id, **kwargs):
        pytest.fail("a snapshot was built for an entry that does not export")

    monkeypatch.setattr(publishing, "org_snapshot", _no_snapshot)
    org("premium")
    org.configured["catalogs"] = [_ATLAS, {**_ATLAS, "id": "atlas-in", "direction": "import"}]

    with pytest.raises(SyncDirectionOff, match="'atlas-in' does not export"):
        await publishing.publish_snapshot(ORG_ID, only="atlas-in")
