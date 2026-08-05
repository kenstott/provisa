# Copyright (c) 2026 Kenneth Stott
# Canary: 5d92a71c-4f38-4b60-8e27-c1a4093fd6b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How an external catalog stays current with Provisa (REQ-1072).

Two paths, one publish:

* **Event-driven.** A metadata change posts an event onto the REQ-942 substrate and fans one
  work item out to this org's egress target. ``drain`` claims that target's pending work,
  publishes, and completes it. A *claim* rather than a repeater read, because two processes
  publishing the same change to the same catalog is the failure this substrate exists to
  prevent.
* **Scheduled reconcile.** ``reconcile_org`` republishes the whole snapshot on the org's
  ``reconcile_cron``, correcting drift from an event that never arrived or a catalog restored
  from a backup.

Both publish the *full* snapshot. A delta on the event path would be cheaper and would need its
own builder, its own ordering argument and its own correctness proof — while the full snapshot
already has one: every adapter upserts by fully-qualified name, which is what makes the two
paths converge on the same catalog state instead of racing each other into different ones.

Everything here is per-org. The events queue lives in the org's own schema (REQ-942), the
config and the credential resolve from that org's settings, and the org id is bound around each
publish — so a reconcile for one org cannot read another's target, let alone publish to it.
"""

# Requirements: REQ-1072, REQ-1073

from __future__ import annotations

import logging

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from provisa.api.metadata_egress.builder import build_snapshot
from provisa.api.metadata_egress.provider import PublishResult
from provisa.api.metadata_egress.registry import metadata_egress
from provisa.control_plane.entitlements import (
    EntitlementError,
    Feature,
    UnknownTierError,
    require_feature,
)
from provisa.core.models import MetadataEgressConfig
from provisa.events import queue

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)

# The dependent-table name metadata-change work items are fanned out to, and the lease owner
# that claims them. The queue lives in the org's own schema, so the name needs no org qualifier.
EGRESS_TARGET = "__metadata_egress__"
PROCESSOR_NAME = "metadata_egress"

# What a model-wide change is posted about. The governed model is one publish unit — a domain
# rename or a relationship edit is not attributable to a single table — so the event names the
# model rather than inventing a table that changed.
MODEL_NODE = "__provisa_model__"

# How often a drain looks for claimed-able work. The substrate's own tick is 5s; metadata is
# published to a third-party catalog over the network, so this one is slower on purpose.
DRAIN_SECONDS = 15

# The dialect the governed view SQL is authored in — the dialect of the SQL being read, not of
# whichever engine executes it. Same value the admin publish endpoint passes.
GOVERNED_DIALECT = "postgres"


class EgressNotAllowed(RuntimeError):  # REQ-1072, REQ-1073
    """The org may not publish right now — disabled, or below the entitled tier."""


async def notify_metadata_change(conn: Any, *, table: str, reason: str) -> int:
    """Post a metadata change for ``table`` and fan one work item to the egress target.

    Called in the same transaction as the change itself, which is what the REQ-942 outbox is
    for: a committed model change with no queued publish is exactly the drift the reconcile
    then has to find.
    """
    event_id = await queue.post_event(
        conn,
        source_table=table,
        event_type="replace",
        payload={"metadata_change": True, "reason": reason},
    )
    await queue.fan_out(conn, event_id, [EGRESS_TARGET])
    return event_id


async def notify_model_changed(org_id: str, *, reason: str) -> int | None:
    """Queue a publish because the governed model changed (REQ-1072).

    Returns the event id, or None when this org does not publish — an org with no target must
    not accumulate work items nobody will ever claim.
    """
    from provisa.api.app import state
    from provisa.api.org_runtime import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        try:
            await _egress_config(org_id)
        except EgressNotAllowed:
            return None
        tenant_db = state.tenant_db
        assert tenant_db is not None  # _egress_config refuses without one
        async with tenant_db.acquire() as conn:
            return await notify_metadata_change(conn, table=MODEL_NODE, reason=reason)
    finally:
        reset_current_org(token)


async def _egress_config(org_id: str) -> MetadataEgressConfig:
    """The org's egress settings, refused unless it may publish.

    Entitlement is checked here rather than only at the admin endpoint (REQ-1073): a scheduled
    reconcile has no request behind it, so a config staged while the org was entitled would
    otherwise keep publishing after the plan lapsed.
    """
    from provisa.api.app import state
    from provisa.control_plane.store import control_plane_store
    from provisa.core.org_settings import resolve_org_config

    try:
        require_feature(control_plane_store(), org_id, Feature.METADATA_EGRESS)
    except (EntitlementError, KeyError, UnknownTierError) as exc:
        raise EgressNotAllowed(f"org {org_id!r} is not entitled to metadata egress: {exc}") from exc
    tenant_db = state.tenant_db
    if tenant_db is None:
        # The org's settings live in its tenant database; without one there is no config to
        # publish from. Reaching here means the runtime was never built, which is a wiring
        # fault to surface rather than a state to publish an empty catalog from.
        raise EgressNotAllowed(f"org {org_id!r} has no tenant database bound")
    resolved = await resolve_org_config(tenant_db)
    config = MetadataEgressConfig(**dict(resolved.get("metadata_egress") or {}))
    if not config.enabled:
        raise EgressNotAllowed(f"org {org_id!r} has metadata egress disabled")
    return config


async def publish_snapshot(org_id: str) -> PublishResult:
    """Publish the org's full metadata snapshot to its configured catalog.

    The single publish path: the admin's **Publish now**, the event drain and the scheduled
    reconcile all arrive here, so what a reconcile sends is what the admin saw it send.
    """
    from provisa.api.app import state
    from provisa.api.org_runtime import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        config = await _egress_config(org_id)
        snapshot = build_snapshot(state.config, org_id=org_id, dialect=GOVERNED_DIALECT)
        return await metadata_egress(config).publish(snapshot)
    finally:
        reset_current_org(token)


async def reconcile_org(org_id: str) -> PublishResult | None:
    """The scheduled full reconcile for one org (REQ-1072).

    An org that is disabled or unentitled is not an error on this path — the job fires on a
    schedule nobody re-armed for that state — so it is logged and skipped. Anything else is
    logged with its traceback and re-raised, because a reconcile that fails quietly leaves a
    catalog drifting with nothing to say so.
    """
    try:
        result = await publish_snapshot(org_id)
    except EgressNotAllowed as exc:
        logger.debug("metadata egress reconcile skipped: %s", exc)
        return None
    except Exception:
        logger.exception("metadata egress reconcile failed for org %s", org_id)
        raise
    if not result.ok:
        logger.warning(
            "metadata egress reconcile for org %s published %d assets with %d rejected: %s",
            org_id,
            result.total_published(),
            len(result.errors),
            "; ".join(f"{e.asset.fqn()}: {e.message}" for e in result.errors[:5]),
        )
    return result


async def drain(conn: Any, org_id: str, *, now: datetime | None = None) -> PublishResult | None:
    """Claim this org's pending metadata-change work, publish, and complete it (REQ-1072).

    Returns None when there was nothing to claim. A publish that fails leaves the work items
    claimed and un-completed, so the REQ-959 reaper returns them to unclaimed once the lease
    lapses and the next drain retries them — a failed publish is never completed away.

    A completion that loses the ownership CAS means a peer reclaimed the work mid-publish. The
    peer will publish the same snapshot to the same fully-qualified names, so the catalog still
    converges; what must not happen is this process reporting work it no longer owns as done.
    """
    at = now or datetime.now(timezone.utc)
    claimed = await queue.claim(
        conn, dependent_table=EGRESS_TARGET, processor_name=PROCESSOR_NAME, now=at
    )
    if not claimed:
        return None
    result = await publish_snapshot(org_id)
    if not result.ok:
        logger.warning(
            "metadata egress publish for org %s rejected %d assets; %d work items stay claimed "
            "for the reaper",
            org_id,
            len(result.errors),
            len(claimed),
        )
        return result
    for event_id in claimed:
        await queue.complete(
            conn,
            event_id=event_id,
            dependent_table=EGRESS_TARGET,
            processor_name=PROCESSOR_NAME,
            now=at,
        )
    return result


async def drain_org(org_id: str) -> PublishResult | None:
    """The scheduled drain for one org — what the interval job fires."""
    from provisa.api.app import state
    from provisa.api.org_runtime import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        tenant_db = state.tenant_db
        if tenant_db is None:
            return None
        async with tenant_db.acquire() as conn:
            return await drain(conn, org_id)
    except EgressNotAllowed as exc:
        # The org stopped publishing between the job being armed and this fire. Its work items
        # stay unclaimed until the config comes back or the job is disarmed.
        logger.debug("metadata egress drain skipped: %s", exc)
        return None
    finally:
        reset_current_org(token)


def reconcile_job_id(org_id: str) -> str:
    return f"metadata_egress:reconcile:{org_id}"


def drain_job_id(org_id: str) -> str:
    return f"metadata_egress:drain:{org_id}"


async def register_org_jobs(scheduler: AsyncIOScheduler, org_id: str) -> None:
    """Arm — or re-arm — this org's drain and reconcile jobs (REQ-1072).

    Called at startup for every built org and again whenever an admin saves the settings, so a
    changed schedule or a newly-configured target takes effect without a restart. An org that
    may not publish has both jobs removed rather than left armed: a job that fires only to skip
    reports a sync cadence the org does not have.
    """
    from provisa.api.org_runtime import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        config = await _egress_config(org_id)
    except EgressNotAllowed:
        for job_id in (drain_job_id(org_id), reconcile_job_id(org_id)):
            if scheduler.get_job(job_id) is not None:
                scheduler.remove_job(job_id)
        return
    finally:
        reset_current_org(token)

    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler.add_job(
        drain_org,
        trigger=IntervalTrigger(seconds=DRAIN_SECONDS),
        args=[org_id],
        id=drain_job_id(org_id),
        name=f"metadata_egress:drain:{org_id}",
        replace_existing=True,
        # One publish at a time per org: overlapping drains would send two snapshots of
        # different vintages to the same catalog with no ordering between them.
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        reconcile_org,
        trigger=CronTrigger.from_crontab(config.reconcile_cron),
        args=[org_id],
        id=reconcile_job_id(org_id),
        name=f"metadata_egress:reconcile:{org_id}",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.info(
        "metadata egress sync armed for org %s: drain every %ds, reconcile %s -> %s",
        org_id,
        DRAIN_SECONDS,
        config.reconcile_cron,
        config.provider,
    )


async def register_all_orgs(scheduler: AsyncIOScheduler) -> None:
    """Arm the sync jobs for every org this process has a runtime for."""
    from provisa.api.app import state

    for org_id in state.org_registry.all_org_ids():
        await register_org_jobs(scheduler, org_id)
