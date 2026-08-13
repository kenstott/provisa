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
  work item out to this org's export target. ``drain`` claims that target's pending work,
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

from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.dq_outcomes import read_latest_outcomes
from provisa.api.metadata_export.provider import PublishResult
from provisa.api.metadata_export.registry import metadata_export
from provisa.control_plane.entitlements import (
    EntitlementError,
    Feature,
    UnknownTierError,
    require_feature,
)
from provisa.core.models import MetadataExportConfig
from provisa.events import queue

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from provisa.core.models import ProvisaConfig

logger = logging.getLogger(__name__)

# The dependent-table name metadata-change work items are fanned out to, and the lease owner
# that claims them. The queue lives in the org's own schema, so the name needs no org qualifier.
EGRESS_TARGET = "__metadata_export__"
PROCESSOR_NAME = "metadata_export"

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


class ExportNotAllowed(RuntimeError):  # REQ-1072, REQ-1073
    """The org may not publish right now — disabled, or below the entitled tier."""


async def notify_metadata_change(conn: Any, *, table: str, reason: str) -> int:
    """Post a metadata change for ``table`` and fan one work item to the export target.

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
            await _export_config(org_id)
        except ExportNotAllowed:
            return None
        tenant_db = state.tenant_db
        assert tenant_db is not None  # _export_config refuses without one
        async with tenant_db.acquire() as conn:
            return await notify_metadata_change(conn, table=MODEL_NODE, reason=reason)
    finally:
        reset_current_org(token)


async def _export_config(org_id: str) -> MetadataExportConfig:
    """The org's export settings, refused unless it may publish.

    Entitlement is checked here rather than only at the admin endpoint (REQ-1073): a scheduled
    reconcile has no request behind it, so a config staged while the org was entitled would
    otherwise keep publishing after the plan lapsed.
    """
    from provisa.api.app import state
    from provisa.control_plane.store import control_plane_store
    from provisa.core.org_settings import resolve_org_config

    try:
        require_feature(control_plane_store(), org_id, Feature.METADATA_EXPORT)
    except (EntitlementError, KeyError, UnknownTierError) as exc:
        raise ExportNotAllowed(f"org {org_id!r} is not entitled to metadata export: {exc}") from exc
    tenant_db = state.tenant_db
    if tenant_db is None:
        # The org's settings live in its tenant database; without one there is no config to
        # publish from. Reaching here means the runtime was never built, which is a wiring
        # fault to surface rather than a state to publish an empty catalog from.
        raise ExportNotAllowed(f"org {org_id!r} has no tenant database bound")
    resolved = await resolve_org_config(tenant_db)
    config = MetadataExportConfig(**dict(resolved.get("metadata_export") or {}))
    if not config.enabled:
        raise ExportNotAllowed(f"org {org_id!r} has metadata export disabled")
    return config


async def _model_for_export() -> "ProvisaConfig":
    """The model the snapshot builds from — the REGISTRATION tables, not the boot YAML.

    Table registration is the source of truth: a dynamically-registered table (openapi
    endpoint, graphql_remote) is a governed asset like any other, and it exists only in the
    DB. ``build_live_config`` is the settled DB→config assembly (name-resolved relationship
    refs, internal meta/ops excluded); sources likewise come from the DB, which holds the
    union of file-declared and runtime-registered ones. Tags ride over from the hydrated
    runtime config — the DB is their source of truth too (REQ-1373/1377).
    """
    from provisa.api.admin.config_export import build_live_config
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.app import state
    from provisa.core.config_loader import parse_config_dict
    from provisa.core.repositories import source as source_repo

    raw = await build_live_config()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        source_rows = await source_repo.list_all(conn)
    # The snapshot needs each source's identity, type and description — never credentials —
    # so the DB rows project onto minimal Source models rather than the credentialed file ones.
    # The __derived__ sentinel is not a source: a derived relation's provenance is the lineage
    # of its definition (REQ-1157), and the sentinel id fails Source validation by design.
    from provisa.core.models import DERIVED_SOURCE_ID

    raw["sources"] = [
        {"id": r["id"], "type": r["type"], "description": r.get("description") or ""}
        for r in source_rows
        if r["id"] != DERIVED_SOURCE_ID
    ]
    config = parse_config_dict(raw)
    if state.config is not None:
        config.tags = state.config.tags
        config.tag_assignments = state.config.tag_assignments
    return config


def _snapshot_uris(snapshot: Any) -> set[str]:
    """Every Provisa URN the snapshot publishes — the bindings worth keeping (REQ-1389)."""
    uris = {source.semantic_uri for source in snapshot.sources}
    for table in snapshot.tables:
        uris.add(table.semantic_uri)
        uris.update(column.semantic_uri for column in table.columns)
    # REQ-1387: term bindings are the OWNERSHIP test for vendor-side glossary items — a
    # pruned term binding would make Provisa's own published term read as external-owned
    # (never modifiable again), so published terms keep their bindings like any asset.
    uris.update(term.semantic_uri for term in snapshot.glossary_terms)
    uris.discard("")
    return uris


async def publish_snapshot(org_id: str) -> PublishResult:
    """Publish the org's full metadata snapshot to its configured catalog.

    The single publish path: the admin's **Publish now**, the event drain and the scheduled
    reconcile all arrive here, so what a reconcile sends is what the admin saw it send —
    and the REQ-1389 binding lifecycle (load stored vendor ids before, persist captured
    ones after) rides every trigger for the same reason.
    """
    from provisa.api.app import state
    from provisa.api.org_runtime import reset_current_org, set_current_org
    from provisa.core.repositories import catalog_binding

    token = set_current_org(org_id)
    try:
        config = await _export_config(org_id)
        model = await _model_for_export()
        tenant_db = state.tenant_db
        assert tenant_db is not None  # _export_config refuses without one
        # REQ-1387: the term graph exports with the model; it lives only in the DB, so it
        # hydrates here the way sources do rather than through the config file vocabulary.
        from provisa.core.repositories import glossary as glossary_repo

        async with tenant_db.acquire() as conn:
            glossary = await glossary_repo.export_graph(conn)
        # REQ-1443: a check's last verdict lives in its results table, which is data rather than
        # config, so it is read here and handed to the builder exactly as the term graph is.
        dq_outcomes = await read_latest_outcomes(model)
        snapshot = build_snapshot(
            model,
            org_id=org_id,
            dialect=GOVERNED_DIALECT,
            glossary=glossary,
            dq_outcomes=dq_outcomes,
        )
        exporter = metadata_export(config)
        async with tenant_db.acquire() as conn:
            # REQ-1389: hand the provider the vendor ids captured by earlier publishes, so
            # a physically re-addressed asset rebinds the SAME catalog entity instead of
            # trusting the vendor's name-keyed upsert.
            exporter.stored_bindings = await catalog_binding.load_bindings(
                conn, config.provider
            )
        result = await exporter.publish(snapshot)
        if result.bindings:
            async with tenant_db.acquire() as conn:
                await catalog_binding.upsert_bindings(conn, config.provider, result.bindings)
                await catalog_binding.remove_stale_bindings(
                    conn, config.provider, keep_uris=_snapshot_uris(snapshot)
                )
        return result
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
    except ExportNotAllowed as exc:
        logger.debug("metadata export reconcile skipped: %s", exc)
        return None
    except Exception:
        logger.exception("metadata export reconcile failed for org %s", org_id)
        raise
    if not result.ok:
        logger.warning(
            "metadata export reconcile for org %s published %d assets with %d rejected: %s",
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
            "metadata export publish for org %s rejected %d assets; %d work items stay claimed "
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
    except ExportNotAllowed as exc:
        # The org stopped publishing between the job being armed and this fire. Its work items
        # stay unclaimed until the config comes back or the job is disarmed.
        logger.debug("metadata export drain skipped: %s", exc)
        return None
    finally:
        reset_current_org(token)


def reconcile_job_id(org_id: str) -> str:
    return f"metadata_export:reconcile:{org_id}"


def drain_job_id(org_id: str) -> str:
    return f"metadata_export:drain:{org_id}"


async def register_org_jobs(scheduler: AsyncIOScheduler, org_id: str) -> None:
    """Arm — or re-arm — this org's drain and reconcile jobs (REQ-1072).

    Called at startup for every built org and again whenever an admin saves the settings, so a
    changed schedule or a newly-configured target takes effect without a restart. An org that
    may not publish has both jobs removed rather than left armed: a job that fires only to skip
    reports a reconcile cadence the org does not have.
    """
    from provisa.api.org_runtime import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        config = await _export_config(org_id)
    except ExportNotAllowed:
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
        name=f"metadata_export:drain:{org_id}",
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
        name=f"metadata_export:reconcile:{org_id}",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.info(
        "metadata export publishing armed for org %s: drain every %ds, reconcile %s -> %s",
        org_id,
        DRAIN_SECONDS,
        config.reconcile_cron,
        config.provider,
    )


async def register_all_orgs(scheduler: AsyncIOScheduler) -> None:
    """Arm the publish jobs for every org this process has a runtime for."""
    from provisa.api.app import state

    for org_id in state.org_registry.all_org_ids():
        await register_org_jobs(scheduler, org_id)
