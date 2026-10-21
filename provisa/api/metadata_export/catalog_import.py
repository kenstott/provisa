# Copyright (c) 2026 Kenneth Stott
# Canary: 1e6c8a42-7d39-4b05-9f21-c3a5e8d0b764
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A catalog's Import, on demand and on its schedule (REQ-2122).

The Import action reads, for every table of the catalog's domains linked to it (by an export or a
steward's pick), the linked asset's current description, column descriptions, tags and glossary
terms, and lists what differs from Provisa's values for the steward to accept, all at once or
table by table. A table that is not linked is listed as such and nothing is guessed for it.

The scheduled import (``import_cron`` on the catalog, run by the existing scheduler) applies the
catalog's current metadata without review to the tables locked to it, and for its other linked
tables holds what changed for a steward's review, never overwriting a local edit.
"""

# Requirements: REQ-2122

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.api.metadata_export import linking
from provisa.api.metadata_export.reader import catalog_reader
from provisa.core.repositories import catalog_link
from provisa.core.schema_org import registered_tables

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from provisa.core.database import Connection
    from provisa.core.models import CatalogConnection

log = logging.getLogger(__name__)

#: How many of a catalog's assets one import reads at once.
_READS_AT_ONCE = 8


def _change_view(change: linking.Change) -> dict[str, Any]:
    return {
        "field": change.field,
        "column": change.column,
        "current": change.current,
        "catalog": change.catalog,
        "local_edit": change.local_edit,
    }


async def _domain_tables(conn: "Connection", catalog: "CatalogConnection") -> list[int]:
    rows = (
        await conn.execute_core(
            select(registered_tables.c.id)
            .where(registered_tables.c.domain_id.in_(catalog.domains))
            .order_by(registered_tables.c.domain_id, registered_tables.c.table_name)
        )
    ).fetchall()
    return [r.id for r in rows]


async def review(
    conn: "Connection", catalog: "CatalogConnection", uri_org_id: str | None
) -> dict[str, Any]:
    """The Import review: each linked table of the catalog's domains with what its asset holds
    differently, and each table not linked to the catalog."""
    reader = catalog_reader(catalog)
    contexts = [
        await linking.table_context(conn, table_id, uri_org_id)
        for table_id in await _domain_tables(conn, catalog)
    ]
    linked = await catalog_link.linked_in(conn, catalog.id)
    gate = asyncio.Semaphore(_READS_AT_ONCE)

    async def _one(ctx: linking.TableContext) -> dict[str, Any]:
        link = linked[ctx.semantic_uri]
        held = (await catalog_link.imports(conn, ctx.id)).get(catalog.id)
        try:
            async with gate:
                meta = await reader.read(link.vendor_ref)
        except Exception as exc:  # noqa: BLE001 - allow-blind-except: the reason IS the answer
            # One asset the catalog could not answer for is reported with the reason it gave,
            # beside every other table's review.
            return {
                "table_id": ctx.id,
                "table": ctx.name,
                "asset": link.name,
                "error": f"{type(exc).__name__}: {exc}",
            }
        found = await linking.changes(conn, ctx, meta, held)
        return {
            "table_id": ctx.id,
            "table": ctx.name,
            "asset": link.name,
            "url": meta.asset.url,
            "locked": held.locked if held is not None else False,
            "imported_at": held.imported_at.isoformat() if held and held.imported_at else None,
            "pending_at": held.pending_at.isoformat() if held and held.pending_at else None,
            "changes": [_change_view(c) for c in found],
        }

    reviewed = await asyncio.gather(*(_one(ctx) for ctx in contexts if ctx.semantic_uri in linked))
    return {
        "catalog": catalog.id,
        "linked": list(reviewed),
        "not_linked": [
            {"table_id": ctx.id, "table": ctx.name}
            for ctx in contexts
            if ctx.semantic_uri not in linked
        ],
    }


async def accept(
    conn: "Connection",
    catalog: "CatalogConnection",
    table_ids: list[int],
    uri_org_id: str | None,
) -> dict[int, dict[str, Any]]:
    """Apply every change the linked asset holds to each of ``table_ids``, as a steward accepting
    them from the review. A table not linked to the catalog is refused by name."""
    reader = catalog_reader(catalog)
    linked = await catalog_link.linked_in(conn, catalog.id)
    applied: dict[int, dict[str, Any]] = {}
    for table_id in table_ids:
        ctx = await linking.table_context(conn, table_id, uri_org_id)
        link = linked.get(ctx.semantic_uri)
        if link is None:
            raise linking.LinkRefused(
                "not_linked",
                f"Table {ctx.name!r} is not linked to an asset of {catalog.id!r}",
                table=ctx.name,
                catalog=catalog.id,
            )
        meta = await reader.read(link.vendor_ref)
        held = (await catalog_link.imports(conn, ctx.id)).get(catalog.id)
        found = await linking.changes(conn, ctx, meta, held)
        applied[table_id] = await linking.apply(
            conn, catalog, ctx, meta, linking.accept_all(ctx, meta, held, found)
        )
    return applied


async def scheduled(
    conn: "Connection", catalog: "CatalogConnection", uri_org_id: str | None
) -> dict[str, int]:
    """One scheduled import: the locked tables take the catalog's metadata, and what changed for
    the other linked tables is held for review. Returns how many of each."""
    reader = catalog_reader(catalog)
    linked = await catalog_link.linked_in(conn, catalog.id)
    locked = await catalog_link.locked_to(conn, catalog.id)
    counts = {"applied": 0, "held": 0}
    for table_id in await _domain_tables(conn, catalog):
        ctx = await linking.table_context(conn, table_id, uri_org_id)
        link = linked.get(ctx.semantic_uri)
        if link is None:
            continue
        meta = await reader.read(link.vendor_ref)
        held = (await catalog_link.imports(conn, ctx.id)).get(catalog.id)
        found = await linking.changes(conn, ctx, meta, held)
        if table_id in locked:
            if found:
                async with conn.transaction():
                    await linking.apply(
                        conn, catalog, ctx, meta, linking.accept_all(ctx, meta, held, found)
                    )
                counts["applied"] += 1
            continue
        await catalog_link.hold_for_review(
            conn, ctx.id, catalog.id, [_change_view(c) for c in found] or None
        )
        counts["held"] += 1 if found else 0
    return counts


def scheduled_job_id(org_id: str, catalog_id: str) -> str:
    return f"catalog_import:{org_id}:{catalog_id}"


async def run_scheduled(org_id: str, catalog_id: str) -> dict[str, int] | None:
    """The scheduled import of ``catalog_id`` for ``org_id``, as its job runs it. A catalog that is
    gone or no longer scheduled does nothing."""
    from provisa.api.admin.catalogs_router import catalogs_config, stored_settings
    from provisa.api.admin.schema_helpers import _rebuild_schemas
    from provisa.api.app import state
    from provisa.core import model_change
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        catalog = catalogs_config(await stored_settings()).by_id().get(catalog_id)
        if catalog is None or not catalog.import_cron:
            return None
        model_db = state.model_db
        assert model_db is not None  # a scheduled org has its runtime built
        async with model_change.scope(f"scheduled import from catalog {catalog_id}"):
            async with model_db.acquire() as conn:
                counts = await scheduled(conn, catalog, org_id if state.multitenancy else None)
        if counts["applied"]:
            await _rebuild_schemas()
        log.info(
            "scheduled import from catalog %s for org %s: %d locked tables updated, "
            "%d tables held for review",
            catalog_id,
            org_id,
            counts["applied"],
            counts["held"],
        )
        return counts
    finally:
        reset_current_org(token)


async def register_org_jobs(scheduler: "AsyncIOScheduler", org_id: str) -> None:
    """Arm, or re-arm, each of the org's catalogs' scheduled import, and disarm the ones that
    are no longer scheduled. Called at startup for every built org and whenever the catalogs are
    saved."""
    from apscheduler.triggers.cron import CronTrigger

    from provisa.api.admin.catalogs_router import catalogs_config, stored_settings
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        catalogs = catalogs_config(await stored_settings()).catalogs
    finally:
        reset_current_org(token)
    prefix = scheduled_job_id(org_id, "")
    wanted = {scheduled_job_id(org_id, c.id): c for c in catalogs if c.import_cron}
    for job in scheduler.get_jobs():
        if job.id.startswith(prefix) and job.id not in wanted:
            scheduler.remove_job(job.id)
    for job_id, catalog in wanted.items():
        scheduler.add_job(
            run_scheduled,
            trigger=CronTrigger.from_crontab(catalog.import_cron),
            args=[org_id, catalog.id],
            id=job_id,
            name=job_id,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )


async def register_all_orgs(scheduler: "AsyncIOScheduler") -> None:
    """Arm the scheduled imports of every org this process has a runtime for."""
    from provisa.api.app import state

    for org_id in state.org_registry.all_org_ids():
        await register_org_jobs(scheduler, org_id)
