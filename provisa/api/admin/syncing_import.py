# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9f40-1a5b-4d38-b6e7-0f3d8a4c9e21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Syncing entry's scheduled Import, which nobody watches (REQ-2247, REQ-2122).

On the entry's ``import_cron`` the run reads the catalog's differences with Provisa, exactly as the
review does, and decides each one Import lists by the steward's standing choice for its kind:

* **take the catalog's value**: written into Provisa and recorded as last exchanged, decided by the
  standing choice, but only when the catalog alone moved off the value last exchanged. A value
  edited in Provisa since the last exchange, or never exchanged, is held: an unattended run never
  overwrites a local edit (REQ-2122), and decides only a difference whose moved side it can tell.
  A kind Provisa cannot take from a catalog yet is held too.
* **take Provisa's value**: Import writes nothing into Provisa and never changes the catalog; the
  difference is left to Export.
* **hold**, or no standing choice: held. A held difference waits in the review list, which reads it
  afresh; the run names it and why.

Each run is recorded with its outcome and what it applied and held. A direction that is off never
runs: its job is not armed, and a run asked of it raises :class:`SyncDirectionOff`.
"""

# Requirements: REQ-2247, REQ-2122

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from provisa.api.admin import syncing_review as sr
from provisa.api.errors import ApiError
from provisa.core.models import SyncDirectionOff
from provisa.core.repositories import catalog_standing_choice as standing
from provisa.core.repositories import catalog_sync_run

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)

#: Who decided a value a run took: the steward's standing choice.
STANDING_CHOICE = "standing_choice"

# Why a difference is held.
NO_CHOICE = "no standing choice"
HELD_BY_CHOICE = "standing choice is hold"
EDITED_IN_PROVISA = "edited in Provisa since the last exchange"
NEVER_EXCHANGED = "never exchanged, so which side moved is unknown"
NOT_IMPORTABLE = "Provisa cannot take this kind from a catalog yet"
# Why a difference is left alone by Import.
LEFT_TO_EXPORT = "standing choice is Provisa's value, which Export sends"


@dataclass
class ImportPlan:  # REQ-2247
    """What one run does with the differences Import lists."""

    take: list[sr.Difference] = field(default_factory=list)
    held: list[tuple[sr.Difference, str]] = field(default_factory=list)
    left: list[tuple[sr.Difference, str]] = field(default_factory=list)


def plan(diffs: list[sr.Difference], choices: dict[str, str]) -> ImportPlan:
    """Decide each difference Import lists by ``choices``, the Import standing choice per kind."""
    out = ImportPlan()
    for diff in diffs:
        if sr.IMPORT not in diff.directions:
            continue  # only Provisa moved: Export's to send, nothing for Import to decide
        choice = choices.get(diff.ours.kind)
        if choice is None:
            out.held.append((diff, NO_CHOICE))
        elif choice == standing.HOLD:
            out.held.append((diff, HELD_BY_CHOICE))
        elif choice == standing.PROVISA:
            out.left.append((diff, LEFT_TO_EXPORT))
        elif diff.changed is None:
            out.held.append((diff, NEVER_EXCHANGED))
        elif diff.changed != sr.CATALOG:
            out.held.append((diff, EDITED_IN_PROVISA))
        elif diff.ours.kind not in sr.IMPORTABLE:
            out.held.append((diff, NOT_IMPORTABLE))
        else:
            out.take.append(diff)
    return out


def _named(diff: sr.Difference, reason: str | None = None) -> dict[str, Any]:
    named: dict[str, Any] = {"kind": diff.ours.kind, "object": diff.ours.object}
    if reason is not None:
        named["reason"] = reason
    return named


async def run_import(org_id: str, entry_id: str, *, trigger: str) -> dict[str, Any]:
    """Run ``entry_id``'s Import once and record it, started by ``trigger`` (its schedule, or an
    administrator's Run now). Returns what it applied and held. Run now decides exactly as the
    schedule does: by the standing choices, never by the administrator who pressed it.

    A read the catalog refused is recorded as a failed run and re-raised: a run that fails quietly
    leaves Provisa drifting with nothing to say so.
    """
    from provisa.api.admin.syncing_router import (
        _entry,
        _model_db,
        entry_differences,
        stored_settings,
    )

    started = datetime.now(timezone.utc)
    entry = _entry(await stored_settings(), entry_id)
    entry.require_runs("import")
    try:
        snapshot, diffs = await entry_differences(org_id, entry)
    except ApiError as exc:
        async with _model_db().acquire() as conn:
            await catalog_sync_run.record(
                conn,
                entry_id,
                sr.IMPORT,
                trigger=trigger,
                outcome=catalog_sync_run.FAILED,
                applied=[],
                held=[],
                started_at=started,
                error=str(exc),
            )
        raise
    async with _model_db().acquire() as conn:
        decided = plan(diffs, (await standing.held(conn, entry_id))[sr.IMPORT])
        await sr.write_into_provisa(conn, snapshot, decided.take)
        await sr.record_taken(
            conn, entry_id, {sr.PROVISA: [], sr.CATALOG: decided.take}, STANDING_CHOICE
        )
        applied = [_named(d) for d in decided.take]
        held = [_named(d, reason) for d, reason in decided.held]
        await catalog_sync_run.record(
            conn,
            entry_id,
            sr.IMPORT,
            trigger=trigger,
            outcome=catalog_sync_run.OK,
            applied=applied,
            held=held,
            started_at=started,
        )
    return {
        "id": entry_id,
        "applied": applied,
        "held": held,
        "left": [_named(d, reason) for d, reason in decided.left],
    }


async def import_job(org_id: str, entry_id: str) -> dict[str, Any] | None:
    """What an entry's import schedule fires, bound to ``org_id``. An entry whose Import was turned
    off or removed since the job was armed is not an error on this path (the job fires on a
    schedule nobody re-armed for that state), so it is logged and nothing runs."""
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        return await run_import(org_id, entry_id, trigger=catalog_sync_run.SCHEDULE)
    except SyncDirectionOff as exc:
        logger.debug("syncing import skipped: %s", exc)
        return None
    except ApiError as exc:
        if exc.status_code == 404:
            logger.debug("syncing import skipped: %s", exc)
            return None
        logger.exception("syncing import failed for org %s entry %s", org_id, entry_id)
        raise
    finally:
        reset_current_org(token)


def import_job_id(org_id: str, entry_id: str) -> str:
    return f"{_job_prefix(org_id)}{entry_id}"


def _job_prefix(org_id: str) -> str:
    return f"syncing:import:{org_id}:"


async def register_import_jobs(scheduler: "AsyncIOScheduler", org_id: str) -> None:
    """Arm, or re-arm, this org's import jobs: one per entry that imports and has an import
    schedule; every other import job of the org is removed. Called at startup and whenever the
    org's entries are saved, so a changed schedule takes effect now."""
    from provisa.api.admin.syncing_router import catalogs_config, stored_settings
    from provisa.core.cron import cron_trigger
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        config = catalogs_config(await stored_settings())
    finally:
        reset_current_org(token)
    scheduled = {c.id: c.import_cron for c in config.catalogs if c.imports and c.import_cron}
    for job in scheduler.get_jobs():
        if job.id.startswith(_job_prefix(org_id)) and job.id[len(_job_prefix(org_id)) :] not in (
            scheduled
        ):
            scheduler.remove_job(job.id)
    for entry_id, cron in scheduled.items():
        scheduler.add_job(
            import_job,
            trigger=cron_trigger(cron),
            args=[org_id, entry_id],
            id=import_job_id(org_id, entry_id),
            name=import_job_id(org_id, entry_id),
            replace_existing=True,
            # One import at a time per entry: two would decide the same differences twice.
            max_instances=1,
            coalesce=True,
        )


async def register_all_orgs(scheduler: "AsyncIOScheduler") -> None:
    """Arm the import jobs for every org this process has a runtime for."""
    from provisa.api.app import state

    for org_id in state.org_registry.all_org_ids():
        await register_import_jobs(scheduler, org_id)
