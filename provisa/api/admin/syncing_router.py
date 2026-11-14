# Copyright (c) 2026 Kenneth Stott
# Canary: 9389d2bc-ca65-4f37-bf97-8e9f5fb5db18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Syncing page's surface (REQ-2247, REQ-2122, REQ-1072, REQ-1074).

One list of entries, each an outside system kept in sync with Provisa: a data catalog (its
provider, endpoint, credentials and the domains that work with it), an ontology, or a custom
system. Each entry states its direction (Export, Import or both), and a data catalog carries its
export settings: the role it publishes as, the DataZone project and the Delta Share option. The
description pick list, Refresh from catalog and the lock read a data catalog through the same entry.

Credentials are write-only (REQ-1074): the read reports whether each is set, a credential left out
of a write keeps the one held, and one sent empty clears it. The connection test is the provider's
own health check, the one the export runs.

Publishing is premium-gated (REQ-1073): Publish now, turning an entry's Export on or changing its
export settings, and the organisation-wide reconcile schedule are refused below the entitled tier.
A direction that is off never runs: Publish now refuses an entry that does not export.
"""

# Requirements: REQ-1068, REQ-1072, REQ-1073, REQ-1074, REQ-2118, REQ-2122, REQ-2152, REQ-2247

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import ValidationError

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError
from provisa.api.metadata_export import metadata_export
from provisa.api.metadata_export.provider import PublishResult
from provisa.api.metadata_export.registry import beta_providers, registered_providers
from provisa.control_plane.entitlements import (
    EntitlementError,
    Feature,
    UnknownTierError,
    min_tier,
)
from provisa.core.models import CatalogConnection, CatalogsConfig, MetadataExportConfig
from provisa.core.org_forget import on_forget

router = APIRouter()

# Where the entries are kept in the org's settings, and the organisation-wide schedule.
CONFIG_KEY = "catalogs"
SCHEDULE_KEY = "metadata_export"

# An entry's credential fields, which the read reports as set/not-set and never returns.
SECRET_FIELDS = (
    "api_key",
    "token",
    "entra_client_secret",
    "google_service_account_json",
    "aws_access_key_id",
    "aws_secret_access_key",
)

# An entry's export settings: what a metadata export target carried (REQ-2247).
EXPORT_FIELDS = ("role", "datazone_project_id", "delta_share")

# Everything else of an entry the page edits, returned as held.
PLAIN_FIELDS = (
    "id",
    "type",
    "direction",
    "provider",
    "endpoint",
    "auth_mode",
    "username",
    "entra_tenant_id",
    "entra_client_id",
    "gcp_project",
    "gcp_location",
    "aws_region",
    "datazone_domain_id",
    "timeout_seconds",
    "domains",
    *EXPORT_FIELDS,
)

# The last publish this process ran, per org and entry. In-process only: it records what the admin
# just triggered so the page can show the per-asset errors a partial publish returned; an empty
# entry after a restart means "this process has not published to it", which is what it says.
_LAST_RUN: dict[str, dict[str, dict[str, Any]]] = {}


@on_forget(size=lambda: len(_LAST_RUN))
def _forget_last_run(org_id: str) -> None:
    _LAST_RUN.pop(org_id, None)


def _model_db():
    """The org's model store, where its settings are kept, or a 503."""
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "syncing.database_not_connected", "Database not connected")
    return state.model_db


async def stored_settings() -> dict[str, Any]:
    """The org's settings as resolved: the deployment's with the org's own over them."""
    from provisa.core.org_settings import resolve_org_config

    return await resolve_org_config(_model_db())


def catalogs_config(settings: dict[str, Any]) -> CatalogsConfig:
    """The org's entries, or a 400 naming what is wrong with them."""
    try:
        return CatalogsConfig(catalogs=list(settings.get(CONFIG_KEY) or []))
    except ValidationError as exc:
        raise ApiError(400, "syncing.invalid", str(exc)) from exc


def _schedule(settings: dict[str, Any]) -> MetadataExportConfig:
    try:
        return MetadataExportConfig(**dict(settings.get(SCHEDULE_KEY) or {}))
    except ValidationError as exc:
        raise ApiError(400, "syncing.invalid", str(exc)) from exc


async def _require_entitled(org_id: str) -> None:
    """REQ-1073: refuse publishing below the premium tier."""
    from provisa.control_plane.entitlements import require_feature

    try:
        await require_feature(org_id, Feature.METADATA_EXPORT)
    except EntitlementError as exc:
        raise ApiError(403, "metadata_export.not_entitled", str(exc)) from exc
    except (KeyError, UnknownTierError) as exc:
        raise ApiError(
            403,
            "metadata_export.tier_unknown",
            f"org {org_id!r} has no resolvable tier, so the feature cannot be entitled",
        ) from exc


async def _is_entitled(org_id: str) -> bool:
    """Whether the org may publish -- the flag the page renders its gate from."""
    try:
        await _require_entitled(org_id)
    except ApiError:
        return False
    return True


def _view(entry: CatalogConnection, last_run: dict[str, Any] | None) -> dict[str, Any]:
    return {
        **{name: getattr(entry, name) for name in PLAIN_FIELDS},
        **{f"{name}_set": bool(getattr(entry, name).get_secret_value()) for name in SECRET_FIELDS},
        "last_run": last_run,
    }


def _merged(stored: list[dict[str, Any]], sent: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The entries as sent, each credential left out of the body kept from the stored entry of the
    same id. An entry left out of the body is removed."""
    by_id = {c.get("id"): c for c in stored}
    merged = []
    for entry in sent:
        prior = by_id.get(entry.get("id"), {})
        merged.append(
            {
                **{name: entry[name] for name in PLAIN_FIELDS if name in entry},
                **{
                    name: entry[name] if name in entry else prior.get(name, "")
                    for name in SECRET_FIELDS
                },
            }
        )
    return merged


def _publishes_differently(entry: CatalogConnection, prior: CatalogConnection | None) -> bool:
    """Whether saving ``entry`` turns its Export on or changes what it is published as."""
    if entry.type != "data_catalog" or not entry.exports:
        return False
    if prior is None or not prior.exports:
        return True
    return any(getattr(entry, name) != getattr(prior, name) for name in EXPORT_FIELDS)


async def _domain_ids() -> set[str]:
    from provisa.core.repositories import domain as domain_repo

    async with _model_db().acquire() as conn:
        return {d["id"] for d in await domain_repo.list_all(conn)}


def _caller_scope(request: Request) -> Any:
    import types

    from provisa.api.admin.catalog_scope import catalog_scope

    return catalog_scope(types.SimpleNamespace(context={"request": request}))


def _publishable_roles(request: Request) -> list[str]:
    from provisa.api.admin.read_rights import role_within
    from provisa.api.app import state

    scope = _caller_scope(request)
    return sorted(r for r in (getattr(state, "roles", None) or {}) if role_within(state, scope, r))


def _outcome(entry_id: str, result: PublishResult) -> dict[str, Any]:
    return {
        "id": entry_id,
        "provider": result.provider_name,
        "ok": result.ok,
        "published": dict(result.published),
        "total_published": result.total_published(),
        "errors": [{"asset": e.asset.fqn(), "message": e.message} for e in result.errors],
    }


@router.get("/admin/syncing")
async def get_syncing(request: Request) -> dict:  # REQ-2247, REQ-2122
    """The org's entries, credentials reported as set/not-set, each with its last run, and what
    the page offers: the providers, the roles an entry may publish as and the schedule."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    settings = await stored_settings()
    config = catalogs_config(settings)
    runs = _LAST_RUN.get(org_id, {})
    return {
        "entitled": await _is_entitled(org_id),
        "required_tier": min_tier(Feature.METADATA_EXPORT).value,
        "providers": sorted(registered_providers()),
        "beta_providers": beta_providers(),
        # REQ-2152 (Q8): the roles an entry may be published as -- those whose reads the caller
        # holds too.
        "publishable_roles": _publishable_roles(request),
        "reconcile_cron": _schedule(settings).reconcile_cron,
        "entries": [_view(c, runs.get(c.id)) for c in config.catalogs],
    }


@router.put("/admin/syncing")
async def set_syncing(request: Request) -> dict:  # REQ-2247, REQ-2122, REQ-1072
    """Keep the org's entries as sent, and the reconcile schedule when sent. Refused by name: an
    entry the model refuses, a data catalog for a domain the org does not have, an entry published
    as a role reading more than its saver, and -- below the entitled tier -- turning an entry's
    Export on, changing its export settings or changing the schedule."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    from provisa.api.admin.read_rights import role_within
    from provisa.api.app import state
    from provisa.core.org_settings import write_org_overrides

    body = await request.json()
    settings = await stored_settings()
    prior = catalogs_config(settings).by_id()
    merged = _merged(list(settings.get(CONFIG_KEY) or []), list(body.get("entries") or []))
    config = catalogs_config({CONFIG_KEY: merged})
    known = await _domain_ids()
    for entry in config.catalogs:
        unknown = sorted(set(entry.domains) - known)
        if unknown:
            raise ApiError(
                400,
                "catalogs.unknown_domain",
                f"catalog {entry.id!r} is assigned to domains the organization does not have: "
                f"{', '.join(unknown)}",
                catalog=entry.id,
                domains=", ".join(unknown),
            )
    if any(_publishes_differently(e, prior.get(e.id)) for e in config.catalogs):
        await _require_entitled(org_id)
    # REQ-2152 (Q8): an entry is published only as a role whose reads the saver holds too -- what
    # you cannot read, you cannot give. A role someone else saved is not re-judged. Refused naming
    # no column.
    scope = _caller_scope(request)
    for entry in config.catalogs:
        was = prior.get(entry.id)
        if entry.role is not None and (was is None or was.role != entry.role):
            if not role_within(state, scope, entry.role):
                raise ApiError(
                    403,
                    "metadata_export.role_beyond_reads",
                    f"A catalog can be published as role {entry.role!r} only by someone who reads "
                    "everything that role reads",
                    role=entry.role,
                )
    updates: dict[str, Any] = {CONFIG_KEY: merged}
    if "reconcile_cron" in body:
        schedule = _schedule({SCHEDULE_KEY: {"reconcile_cron": body["reconcile_cron"]}})
        if schedule.reconcile_cron != _schedule(settings).reconcile_cron:
            await _require_entitled(org_id)
        updates[SCHEDULE_KEY] = {"reconcile_cron": schedule.reconcile_cron}
    identity = getattr(request.state, "identity", None)
    await write_org_overrides(
        _model_db(), updates, updated_by=getattr(identity, "user_id", "anonymous")
    )
    # REQ-1072: re-arm this org's sync jobs against what was just saved, so a changed schedule or
    # an entry whose Export was turned on takes effect now rather than at the next restart.
    scheduler = getattr(state, "_scheduler", None)
    if scheduler is not None:
        from provisa.api.metadata_export.publishing import register_org_jobs

        await register_org_jobs(scheduler, org_id)
    return {"success": True, "entries": [c.id for c in config.catalogs]}


def _entry(settings: dict[str, Any], entry_id: str) -> CatalogConnection:
    entry = catalogs_config(settings).by_id().get(entry_id)
    if entry is None:
        raise ApiError(404, "syncing.not_found", f"There is no entry {entry_id!r}", entry=entry_id)
    return entry


@router.post("/admin/syncing/{entry_id}/health")
async def check_entry(request: Request, entry_id: str) -> dict:  # REQ-2122
    """Test one data catalog's connection with its provider's health check, the export's own."""
    require_org_settings(request)
    require_active_org_id(request)
    entry = _entry(await stored_settings(), entry_id)
    if entry.type != "data_catalog":
        raise ApiError(
            400, "syncing.no_connection", f"{entry_id!r} has no connection", entry=entry_id
        )
    try:
        await metadata_export(entry.export_target()).health()
    except Exception as exc:  # noqa: BLE001 - allow-blind-except: the message IS the answer
        # The provider raises whatever its transport raised; the admin needs that text to tell a
        # wrong address from a refused credential, so it is reported rather than classified.
        return {
            "id": entry_id,
            "provider": entry.provider,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {"id": entry_id, "provider": entry.provider, "ok": True}


async def _publish(org_id: str, only: str | None) -> dict[str, Any]:
    """The publish the drain and the reconcile run (REQ-1072), so what an admin sees is what the
    scheduled paths send; each entry's outcome is kept as its last run."""
    from provisa.api.metadata_export.publishing import ExportNotAllowed, publish_snapshot

    try:
        results = await publish_snapshot(org_id, only=only)
    except ExportNotAllowed as exc:
        raise ApiError(400, "syncing.nothing_exports", str(exc)) from exc
    outcomes = [_outcome(entry_id, result) for entry_id, result in results.items()]
    runs = _LAST_RUN.setdefault(org_id, {})
    for outcome in outcomes:
        runs[outcome["id"]] = outcome
    return {"ok": all(o["ok"] for o in outcomes), "entries": outcomes}


@router.post("/admin/syncing/publish")
async def publish_all(request: Request) -> dict:  # REQ-1072, REQ-1074
    """Publish a full snapshot now to every data catalog whose direction includes Export -- the
    on-demand form of the REQ-1072 reconcile, which `provisa metadata export` calls."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    await _require_entitled(org_id)
    return await _publish(org_id, None)


@router.post("/admin/syncing/{entry_id}/publish")
async def publish_entry(request: Request, entry_id: str) -> dict:  # REQ-2247, REQ-1074
    """Publish now, to one entry. Refused when the entry does not export: a direction that is off
    never runs, by hand as on a schedule."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    await _require_entitled(org_id)
    entry = _entry(await stored_settings(), entry_id)
    if entry.type != "data_catalog":
        raise ApiError(
            400, "syncing.no_publish", f"{entry_id!r} has no publish to run", entry=entry_id
        )
    if not entry.exports:
        raise ApiError(
            400,
            "syncing.direction_off",
            f"{entry_id!r} does not export: its direction is off",
            entry=entry_id,
        )
    return await _publish(org_id, entry_id)
