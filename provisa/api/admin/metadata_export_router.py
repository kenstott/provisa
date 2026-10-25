# Copyright (c) 2026 Kenneth Stott
# Canary: 3b6c9e14-8d27-4f05-a1c3-6e908d24b5f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin surface for metadata export (REQ-1074).

Four operations back the admin tab: read the org's export settings, write them, check each
target is reachable, and publish a full snapshot on demand to every target (REQ-2118: an org may
publish to several catalogs, two of one provider among them). Every one of them is gated twice —
on the ``org_settings`` right, and on the org's REQ-1073 entitlement — because a premium
feature reachable through an un-entitled org's admin API is not gated at all.

A target names a catalog of the Catalogs panel (REQ-2122), which holds its endpoint and
credentials (``catalogs_router``); the export keeps only its own settings for it.
"""

# Requirements: REQ-1068, REQ-1072, REQ-1073, REQ-1074, REQ-2118, REQ-2122

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
from provisa.core.models import MetadataExportConfig, MetadataExportTarget
from provisa.core.org_forget import on_forget

router = APIRouter()

CONFIG_KEY = "metadata_export"

# A target's fields: the catalog it names in the Catalogs panel and its export settings alone
# (REQ-2122). The catalog holds the endpoint and the credentials.
_TARGET_FIELDS = ("catalog", "datazone_project_id", "delta_share", "role")  # REQ-2152: role

# The settings of the export as a whole.
_EXPORT_FIELDS = ("enabled", "reconcile_cron")

# The last publish this process performed, per org. In-process only: it records what the admin
# just triggered so the tab can show the per-asset errors a partial publish returned, and an
# empty entry after a restart means "this process has not published", which is what it says.
_LAST_PUBLISH: dict[str, dict[str, Any]] = {}


@on_forget(size=lambda: len(_LAST_PUBLISH))
def _forget_last_publish(org_id: str) -> None:
    _LAST_PUBLISH.pop(org_id, None)


async def _require_entitled(org_id: str) -> None:
    """REQ-1073: refuse the whole surface below the premium tier."""
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
    """Whether the org may use the feature — the flag the tab renders its gate from."""
    from provisa.control_plane.entitlements import require_feature

    try:
        await require_feature(org_id, Feature.METADATA_EXPORT)
    except (EntitlementError, KeyError, UnknownTierError):
        return False
    return True


def _model_db():
    """The org's model store (REQ-1919), or a 503.

    The export settings live in it, so a runtime with none bound cannot answer this endpoint at
    all -- that is a wiring fault to surface, not an empty settings document to serve.
    """
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "metadata_export.database_not_connected", "Database not connected")
    return state.model_db


async def _stored() -> dict[str, Any]:
    """The org's export settings as stored, with the deployment's beneath them."""
    from provisa.core.org_settings import resolve_org_config

    cfg = await resolve_org_config(_model_db())
    return dict(cfg.get(CONFIG_KEY) or {})


def _export_config(stored: dict[str, Any]) -> MetadataExportConfig:
    try:
        return MetadataExportConfig(**stored)
    except ValidationError as exc:
        raise ApiError(400, "metadata_export.invalid_config", str(exc)) from exc


async def _targets(config: MetadataExportConfig) -> list[MetadataExportTarget]:
    """Each target built from the catalog it names (REQ-2122), or a 400 naming the target that
    names no catalog of the org."""
    from provisa.api.admin.catalogs_router import catalogs_config, stored_settings

    try:
        return config.export_targets(catalogs_config(await stored_settings()))
    except ValueError as exc:
        raise ApiError(400, "metadata_export.invalid_config", str(exc)) from exc


def _publish_payload(results: dict[str, PublishResult]) -> dict[str, Any]:
    return {
        "ok": all(r.ok for r in results.values()),
        "targets": [
            {
                "id": target_id,
                "provider": result.provider_name,
                "ok": result.ok,
                "published": dict(result.published),
                "total_published": result.total_published(),
                "errors": [{"asset": e.asset.fqn(), "message": e.message} for e in result.errors],
            }
            for target_id, result in results.items()
        ],
    }


@router.get("/admin/metadata-export")
async def get_metadata_export(request: Request) -> dict:  # REQ-1074
    """The org's export settings, with credentials reported as set/not-set."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    stored = await _stored()
    config = _export_config(stored)
    return {
        "entitled": await _is_entitled(org_id),
        "required_tier": min_tier(Feature.METADATA_EXPORT).value,
        "providers": sorted(registered_providers()),
        "beta_providers": beta_providers(),
        "config": {
            **{name: getattr(config, name) for name in _EXPORT_FIELDS},
            "targets": [
                {name: getattr(t, name) for name in _TARGET_FIELDS} for t in config.targets
            ],
        },
        "last_publish": _LAST_PUBLISH.get(org_id),
    }


@router.put("/admin/metadata-export")
async def set_metadata_export(request: Request) -> dict:  # REQ-1074
    """Persist the org's export settings: the export's own and its list of targets.

    Each target names a catalog of the Catalogs panel, which holds its endpoint and credentials,
    and carries the export's own settings for it (REQ-2122). A target the body leaves out of its
    list is removed.
    """
    require_org_settings(request)
    org_id = require_active_org_id(request)
    await _require_entitled(org_id)

    from provisa.api.app import state
    from provisa.core.org_settings import write_org_overrides

    body = await request.json()
    stored = await _stored()
    updated = dict(stored)
    for name in _EXPORT_FIELDS:
        if name in body:
            updated[name] = body[name]
    if "targets" in body:
        updated["targets"] = [
            {name: t[name] for name in _TARGET_FIELDS if name in t} for t in body["targets"]
        ]

    # Validate before persisting: an enabled export with no target, or a target naming no catalog
    # of the org, is refused here, naming the setting, rather than at the next publish.
    config = _export_config(updated)
    await _targets(config)
    identity = getattr(request.state, "identity", None)
    await write_org_overrides(
        _model_db(),
        {CONFIG_KEY: updated},
        updated_by=getattr(identity, "user_id", "anonymous"),
    )
    # REQ-1072: re-arm this org's sync jobs against what was just saved, so a changed schedule
    # or a newly-configured target takes effect now rather than at the next restart.
    scheduler = getattr(state, "_scheduler", None)
    if scheduler is not None:
        from provisa.api.metadata_export.publishing import register_org_jobs

        await register_org_jobs(scheduler, org_id)
    return {
        "success": True,
        "enabled": config.enabled,
        "targets": [t.catalog for t in config.targets],
    }


@router.post("/admin/metadata-export/health")
async def check_metadata_export(request: Request) -> dict:  # REQ-1074, REQ-2118
    """Ask each configured target's adapter whether it accepts it, and report what each said."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    await _require_entitled(org_id)
    targets = await _targets(_export_config(await _stored()))
    checks = []
    for target in targets:
        try:
            # Adapter construction is inside the try on purpose: an unknown provider raises
            # MetadataExportNotConfiguredError here, and the admin pressing "Test connection"
            # needs that text, not a bare 500.
            export = metadata_export(target)
            await export.health()
        except Exception as exc:  # noqa: BLE001 - allow-blind-except: the message IS the answer
            # The adapter raises whatever the transport raised; the admin needs that text to tell
            # a wrong URL from a rejected credential, so it is reported rather than classified.
            checks.append(
                {
                    "id": target.id,
                    "provider": target.provider,
                    "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        checks.append({"id": target.id, "provider": target.provider, "ok": True})
    return {"ok": all(c["ok"] for c in checks), "targets": checks}


@router.post("/admin/metadata-export/publish")
async def publish_metadata_export(request: Request) -> dict:  # REQ-1072, REQ-1074
    """Publish a full snapshot now — the on-demand form of the REQ-1072 reconcile."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    await _require_entitled(org_id)

    from provisa.api.metadata_export.publishing import ExportNotAllowed, publish_snapshot

    config = _export_config(await _stored())
    if not config.enabled:
        raise ApiError(
            400,
            "metadata_export.disabled",
            "metadata export is disabled for this org; enable it before publishing",
        )
    # The same publish the drain and the reconcile run (REQ-1072), so what an admin sees here is
    # what the scheduled paths send — not a second assembly of the same snapshot.
    try:
        results = await publish_snapshot(org_id)
    except ExportNotAllowed as exc:
        raise ApiError(403, "metadata_export.not_entitled", str(exc)) from exc
    payload = _publish_payload(results)
    _LAST_PUBLISH[org_id] = payload
    return payload
