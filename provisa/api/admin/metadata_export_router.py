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

Four operations back the admin tab: read the org's export settings, write them, check the
target is reachable, and publish a full snapshot on demand. Every one of them is gated twice —
on the ``org_settings`` right, and on the org's REQ-1073 entitlement — because a premium
feature reachable through an un-entitled org's admin API is not gated at all.

Secrets are write-only through this surface. The GET reports whether each credential is set,
never its value: an admin who can read the tab must not be able to read back a token the org
stored, and a redacted-looking string in the field is worse than an explicit "set" flag
because it gets saved back as the literal redaction.
"""

# Requirements: REQ-1068, REQ-1072, REQ-1073, REQ-1074

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import ValidationError

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError
from provisa.api.metadata_export import metadata_export
from provisa.api.metadata_export.provider import PublishResult
from provisa.api.metadata_export.registry import registered_providers
from provisa.control_plane.entitlements import (
    EntitlementError,
    Feature,
    UnknownTierError,
    min_tier,
)
from provisa.control_plane.store import control_plane_store
from provisa.core.models import MetadataExportConfig

router = APIRouter()

CONFIG_KEY = "metadata_export"

# Credential fields, which the read side reports as set/not-set and never returns.
_SECRET_FIELDS = ("api_key", "token", "entra_client_secret")

# Everything else the admin tab edits, returned verbatim.
_PLAIN_FIELDS = (
    "enabled",
    "provider",
    "endpoint",
    "auth_mode",
    "username",
    "entra_tenant_id",
    "entra_client_id",
    "reconcile_cron",
    "timeout_seconds",
)

# The last publish this process performed, per org. In-process only: it records what the admin
# just triggered so the tab can show the per-asset errors a partial publish returned, and an
# empty entry after a restart means "this process has not published", which is what it says.
_LAST_PUBLISH: dict[str, dict[str, Any]] = {}


def _require_entitled(org_id: str) -> None:
    """REQ-1073: refuse the whole surface below the premium tier."""
    from provisa.control_plane.entitlements import require_feature

    try:
        require_feature(control_plane_store(), org_id, Feature.METADATA_EXPORT)
    except EntitlementError as exc:
        raise ApiError(403, "metadata_export.not_entitled", str(exc)) from exc
    except (KeyError, UnknownTierError) as exc:
        raise ApiError(
            403,
            "metadata_export.tier_unknown",
            f"org {org_id!r} has no resolvable tier, so the feature cannot be entitled",
        ) from exc


def _is_entitled(org_id: str) -> bool:
    """Whether the org may use the feature — the flag the tab renders its gate from."""
    from provisa.control_plane.entitlements import require_feature

    try:
        require_feature(control_plane_store(), org_id, Feature.METADATA_EXPORT)
    except (EntitlementError, KeyError, UnknownTierError):
        return False
    return True


async def _stored() -> dict[str, Any]:
    """The org's export settings as stored, with the deployment's beneath them."""
    from provisa.api.app import state
    from provisa.core.org_settings import resolve_org_config

    cfg = await resolve_org_config(state.tenant_db)
    return dict(cfg.get(CONFIG_KEY) or {})


def _export_config(stored: dict[str, Any]) -> MetadataExportConfig:
    try:
        return MetadataExportConfig(**stored)
    except ValidationError as exc:
        raise ApiError(400, "metadata_export.invalid_config", str(exc)) from exc


def _publish_payload(result: PublishResult) -> dict[str, Any]:
    return {
        "provider": result.provider_name,
        "ok": result.ok,
        "published": dict(result.published),
        "total_published": result.total_published(),
        "errors": [{"asset": e.asset.fqn(), "message": e.message} for e in result.errors],
    }


@router.get("/admin/metadata-export")
async def get_metadata_export(request: Request) -> dict:  # REQ-1074
    """The org's export settings, with credentials reported as set/not-set."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    stored = await _stored()
    config = _export_config(stored)
    return {
        "entitled": _is_entitled(org_id),
        "required_tier": min_tier(Feature.METADATA_EXPORT).value,
        "providers": sorted(registered_providers()),
        "config": {
            **{name: getattr(config, name) for name in _PLAIN_FIELDS},
            **{
                f"{name}_set": bool(getattr(config, name).get_secret_value())
                for name in _SECRET_FIELDS
            },
        },
        "last_publish": _LAST_PUBLISH.get(org_id),
    }


@router.put("/admin/metadata-export")
async def set_metadata_export(request: Request) -> dict:  # REQ-1074
    """Persist the org's export settings.

    A credential absent from the body keeps the stored one; a credential sent empty CLEARS it.
    Both are explicit acts — the tab cannot send back a value it was never given, so "absent"
    has to mean "unchanged" or an admin editing the endpoint would wipe the token.
    """
    require_org_settings(request)
    org_id = require_active_org_id(request)
    _require_entitled(org_id)

    from provisa.api.app import state
    from provisa.core.org_settings import write_org_overrides

    body = await request.json()
    stored = await _stored()
    updated = dict(stored)
    for name in _PLAIN_FIELDS:
        if name in body:
            updated[name] = body[name]
    for name in _SECRET_FIELDS:
        if name in body:
            updated[name] = body[name]

    # Validate before persisting: an enabled export with no provider or endpoint is refused
    # here, naming the setting, rather than at the next publish, naming a connection.
    config = _export_config(updated)
    identity = getattr(request.state, "identity", None)
    await write_org_overrides(
        state.tenant_db,
        {CONFIG_KEY: updated},
        updated_by=getattr(identity, "user_id", "anonymous"),
    )
    # REQ-1072: re-arm this org's sync jobs against what was just saved, so a changed schedule
    # or a newly-configured target takes effect now rather than at the next restart.
    scheduler = getattr(state, "_scheduler", None)
    if scheduler is not None:
        from provisa.api.metadata_export.publishing import register_org_jobs

        await register_org_jobs(scheduler, org_id)
    return {"success": True, "provider": config.provider, "enabled": config.enabled}


@router.post("/admin/metadata-export/health")
async def check_metadata_export(request: Request) -> dict:  # REQ-1074
    """Ask the configured adapter whether the target accepts it, and report what it said."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    _require_entitled(org_id)
    config = _export_config(await _stored())
    try:
        # Adapter construction is inside the try on purpose: a not-yet-saved or disabled
        # config raises MetadataExportNotConfiguredError here, and the admin pressing
        # "Test connection" needs that text ("save/enable first"), not a bare 500.
        export = metadata_export(config)
        await export.health()
    except Exception as exc:  # noqa: BLE001 - allow-blind-except: the message IS the answer
        # The adapter raises whatever the transport raised; the admin needs that text to tell a
        # wrong URL from a rejected credential, so it is reported rather than classified.
        return {"ok": False, "provider": config.provider, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": True, "provider": config.provider}


@router.post("/admin/metadata-export/publish")
async def publish_metadata_export(request: Request) -> dict:  # REQ-1072, REQ-1074
    """Publish a full snapshot now — the on-demand form of the REQ-1072 reconcile."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    _require_entitled(org_id)

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
        result = await publish_snapshot(org_id)
    except ExportNotAllowed as exc:
        raise ApiError(403, "metadata_export.not_entitled", str(exc)) from exc
    payload = _publish_payload(result)
    _LAST_PUBLISH[org_id] = payload
    return payload
