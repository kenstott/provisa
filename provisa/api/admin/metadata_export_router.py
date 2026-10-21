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

Secrets are write-only through this surface. The GET reports whether each credential is set,
never its value: an admin who can read the tab must not be able to read back a token the org
stored, and a redacted-looking string in the field is worse than an explicit "set" flag
because it gets saved back as the literal redaction.
"""

# Requirements: REQ-1068, REQ-1072, REQ-1073, REQ-1074, REQ-2118

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
from provisa.core.models import MetadataExportConfig

router = APIRouter()

CONFIG_KEY = "metadata_export"

# A target's credential fields, which the read side reports as set/not-set and never returns.
_SECRET_FIELDS = (
    "api_key",
    "token",
    "entra_client_secret",
    "google_service_account_json",
    "aws_access_key_id",
    "aws_secret_access_key",
)

# Everything else of a target the admin tab edits, returned verbatim.
_PLAIN_FIELDS = (
    "id",
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
    "datazone_project_id",
    "timeout_seconds",
)

# The settings of the export as a whole.
_EXPORT_FIELDS = ("enabled", "reconcile_cron")

# The last publish this process performed, per org. In-process only: it records what the admin
# just triggered so the tab can show the per-asset errors a partial publish returned, and an
# empty entry after a restart means "this process has not published", which is what it says.
_LAST_PUBLISH: dict[str, dict[str, Any]] = {}


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


def _target_view(target: Any) -> dict[str, Any]:
    """A target as the tab reads it: its settings, and whether each credential is set."""
    return {
        **{name: getattr(target, name) for name in _PLAIN_FIELDS},
        **{
            f"{name}_set": bool(getattr(target, name).get_secret_value()) for name in _SECRET_FIELDS
        },
    }


def _merged_targets(stored: list[dict[str, Any]], sent: list[dict[str, Any]]) -> list[dict]:
    """The targets as sent, each credential absent from the body kept from the stored target of
    the same id. A target left out of the body is removed."""
    by_id = {t.get("id"): t for t in stored}
    merged = []
    for target in sent:
        prior = by_id.get(target.get("id"), {})
        merged.append(
            {
                **{name: target[name] for name in _PLAIN_FIELDS if name in target},
                **{
                    name: target[name] if name in target else prior.get(name, "")
                    for name in _SECRET_FIELDS
                },
            }
        )
    return merged


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
        "config": {
            **{name: getattr(config, name) for name in _EXPORT_FIELDS},
            "targets": [_target_view(t) for t in config.targets],
        },
        "last_publish": _LAST_PUBLISH.get(org_id),
    }


@router.put("/admin/metadata-export")
async def set_metadata_export(request: Request) -> dict:  # REQ-1074
    """Persist the org's export settings: the export's own and its list of targets.

    A target's credential absent from the body keeps the one stored for the target of that id; a
    credential sent empty CLEARS it. Both are explicit acts — the tab cannot send back a value it
    was never given, so "absent" has to mean "unchanged" or an admin editing an endpoint would
    wipe the token. A target the body leaves out of its list is removed.
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
        updated["targets"] = _merged_targets(list(stored.get("targets") or []), body["targets"])

    # Validate before persisting: an enabled export with no provider or endpoint is refused
    # here, naming the setting, rather than at the next publish, naming a connection.
    config = _export_config(updated)
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
        "targets": [t.id for t in config.targets],
    }


@router.post("/admin/metadata-export/health")
async def check_metadata_export(request: Request) -> dict:  # REQ-1074, REQ-2118
    """Ask each configured target's adapter whether it accepts it, and report what each said."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    await _require_entitled(org_id)
    config = _export_config(await _stored())
    checks = []
    for target in config.targets:
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
