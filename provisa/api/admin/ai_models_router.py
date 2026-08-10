# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8a2c4d-1b3e-4f6a-8c9d-2e5f7a0b1c3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Admin AI-models / vector-models / NL rate-limit REST endpoints.

Edits three config keys: ``ai_models`` (per-role model assignments,
:class:`AIModelsConfig`), ``vector_models`` (embedding-model registry, a list of
:class:`VectorModelConfig`), and ``nl.rate_limit`` (:class:`NlConfig`).

REQ-1349: these three are ORG-scoped. A write lands in the acting org's ``org_settings`` table,
layered over the deployment config, and takes effect on the next request — an org administrator
owns which LLM provider answers their org's NL queries. The surface is gated on the
``org_settings`` right, which org_admin holds in both tenancy modes; before REQ-1349 it had no
server-side gate at all and wrote the deployment config file directly.
"""

# Requirements: REQ-464, REQ-419, REQ-500, REQ-370, REQ-1349

from fastapi import APIRouter, Request

from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError

router = APIRouter()

_AI_MODEL_ROLES = (
    "table_description",
    "column_description",
    "relationship_inference",
    "sql_generation",
    "table_selection",
)

# REQ-1349: org-scoped writes are read back per request, so no restart is involved. The note is
# kept in the payload because the UI renders it; it now states the actual behavior.
_RESTART_NOTE = "AI model settings apply to this org and take effect on the next query."


def _model_default(key: str):
    """The single source of truth for a model-role default — the AIModelsConfig field."""
    from provisa.core.models import AIModelsConfig

    return AIModelsConfig.model_fields[key].default


async def _effective_config() -> dict:
    """Deployment config with the acting org's overrides layered on (REQ-1349)."""
    from provisa.api.app import state
    from provisa.core.org_settings import resolve_org_config

    assert state.tenant_db is not None
    return await resolve_org_config(state.tenant_db)


@router.get("/admin/ai-models")
async def get_ai_models(request: Request):  # REQ-464, REQ-419, REQ-500, REQ-370, REQ-1349
    """Return the acting org's AI-model assignments, vector-model registry, and NL rate limit."""
    require_org_settings(request)
    from provisa.api.app import state
    from provisa.core.org_secrets import LLM_VENDORS, read_org_api_keys

    assert state.tenant_db is not None
    cfg = await _effective_config()
    ai = cfg.get("ai_models", {}) or {}
    nl = cfg.get("nl", {}) or {}

    def _assignment(key: str):
        # str form returned as-is; dict (full {vendor, model, fallback}) form returned verbatim.
        val = ai.get(key, _model_default(key))
        return val if isinstance(val, str) else dict(val)

    # REQ-1395, REQ-1398: keys themselves are never echoed back — only whether each vendor has
    # one set.
    configured = await read_org_api_keys(state.tenant_db)
    api_keys_set = {vendor: vendor in configured for vendor in sorted(LLM_VENDORS)}

    return {
        "ai_models": {k: _assignment(k) for k in _AI_MODEL_ROLES},
        "vector_models": list(cfg.get("vector_models", []) or []),
        "nl": {"rate_limit": nl.get("rate_limit")},
        "api_keys_set": api_keys_set,
        "restart_required_note": _RESTART_NOTE,
    }


@router.get("/admin/ai-models/vendors/{vendor}/models")
async def get_vendor_models(request: Request, vendor: str):  # REQ-1395, REQ-1398, REQ-1409
    """The model names ``vendor`` currently serves, for the model picker.

    Read live from the vendor's own list-models API with the org's key, so a model released after
    this build shipped is selectable the day the vendor serves it.
    """
    require_org_settings(request)
    import os

    import httpx

    from provisa.api.app import state
    from provisa.core.org_secrets import read_org_api_keys
    from provisa.llm.vendor_models import (
        VENDOR_API_KEY_ENV,
        VENDOR_MODEL_APIS,
        fetch_vendor_models,
    )

    if vendor not in VENDOR_MODEL_APIS:
        raise ApiError(
            400,
            "ai_models.vendor_has_no_model_api",
            f"'{vendor}' publishes no list-models API; enter the model name directly",
        )

    assert state.tenant_db is not None
    # The org's key when it has set one; otherwise the deployment credential this vendor's calls
    # already run on — the same resolution order the LLM client uses, so the picker lists exactly
    # the models the org's queries would reach (REQ-1395, REQ-1398).
    api_key = (await read_org_api_keys(state.tenant_db)).get(vendor)
    if not api_key and vendor in VENDOR_API_KEY_ENV:
        api_key = os.environ.get(VENDOR_API_KEY_ENV[vendor])
    if not api_key:
        raise ApiError(
            400,
            "ai_models.vendor_key_required",
            f"set an API key for '{vendor}' to list its models",
        )

    try:
        models = await fetch_vendor_models(vendor, api_key)
    except httpx.HTTPStatusError as exc:
        raise ApiError(
            502,
            "ai_models.vendor_list_failed",
            f"{vendor} rejected the model listing: HTTP {exc.response.status_code}",
        ) from exc
    except httpx.HTTPError as exc:
        raise ApiError(
            502,
            "ai_models.vendor_unreachable",
            f"{vendor} model listing is unreachable: {exc}",
        ) from exc

    return {"vendor": vendor, "models": models}


@router.put("/admin/ai-models")
async def set_ai_models(request: Request):  # REQ-464, REQ-419, REQ-500, REQ-370, REQ-1349
    """Persist the acting org's AI-model, vector-model, and NL rate-limit overrides.

    Writes only the org's DELTA against the deployment config: a blank/empty model string removes
    the org's override for that role, so the org falls back to the deployment's choice rather than
    to a value baked in here.
    """
    require_org_settings(request)
    from provisa.api.app import state
    from provisa.core.org_settings import read_org_overrides, write_org_overrides

    assert state.tenant_db is not None
    body = await request.json()
    overrides = await read_org_overrides(state.tenant_db)
    updates: dict = {}
    updated: list[str] = []

    if "ai_models" in body:
        ai = dict(overrides.get("ai_models") or {})
        for k, v in (body["ai_models"] or {}).items():
            if k not in _AI_MODEL_ROLES:
                continue
            # Blank/empty → drop the org override for this role (deployment config governs again).
            if isinstance(v, str) and not v.strip():
                ai.pop(k, None)
            else:
                ai[k] = v
            updated.append(f"ai_models.{k}")
        # An empty delta means "override nothing" — store None so the row is deleted outright.
        updates["ai_models"] = ai or None

    if "vector_models" in body:
        # Full-list replace. Validate the required fields (REQ-500) before persisting.
        vms = body["vector_models"] or []
        for vm in vms:
            if not (vm.get("id") and vm.get("provider") and vm.get("dimensions")):
                raise ApiError(
                    400,
                    "ai_models.vector_model_fields_required",
                    "each vector_models entry requires id, provider, and dimensions",
                )
        updates["vector_models"] = vms or None
        updated.append("vector_models")

    if "nl" in body and "rate_limit" in body["nl"]:
        nl = dict(overrides.get("nl") or {})
        rl = body["nl"]["rate_limit"]
        if rl in (None, ""):
            nl.pop("rate_limit", None)
        else:
            nl["rate_limit"] = int(rl)
        updates["nl"] = nl or None
        updated.append("nl.rate_limit")

    identity = getattr(request.state, "identity", None)
    updated_by = getattr(identity, "user_id", "anonymous")

    await write_org_overrides(state.tenant_db, updates, updated_by=updated_by)

    # REQ-1395, REQ-1398: the org's own per-vendor API keys — secrets, not config overrides, so
    # they go through org_secrets (encrypted at rest) rather than write_org_overrides. A
    # blank/empty string clears a vendor's key, reverting that vendor's LLM calls to the
    # deployment's env-var credential (where one exists).
    if "api_keys" in body:
        from provisa.core.org_secrets import LLM_VENDORS, write_org_secret

        for vendor, raw_key in (body["api_keys"] or {}).items():
            if vendor not in LLM_VENDORS:
                continue
            await write_org_secret(
                state.tenant_db,
                f"{vendor}_api_key",
                raw_key.strip() if isinstance(raw_key, str) and raw_key.strip() else None,
                updated_by=updated_by,
            )
            updated.append(f"api_keys.{vendor}")

    return {"success": True, "updated": updated, "restart_required": False}
