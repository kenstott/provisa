# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8a2c4d-1b3e-4f6a-8c9d-2e5f7a0b1c3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Admin AI-models / vector-models / NL rate-limit REST endpoints.

Edits three platform-config keys: ``ai_models`` (per-role model assignments,
:class:`AIModelsConfig`), ``vector_models`` (embedding-model registry, a list of
:class:`VectorModelConfig`), and ``nl.rate_limit`` (:class:`NlConfig`). All three
bind at startup, so changes take effect only after a service restart.
"""

# Requirements: REQ-464, REQ-419, REQ-500, REQ-370

from fastapi import APIRouter, HTTPException, Request

from provisa.api.admin._config_io import config_path, read_config, write_config

router = APIRouter()

_AI_MODEL_ROLES = (
    "table_description",
    "column_description",
    "relationship_inference",
    "sql_generation",
    "table_selection",
)

_RESTART_NOTE = "AI model settings take effect after a service restart."


def _model_default(key: str):
    """The single source of truth for a model-role default — the AIModelsConfig field."""
    from provisa.core.models import AIModelsConfig

    return AIModelsConfig.model_fields[key].default


@router.get("/admin/ai-models")
async def get_ai_models():  # REQ-464, REQ-419, REQ-500, REQ-370
    """Return the current AI-model assignments, vector-model registry, and NL rate limit."""
    cfg = read_config()
    ai = cfg.get("ai_models", {}) or {}
    nl = cfg.get("nl", {}) or {}

    def _assignment(key: str):
        # str form returned as-is; dict (full {vendor, model, fallback}) form returned verbatim.
        val = ai.get(key, _model_default(key))
        return val if isinstance(val, str) else dict(val)

    return {
        "ai_models": {k: _assignment(k) for k in _AI_MODEL_ROLES},
        "vector_models": list(cfg.get("vector_models", []) or []),
        "nl": {"rate_limit": nl.get("rate_limit")},
        "restart_required_note": _RESTART_NOTE,
    }


@router.put("/admin/ai-models")
async def set_ai_models(request: Request):  # REQ-464, REQ-419, REQ-500, REQ-370
    """Persist AI-model assignments, vector-model registry, and NL rate limit. Applied on restart."""
    body = await request.json()
    path = config_path()
    cfg = read_config()
    updated: list[str] = []

    if "ai_models" in body:
        ai = dict(cfg.get("ai_models", {}) or {})
        for k, v in (body["ai_models"] or {}).items():
            if k not in _AI_MODEL_ROLES:
                continue
            # Blank/empty → reset to the AIModelsConfig field default (pop the key).
            if isinstance(v, str) and not v.strip():
                ai.pop(k, None)
            else:
                ai[k] = v
            updated.append(f"ai_models.{k}")
        cfg["ai_models"] = ai

    if "vector_models" in body:
        # Full-list replace. Validate the required fields (REQ-500) before persisting.
        vms = body["vector_models"] or []
        for vm in vms:
            if not (vm.get("id") and vm.get("provider") and vm.get("dimensions")):
                raise HTTPException(
                    status_code=400,
                    detail="each vector_models entry requires id, provider, and dimensions",
                )
        cfg["vector_models"] = vms
        updated.append("vector_models")

    if "nl" in body and "rate_limit" in body["nl"]:
        nl = dict(cfg.get("nl", {}) or {})
        rl = body["nl"]["rate_limit"]
        nl["rate_limit"] = int(rl) if rl not in (None, "") else None
        cfg["nl"] = nl
        updated.append("nl.rate_limit")

    write_config(path, cfg)
    return {"success": True, "updated": updated, "restart_required": True}
