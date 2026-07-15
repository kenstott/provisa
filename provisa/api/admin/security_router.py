# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8a2c4d-1b3e-4f6a-8c9d-2e5f7a0b1c3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Admin security-posture REST endpoints (security.mode)."""

# Requirements: REQ-693

from fastapi import APIRouter, HTTPException, Request

from provisa.api.admin._config_io import config_path, read_config, write_config

router = APIRouter()

_SECURITY_MODES = [
    {
        "key": "standard",
        "label": "Standard",
        "description": "Default posture. Data APIs (REST/GraphQL/pgwire) reachable subject to auth + governance.",
    },
    {
        "key": "high",
        "label": "High (zero-trust)",
        "description": "pgwire server disabled; REST & GraphQL data endpoints return 403; only clients that decrypt locally (kms_key_arn configured) may reach data.",
    },
]


@router.get("/admin/security")
async def get_security():  # REQ-693
    """Security posture (security.mode) for the admin UI."""
    cfg = read_config()
    sec = cfg.get("security", {}) or {}
    mode = sec.get("mode", "standard")
    return {
        "mode": mode,
        "modes": _SECURITY_MODES,
        "restart_required_note": "The security posture binds at startup — changes take effect after a service restart.",
    }


@router.put("/admin/security")
async def set_security(request: Request):  # REQ-693
    """Persist the security posture (security.mode). Applied on service restart."""
    body = await request.json()
    mode = body.get("mode")
    if mode not in {m["key"] for m in _SECURITY_MODES}:
        raise HTTPException(status_code=400, detail=f"unknown security mode {mode!r}")
    path = config_path()
    cfg = read_config()
    sec = dict(cfg.get("security", {}) or {})
    sec["mode"] = mode
    cfg["security"] = sec
    write_config(path, cfg)
    return {"success": True, "restart_required": True}
