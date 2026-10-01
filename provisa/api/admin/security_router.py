# Copyright (c) 2026 Kenneth Stott
# Canary: 0cfd48db-9a06-4435-8b2b-fbb9b4d6b131
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Admin security-posture REST endpoints (security.mode)."""

# Requirements: REQ-693

from fastapi import APIRouter, Request

from provisa.api.admin._platform_guard import is_anonymous, require_deployment_settings
from provisa.api.errors import ApiError
from provisa.core import settings_registry

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
async def get_security(request: Request):  # REQ-693, REQ-1913
    """Security posture (security.mode) for the admin UI. A deployment setting: the platform
    administrator's, in every deployment."""
    require_deployment_settings(request)
    return {
        "mode": settings_registry.resolve("security.mode").value,
        "modes": _SECURITY_MODES,
        "restart_required_note": "The security posture binds at startup — changes take effect after a service restart.",
    }


@router.put("/admin/security")
async def set_security(request: Request):  # REQ-693, REQ-1913
    """Store the security posture (security.mode). Applied on service restart.

    REQ-1913: the mode is an operator setting stored in the control plane (it used to be written
    to this node's config file), and this endpoint carried no authorization check at all. It is a
    guarded setting — high mode refuses every client that cannot decrypt results itself — so it is
    the platform administrator's, and not the anonymous caller's on a deployment with no auth
    provider.
    """
    from provisa.api.app import state

    require_deployment_settings(request)
    if is_anonymous(request):
        raise ApiError(
            403,
            "settings.auth_required",
            "the security mode can only be changed by an authenticated platform administrator",
            field="security.mode",
            reason="auth_required",
        )
    body = await request.json()
    mode = body.get("mode")
    if mode not in {m["key"] for m in _SECURITY_MODES}:
        raise ApiError(
            400, "security.unknown_mode", f"unknown security mode {mode!r}", mode=str(mode)
        )
    assert state.admin_db is not None, "the security mode needs the platform control plane"
    settings_registry.store(
        state.admin_db, {"security.mode": mode}, updated_by=request.state.identity.user_id
    )
    return {"success": True, "restart_required": True}
