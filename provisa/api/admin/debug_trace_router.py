# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2a9f81-4c7e-4b15-8e30-a9b1c5d7f264
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The operator's control over debug tracing (REQ-1910).

Start a debug-trace window for an org, a role or a source for a stated number of minutes, see the
windows that are open and how long each has left, stop one early, and permit the per-request
debug-trace hint per role. A debug trace adds load to the deployment, so the listing and a window
over an org as a whole are the platform's: the deployment check (REQ-1327). A window naming a role
or source in an org is that org's administrator's: ``org_settings`` in that org (REQ-1327). Stopping
a window and the hint permission are gated on ``platform_settings``.
"""

# Requirements: REQ-1910, REQ-1327

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin._platform_guard import (
    is_anonymous,
    require_deployment_settings,
    require_platform_settings,
)
from provisa.api.errors import ApiError
from provisa.core import trace_scope
from provisa.security.rights import Capability

router = APIRouter()


def _admin_pool():
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


class WindowBody(BaseModel):
    scope: str  # org | role | source
    org_id: str
    # The role id or source id the window covers; omitted for an org-wide window.
    target: str | None = None
    minutes: int


class HintRoleBody(BaseModel):
    org_id: str
    role_id: str
    permitted: bool


def _serialize_window(window: trace_scope.DebugWindow, now: datetime) -> dict:
    return {
        "id": window.id,
        "scope": window.scope,
        "org_id": window.org_id,
        "target": window.target,
        "started_at": window.started_at.isoformat(),
        "expires_at": window.expires_at.isoformat(),
        # Computed here so the page shows time remaining by the server's clock, not the browser's.
        "remaining_seconds": max(0, int((window.expires_at - now).total_seconds())),
        "created_by": window.created_by,
    }


async def _read(db) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "windows": [_serialize_window(w, now) for w in await trace_scope.list_windows(db, now)],
        "hint_roles": [
            {"org_id": org_id, "role_id": role_id}
            for org_id, role_id in await trace_scope.list_hint_roles(db)
        ],
        "max_minutes": trace_scope.MAX_WINDOW_MINUTES,
        "hint_setting": trace_scope.HINT_SETTING,
    }


def _require_org_settings_in(request: Request, org_id: str, scope: str) -> None:  # REQ-1327
    """Raise 403 unless the caller holds ``org_settings`` in ``org_id`` — acting in that org. A
    window naming a role or source is the org admin's, so the platform's cross_org reaches no org
    here, and org_settings held in the org one is acting in reaches no other."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    if is_anonymous(request):
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(request.state.identity, state)
    active_org = getattr(request.state, "active_org_id", None)
    if Capability.ORG_SETTINGS.value in caps and active_org == org_id:
        return
    raise ApiError(
        403,
        "debug_trace.org_settings_in_org_required",
        f"a debug-trace window naming a {scope} is opened by that org's administrator: "
        f"org_settings in {org_id} required",
        org_id=org_id,
        scope=scope,
    )


@router.get("/admin/platform/debug-trace")
async def read_debug_trace(request: Request) -> dict:  # REQ-1910
    """The open debug-trace windows and the roles permitted the per-request hint."""
    require_deployment_settings(request)  # REQ-1327
    return await _read(_admin_pool())


@router.post("/admin/platform/debug-trace/windows")
async def start_debug_window(request: Request, body: WindowBody) -> dict:  # REQ-1910
    """Turn debug tracing on for an org, a role or a source for ``minutes``; it ends on its own.

    REQ-1327: the platform opens a window over an org as a whole, never one naming a role or source
    in it; that is the org admin's."""
    if body.scope == "org":
        require_deployment_settings(request)
    else:
        _require_org_settings_in(request, body.org_id, body.scope)
    identity = getattr(request.state, "identity", None)
    db = _admin_pool()
    try:
        await trace_scope.start_window(
            db,
            scope=body.scope,
            org_id=body.org_id,
            target=body.target.strip() if body.target and body.target.strip() else None,
            minutes=body.minutes,
            created_by=getattr(identity, "user_id", None),
        )
    except ValueError as exc:
        raise ApiError(400, "debug_trace.invalid_window", str(exc)) from exc
    return await _read(db)


@router.delete("/admin/platform/debug-trace/windows/{window_id}")
async def stop_debug_window(request: Request, window_id: str) -> dict:  # REQ-1910
    """End a debug-trace window before its time."""
    require_platform_settings(request)  # REQ-1337
    db = _admin_pool()
    if not await trace_scope.stop_window(db, window_id):
        raise ApiError(
            404,
            "debug_trace.window_not_found",
            f"No debug-trace window {window_id!r}",
            window_id=window_id,
        )
    return await _read(db)


@router.put("/admin/platform/debug-trace/hint-roles")
async def set_hint_role(request: Request, body: HintRoleBody) -> dict:  # REQ-1910
    """Permit, or stop permitting, the per-request debug-trace hint for a role."""
    require_platform_settings(request)  # REQ-1337
    identity = getattr(request.state, "identity", None)
    db = _admin_pool()
    role_id = body.role_id.strip()
    if not role_id:
        raise ApiError(400, "debug_trace.role_required", "role_id is required")
    await trace_scope.set_hint_permission(
        db, body.org_id, role_id, body.permitted, updated_by=getattr(identity, "user_id", None)
    )
    return await _read(db)
