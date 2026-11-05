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
debug-trace hint per role. One scope rule covers opening, stopping and listing (REQ-1327): a window
over an org as a whole is load on the deployment, so it is the platform's (the deployment check); a
window naming a role or source in an org, and the per-role hint, are that org's administrator's
(``org_settings`` in that org). The listing shows each caller the windows of its own reach: the
platform sees the org-wide windows, an org admin sees its own org's windows and hint roles.
"""

# Requirements: REQ-1910, REQ-1327

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin._platform_guard import (
    has_deployment_settings,
    is_anonymous,
    require_deployment_settings,
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


# Whether a window (or, as scope "role", a hint role) of this scope in this org is in the caller's
# reach: (scope, org_id) -> bool.
_Reach = Callable[[str, str], bool]


async def _read(db, reach: _Reach) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "windows": [
            _serialize_window(w, now)
            for w in await trace_scope.list_windows(db, now)
            if reach(w.scope, w.org_id)
        ],
        "hint_roles": [
            {"org_id": org_id, "role_id": role_id}
            for org_id, role_id in await trace_scope.list_hint_roles(db)
            if reach("role", org_id)
        ],
        "max_minutes": trace_scope.MAX_WINDOW_MINUTES,
        "hint_setting": trace_scope.HINT_SETTING,
    }


def _org_settings_org(request: Request) -> str | None:  # REQ-1327
    """The org the caller administers here: the org it is acting in, when it holds ``org_settings``
    — org_settings held in the org one is acting in reaches no other."""
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state

    caps = _resolved_capabilities(request.state.identity, state)
    if Capability.ORG_SETTINGS.value in caps:
        return getattr(request.state, "active_org_id", None)
    return None


def _require_org_settings_in(request: Request, org_id: str, subject: str) -> None:  # REQ-1327
    """Raise 403 unless the caller holds ``org_settings`` in ``org_id`` — acting in that org. A
    window naming a role or source, and the per-role hint, are the org admin's, so the platform's
    cross_org reaches no org here."""
    if is_anonymous(request):
        return  # dev mode — no auth configured
    if _org_settings_org(request) == org_id:
        return
    raise ApiError(
        403,
        "debug_trace.org_settings_in_org_required",
        f"{subject} is that org's administrator's: org_settings in {org_id} required",
        org_id=org_id,
        subject=subject,
    )


def _reach(request: Request) -> _Reach:  # REQ-1327
    """What the caller's listing shows, by the rule that decides who opens a window: the platform
    (the deployment check) sees the org-wide windows of every org; an org admin (``org_settings``
    in the org it acts in) sees that org's windows and hint roles. 403 for a caller with neither."""
    if is_anonymous(request):
        return lambda _scope, _org_id: True  # dev mode — no auth configured
    platform = has_deployment_settings(request)
    own_org = _org_settings_org(request)
    if not platform and own_org is None:
        raise ApiError(
            403,
            "debug_trace.no_reach",
            "debug-trace windows are listed to the platform administrator (org-wide windows) "
            "and to an org's administrator (its own org's)",
        )
    return lambda scope, org_id: (platform and scope == "org") or org_id == own_org


def _window_not_found(window_id: str) -> ApiError:
    return ApiError(
        404,
        "debug_trace.window_not_found",
        f"No debug-trace window {window_id!r}",
        window_id=window_id,
    )


async def _open_window(window_id: str) -> trace_scope.DebugWindow | None:
    """The open window with this id, or None."""
    for window in await trace_scope.list_windows(_admin_pool()):
        if window.id == window_id:
            return window
    return None


@router.get("/admin/platform/debug-trace")
async def read_debug_trace(request: Request) -> dict:  # REQ-1910
    """The open debug-trace windows and the roles permitted the per-request hint, as far as the
    caller's reach goes (REQ-1327)."""
    reach = _reach(request)
    return await _read(_admin_pool(), reach)


@router.post("/admin/platform/debug-trace/windows")
async def start_debug_window(request: Request, body: WindowBody) -> dict:  # REQ-1910
    """Turn debug tracing on for an org, a role or a source for ``minutes``; it ends on its own.

    REQ-1327: the platform opens a window over an org as a whole, never one naming a role or source
    in it; that is the org admin's."""
    if body.scope == "org":
        require_deployment_settings(request)
    else:
        _require_org_settings_in(
            request, body.org_id, f"a debug-trace window naming a {body.scope}"
        )
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
    return await _read(db, _reach(request))


@router.delete("/admin/platform/debug-trace/windows/{window_id}")
async def stop_debug_window(request: Request, window_id: str) -> dict:  # REQ-1910
    """End a debug-trace window before its time.

    REQ-1327: stopped by whoever may open it — an org-wide window by the platform, a window naming
    a role or source by that org's administrator. A window outside the caller's reach is reported
    as not found, as the listing does not show it."""
    reach = _reach(request)
    window = await _open_window(window_id)
    if window is None or not reach(window.scope, window.org_id):
        raise _window_not_found(window_id)
    if window.scope == "org":
        require_deployment_settings(request)
    else:
        _require_org_settings_in(
            request, window.org_id, f"a debug-trace window naming a {window.scope}"
        )
    db = _admin_pool()
    if not await trace_scope.stop_window(db, window_id):
        raise _window_not_found(window_id)
    return await _read(db, reach)


@router.put("/admin/platform/debug-trace/hint-roles")
async def set_hint_role(request: Request, body: HintRoleBody) -> dict:  # REQ-1910
    """Permit, or stop permitting, the per-request debug-trace hint for a role.

    REQ-1327: a hint names a role in an org, so it is that org's administrator's."""
    _require_org_settings_in(request, body.org_id, "the per-request debug-trace hint for a role")
    identity = getattr(request.state, "identity", None)
    db = _admin_pool()
    role_id = body.role_id.strip()
    if not role_id:
        raise ApiError(400, "debug_trace.role_required", "role_id is required")
    await trace_scope.set_hint_permission(
        db, body.org_id, role_id, body.permitted, updated_by=getattr(identity, "user_id", None)
    )
    return await _read(db, _reach(request))
