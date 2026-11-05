# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2e8a17-9d3b-4f60-b7a4-1e6d0c9f2b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Platform routes require the deployment check, not platform_settings alone (REQ-1327).

A single-tenant deployment grants platform_settings to org_admin, so a route checking that right
alone showed the org admin a platform page it could use. REQ-1327 (amended 2026-11-05): a platform
route requires the deployment check (platform_settings AND cross_org). The mail transport, the
maintenance banner, the desktop shutdown switch, opening or stopping an org-wide debug-trace window
and the sandbox-membership fix are platform routes. (The debug-trace listing was one; it now shows
each caller its own reach, test_debug_trace_router.py.)

Each route is called with the real gate: the single-tenant org admin is refused by the deployment
check's code before anything is read or written; the platform admin passes it, which the first
step after the gate reports by raising ``_Passed``.
"""

# Requirements: REQ-1327, REQ-1913

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from provisa.api.admin import debug_trace_router as dr
from provisa.api.admin import environments_router as er
from provisa.api.admin import lifecycle_router as lr
from provisa.api.admin import mail_router as mailr
from provisa.api.admin import maintenance_router as mr
from provisa.api.errors import ApiError
from provisa.core import trace_scope as ts

_PLATFORM_ADMIN = {"platform_settings", "cross_org"}
_SINGLE_TENANT_ORG_ADMIN = {"platform_settings", "org_settings", "user_management"}


class _Passed(Exception):
    pass


def _raise_passed(*_args, **_kwargs):
    raise _Passed


async def _json_passed():
    raise _Passed


@pytest.fixture
def after_the_gate(monkeypatch):
    """Every route's first step past its gate raises _Passed, so nothing real is reached."""
    monkeypatch.setattr(mailr, "read_config", _raise_passed)
    monkeypatch.setattr(mailr, "_admin_db", _raise_passed)
    monkeypatch.setattr(mr, "_admin_pool", _raise_passed)
    monkeypatch.setattr(lr, "_is_desktop_runtime", _raise_passed)
    monkeypatch.setattr(dr, "_admin_pool", _raise_passed)
    monkeypatch.setattr(dr, "_open_window", _org_wide_window_in_root)


async def _org_wide_window_in_root(window_id):
    """The window a stop finds: org-wide, in root, where the single-tenant org admin acts and so
    sees it listed — what refuses its stop is the deployment check, not its reach."""
    now = datetime.now(timezone.utc)
    return ts.DebugWindow(
        id=window_id,
        scope="org",
        org_id="root",
        target=None,
        started_at=now,
        expires_at=now + timedelta(minutes=5),
        created_by="ops",
    )


def _request(monkeypatch, caps: set[str]):
    monkeypatch.setattr(
        "provisa.api.admin.capabilities._resolved_capabilities", lambda _identity, _state: caps
    )
    return SimpleNamespace(
        state=SimpleNamespace(
            identity=SimpleNamespace(user_id="caller@example.com"), active_org_id="root"
        ),
        json=_json_passed,
    )


_ROUTES = {
    "GET /admin/mail": mailr.get_mail,
    "PUT /admin/mail": mailr.set_mail,
    "GET /admin/mail/stats": mailr.get_mail_stats,
    "POST /admin/mail/test": mailr.send_test_mail,
    "PUT /admin/platform/maintenance": lambda r: mr.set_maintenance(r, mr.NoticeBody(active=True)),
    "POST /admin/lifecycle/shutdown": lr.shutdown,
    "POST /admin/platform/debug-trace/windows (org-wide)": lambda r: dr.start_debug_window(
        r, dr.WindowBody(scope="org", org_id="root", minutes=5)
    ),
    "DELETE /admin/platform/debug-trace/windows/{id} (org-wide)": lambda r: dr.stop_debug_window(
        r, "w-org"
    ),
    "POST /admin/orgs/{id}/environments/-/fix-sandbox-membership": er.fix_sandbox_membership,
}


@pytest.mark.asyncio
@pytest.mark.parametrize("route", sorted(_ROUTES))
async def test_a_single_tenant_org_admin_is_refused(after_the_gate, monkeypatch, route):
    with pytest.raises(ApiError) as exc:
        await _ROUTES[route](_request(monkeypatch, _SINGLE_TENANT_ORG_ADMIN))
    assert exc.value.status_code == 403
    assert exc.value.code == "platform.control_plane_role_required"


@pytest.mark.asyncio
@pytest.mark.parametrize("route", sorted(_ROUTES))
async def test_the_platform_admin_passes(after_the_gate, monkeypatch, route):
    with pytest.raises(_Passed):
        await _ROUTES[route](_request(monkeypatch, _PLATFORM_ADMIN))
