# Copyright (c) 2026 Kenneth Stott
# Canary: a8a0b18d-2f89-4784-bd6f-f2d29cabd489
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1327: a route whose path names an organization acts in that organization only, and reads
the caller's rights in it.

Every route of the organization routers whose path carries ``{org_id}`` is enumerated here, so a
route added without its guard fails. Each is one of three kinds:

* an organization's own act: guarded by ``_require_org_admin`` (or ``_require_org_admin_rights``),
  which refuses a request bound to another organization than the path's before anything is read
  or built;
* a platform act (``/admin/platform/...``, an org's lifecycle): guarded by the deployment check,
  which needs ``cross_org`` -- a right of the deployment, not of any one organization;
* a person's own standing: the org's creator polling or retrying its provisioning, a member
  leaving. No right is read in the organization.
"""

# Requirements: REQ-1327

from __future__ import annotations

import inspect
import typing
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.routing import APIRoute
from pydantic import BaseModel

from provisa.api.admin import org_storage_router, orgs_router, regions_router, settings_router
from provisa.api.errors import ApiError

ORG_A = "org_a"  # the organization the request is bound to, where the caller is org_admin
ORG_B = "org_b"  # the organization the path names, where the caller is a member too

ROUTERS = (orgs_router, regions_router, settings_router, org_storage_router)

#: Platform acts: the deployment check (cross_org), not a right held in an organization.
PLATFORM = {
    ("PUT", "/admin/orgs/{org_id}"),
    ("PUT", "/admin/orgs/{org_id}/tier"),
    ("GET", "/admin/platform/orgs/{org_id}/regions"),
    ("PUT", "/admin/platform/orgs/{org_id}/regions"),
    ("GET", "/admin/platform/orgs/{org_id}/storage"),
    ("PUT", "/admin/platform/orgs/{org_id}/storage"),
}

#: A person's own standing: the creator of an org (or the platform) for its provisioning, and the
#: member leaving. Neither reads a right in the organization.
OWN_STANDING = {
    ("GET", "/admin/orgs/{org_id}/status"),
    ("POST", "/admin/orgs/{org_id}/retry"),
    ("POST", "/admin/orgs/{org_id}/leave"),
}


def _org_path_routes() -> list[tuple[str, str, Any]]:
    found = []
    for module in ROUTERS:
        for route in module.router.routes:
            assert isinstance(route, APIRoute)
            if "{org_id}" not in route.path:
                continue
            for method in sorted(route.methods - {"HEAD", "OPTIONS"}):
                found.append((method, route.path, route.endpoint))
    return found


ROUTES = _org_path_routes()
ORG_ACTS = [r for r in ROUTES if (r[0], r[1]) not in PLATFORM | OWN_STANDING]
PLATFORM_ACTS = [r for r in ROUTES if (r[0], r[1]) in PLATFORM]


def _ids(routes):
    return [f"{m} {p}" for m, p, _ in routes]


def test_every_listed_route_exists():
    present = {(m, p) for m, p, _ in ROUTES}
    assert PLATFORM <= present
    assert OWN_STANDING <= present
    assert len(ORG_ACTS) >= 16


class _Built(AssertionError):
    """Something past the guard ran for a caller the guard should have refused."""


class _Pool:
    """The admin plane: the guard's membership read answers "member"; anything else fails."""

    def __init__(self, guard_only: bool):
        self.guard_only = guard_only

    def acquire(self):
        pool = self

        class _Conn:
            async def __aenter__(self):
                if not pool.guard_only:
                    raise _Built("the route read the admin plane before its guard refused")
                return self

            async def __aexit__(self, *_exc):
                return False

            async def execute_core(self, _stmt):
                return SimpleNamespace(fetchone=lambda: (ORG_B,))

        return _Conn()


def _built(*_a, **_k):
    raise _Built("the route reached the organization before its guard refused")


@pytest.fixture
def caller(monkeypatch):
    """org_admin of ORG_A, holding every organization right and not cross_org, a member of both."""
    from provisa.api.admin import invites_router
    from provisa.security.rights import Capability

    caps = {c.value for c in Capability} - {Capability.CROSS_ORG.value}
    monkeypatch.setattr(
        "provisa.api.admin.capabilities._resolved_capabilities", lambda _identity, _state: caps
    )
    monkeypatch.setattr(orgs_router, "_platform_capabilities", lambda _identity: caps)
    monkeypatch.setattr(invites_router, "_pool", lambda _request: _Pool(guard_only=True))
    monkeypatch.setattr(regions_router, "_in_demo_sandbox", lambda: False)
    for module in ROUTERS:
        for name in ("_admin_pool", "_pool", "_org_model_db", "_org_record_db", "_require_org"):
            if hasattr(module, name):
                monkeypatch.setattr(module, name, _built)
    monkeypatch.setattr(org_storage_router, "_storage_url_set", _built)

    def request(active_org: str):
        state = SimpleNamespace(
            identity=SimpleNamespace(user_id="u1", roles=["org_admin"], acting_roles=["org_admin"]),
            active_org_id=active_org,
        )
        return SimpleNamespace(state=state, headers={}, query_params={})

    return request


async def _invoke(endpoint, request) -> None:
    kwargs: dict[str, Any] = {}
    hints = typing.get_type_hints(endpoint)
    for name, param in inspect.signature(endpoint).parameters.items():
        hint = hints.get(name)
        if name == "org_id":
            kwargs[name] = ORG_B
        elif name == "request":
            kwargs[name] = request
        elif name == "user_id":
            kwargs[name] = "u2"
        elif isinstance(hint, type) and issubclass(hint, BaseModel):
            kwargs[name] = hint.model_construct()
        elif param.default is not inspect.Parameter.empty:
            kwargs[name] = param.default
        else:
            raise AssertionError(f"{endpoint.__name__}: no value for {name!r}")
    await endpoint(**kwargs)


@pytest.mark.parametrize(("method", "path", "endpoint"), ORG_ACTS, ids=_ids(ORG_ACTS))
async def test_a_right_held_in_another_org_is_refused(caller, method, path, endpoint):
    """org_admin of org_a, member of org_b, on org_b's path: refused before anything is read."""
    with pytest.raises(ApiError) as refused:
        await _invoke(endpoint, caller(ORG_A))
    assert refused.value.status_code == 403
    assert refused.value.code == "invites.user_management_in_org_required"


@pytest.mark.parametrize(("method", "path", "endpoint"), PLATFORM_ACTS, ids=_ids(PLATFORM_ACTS))
async def test_a_platform_act_needs_the_deployment_right_even_in_the_paths_org(
    caller, method, path, endpoint
):
    """Every organization right, bound to the path's org, and no cross_org: refused."""
    with pytest.raises(ApiError) as refused:
        await _invoke(endpoint, caller(ORG_B))
    assert refused.value.status_code == 403
    assert refused.value.code in {
        "platform.control_plane_role_required",
        "orgs.platform_admin_required",
    }
