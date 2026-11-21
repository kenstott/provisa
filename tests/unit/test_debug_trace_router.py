# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8f1d27-6e49-4a0c-9d52-7c1e4f6a8b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The operator's debug-trace admin API (REQ-1910).

Start, list and stop debug-trace windows and set the per-role hint permission, against a real
control-plane store. What is pinned: an org-wide window is the platform's (the deployment check), a
window naming a role or source and the per-role hint are the org admin's (REQ-1327), for opening,
stopping and listing alike; a window reports the time it has left, and a bad window is a 400 naming
what is wrong.
"""

# Requirements: REQ-1910, REQ-1327

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin import debug_trace_router as dr
from provisa.api.errors import ApiError
from provisa.core import trace_scope as ts
from provisa.core.schema_admin import orgs
from provisa.core.database import Database


@pytest.fixture
def db(control_plane, monkeypatch) -> Database:
    database = control_plane.platform
    with database.engine.begin() as conn:
        conn.execute(orgs.insert().values(id="acme", name="Acme"))  # the org the windows are for
    monkeypatch.setattr(dr, "_admin_pool", lambda: database)
    monkeypatch.setattr(dr, "require_deployment_settings", lambda _r: None)
    monkeypatch.setattr(dr, "_require_org_settings_in", lambda _r, _org, _subject: None)
    monkeypatch.setattr(dr, "_reach", lambda _r: lambda _scope, _org_id: True)
    ts.invalidate()
    return database


def _request(user_id: str = "ops@example.com"):
    return SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id=user_id)))


@pytest.mark.asyncio
async def test_a_deployment_with_no_windows_reads_as_none_open(db):
    result = await dr.read_debug_trace(_request())
    assert result["windows"] == []
    assert result["hint_roles"] == []
    assert result["max_minutes"] == ts.MAX_WINDOW_MINUTES
    assert result["hint_setting"] == ts.HINT_SETTING


@pytest.mark.asyncio
async def test_starting_a_window_lists_it_with_its_time_remaining_and_who_opened_it(db):
    result = await dr.start_debug_window(
        _request(), dr.WindowBody(scope="org", org_id="acme", minutes=15)
    )
    (window,) = result["windows"]
    assert (window["scope"], window["org_id"], window["target"]) == ("org", "acme", None)
    assert 15 * 60 - 5 <= window["remaining_seconds"] <= 15 * 60
    assert window["created_by"] == "ops@example.com"


@pytest.mark.asyncio
async def test_a_role_window_and_a_source_window_carry_their_target(db):
    await dr.start_debug_window(
        _request(), dr.WindowBody(scope="role", org_id="acme", target=" analyst ", minutes=5)
    )
    result = await dr.start_debug_window(
        _request(), dr.WindowBody(scope="source", org_id="acme", target="sales_pg", minutes=10)
    )
    assert [(w["scope"], w["target"]) for w in result["windows"]] == [
        ("role", "analyst"),
        ("source", "sales_pg"),
    ]


@pytest.mark.asyncio
async def test_stopping_a_window_removes_it_and_an_unknown_one_is_404(db):
    started = await dr.start_debug_window(
        _request(), dr.WindowBody(scope="org", org_id="acme", minutes=15)
    )
    window_id = started["windows"][0]["id"]
    assert (await dr.stop_debug_window(_request(), window_id))["windows"] == []
    with pytest.raises(ApiError) as exc:
        await dr.stop_debug_window(_request(), window_id)
    assert exc.value.status_code == 404
    assert exc.value.code == "debug_trace.window_not_found"


@pytest.mark.parametrize(
    "body",
    [
        dr.WindowBody(scope="org", org_id="acme", minutes=0),
        dr.WindowBody(scope="org", org_id="acme", minutes=ts.MAX_WINDOW_MINUTES + 1),
        dr.WindowBody(scope="role", org_id="acme", minutes=5),
        dr.WindowBody(scope="source", org_id="acme", target="  ", minutes=5),
        dr.WindowBody(scope="table", org_id="acme", target="t", minutes=5),
    ],
)
@pytest.mark.asyncio
async def test_a_window_without_a_bounded_duration_or_its_target_is_refused(db, body):
    with pytest.raises(ApiError) as exc:
        await dr.start_debug_window(_request(), body)
    assert exc.value.status_code == 400
    assert exc.value.code == "debug_trace.invalid_window"
    assert (await dr.read_debug_trace(_request()))["windows"] == []


@pytest.mark.asyncio
async def test_hint_permission_is_set_and_cleared_per_role(db):
    result = await dr.set_hint_role(
        _request(), dr.HintRoleBody(org_id="acme", role_id="analyst", permitted=True)
    )
    assert result["hint_roles"] == [{"org_id": "acme", "role_id": "analyst"}]
    result = await dr.set_hint_role(
        _request(), dr.HintRoleBody(org_id="acme", role_id="analyst", permitted=False)
    )
    assert result["hint_roles"] == []


@pytest.mark.parametrize("call", ["read", "start", "start_role", "stop", "hint"])
@pytest.mark.asyncio
async def test_every_endpoint_is_gated_before_anything_is_stored(db, monkeypatch, call):
    """Each endpoint refuses a caller its gate denies and stores nothing. REQ-1327 (amended
    2026-11-05) moved the org-wide window to the deployment check, a role or source window and the
    hint to the org admin's right, and the listing to the caller's reach, so every gate is denied
    here, not only platform_settings."""

    def _deny(*_args):
        raise ApiError(403, "denied", "denied")

    monkeypatch.setattr(dr, "require_deployment_settings", _deny)
    monkeypatch.setattr(dr, "_require_org_settings_in", _deny)
    monkeypatch.setattr(dr, "_reach", _deny)
    calls = {
        "read": lambda: dr.read_debug_trace(_request()),
        "start": lambda: dr.start_debug_window(
            _request(), dr.WindowBody(scope="org", org_id="acme", minutes=5)
        ),
        "start_role": lambda: dr.start_debug_window(
            _request(), dr.WindowBody(scope="role", org_id="acme", target="analyst", minutes=5)
        ),
        "stop": lambda: dr.stop_debug_window(_request(), "any"),
        "hint": lambda: dr.set_hint_role(
            _request(), dr.HintRoleBody(org_id="acme", role_id="analyst", permitted=True)
        ),
    }
    with pytest.raises(ApiError) as exc:
        await calls[call]()
    assert exc.value.status_code == 403
    assert await ts.list_windows(db) == []
    assert await ts.list_hint_roles(db) == []


# ── REQ-1327 (amended 2026-11-05): who opens which window, with the real gates ─────────────────

_PLATFORM_ADMIN = {"platform_settings", "cross_org"}
# A single-tenant org admin holds platform_settings too, never cross_org.
_SINGLE_TENANT_ORG_ADMIN = {"platform_settings", "org_settings", "user_management"}
_ORG_ADMIN = {"org_settings", "user_management"}


@pytest.fixture
def gated(control_plane, monkeypatch) -> Database:
    database = control_plane.platform
    with database.engine.begin() as conn:
        conn.execute(orgs.insert().values(id="acme", name="Acme"))
        conn.execute(orgs.insert().values(id="beta", name="Beta"))
    monkeypatch.setattr(dr, "_admin_pool", lambda: database)
    ts.invalidate()
    return database


def _caller(monkeypatch, caps: set[str], active_org: str):
    monkeypatch.setattr(
        "provisa.api.admin.capabilities._resolved_capabilities", lambda _identity, _state: caps
    )
    return SimpleNamespace(
        state=SimpleNamespace(
            identity=SimpleNamespace(user_id="caller@example.com"), active_org_id=active_org
        )
    )


@pytest.mark.asyncio
async def test_the_platform_opens_a_window_for_an_org_as_a_whole(gated, monkeypatch):
    request = _caller(monkeypatch, _PLATFORM_ADMIN, "root")
    result = await dr.start_debug_window(
        request, dr.WindowBody(scope="org", org_id="acme", minutes=5)
    )
    assert [(w["scope"], w["org_id"]) for w in result["windows"]] == [("org", "acme")]


@pytest.mark.parametrize("scope,target", [("role", "analyst"), ("source", "sales_pg")])
@pytest.mark.asyncio
async def test_the_platform_is_refused_a_window_naming_a_role_or_source(
    gated, monkeypatch, scope, target
):
    request = _caller(monkeypatch, _PLATFORM_ADMIN, "root")
    with pytest.raises(ApiError) as exc:
        await dr.start_debug_window(
            request, dr.WindowBody(scope=scope, org_id="acme", target=target, minutes=5)
        )
    assert exc.value.status_code == 403
    assert exc.value.code == "debug_trace.org_settings_in_org_required"
    assert "org_settings in acme" in exc.value.detail
    assert await ts.list_windows(gated) == []


@pytest.mark.asyncio
async def test_org_settings_held_in_another_org_does_not_name_a_role_here(gated, monkeypatch):
    # The bootstrap platform admin is also root's org admin: acting in root, it holds org_settings
    # there and nowhere else.
    request = _caller(monkeypatch, _PLATFORM_ADMIN | _ORG_ADMIN, "root")
    with pytest.raises(ApiError) as exc:
        await dr.start_debug_window(
            request, dr.WindowBody(scope="role", org_id="acme", target="analyst", minutes=5)
        )
    assert exc.value.status_code == 403
    assert exc.value.code == "debug_trace.org_settings_in_org_required"
    assert await ts.list_windows(gated) == []


@pytest.mark.parametrize("scope,target", [("role", "analyst"), ("source", "sales_pg")])
@pytest.mark.asyncio
async def test_the_org_admin_opens_a_window_naming_a_role_or_source(
    gated, monkeypatch, scope, target
):
    request = _caller(monkeypatch, _ORG_ADMIN, "acme")
    result = await dr.start_debug_window(
        request, dr.WindowBody(scope=scope, org_id="acme", target=target, minutes=5)
    )
    assert [(w["scope"], w["target"]) for w in result["windows"]] == [(scope, target)]


@pytest.mark.parametrize("caps", [_ORG_ADMIN, _SINGLE_TENANT_ORG_ADMIN])
@pytest.mark.asyncio
async def test_an_org_admin_is_refused_the_org_wide_window(gated, monkeypatch, caps):
    # An org-wide window is load on the deployment: the platform's, by the deployment check, which
    # a single-tenant org admin's platform_settings alone does not pass.
    request = _caller(monkeypatch, caps, "acme")
    with pytest.raises(ApiError) as exc:
        await dr.start_debug_window(request, dr.WindowBody(scope="org", org_id="acme", minutes=5))
    assert exc.value.status_code == 403
    assert exc.value.code == "platform.control_plane_role_required"
    assert await ts.list_windows(gated) == []


# ── REQ-1327 (amended 2026-11-05): listing, stopping and the hint follow the same scope rule ─────


async def _seed(db: Database) -> dict[tuple[str, str], str]:
    """An org-wide window in acme, a role window in acme and one in beta, and a hint role in each
    org. Returns the window ids by (org, scope)."""
    for scope, org_id, target in [
        ("org", "acme", None),
        ("role", "acme", "analyst"),
        ("role", "beta", "analyst"),
    ]:
        await ts.start_window(
            db, scope=scope, org_id=org_id, target=target, minutes=5, created_by="seed"
        )
    for org_id in ("acme", "beta"):
        await ts.set_hint_permission(db, org_id, "analyst", True, updated_by="seed")
    return {(w.org_id, w.scope): w.id for w in await ts.list_windows(db)}


def _listed(result: dict) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    return (
        sorted((w["org_id"], w["scope"]) for w in result["windows"]),
        sorted((h["org_id"], h["role_id"]) for h in result["hint_roles"]),
    )


@pytest.mark.asyncio
async def test_the_listing_shows_the_platform_the_org_wide_windows_only(gated, monkeypatch):
    """REQ-1327 (amended 2026-11-05): this test used to assert that the listing needed the
    deployment check and showed everything. The listing now follows the rule that decides who opens
    a window: the platform sees the org-wide windows, never a role or source window or a hint role,
    which name things inside an org."""
    await _seed(gated)
    result = await dr.read_debug_trace(_caller(monkeypatch, _PLATFORM_ADMIN, "root"))
    assert _listed(result) == ([("acme", "org")], [])


@pytest.mark.parametrize("caps", [_ORG_ADMIN, _SINGLE_TENANT_ORG_ADMIN])
@pytest.mark.asyncio
async def test_an_org_admins_listing_shows_only_its_orgs_windows(gated, monkeypatch, caps):
    await _seed(gated)
    result = await dr.read_debug_trace(_caller(monkeypatch, caps, "acme"))
    assert _listed(result) == ([("acme", "org"), ("acme", "role")], [("acme", "analyst")])


@pytest.mark.asyncio
async def test_the_bootstrap_admin_lists_the_org_wide_windows_and_its_own_orgs(gated, monkeypatch):
    # Both rights at once (the bootstrap claimant holds both in root), acting in beta: the org-wide
    # windows of every org, and only beta's own role windows and hint roles.
    await _seed(gated)
    result = await dr.read_debug_trace(_caller(monkeypatch, _PLATFORM_ADMIN | _ORG_ADMIN, "beta"))
    assert _listed(result) == (
        [("acme", "org"), ("beta", "role")],
        [("beta", "analyst")],
    )


@pytest.mark.asyncio
async def test_a_caller_with_neither_reach_is_refused_the_listing(gated, monkeypatch):
    await _seed(gated)
    with pytest.raises(ApiError) as exc:
        await dr.read_debug_trace(_caller(monkeypatch, {"user_management", "usage"}, "acme"))
    assert exc.value.status_code == 403
    assert exc.value.code == "debug_trace.no_reach"


@pytest.mark.asyncio
async def test_the_platform_stops_an_org_wide_window(gated, monkeypatch):
    ids = await _seed(gated)
    result = await dr.stop_debug_window(
        _caller(monkeypatch, _PLATFORM_ADMIN, "root"), ids[("acme", "org")]
    )
    assert result["windows"] == []
    assert ("acme", "org") not in {(w.org_id, w.scope) for w in await ts.list_windows(gated)}


@pytest.mark.asyncio
async def test_the_platform_cannot_stop_a_role_window(gated, monkeypatch):
    # Outside its reach, so it is not found, as the listing does not show it; the window stays open.
    ids = await _seed(gated)
    with pytest.raises(ApiError) as exc:
        await dr.stop_debug_window(
            _caller(monkeypatch, _PLATFORM_ADMIN, "root"), ids[("acme", "role")]
        )
    assert exc.value.status_code == 404
    assert exc.value.code == "debug_trace.window_not_found"
    assert len(await ts.list_windows(gated)) == 3


@pytest.mark.parametrize("caps", [_ORG_ADMIN, _SINGLE_TENANT_ORG_ADMIN])
@pytest.mark.asyncio
async def test_an_org_admin_cannot_stop_the_org_wide_window_it_sees(gated, monkeypatch, caps):
    ids = await _seed(gated)
    with pytest.raises(ApiError) as exc:
        await dr.stop_debug_window(_caller(monkeypatch, caps, "acme"), ids[("acme", "org")])
    assert exc.value.status_code == 403
    assert exc.value.code == "platform.control_plane_role_required"
    assert len(await ts.list_windows(gated)) == 3


@pytest.mark.asyncio
async def test_an_org_admin_stops_its_orgs_role_window_and_not_another_orgs(gated, monkeypatch):
    ids = await _seed(gated)
    with pytest.raises(ApiError) as exc:
        await dr.stop_debug_window(_caller(monkeypatch, _ORG_ADMIN, "acme"), ids[("beta", "role")])
    assert exc.value.status_code == 404
    result = await dr.stop_debug_window(
        _caller(monkeypatch, _ORG_ADMIN, "acme"), ids[("acme", "role")]
    )
    assert [(w["org_id"], w["scope"]) for w in result["windows"]] == [("acme", "org")]
    assert sorted((w.org_id, w.scope) for w in await ts.list_windows(gated)) == [
        ("acme", "org"),
        ("beta", "role"),
    ]


@pytest.mark.asyncio
async def test_the_platform_is_refused_the_hint(gated, monkeypatch):
    with pytest.raises(ApiError) as exc:
        await dr.set_hint_role(
            _caller(monkeypatch, _PLATFORM_ADMIN, "root"),
            dr.HintRoleBody(org_id="acme", role_id="analyst", permitted=True),
        )
    assert exc.value.status_code == 403
    assert exc.value.code == "debug_trace.org_settings_in_org_required"
    assert "org_settings in acme" in exc.value.detail
    assert await ts.list_hint_roles(gated) == []


@pytest.mark.asyncio
async def test_an_org_admin_sets_the_hint_in_its_org_only(gated, monkeypatch):
    with pytest.raises(ApiError) as exc:
        await dr.set_hint_role(
            _caller(monkeypatch, _ORG_ADMIN, "acme"),
            dr.HintRoleBody(org_id="beta", role_id="analyst", permitted=True),
        )
    assert exc.value.status_code == 403
    assert exc.value.code == "debug_trace.org_settings_in_org_required"
    result = await dr.set_hint_role(
        _caller(monkeypatch, _ORG_ADMIN, "acme"),
        dr.HintRoleBody(org_id="acme", role_id="analyst", permitted=True),
    )
    assert result["hint_roles"] == [{"org_id": "acme", "role_id": "analyst"}]
    assert await ts.list_hint_roles(gated) == [("acme", "analyst")]
