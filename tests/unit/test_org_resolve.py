# Copyright (c) 2026 Kenneth Stott
# Canary: 1f2e3d4c-5b6a-7980-a1b2-c3d4e5f60718
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1266: org resolution for the non-HTTP protocol entrypoints (pgwire/bolt/flight/gRPC).

``resolve_session_org`` mirrors the HTTP ``AuthMiddleware`` membership rule at session
establishment. The invariant these tests pin: it NEVER silently defaults an authenticated
multitenant principal to some org — an unresolvable principal raises, because a wrong default
here is a cross-tenant data escape. Single-org deployments short-circuit to None (the AppState
shims then resolve the default runtime)."""

from __future__ import annotations

import pytest

from provisa.api.org_resolve import OrgResolutionError, resolve_session_org


class _FakeRow:
    def __init__(self, org_id: str) -> None:
        self._mapping = {"org_id": org_id}


class _FakeResult:
    def __init__(self, org_ids: list[str]) -> None:
        self._rows = [_FakeRow(o) for o in org_ids]

    def fetchall(self) -> list[_FakeRow]:
        return self._rows


class _FakeConn:
    def __init__(self, org_ids: list[str]) -> None:
        self._org_ids = org_ids

    async def execute_core(self, _stmt) -> _FakeResult:
        return _FakeResult(self._org_ids)


class _FakeAcquire:
    def __init__(self, org_ids: list[str]) -> None:
        self._org_ids = org_ids

    async def __aenter__(self) -> _FakeConn:
        return _FakeConn(self._org_ids)

    async def __aexit__(self, *_exc) -> bool:
        return False


class _FakeAdminDB:
    def __init__(self, org_ids: list[str]) -> None:
        self._org_ids = org_ids

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._org_ids)


class _FakeState:
    def __init__(self, *, multitenancy: bool, org_ids: list[str] | None = None) -> None:
        self.multitenancy = multitenancy
        self.admin_db = _FakeAdminDB(org_ids or [])


@pytest.mark.asyncio
async def test_single_org_returns_none_even_with_user():
    # multitenancy off → always None; caller leaves current_org unset → default runtime.
    state = _FakeState(multitenancy=False, org_ids=["acme"])
    assert await resolve_session_org(state, user_id="u1", requested_org="acme") is None


@pytest.mark.asyncio
async def test_lone_membership_auto_selects():
    state = _FakeState(multitenancy=True, org_ids=["acme"])
    assert await resolve_session_org(state, user_id="u1") == "acme"


@pytest.mark.asyncio
async def test_requested_org_member_is_honored():
    state = _FakeState(multitenancy=True, org_ids=["acme", "beta"])
    assert await resolve_session_org(state, user_id="u1", requested_org="beta") == "beta"


@pytest.mark.asyncio
async def test_requested_org_non_member_raises():
    state = _FakeState(multitenancy=True, org_ids=["acme"])
    with pytest.raises(OrgResolutionError, match="not a member of org 'beta'"):
        await resolve_session_org(state, user_id="u1", requested_org="beta")


@pytest.mark.asyncio
async def test_platform_admin_requested_non_member_is_honored():
    # A platform admin may act on any org even without a membership row.
    state = _FakeState(multitenancy=True, org_ids=[])
    resolved = await resolve_session_org(
        state, user_id="admin", is_platform_admin=True, requested_org="beta"
    )
    assert resolved == "beta"


@pytest.mark.asyncio
async def test_platform_admin_no_membership_no_request_returns_none():
    state = _FakeState(multitenancy=True, org_ids=[])
    resolved = await resolve_session_org(state, user_id="admin", is_platform_admin=True)
    assert resolved is None


@pytest.mark.asyncio
async def test_ambiguous_membership_raises():
    # Multiple orgs, no explicit request, not a platform admin → must fail loud, never default.
    state = _FakeState(multitenancy=True, org_ids=["acme", "beta"])
    with pytest.raises(OrgResolutionError, match="org selection required"):
        await resolve_session_org(state, user_id="u1")


@pytest.mark.asyncio
async def test_no_membership_no_request_raises():
    # An authenticated principal with zero memberships and no request is unresolvable → raise.
    state = _FakeState(multitenancy=True, org_ids=[])
    with pytest.raises(OrgResolutionError, match="belongs to 0 orgs"):
        await resolve_session_org(state, user_id="u1")
