# Copyright (c) 2026 Kenneth Stott
# Canary: f5ee3030-207d-4ed6-86cb-ecaf39f6fc80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1568: /auth/auto-join-offers is asked only where sign-in auto-joins — multitenant.

The middleware's single-claim join runs inside its multitenant branch only. A single-tenant
deployment used to answer this endpoint anyway, so the onboarding page put a lone claiming org to
the person as a choice that sign-in never made. Both modes are asserted: single-tenant offers
nothing even with a claiming org, multitenant offers the claims the caller is not yet a member of.
"""

# Requirements: REQ-1568, REQ-1285

from __future__ import annotations

import types

import pytest


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Conn:
    """The handler reads org names, then the caller's memberships."""

    def __init__(self):
        self._call = 0

    async def execute_core(self, stmt):
        self._call += 1
        if self._call == 1:
            return _Result([("acme", "Acme"), ("acme-eu", "Acme EU")])
        return _Result([])


class _Db:
    def acquire(self):
        conn = _Conn()

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


def _request():
    identity = types.SimpleNamespace(user_id="dana", email="dana@eu.acme.com")
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity))


@pytest.fixture
def claims(monkeypatch):
    from provisa.api.app import state

    async def _resolve(admin_db, email, user_id):
        return [("acme", "analyst"), ("acme-eu", "analyst")]

    monkeypatch.setattr("provisa.core.org_membership.resolve_auto_join_orgs", _resolve)
    monkeypatch.setattr(state, "admin_db", _Db())
    return state


@pytest.mark.asyncio
async def test_single_tenant_offers_nothing_even_when_orgs_claim_the_address(claims, monkeypatch):
    from provisa.api.auth_router import _auto_join_offers

    monkeypatch.setattr(claims, "multitenancy", False)
    assert await _auto_join_offers(_request()) == []


@pytest.mark.asyncio
async def test_multitenant_offers_every_claiming_org_the_caller_has_not_joined(claims, monkeypatch):
    from provisa.api.auth_router import _auto_join_offers

    monkeypatch.setattr(claims, "multitenancy", True)
    assert await _auto_join_offers(_request()) == [
        {"org_id": "acme", "org_name": "Acme", "role_id": "analyst"},
        {"org_id": "acme-eu", "org_name": "Acme EU", "role_id": "analyst"},
    ]
