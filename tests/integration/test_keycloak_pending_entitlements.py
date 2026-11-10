# Copyright (c) 2026 Kenneth Stott
# Canary: 6e0b3f58-2c19-4d7a-b8f4-a13e95c07d22
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, ONE API, THREE WAYS TO USE IT: a person not yet seen is granted by typed email, and the
grant attaches when a sign-in token's email matches -- here a real Keycloak token, signed in through
the auth middleware's sign-in hook against a control plane of the test's own.

The realm is the ``keycloak`` service of the isolated test stack
(tests/fixtures/keycloak/provisa-test-realm.json): carol carries a plain role grant and no lists,
dana sends her viewer tables list, which wins over anything Provisa holds.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import select

from provisa.auth import middleware
from provisa.core.schema_org import entitlement_pending
from provisa.entitlements import pending
from tests.integration.test_keycloak_entitlement_claims import _signed_in

pytestmark = [pytest.mark.integration, pytest.mark.requires_keycloak]


@pytest.fixture
async def org(control_plane, monkeypatch):
    state = SimpleNamespace(held_entitlements={}, idp_entitlements={})
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(middleware, "_recorded_idp_lists", {})
    monkeypatch.setattr(middleware, "_attached_pending", set())
    return SimpleNamespace(db=control_plane.org(), state=state)


async def _grant_by_email(db, email: str) -> None:
    async with db.acquire() as conn:
        await pending.set_pending(
            conn,
            email,
            "viewer",
            "tables",
            ["finance.revenue"],
            expires_at=None,
            set_by="admin",
            reason="joining finance",
        )


async def _last_outcome(db) -> tuple[str | None, str | None]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            select(entitlement_pending).order_by(entitlement_pending.c.id)
        )
        last = rows.fetchall()[-1]
    return last.outcome, last.principal


async def test_a_grant_by_email_attaches_on_that_persons_first_keycloak_sign_in(org):
    await _grant_by_email(org.db, "Carol@Provisa.test")
    identity = await _signed_in("carol")
    assert identity.email == "carol@provisa.test"

    await middleware._record_idp_entitlements(org.db, identity)  # pyright: ignore[reportPrivateUsage]

    held = org.state.held_entitlements[identity.user_id].lists
    assert [(h.role_id, h.kind, h.identifiers, h.set_by, h.reason) for h in held] == [
        ("viewer", "tables", ("finance.revenue",), "admin", "joining finance")
    ]
    assert await _last_outcome(org.db) == (pending.ATTACHED, identity.user_id)
    async with org.db.acquire() as conn:
        assert await pending.load_pending(conn) == []


async def test_a_grant_the_keycloak_token_supersedes_stays_refused(org):
    await _grant_by_email(org.db, "dana@provisa.test")
    identity = await _signed_in("dana")
    assert identity.email == "dana@provisa.test"

    await middleware._record_idp_entitlements(org.db, identity)  # pyright: ignore[reportPrivateUsage]

    assert identity.user_id not in org.state.held_entitlements
    assert org.state.idp_entitlements[identity.user_id]["viewer"]["tables"] == ("sales.orders",)
    assert await _last_outcome(org.db) == (pending.REFUSED, identity.user_id)
