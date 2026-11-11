# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e2a17-5b3d-4f80-a6e1-d27f0b8c3e94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, IDENTITY PROVIDER ROLES ARE MIRRORED: where roles come from the identity provider's
claims, a sign-in records the person's role claims, and /admin/entitlements/roles lists the roles
they hold with the identity provider as their origin -- here a real Keycloak token, signed in
through the auth middleware's sign-in hook against a control plane of the test's own.

The realm is the ``keycloak`` service of the isolated test stack
(tests/fixtures/keycloak/provisa-test-realm.json): dana holds viewer and analyst; Keycloak's own
default realm roles ride along in the token and, being roles the org does not define, are left
out of the list.
"""

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from provisa.api.admin.entitlements_router import router
from provisa.auth import middleware
from provisa.core.schema_org import user_idp_roles
from provisa.entitlements import runtime
from tests.integration.test_keycloak_entitlement_claims import _signed_in
from tests.pg_control_plane import ORG
from tests.unit.test_entitlements_api import _identity, _state

pytestmark = [pytest.mark.integration, pytest.mark.requires_keycloak]


@pytest.fixture
async def org(control_plane, monkeypatch):
    state = _state(control_plane.org())
    state.auth_config = {"provider": "keycloak", "assignments_source": "claims"}
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr(middleware, "_recorded_idp_roles", {})

    app = FastAPI()
    app.include_router(router)

    @app.middleware("http")
    async def _bind(request, call_next):  # pyright: ignore[reportUnusedFunction]
        identity = _identity()
        request.state.identity = identity
        request.state.active_org_id = ORG
        with runtime.entitlements_scope(identity):
            return await call_next(request)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        yield SimpleNamespace(client=client, db=state.model_db)


async def test_a_keycloak_sign_in_mirrors_its_role_claims_listed_with_origin_idp(org):
    identity = await _signed_in("dana")
    assert {"viewer", "analyst"} <= set(identity.roles)

    await middleware._record_idp_roles(org.db, identity)  # pyright: ignore[reportPrivateUsage]

    async with org.db.acquire() as conn:
        rows = await conn.execute_core(
            select(user_idp_roles.c.role_id, user_idp_roles.c.domain_id).where(
                user_idp_roles.c.user_id == identity.user_id
            )
        )
        mirrored = {(r.role_id, r.domain_id) for r in rows.fetchall()}
    assert mirrored == {(role, "*") for role in identity.roles}

    response = await org.client.get(
        "/admin/entitlements/roles", params={"principal": identity.user_id}
    )
    assert response.status_code == 200, response.text
    assert response.json()["roles"] == [
        {"role_id": "analyst", "origins": ["idp"], "domains": ["*"]},
        {"role_id": "viewer", "origins": ["idp"], "domains": ["*"]},
    ]
