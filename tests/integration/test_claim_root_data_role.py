# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8d27a1-c64e-4b9a-8e15-a7d0b2c93f64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1296 (amended 2026-11-27): the admin who claims the install lands on data in root.

The real app boots multitenant with its deployment org named 'root' (ORG_ID, the control-plane
resolver's value, REQ-1286). Before anyone signs in, root already carries the demo assets: the
config's source and table, and the built-in ops and meta domains. The first identity then claims
the platform-admin slot over HTTP. platform_admin holds no data capability, so the claim also seats
them with a data role in root: /auth/me lists it for the role selector, and under it the demo
tables are listed and queried. platform_admin itself still carries only the two platform rights.
"""

# Requirements: REQ-1296

from __future__ import annotations

import base64
import os

import bcrypt
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, insert, select

_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "org_demo_data_config.yaml")
)

_ROOT = "root"
pytestmark = [
    pytest.mark.requires_trino,
    pytest.mark.integration,
    pytest.mark.deployment_org(_ROOT),
]

_CLAIMANT = "rootclaimant"
_PASSWORD = "pw-rootclaimant"
_CONTROL_HOST = "cloud.provisa.test"
# The rights platform_admin carries, and nothing else (REQ-1297, REQ-1337).
_PLATFORM_RIGHTS = {"platform_settings", "cross_org"}


def _headers() -> dict[str, str]:
    raw = f"{_CLAIMANT}:{_PASSWORD}".encode()
    return {"Authorization": "Basic " + base64.b64encode(raw).decode(), "host": _CONTROL_HOST}


def _in_root(role: str | None = None) -> dict[str, str]:
    headers = {**_headers(), "x-org-provisa": _ROOT}
    if role is not None:
        headers["X-Provisa-Role"] = role
    return headers


async def _unclaimed_slot_and_account(state) -> None:
    """A deployment nobody has signed in to: the slot is free and the claimant is a member of no
    org. The account exists, as a Basic-auth identity has to before it can authenticate."""
    from provisa.core.schema_admin import local_users, superadmin_bootstrap, user_org_memberships

    assert state.admin_db is not None
    async with state.admin_db.acquire() as conn:
        await conn.execute_core(delete(superadmin_bootstrap))
        await conn.execute_core(
            delete(user_org_memberships).where(user_org_memberships.c.user_id == _CLAIMANT)
        )
        await conn.execute_core(delete(local_users).where(local_users.c.id == _CLAIMANT))
        pw_hash = bcrypt.hashpw(_PASSWORD.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        await conn.execute_core(
            insert(local_users).values(
                id=_CLAIMANT,
                username=_CLAIMANT,
                password_hash=pw_hash,
                email=f"{_CLAIMANT}@example.com",
                display_name=_CLAIMANT,
                is_active=True,
            )
        )
    from provisa.core.schema_org import user_role_assignments
    from tests.conftest import as_deployment_org

    with as_deployment_org():
        assert state.model_db is not None
        async with state.model_db.acquire() as conn:
            await conn.execute_core(
                delete(user_role_assignments).where(user_role_assignments.c.user_id == _CLAIMANT)
            )


async def _root_assets(state) -> dict[str, set[str]]:
    """What root's model store holds, read straight from it: no identity is involved."""
    from provisa.core.schema_org import domains, registered_tables, sources
    from tests.conftest import as_deployment_org

    with as_deployment_org():
        assert state.model_db is not None
        async with state.model_db.acquire() as conn:
            src = (await conn.execute_core(select(sources.c.id))).fetchall()
            dom = (await conn.execute_core(select(domains.c.id))).fetchall()
            tbl = (await conn.execute_core(select(registered_tables.c.table_name))).fetchall()
    return {
        "sources": {r[0] for r in src},
        "domains": {r[0] for r in dom},
        "tables": {r[0] for r in tbl},
    }


@pytest.fixture(scope="module")
async def root_boot():
    """Boot the real multitenant app with root as its deployment org; record root's assets and
    the slot's state before the first sign-in, then claim. Yields the client, the app state and
    what was observed before the claim."""
    from _pytest.monkeypatch import MonkeyPatch

    from provisa.api.app import create_app, state

    mp = MonkeyPatch()
    mp.setenv("ORG_ID", _ROOT)
    mp.setenv("PROVISA_ENGINE", "trino")
    mp.setenv("PROVISA_CONFIG", _CONFIG)
    mp.setenv("TRINO_HOST", os.environ.get("TRINO_HOST", "localhost"))
    mp.setenv("TRINO_PORT", os.environ.get("TRINO_PORT", "8080"))
    mp.setenv("PG_PASSWORD", os.environ.get("PG_PASSWORD", "provisa"))
    mp.setenv("PROVISA_SKIP_TRINO_WAIT", os.environ.get("PROVISA_SKIP_TRINO_WAIT", "1"))
    try:
        from provisa.federation.engine import build_engine
        from provisa.federation.runtime import EngineRuntime

        from tests.conftest import as_deployment_org

        app = create_app()
        async with app.router.lifespan_context(app):
            with as_deployment_org():
                if getattr(state.federation_engine.engine, "name", None) != "trino":
                    state.federation_engine = EngineRuntime(build_engine(), state)
            await _unclaimed_slot_and_account(state)
            before = {"org_id": state.org_id, "assets": await _root_assets(state)}

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                before["status"] = (await client.get("/auth/bootstrap-status")).json()
                claim = await client.post("/auth/claim-bootstrap", headers=_headers())
                assert claim.status_code == 200, claim.text
                before["claim"] = claim.json()
                yield client, state, before
    finally:
        mp.undo()


async def test_root_carries_the_demo_assets_before_the_first_sign_in(root_boot):
    _client, _state, before = root_boot
    assert before["org_id"] == _ROOT
    assert before["status"] == {"unclaimed": True}
    assets = before["assets"]
    assert "sales-pg" in assets["sources"], assets
    assert "orders" in assets["tables"], assets
    assert {"sales-analytics", "ops", "meta"} <= assets["domains"], assets


async def test_the_claim_lands_the_admin_in_root(root_boot):
    _client, _state, before = root_boot
    assert before["claim"] == {"claimed": True, "claimed_by": _CLAIMANT, "org_id": _ROOT}


async def test_the_role_selector_offers_a_data_role_in_root(root_boot):
    client, state, _before = root_boot
    me = await client.get("/auth/me", headers=_in_root())
    assert me.status_code == 200, me.text
    body = me.json()
    assert body["active_org_id"] == _ROOT
    assert [m["org_id"] for m in body["org_memberships"]] == [_ROOT]
    held = {a["role_id"] for a in body["assignments"]}
    assert "platform_admin" in held, held
    # The data role the selector offers: a role beside platform_admin that is not control-plane.
    data_roles = {
        r for r in held if "cross_org" not in (state.roles.get(r, {}).get("capabilities") or [])
    }
    assert data_roles, held


async def test_the_demo_tables_list_under_the_data_role(root_boot):
    client, _state, _before = root_boot
    resp = await client.post(
        "/admin/graphql",
        json={"query": "{ domains { id } tables { tableName domainId } }"},
        headers=_in_root("org_admin"),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body["errors"]
    assert {"sales-analytics", "ops", "meta"} <= {d["id"] for d in body["data"]["domains"]}
    assert "orders" in {t["tableName"] for t in body["data"]["tables"]}


async def test_the_demo_data_answers_under_the_data_role(root_boot):
    client, _state, _before = root_boot
    resp = await client.post(
        "/data/graphql",
        json={"query": "{ orders(limit: 5) { id amount } }"},
        headers=_in_root("org_admin"),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body["errors"]
    rows = body["data"]["orders"]
    assert isinstance(rows, list) and rows, body


async def test_platform_admin_still_holds_no_data_capability(root_boot):
    client, _state, _before = root_boot
    resp = await client.get("/admin/roles/", headers=_in_root())
    assert resp.status_code == 200, resp.text
    roles = {r["id"]: r for r in resp.json()}
    assert set(roles["platform_admin"]["capabilities"]) == _PLATFORM_RIGHTS, roles["platform_admin"]
    assert roles["platform_admin"]["domain_access"] == [], roles["platform_admin"]
