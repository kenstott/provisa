# Copyright (c) 2026 Kenneth Stott
# Canary: 9c74b1e8-206d-4a3f-b5c9-e10f83d7a542
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1321: demo data requested at org creation lands in the new org AND is usable by its creator.

Reported failure: a user created an org with demo data, and after re-logging in saw "parts of the
demo data, but not all" — the ``meta`` and ``ops`` domains were absent and nothing was queryable.
"Present in the schema" and "usable by the person who asked for it" are different claims, and only
the first was covered anywhere (``test_org_demo_seed_visible`` reads the admin surface under a
synthesized org_admin identity, never the data plane, and never through HTTP ``create_org``).

So this drives the whole thing for real, end to end, with nothing stubbed:

  * the real app, multitenant, Basic auth, on a live Postgres + Trino;
  * a member-less user POSTs ``/admin/orgs/`` with ``include_demo: true`` from the control-plane
    host (no subdomain — what production serves) and polls to ``ready``;
  * then, in the SAME session, that creator reads the admin surface (domains — ``meta``, ``ops``
    and the demo domain) and QUERIES the demo data over ``/data/graphql``, expecting rows.

The fixture config mirrors production's demo shape (three roles, per-column ``visible_to``), so a
column the creator's role cannot see fails here the same way it failed in the browser.
"""

from __future__ import annotations

import asyncio
import base64
import os

import bcrypt
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import insert

_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "org_demo_data_config.yaml")
)

# The repo default is a session-scoped loop for both fixtures and tests (pyproject asyncio_*).
# Overriding only the tests to a module loop would leave the module-scoped fixture on the session
# loop and every engine it opened would be "attached to a different loop" here — so don't override.
pytestmark = [pytest.mark.integration]

_ORG_ID = "demodata"
_ACCOUNTS = {"founder": "pw-founder", "creator": "pw-creator"}
# No org subdomain: production (cloud.provisa.dev) serves the whole flow from the control-plane
# host, so org selection falls to the sole-membership rule rather than the Host header.
_CONTROL_HOST = "cloud.provisa.test"


def _headers(username: str) -> dict[str, str]:
    raw = f"{username}:{_ACCOUNTS[username]}".encode()
    return {
        "Authorization": "Basic " + base64.b64encode(raw).decode(),
        "host": _CONTROL_HOST,
    }


async def _drop_org_schemas(state) -> None:
    """Drop every schema derived from _ORG_ID so a rerun starts from a genuinely new org."""
    from provisa.api.org_runtime import reset_current_org, set_current_org

    # The tenant_db shim resolves the ContextVar-selected runtime; bind the default org explicitly
    # so this never depends on whatever a previous test left bound.
    token = set_current_org(state.org_id)
    try:
        assert state.tenant_db is not None
        async with state.tenant_db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT nspname FROM pg_namespace WHERE nspname = $1 OR nspname LIKE $2",
                f"org_{_ORG_ID}",
                f"org_{_ORG_ID}_%",
            )
            for row in rows:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{row["nspname"]}" CASCADE')
    finally:
        reset_current_org(token)


async def _clear_admin_rows(state) -> None:
    """Remove this test's org + accounts from the platform plane so the flow creates them itself."""
    from sqlalchemy import delete

    from provisa.core.schema_admin import local_users, org_invites, orgs, user_org_memberships

    assert state.admin_db is not None
    async with state.admin_db.acquire() as conn:
        await conn.execute_core(delete(org_invites).where(org_invites.c.org_id == _ORG_ID))
        await conn.execute_core(
            delete(user_org_memberships).where(user_org_memberships.c.org_id == _ORG_ID)
        )
        await conn.execute_core(delete(orgs).where(orgs.c.id == _ORG_ID))
        await conn.execute_core(delete(local_users).where(local_users.c.id.in_(_ACCOUNTS)))


async def _seed_accounts(state) -> None:
    from provisa.core.schema_admin import local_users

    assert state.admin_db is not None
    async with state.admin_db.acquire() as conn:
        for username, password in _ACCOUNTS.items():
            pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
            await conn.execute_core(
                insert(local_users).values(
                    id=username,
                    username=username,
                    password_hash=pw_hash,
                    email=f"{username}@example.com",
                    display_name=username,
                    is_active=True,
                )
            )


@pytest.fixture(scope="module")
async def demo_org_client():
    """Boot the real multitenant app, then run the create-org-with-demo flow over HTTP.

    Yields the client and the creator's poll result. The client is credential-based (Basic on every
    request), so every later call is the SAME session as the one that created the org — no re-login
    anywhere, which is precisely the property under test."""
    from _pytest.monkeypatch import MonkeyPatch

    from provisa.api.app import create_app, state

    # Scoped to this fixture, never module import: pytest imports every test module before running
    # anything, so an import-time PROVISA_CONFIG would repoint every other in-process create_app.
    mp = MonkeyPatch()
    mp.setenv("PROVISA_ENGINE", "trino")
    mp.setenv("PROVISA_CONFIG", _CONFIG)
    mp.setenv("PROVISA_CONFIG_REPLACE", "1")
    mp.setenv("TRINO_HOST", os.environ.get("TRINO_HOST", "localhost"))
    mp.setenv("TRINO_PORT", os.environ.get("TRINO_PORT", "8080"))
    mp.setenv("PG_PASSWORD", os.environ.get("PG_PASSWORD", "provisa"))
    mp.setenv("PROVISA_SKIP_TRINO_WAIT", os.environ.get("PROVISA_SKIP_TRINO_WAIT", "1"))

    try:
        from provisa.federation.engine import build_engine
        from provisa.federation.runtime import EngineRuntime

        if getattr(state.federation_engine.engine, "name", None) != "trino":
            state.federation_engine = EngineRuntime(build_engine(), state)

        app = create_app()
        async with app.router.lifespan_context(app):
            state.org_registry.invalidate(_ORG_ID)
            await _drop_org_schemas(state)
            await _clear_admin_rows(state)
            await _seed_accounts(state)

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                # The platform superadmin is claimed explicitly, by person 1 — the creator below is
                # deliberately NOT a platform admin, so nothing here is answered by a bypass.
                claim = await client.post("/auth/claim-bootstrap", headers=_headers("founder"))
                assert claim.status_code == 200, claim.text

                created = await client.post(
                    "/admin/orgs/",
                    json={"id": _ORG_ID, "name": "Demo Data", "include_demo": True},
                    headers=_headers("creator"),
                )
                assert created.status_code == 200, created.text
                assert created.json()["provisioning_state"] == "provisioning", created.text

                status = await _poll_ready(client, _ORG_ID)
                assert status["provisioning_state"] == "ready", status
                assert status["provisioning_error"] is None, status

                try:
                    yield client, state
                finally:
                    await _drop_org_schemas(state)
                    await _clear_admin_rows(state)
                    state.org_registry.invalidate(_ORG_ID)
    finally:
        mp.undo()


async def _poll_ready(client: AsyncClient, org_id: str) -> dict:
    """Poll until provisioning leaves ``provisioning``. Real seeding + catalog creation, so slow."""
    for _ in range(600):
        resp = await client.get(f"/admin/orgs/{org_id}/status", headers=_headers("creator"))
        assert resp.status_code == 200, resp.text
        record = resp.json()
        if record["provisioning_state"] != "provisioning":
            return record
        await asyncio.sleep(0.5)
    raise AssertionError("org never left 'provisioning' state")


async def _admin_gql(client: AsyncClient, query: str) -> dict:
    resp = await client.post("/admin/graphql", json={"query": query}, headers=_headers("creator"))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body["errors"]
    return body["data"]


async def test_demo_domains_and_built_ins_are_present_in_the_new_org(demo_org_client):
    # The reported symptom named these three exactly: the demo domain arrived, meta and ops did not.
    # ops/meta are seeded per org by _seed_built_in_sources; the demo domain by the config load.
    client, _state = demo_org_client
    ids = {d["id"] for d in (await _admin_gql(client, "{ domains { id } }"))["domains"]}
    assert "sales-analytics" in ids, ids
    assert "meta" in ids, ids
    assert "ops" in ids, ids


async def test_demo_tables_are_registered_in_the_new_org(demo_org_client):
    client, _state = demo_org_client
    data = await _admin_gql(client, "{ tables { sourceId tableName domainId } }")
    names = {t["tableName"] for t in data["tables"]}
    assert "orders" in names, sorted(names)


async def test_creator_can_query_the_demo_data(demo_org_client):
    # The whole point of "usable": rows come back over the data plane, in the same session that
    # asked for the demo, under the creator's own org_admin role — no platform bypass, no re-login.
    client, _state = demo_org_client
    resp = await client.post(
        "/data/graphql",
        json={"query": "{ orders(limit: 5) { id amount } }"},
        headers=_headers("creator"),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body["errors"]
    rows = body["data"]["orders"]
    assert isinstance(rows, list) and len(rows) > 0, body
    assert {"id", "amount"} <= set(rows[0]), rows[0]
