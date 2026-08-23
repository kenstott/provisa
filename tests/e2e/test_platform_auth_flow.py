# Copyright (c) 2026 Kenneth Stott
# Canary: 67675c68-136b-46da-878f-1af930493895
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the multitenant auth plane over the full app against the live stack.

One app, auth ENFORCED (basic provider, HTTP Basic credentials against the real
``local_users`` control-plane table), multitenancy on, bootstrap claiming enabled.
The module walks the real deployment lifecycle in order; later classes depend on the
state earlier ones created (pytest runs them in file order):

- REQ-1290: platform-admin bootstrap is an explicit POST /auth/claim-bootstrap —
  authenticating alone never claims the slot; the first writer wins; a losing claim
  reads back the holder; no identity answers 401.
- REQ-1284: an org email-address rule gates invite redemption — matching email joins,
  mismatching email is refused with a stable code, no rule accepts any email.
- REQ-1295: X-Provisa-Role is honored only for a role the caller is actually ASSIGNED.
- REQ-1327: platform_admin is control-plane only — zero data-plane capability anywhere
  (no data surface at all, platform bypass notwithstanding), no entry into an org
  without membership, and the audited grant_org_admin recovery operation.
- REQ-1293: the tenant plane is isolated by SCHEMA; admin resolvers apply no row-level
  org_id filter, so rows seeded into an org schema that carry org_id='root' stay
  visible to that org's admin, while another org's rows never appear.
"""

import asyncio
import base64
import copy
import os
import uuid

import bcrypt
import psycopg2
import pytest
import yaml
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_PASSWORD = "e2e-pass"
_ORG2 = "acmeco"  # the self-created tenant org (REQ-1293)

# Seeded local accounts: username -> email. Emails drive the REQ-1284 rule checks.
_USERS = {
    "founder": "founder@acme.com",
    "alice": "alice@acme.com",
    "bob": "bob@rival.com",
    "carol": "carol@newco.com",
    "erin": "erin@rival.com",
    "dave": "dave@newco.com",
    "opsbot": "ops@acme.com",
}

# Cross-test state (tests run in file order and build on one another).
_ctx: dict = {}

# The data-plane capabilities the org_admin template role carries (schema.sql seed);
# platform_admin must hold NONE of them (REQ-1327).
_DATA_PLANE_CAPS = {
    "source_registration",
    "table_registration",
    "create_relationship",
    "create_view",
    "approve_view",
    "approve_relationship",
    "access_config",
    "user_management",
    "masking_config",
    "column_grant",
    "view_governance",
    "query_development",
    "full_results",
    "write",
    "usage",
}


def _basic(username: str) -> dict[str, str]:
    raw = f"{username}:{_PASSWORD}".encode()
    return {"Authorization": "Basic " + base64.b64encode(raw).decode()}


def _pg_conn():
    """Connect to the SAME Postgres the app's control planes use.

    Derived from PLATFORM_DATABASE_URL when set (the session conftest exports it for the
    isolated stack) so a port re-allocation can never point the test at a different
    server than the app."""
    url = os.environ.get("PLATFORM_DATABASE_URL")
    if url:
        from sqlalchemy.engine import make_url

        u = make_url(url)
        return psycopg2.connect(
            host=u.host or "localhost",
            port=u.port or 5432,
            dbname=u.database,
            user=u.username,
            password=u.password,
        )
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname="provisa",
        user="provisa",
        password="provisa",
    )


def _admin_schema(cur) -> str:
    """The schema the app's platform plane created its registry tables in."""
    cur.execute(
        "SELECT table_schema FROM information_schema.tables WHERE table_name = 'local_users'"
    )
    rows = [r[0] for r in cur.fetchall()]
    assert rows, "platform plane did not create local_users — app startup is broken"
    return rows[0]


def _seed_users() -> dict[str, str]:
    """Insert the module's local accounts into the REAL platform plane; return user ids."""
    ids: dict[str, str] = {}
    pw_hash = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt(rounds=4)).decode()
    with _pg_conn() as conn, conn.cursor() as cur:
        schema = _admin_schema(cur)
        # Idempotence against a rerun on a reused stack: this module owns these rows.
        cur.execute(f"DELETE FROM {schema}.local_users WHERE username = ANY(%s)", (list(_USERS),))
        cur.execute(f"DELETE FROM {schema}.superadmin_bootstrap")
        cur.execute(f"DELETE FROM {schema}.orgs WHERE id = %s", (_ORG2,))
        cur.execute(f"DROP SCHEMA IF EXISTS org_{_ORG2} CASCADE")
        for username, email in _USERS.items():
            uid = str(uuid.uuid4())
            ids[username] = uid
            cur.execute(
                f"INSERT INTO {schema}.local_users"
                " (id, username, password_hash, email, display_name, roles, attributes, is_active)"
                " VALUES (%s, %s, %s, %s, %s, '[]', '{}', TRUE)",
                (uid, username, pw_hash, email, username),
            )
    return ids


@pytest.fixture(scope="module")
async def client(tmp_path_factory):
    """The full app with auth enforced: basic provider + bootstrap claiming + multitenancy."""
    os.environ.setdefault("PG_PASSWORD", "provisa")

    base_cfg_path = os.environ["PROVISA_CONFIG"]
    with open(base_cfg_path) as f:
        cfg = yaml.safe_load(f)
    cfg = copy.deepcopy(cfg)
    cfg["multitenancy"] = True
    cfg["auth"] = {
        "provider": "basic",
        "assignments_source": "provisa",
        "bootstrap_superadmin": True,
    }
    cfg_path = tmp_path_factory.mktemp("auth-cfg") / "provisa-auth-e2e.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False))

    prev_config = os.environ["PROVISA_CONFIG"]
    os.environ["PROVISA_CONFIG"] = str(cfg_path)
    try:
        from provisa.api.app import create_app

        app = create_app()
        async with app.router.lifespan_context(app):
            _ctx["user_ids"] = _seed_users()
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                yield c
    finally:
        os.environ["PROVISA_CONFIG"] = prev_config
        _ctx.clear()


class TestBootstrapClaimIsExplicit:
    """REQ-1290: claiming the sole platform-admin slot is an explicit POST, never a side
    effect of authentication."""

    async def test_unauthenticated_claim_is_401(self, client):
        resp = await client.post("/auth/claim-bootstrap")
        assert resp.status_code == 401, resp.text

    async def test_authenticating_does_not_claim(self, client):
        before = await client.get("/auth/bootstrap-status")
        assert before.status_code == 200
        assert before.json() == {"unclaimed": True}

        # A fully authenticated request — the middleware validates founder's credential.
        me = await client.get("/auth/me", headers=_basic("founder"))
        assert me.status_code == 200, me.text
        assert me.json()["user_id"] == _ctx["user_ids"]["founder"]

        # The slot must STILL be unclaimed: authentication alone never claims it.
        after = await client.get("/auth/bootstrap-status")
        assert after.json() == {"unclaimed": True}

    async def test_explicit_claim_wins_and_seats_claimant(self, client):
        resp = await client.post("/auth/claim-bootstrap", headers=_basic("founder"))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["claimed"] is True
        assert body["claimed_by"] == _ctx["user_ids"]["founder"]
        assert body["org_id"]
        _ctx["org1"] = body["org_id"]

        status = await client.get("/auth/bootstrap-status")
        assert status.json() == {"unclaimed": False}

    async def test_losing_claim_reads_back_holder(self, client):
        resp = await client.post("/auth/claim-bootstrap", headers=_basic("alice"))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["claimed"] is False
        assert body["claimed_by"] == _ctx["user_ids"]["founder"]
        # The loser is granted nothing.
        me = await client.get("/auth/me", headers=_basic("alice"))
        assert me.json()["assignments"] == []


class TestOrgEmailRuleOnRedemption:
    """REQ-1572: the org's email-address rule does not gate invite redemption.

    The rule decides who may join on their own initiative. An invitation is an admin naming a
    person — that decision already made, single-use and expiring — so it admits an address the
    rule would refuse.
    """

    async def _invite(self, client) -> str:
        resp = await client.post(
            "/admin/invites/",
            json={"org_id": _ctx["org1"], "role_id": "analyst"},
            headers=_basic("founder"),
        )
        assert resp.status_code == 200, resp.text
        return resp.json()["token"]

    async def test_matching_email_joins(self, client):
        rule = await client.patch(
            f"/admin/orgs/{_ctx['org1']}/settings",
            json={"email_rule": "@acme\\.com$"},
            headers=_basic("founder"),
        )
        assert rule.status_code == 200, rule.text
        assert rule.json()["email_rule"] == "@acme\\.com$"

        token = await self._invite(client)
        resp = await client.post(
            "/auth/redeem-invite", json={"token": token}, headers=_basic("alice")
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {
            "user_id": _ctx["user_ids"]["alice"],
            "org_id": _ctx["org1"],
            "role_id": "analyst",
        }

    async def test_the_rule_reads_back_for_the_admin_who_set_it(self, client):
        """REQ-1569: a rule that decides who joins has to be readable by its owner, or nobody can
        audit or correct what the org is actually admitting."""
        read = await client.get(f"/admin/orgs/{_ctx['org1']}/settings", headers=_basic("founder"))
        assert read.status_code == 200, read.text
        assert read.json() == {
            "id": _ctx["org1"],
            "email_rule": "@acme\\.com$",
            "auto_join": False,
            "auto_join_role": None,
        }

    async def test_an_address_the_rule_would_refuse_joins_by_invitation(self, client):
        """bob@rival.com cannot satisfy `@acme.com$`, and is invited anyway — the contractor or
        auditor case. The invitation is the admission decision, so he joins."""
        token = await self._invite(client)
        resp = await client.post(
            "/auth/redeem-invite", json={"token": token}, headers=_basic("bob")
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {
            "user_id": _ctx["user_ids"]["bob"],
            "org_id": _ctx["org1"],
            "role_id": "analyst",
        }
        me = await client.get("/auth/me", headers=_basic("bob"))
        assert _ctx["org1"] in [m["org_id"] for m in me.json()["org_memberships"]]

    async def test_no_rule_accepts_any_email(self, client):
        cleared = await client.patch(
            f"/admin/orgs/{_ctx['org1']}/settings",
            json={"email_rule": None},
            headers=_basic("founder"),
        )
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["email_rule"] is None

        token = await self._invite(client)
        resp = await client.post(
            "/auth/redeem-invite",
            json={"token": token},
            headers=_basic("erin"),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["user_id"] == _ctx["user_ids"]["erin"]


class TestOnlyAssignedRoleRidesHeader:
    """REQ-1295: with auth enforced, X-Provisa-Role is honored only for an ASSIGNED role."""

    async def test_assigned_role_is_honored(self, client):
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__customers { id name } }"},
            headers={**_basic("alice"), "X-Provisa-Role": "analyst"},
        )
        assert resp.status_code == 200, resp.text
        rows = resp.json()["data"]["sa__customers"]
        assert len(rows) > 0

    async def test_unassigned_role_is_403(self, client):
        # org_admin EXISTS but alice is not assigned it — the server must refuse to act as it.
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sa__customers { id name } }"},
            headers={**_basic("alice"), "X-Provisa-Role": "org_admin"},
        )
        assert resp.status_code == 403, resp.text
        assert "is not assigned to this user" in resp.json()["detail"]


class TestPlatformAdminHasZeroDataPlane:
    """REQ-1327: platform_admin is a purely control-plane role — no data surface anywhere,
    and the platform bypass exempts no data-plane capability check."""

    async def test_grant_platform_admin_only(self, client):
        org1, ids = _ctx["org1"], _ctx["user_ids"]
        member = await client.post(
            f"/admin/orgs/{org1}/members",
            json={"user_id": ids["opsbot"]},
            headers=_basic("founder"),
        )
        assert member.status_code == 200, member.text
        assign = await client.post(
            f"/admin/users/{ids['opsbot']}/assignments",
            json={"role_id": "platform_admin", "domain_id": "*"},
            headers=_basic("founder"),
        )
        assert assign.status_code == 200, assign.text

    async def test_role_carries_no_data_capability(self, client):
        resp = await client.get("/admin/roles/", headers=_basic("founder"))
        assert resp.status_code == 200, resp.text
        roles = {r["id"]: r for r in resp.json()}
        caps = set(roles["platform_admin"]["capabilities"])
        assert "cross_org" in caps
        assert not caps & _DATA_PLANE_CAPS, f"platform_admin holds data caps: {caps}"

    async def test_control_plane_works_but_data_surfaces_refuse(self, client):
        # Control plane: the org registry answers a platform_admin.
        orgs = await client.get("/admin/orgs/", headers=_basic("opsbot"))
        assert orgs.status_code == 200, orgs.text
        assert any(o["id"] == _ctx["org1"] for o in orgs.json())

        # Data plane, GraphQL: refused outright — no schema exists for the role, even
        # though the role holds the admin/superadmin platform-bypass capabilities.
        gql = await client.post(
            "/data/graphql",
            json={"query": "{ sa__customers { id } }"},
            headers=_basic("opsbot"),
        )
        assert gql.status_code == 400, gql.text
        body = gql.json()
        assert body["code"] == "data.no_schema_available_for_role"
        assert body["params"] == {"role_id": "platform_admin"}

        # Data plane, JSON:API: the same refusal.
        japi = await client.get(
            "/data/jsonapi/sales-analytics/customers",
            headers={**_basic("opsbot"), "Accept": "application/vnd.api+json"},
        )
        assert japi.status_code == 400, japi.text
        assert "No schema available" in japi.text


class TestSchemaIsolatedTenantPlane:
    """REQ-1293 (+ the REQ-1327 membership gate and audited recovery grant, which need a
    second org): the tenant plane is isolated by schema; admin resolvers apply no
    row-level org_id filter."""

    async def test_self_service_org_provisions_ready(self, client):
        created = await client.post(
            "/admin/orgs/",
            json={"id": _ORG2, "name": "Acme Co", "include_demo": False},
            headers=_basic("carol"),
        )
        assert created.status_code == 200, created.text
        assert created.json()["provisioning_state"] == "provisioning"

        deadline = asyncio.get_event_loop().time() + 300
        while True:
            status = await client.get(f"/admin/orgs/{_ORG2}/status", headers=_basic("carol"))
            assert status.status_code == 200, status.text
            record = status.json()
            if record["provisioning_state"] != "provisioning":
                break
            assert asyncio.get_event_loop().time() < deadline, "org never left provisioning"
            await asyncio.sleep(1)
        assert record["provisioning_state"] == "ready", record

    async def test_seeded_rows_with_foreign_org_id_stay_visible(self, client):
        # The org schema's seeded domains carry no org_id of their own; stamp them with
        # 'root' — the exact historical seed state REQ-1293 exists for — and the admin
        # resolver must STILL return them: the schema is the boundary, not the column.
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT id, org_id FROM org_{_ORG2}.domains WHERE id <> ''")
            seeded = dict(cur.fetchall())
            assert "meta" in seeded and "ops" in seeded, seeded
            cur.execute(f"UPDATE org_{_ORG2}.domains SET org_id = 'root'")

        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ domains { id } }"},
            headers=_basic("carol"),
        )
        assert resp.status_code == 200, resp.text
        payload = resp.json()
        assert not payload.get("errors"), payload
        ids = {d["id"] for d in payload["data"]["domains"]}
        assert {"meta", "ops"} <= ids, f"seeded domains vanished behind a row filter: {ids}"

    async def test_isolation_is_the_schema_not_a_row_filter(self, client):
        # A domain created in carol's org must never surface in the default org.
        created = await client.post(
            "/admin/graphql",
            json={
                "query": 'mutation { createDomain(input: {id: "acmeprivate", '
                'description: "Acme-only"}) { success message } }'
            },
            headers=_basic("carol"),
        )
        assert created.status_code == 200, created.text
        result = created.json()["data"]["createDomain"]
        assert result["success"] is True, result

        mine = await client.post(
            "/admin/graphql",
            json={"query": "{ domains { id } }"},
            headers=_basic("carol"),
        )
        assert "acmeprivate" in {d["id"] for d in mine.json()["data"]["domains"]}

        theirs = await client.post(
            "/admin/graphql",
            json={"query": "{ domains { id } }"},
            headers=_basic("founder"),
        )
        assert theirs.status_code == 200, theirs.text
        founder_ids = {d["id"] for d in theirs.json()["data"]["domains"]}
        assert "acmeprivate" not in founder_ids

    async def test_platform_admin_cannot_enter_org_without_membership(self, client):
        # REQ-1327: membership is the only way into an org — no platform-admin escape.
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ domains { id } }"},
            headers={**_basic("opsbot"), "host": f"{_ORG2}.provisa.test"},
        )
        assert resp.status_code == 403, resp.text
        assert f"Not a member of org '{_ORG2}'" in resp.json()["detail"]

    async def test_recovery_runs_through_audited_grant(self, client):
        # REQ-1327: the audited grant_org_admin operation is the named recovery path.
        org2, ids = _ORG2, _ctx["user_ids"]
        added = await client.post(
            f"/admin/orgs/{org2}/members",
            json={"user_id": ids["dave"]},
            headers=_basic("carol"),
        )
        assert added.status_code == 200, added.text

        granted = await client.post(
            f"/admin/orgs/{org2}/admins/{ids['dave']}", headers=_basic("opsbot")
        )
        assert granted.status_code == 200, granted.text
        assert granted.json()["role_id"] == "org_admin"

        # The intervention is written into THAT org's audit trail.
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT actor_id, subject_id FROM org_{org2}.admin_audit_log"
                " WHERE action = 'grant_org_admin'"
            )
            rows = cur.fetchall()
        assert (ids["opsbot"], ids["dave"]) in rows

        # And it is revocable through the same operation.
        revoked = await client.delete(
            f"/admin/orgs/{org2}/admins/{ids['dave']}", headers=_basic("opsbot")
        )
        assert revoked.status_code == 200, revoked.text
