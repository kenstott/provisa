# Copyright (c) 2026 Kenneth Stott
# Canary: 3fc1caa2-e863-4f77-81ef-1999d858ea19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""AuthMiddleware multitenancy paths: assignment→identity.roles mirroring, X-Org-Id
membership enforcement, the REQ-1266 bootstrap fall-through under multitenancy,
and REQ-1276 Host subdomain-based org resolution."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider


@pytest.fixture(autouse=True)
def _stub_org_runtime(monkeypatch):
    """REQ-1266: for a non-default org member the middleware re-reads assignments from that org's
    schema, pre-building its data-plane runtime via ensure_org_runtime. That build is an integration
    concern (needs the real admin plane); here the fake db_pool ignores the bound org, so stub it."""

    async def _noop(_org_id: str):
        return None

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _noop, raising=False)


@pytest.fixture(autouse=True)
def _seeded_roles(monkeypatch):
    """REQ-1337: the middleware decides nothing from a role NAME — it resolves the assigned role ids
    to the RIGHTS those roles carry. That resolution reads the loaded roles registry, which in a real
    process comes from the schema.sql seed; these tests never build app state, so mirror the seed
    here. ``cross_org`` is what makes platform_admin control-plane."""
    monkeypatch.setattr(
        "provisa.auth.middleware._loaded_roles",
        lambda: {
            "platform_admin": {
                "id": "platform_admin",
                "capabilities": ["admin", "superadmin", "platform_settings", "cross_org"],
            },
            "org_admin": {"id": "org_admin", "capabilities": ["user_management"]},
            "developer": {"id": "developer", "capabilities": ["query_development"]},
            "analyst": {"id": "analyst", "capabilities": ["usage"]},
        },
    )


class _Provider(AuthProvider):
    """Accepts 'tok:<user_id>' → identity for that user; rejects anything else."""

    provider_name = "firebase"

    async def validate_token(self, token: str) -> AuthIdentity:
        if token.startswith("tok:"):
            uid = token[len("tok:") :]
            return AuthIdentity(
                user_id=uid, email=f"{uid}@x.io", display_name=uid, roles=[], raw_claims={}
            )
        raise ValueError("Invalid token")


class _Row:
    def __init__(self, mapping: dict) -> None:
        self._mapping = mapping

    def __getitem__(self, idx: int):
        return list(self._mapping.values())[idx]


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = [_Row(m) for m in rows]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    """Fake pooled connection. execute_core returns preconfigured rows keyed by the
    queried table name; the superadmin_bootstrap row is whoever already holds the slot."""

    def __init__(self, pool: "_Pool") -> None:
        self._pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute_core(self, stmt):
        table = stmt.get_final_froms()[0].name
        # REQ-1290: the middleware READS this table and never writes it, so the claimant is just
        # another preconfigured row — an unclaimed deployment is the empty list.
        if table == "superadmin_bootstrap":
            claimant = self._pool.claimant
            return _Result([{"user_id": claimant}] if claimant is not None else [])
        return _Result(self._pool.rows_by_table.get(table, []))

    async def upsert(self, *a, **k):
        return None

    async def upsert_returning(self, *a, **k):
        raise AssertionError(
            "REQ-1290: the middleware must never claim the platform-admin slot — claiming is an "
            "explicit POST /auth/claim-bootstrap from the first-login page"
        )


class _Pool:
    def __init__(self, rows_by_table=None, claimant=None) -> None:
        self.rows_by_table = rows_by_table or {}
        self.claimant = claimant

    def acquire(self):
        return _Conn(self)


def _make_app(**kw):
    app = FastAPI()
    app.add_middleware(AuthMiddleware, provider=_Provider(), **kw)

    @app.get("/test")
    async def _test(request: Request):
        return {
            "user_id": request.state.identity.user_id,
            "roles": request.state.identity.roles,
            "role": request.state.role,
            "active_org_id": request.state.active_org_id,
        }

    @app.get("/auth/me")
    async def _me(request: Request):
        return {
            "roles": request.state.identity.roles,
            "active_org_id": request.state.active_org_id,
        }

    return app


def _auth(uid: str) -> dict:
    return {"Authorization": f"Bearer tok:{uid}"}


# --- assignment → identity.roles mirroring (provisa mode) ----------------------


def test_provisa_db_assignments_mirror_into_identity_roles():
    # A Firebase token carries no roles claim; the DB row must surface to identity.roles
    # so the capability layer sees the granted org role.
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    client = TestClient(app)
    resp = client.get("/test", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["org_admin"]
    assert resp.json()["active_org_id"] == "acme"


def test_domain_scoped_assignment_renders_colon_claim():
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "analyst", "domain_id": "sales"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app).get("/test", headers=_auth("u1"))
    assert resp.json()["roles"] == ["analyst:sales"]


def test_empty_db_rows_fall_back_to_default_assignments():
    db = _Pool(rows_by_table={"user_role_assignments": []})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_assignments=[{"role_id": "viewer", "domain_id": "*"}],
    )
    resp = TestClient(app).get("/test", headers=_auth("u1"))
    assert resp.json()["roles"] == ["viewer"]


def test_no_org_selection_required_on_tenant_path():
    # Member of two orgs, no subdomain org, non-platform path → must choose.
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}, {"org_id": "beta"}]})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app).get("/test", headers=_auth("u1"))
    assert resp.status_code == 401
    assert "Org selection" in resp.json()["detail"]


def test_member_less_user_allowed_on_platform_plane():
    # Just-authenticated invitee with no membership yet: /auth/* must not 401; active org None.
    db = _Pool(rows_by_table={"user_role_assignments": []})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app).get("/auth/me", headers=_auth("newbie"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] is None


def test_member_less_user_blocked_on_tenant_plane():
    db = _Pool(rows_by_table={"user_role_assignments": []})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app).get("/test", headers=_auth("newbie"))
    assert resp.status_code == 401


# --- REQ-1266 bootstrap fall-through under multitenancy ------------------------


def test_bootstrap_claimant_is_granted_platform_admin():
    admin = _Pool(rows_by_table={"user_org_memberships": []}, claimant="first")
    # A tenant pool is bound on every real request — REQ-1439 writes the user_directory mirror
    # through it on the same sign-in that upserts the platform profile.
    db = _Pool(rows_by_table={"user_role_assignments": []})
    app = _make_app(bootstrap_superadmin=True, db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app).get("/test", headers=_auth("first"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["platform_admin"]  # REQ-1297


# --- REQ-1338: the claimant keeps the org_admin seat the claim writes ----------------
#
# _seat_claimant_in_root (api/auth_router.py) grants the claimant BOTH platform_admin and org_admin
# in the bootstrap org. The middleware used to short-circuit the claimant with a hard-coded
# [platform_admin] and return, so that org_admin seat was never read: every data request from the
# deployment's own administrator resolved the control-plane role, and REQ-1327 builds no data schema
# for a cross_org role — /data/graphql answered 400 "No schema available for role 'platform_admin'".


def _claimant_app(assignment_rows, **kw):
    db = _Pool(rows_by_table={"user_role_assignments": assignment_rows})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "root"}]}, claimant="first")
    return _make_app(
        bootstrap_superadmin=True,
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_org_id="root",
        **kw,
    )


_SEATED = [
    {"role_id": "platform_admin", "domain_id": "*"},
    {"role_id": "org_admin", "domain_id": "*"},
]


def test_bootstrap_claimant_keeps_the_org_admin_seat_granted_by_the_claim():
    resp = TestClient(_claimant_app(_SEATED)).get("/test", headers=_auth("first"))
    assert resp.status_code == 200
    assert sorted(resp.json()["roles"]) == ["org_admin", "platform_admin"]
    assert resp.json()["active_org_id"] == "root"


def test_the_acting_role_is_one_the_caller_holds_not_the_configured_default():
    # REQ-1338: default_role answers from deployment config, not from this user. With no header the
    # claimant acted as "analyst" — a role nobody granted them — instead of their org_admin seat.
    resp = TestClient(_claimant_app(_SEATED, default_role="analyst")).get(
        "/test", headers=_auth("first")
    )
    assert resp.status_code == 200
    assert resp.json()["role"] == "org_admin"


def test_an_ordinary_user_also_acts_as_an_assigned_role_not_the_default():
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "developer", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_role="analyst",
    )
    resp = TestClient(app).get("/test", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["role"] == "developer"


def test_bootstrap_claimant_acts_as_a_data_plane_role_not_platform_admin():
    # The acting role is what data/endpoint.py resolves the schema by. platform_admin here is the
    # 400 the user saw.
    resp = TestClient(_claimant_app(_SEATED, default_role="platform_admin")).get(
        "/test", headers=_auth("first")
    )
    assert resp.status_code == 200
    assert resp.json()["role"] == "org_admin"


def test_bootstrap_claimant_asking_for_platform_admin_gets_their_data_role():
    # REQ-1327 at the header: the UI may send platform_admin (it is a genuinely assigned role); the
    # data surfaces must still act as the caller's data-plane role rather than refuse the request.
    resp = TestClient(_claimant_app(_SEATED)).get(
        "/test", headers={**_auth("first"), "X-Provisa-Role": "platform_admin"}
    )
    assert resp.status_code == 200
    assert resp.json()["role"] == "org_admin"


def test_bootstrap_claimant_may_still_name_a_specific_data_role():
    resp = TestClient(_claimant_app([*_SEATED, {"role_id": "analyst", "domain_id": "*"}])).get(
        "/test", headers={**_auth("first"), "X-Provisa-Role": "analyst"}
    )
    assert resp.status_code == 200
    assert resp.json()["role"] == "analyst"


def test_holding_the_bootstrap_slot_grants_platform_admin_even_with_no_tenant_row():
    # The slot IS the grant (REQ-1297): on a deployment whose tenant plane holds nothing for the
    # claimant, the control plane must still be reachable.
    resp = TestClient(_claimant_app([])).get("/test", headers=_auth("first"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["platform_admin"]


def test_a_non_claimant_is_not_handed_platform_admin():
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]}
    )
    admin = _Pool(
        rows_by_table={"user_org_memberships": [{"org_id": "root"}]}, claimant="somebody-else"
    )
    app = _make_app(
        bootstrap_superadmin=True,
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_org_id="root",
    )
    resp = TestClient(app).get("/test", headers=_auth("second"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["org_admin"]


def test_an_unclaimed_slot_is_not_taken_by_merely_authenticating():
    # REQ-1290: this is the defect that reached production. Claiming on any authenticated request
    # meant a browser holding a still-valid token took platform admin on a page refresh, before the
    # first-login disclosure (REQ-1288) could render. _Conn.upsert_returning now fails the test if
    # the middleware writes the slot at all.
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]}, claimant=None)
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "analyst", "domain_id": "*"}]})
    app = _make_app(
        bootstrap_superadmin=True,
        admin_pool=admin,
        db_pool=db,
        assignments_source="provisa",
        multitenancy=True,
    )
    resp = TestClient(app).get("/test", headers=_auth("passer-by"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["analyst"], "authenticating must not confer admin"


def test_bootstrap_second_user_403_when_single_tenant():
    admin = _Pool(rows_by_table={"user_org_memberships": []}, claimant="first")
    app = _make_app(bootstrap_superadmin=True, admin_pool=admin, multitenancy=False)
    resp = TestClient(app).get("/test", headers=_auth("second"))
    assert resp.status_code == 403
    assert "single administrator" in resp.json()["detail"]


def test_bootstrap_second_user_falls_through_when_multitenant():
    # Not the claimant → not 403; resolves via DB assignments + redeem-invite flow.
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]}, claimant="first")
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]}
    )
    app = _make_app(
        bootstrap_superadmin=True,
        admin_pool=admin,
        db_pool=db,
        assignments_source="provisa",
        multitenancy=True,
    )
    resp = TestClient(app).get("/test", headers=_auth("second"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["org_admin"]
    assert resp.json()["active_org_id"] == "acme"


# --- REQ-1276 Host subdomain-based org resolution ------


def test_subdomain_host_resolves_org():
    # REQ-1276: acme.provisa.org → org "acme"
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "analyst", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app, base_url="http://acme.provisa.org").get("/test", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "acme"


def test_apex_host_resolves_to_none():
    # REQ-1276: provisa.org → no org (None)
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app, base_url="http://provisa.org").get("/auth/me", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] is None


def test_localhost_resolves_to_none():
    # REQ-1276: localhost → no org (None)
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app, base_url="http://localhost:3000").get("/auth/me", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] is None


def test_control_plane_host_cloud_uses_x_org_provisa_header():
    # REQ-1276: cloud.provisa.dev requires x-org-provisa header for org
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app, base_url="http://cloud.provisa.dev").get(
        "/test",
        headers={**_auth("u1"), "x-org-provisa": "acme"},
    )
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "acme"


def test_control_plane_host_cloud_without_header_resolves_to_none():
    # REQ-1276: cloud.provisa.dev with no header → None (platform-plane auth allowed)
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app, base_url="http://cloud.provisa.dev").get("/auth/me", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] is None


def test_subdomain_non_member_rejected_req1276():
    # REQ-1276: user not a member of org from subdomain → 403
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "analyst", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app, base_url="http://evil.provisa.org").get(
        "/test",
        headers=_auth("u1"),
    )
    assert resp.status_code == 403
    assert "evil" in resp.json()["detail"]


def test_control_plane_host_with_non_member_org_header_rejected():
    # REQ-1276: x-org-provisa header with non-member org → 403
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "analyst", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app, base_url="http://cloud.provisa.dev").get(
        "/test",
        headers={**_auth("u1"), "x-org-provisa": "evil"},
    )
    assert resp.status_code == 403
    assert "evil" in resp.json()["detail"]


def test_platform_admin_subdomain_nonmember_org_denied_req1327():
    # REQ-1327: membership is the ONLY way into an org — the platform_admin role is control-plane
    # and confers no tenant binding. Naming a non-member org via subdomain is rejected, same as
    # for any other identity (the audited REQ-1303 recovery grant is the sanctioned way in).
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True)
    resp = TestClient(app, base_url="http://anyorg.provisa.org").get(
        "/test",
        headers=_auth("root"),
    )
    assert resp.status_code == 403
    assert "anyorg" in resp.json()["detail"]


# --- REQ-1318: a session must be usable on EVERY plane, not just /auth/me ------
#
# The class of defect these cover: the client decides "am I signed in?" from one endpoint and then
# issues requests to another. When the two planes disagree — /auth/me says platform_admin, the data
# plane says 401 — the UI reads the 401 as a dead credential, drops the token and forces a fresh
# sign-in. Every case below asserts plane AGREEMENT for one identity, which is the property that was
# actually broken; asserting only the happy endpoint is what let it ship.


def test_platform_admin_without_memberships_is_usable_on_tenant_plane():
    # REQ-1318: the platform operator is not a tenant, so they hold zero org memberships. They named
    # no org (apex host, no header) and matched no membership, so they fell to the tenant-path 401 —
    # /auth/me returned 200 with platform_admin while /admin/graphql 401'd on the same token.
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_org_id="root",
    )
    client = TestClient(app)
    assert client.get("/auth/me", headers=_auth("root")).status_code == 200
    tenant = client.get("/test", headers=_auth("root"))
    assert tenant.status_code == 200, "platform admin 401'd on the tenant plane"
    assert tenant.json()["active_org_id"] == "root", "with no org named, acts in the default org"


def test_bootstrap_platform_admin_is_usable_on_tenant_plane_immediately():
    # REQ-1318: the same agreement for the identity that just claimed the bootstrap slot. This is the
    # reported symptom — claim platform_admin, get no access, sign out and back in, and it works. The
    # claimant has no membership either, so it took the same 401 branch.
    admin = _Pool(rows_by_table={"user_org_memberships": []}, claimant="first")
    db = _Pool(rows_by_table={"user_role_assignments": []})
    app = _make_app(
        bootstrap_superadmin=True,
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_org_id="root",
    )
    client = TestClient(app)
    assert client.get("/auth/me", headers=_auth("first")).status_code == 200
    tenant = client.get("/test", headers=_auth("first"))
    assert tenant.status_code == 200
    assert tenant.json()["roles"] == ["platform_admin"]


def test_platform_admin_named_org_requires_membership_req1327():
    # REQ-1327: naming a tenant org selects it ONLY for members. A platform admin who was granted
    # membership + a role in that org (the audited REQ-1303 path) binds it and resolves the role
    # THAT org assigned — the platform set never carries across.
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_org_id="root",
    )
    resp = TestClient(app, base_url="http://cloud.provisa.dev").get(
        "/test", headers={**_auth("root"), "x-org-provisa": "acme"}
    )
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "acme"


def test_non_admin_member_less_user_is_still_blocked_on_the_tenant_plane():
    # The platform-admin branch must not widen into "anyone with no memberships gets the default
    # org" — that would hand every just-authenticated stranger the default org's data.
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "analyst", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_org_id="root",
    )
    assert TestClient(app).get("/test", headers=_auth("stranger")).status_code == 401


# --- REQ-1297 platform_admin is actively ignored in a tenant org ----------------


def test_tenant_org_assignment_naming_platform_admin_is_stripped():
    # The tenant schema CAN hold an assignment naming platform_admin — an org_admin can grant it via
    # user_management, and a config load carried it in. In a tenant org it must resolve to nothing:
    # not in identity.roles, so no capability, and /auth/me never reports it there.
    db = _Pool(
        rows_by_table={
            "user_role_assignments": [
                {"role_id": "platform_admin", "domain_id": "*"},
                {"role_id": "analyst", "domain_id": "*"},
            ]
        }
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_org_id="root",
    )
    resp = TestClient(app).get("/test", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["analyst"]
    assert resp.json()["active_org_id"] == "acme"


def test_platform_admin_only_assignment_in_tenant_org_is_refused():
    # Nothing remains once platform_admin is dropped, so there is no role to act as. Refusing is the
    # design: silently acting as platform_admin would be exactly the tenant-data access REQ-1297
    # forbids, and inventing a data role would be a fallback.
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_role="platform_admin",
        default_org_id="root",
    )
    resp = TestClient(app).get("/test", headers=_auth("u1"))
    assert resp.status_code == 403
    assert "confers no rights" in resp.json()["detail"]


def test_platform_admin_survives_in_the_root_org():
    # Root IS the control plane's org, so the strip must not reach it — otherwise the deployment's
    # own administrator loses the platform surfaces.
    db = _Pool(
        rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]}
    )
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "root"}]})
    app = _make_app(
        assignments_source="provisa",
        db_pool=db,
        admin_pool=admin,
        multitenancy=True,
        default_org_id="root",
    )
    resp = TestClient(app).get("/test", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["platform_admin"]
    assert resp.json()["active_org_id"] == "root"
