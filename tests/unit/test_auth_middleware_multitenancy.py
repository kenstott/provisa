# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
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
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
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
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
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
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]})
    admin = _Pool(
        rows_by_table={"user_org_memberships": [{"org_id": "acme"}, {"org_id": "beta"}]}
    )
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app).get("/test", headers=_auth("u1"))
    assert resp.status_code == 401
    assert "Org selection" in resp.json()["detail"]


def test_member_less_user_allowed_on_platform_plane():
    # Just-authenticated invitee with no membership yet: /auth/* must not 401; active org None.
    db = _Pool(rows_by_table={"user_role_assignments": []})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app).get("/auth/me", headers=_auth("newbie"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] is None


def test_member_less_user_blocked_on_tenant_plane():
    db = _Pool(rows_by_table={"user_role_assignments": []})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app).get("/test", headers=_auth("newbie"))
    assert resp.status_code == 401


# --- REQ-1266 bootstrap fall-through under multitenancy ------------------------


def test_bootstrap_claimant_is_granted_platform_admin():
    admin = _Pool(rows_by_table={"user_org_memberships": []}, claimant="first")
    app = _make_app(
        bootstrap_superadmin=True, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app).get("/test", headers=_auth("first"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["platform_admin"]  # REQ-1297


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
    app = _make_app(
        bootstrap_superadmin=True, admin_pool=admin, multitenancy=False
    )
    resp = TestClient(app).get("/test", headers=_auth("second"))
    assert resp.status_code == 403
    assert "single administrator" in resp.json()["detail"]


def test_bootstrap_second_user_falls_through_when_multitenant():
    # Not the claimant → not 403; resolves via DB assignments + redeem-invite flow.
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]}, claimant="first")
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]})
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
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app, base_url="http://acme.provisa.org").get("/test", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "acme"


def test_apex_host_resolves_to_none():
    # REQ-1276: provisa.org → no org (None)
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app, base_url="http://provisa.org").get("/auth/me", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] is None


def test_localhost_resolves_to_none():
    # REQ-1276: localhost → no org (None)
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app, base_url="http://localhost:3000").get("/auth/me", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] is None


def test_control_plane_host_cloud_uses_x_org_provisa_header():
    # REQ-1276: cloud.provisa.dev requires x-org-provisa header for org
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app, base_url="http://cloud.provisa.dev").get(
        "/test",
        headers={**_auth("u1"), "x-org-provisa": "acme"},
    )
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "acme"


def test_control_plane_host_cloud_without_header_resolves_to_none():
    # REQ-1276: cloud.provisa.dev with no header → None (platform-plane auth allowed)
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app, base_url="http://cloud.provisa.dev").get("/auth/me", headers=_auth("u1"))
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] is None


def test_subdomain_non_member_rejected_req1276():
    # REQ-1276: user not a member of org from subdomain → 403
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "analyst", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
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
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app, base_url="http://cloud.provisa.dev").get(
        "/test",
        headers={**_auth("u1"), "x-org-provisa": "evil"},
    )
    assert resp.status_code == 403
    assert "evil" in resp.json()["detail"]


def test_platform_admin_with_subdomain_org_allowed_req1276():
    # REQ-1276: platform admin acts in any org via subdomain
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "platform_admin", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app, base_url="http://anyorg.provisa.org").get(
        "/test",
        headers=_auth("root"),
    )
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "anyorg"
