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
membership enforcement, and the REQ-1266 bootstrap fall-through under multitenancy."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity, AuthProvider


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


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = [_Row(m) for m in rows]

    def fetchall(self):
        return self._rows


class _Conn:
    """Fake pooled connection. execute_core returns preconfigured rows keyed by the
    queried table name; upsert_returning returns the bootstrap claimant."""

    def __init__(self, pool: "_Pool") -> None:
        self._pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute_core(self, stmt):
        table = stmt.get_final_froms()[0].name
        return _Result(self._pool.rows_by_table.get(table, []))

    async def upsert(self, *a, **k):
        return None

    async def upsert_returning(self, *a, **k):
        return self._pool.claimant


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


# --- X-Org-Id membership enforcement -------------------------------------------


def test_x_org_id_non_member_rejected():
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": [{"org_id": "acme"}]})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app).get("/test", headers={**_auth("u1"), "X-Org-Id": "evilcorp"})
    assert resp.status_code == 403
    assert "evilcorp" in resp.json()["detail"]


def test_x_org_id_member_honored():
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "org_admin", "domain_id": "*"}]})
    admin = _Pool(
        rows_by_table={"user_org_memberships": [{"org_id": "acme"}, {"org_id": "beta"}]}
    )
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app).get("/test", headers={**_auth("u1"), "X-Org-Id": "beta"})
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "beta"


def test_platform_admin_bypasses_membership_for_any_org():
    # A global admin may act in any org even with zero membership rows.
    db = _Pool(rows_by_table={"user_role_assignments": [{"role_id": "admin", "domain_id": "*"}]})
    admin = _Pool(rows_by_table={"user_org_memberships": []})
    app = _make_app(
        assignments_source="provisa", db_pool=db, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app).get("/test", headers={**_auth("root"), "X-Org-Id": "anyorg"})
    assert resp.status_code == 200
    assert resp.json()["active_org_id"] == "anyorg"


def test_no_org_selection_required_on_tenant_path():
    # Member of two orgs, no X-Org-Id, non-platform path → must choose.
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


def test_bootstrap_first_user_claims_superadmin():
    admin = _Pool(rows_by_table={"user_org_memberships": []}, claimant="first")
    app = _make_app(
        bootstrap_superadmin=True, admin_pool=admin, multitenancy=True
    )
    resp = TestClient(app).get("/test", headers=_auth("first"))
    assert resp.status_code == 200
    assert resp.json()["roles"] == ["admin"]


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
