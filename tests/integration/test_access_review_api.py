# Copyright (c) 2026 Kenneth Stott
# Canary: 8f4b2e17-6c39-4a05-b8d1-2e7a9c5f0d63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Access reviews over HTTP against a live control plane (REQ-2266).

An org administrator takes a review; the owner of the sales domain (the ``domain_steward``
entitlement over ``sales``) sees only the sales rows, revokes one with a reason, and the member's
next request no longer holds the role; the decision is on the row and in the org's trail; the
export carries it; the schedule is set and refused when it is not a cron expression. DDL and row
checks run on a synchronous engine, as in test_org_lifecycle, so the async control-plane engines
are driven only inside the TestClient's event loop.
"""

# Requirements: REQ-2266

from __future__ import annotations

import csv
import io
import os
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text

from provisa.api.admin.access_review_router import router as access_review_router
from provisa.api.admin.local_users_router import router as users_router
from provisa.api.errors import ApiError
from provisa.api.org_runtime import ActiveOrgPool
from provisa.auth.middleware import AuthMiddleware
from provisa.core import pgvector
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import orgs, user_org_memberships
from provisa.core.schema_org import (
    admin_audit_log,
    domains,
    entitlements,
    query_audit_log,
    roles,
    user_role_assignments,
)
from provisa.core.schema_org import metadata as org_metadata
from provisa.entitlements.lists import HeldEntitlements, HeldList
from tests.integration.test_auth_integration import _FirebaseLikeProvider
from tests.served_config import served_config

pytestmark = [pytest.mark.integration, pytest.mark.deployment_org("acme")]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_URL = f"postgresql+psycopg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_accrev_admin"
_ORG_SCHEMA = "test_accrev_acme"

_ROLE_CAPS: dict[str, list[str]] = {
    "org_admin": ["user_management", "org_settings"],
    "sales_steward": ["domain_steward"],
    "analyst": ["usage", "query_development"],
}
# alice administers acme; sam owns the sales domain; bob and hal are members.
_TOKENS = {"tok-alice": "alice", "tok-sam": "sam", "tok-bob": "bob", "tok-hal": "hal"}
_STEWARD_ROW = HeldList("sales_steward", "domains", ("sales",), capability="domain_steward")


def _prepare():
    engine = create_engine(_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        for schema in (_ADMIN_SCHEMA, _ORG_SCHEMA):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.execute(text(f"CREATE SCHEMA {schema}"))
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id="acme", name="Acme", created_by="alice"))
        for user_id in ("alice", "sam", "bob", "hal"):
            conn.execute(insert(user_org_memberships).values(user_id=user_id, org_id="acme"))

        conn.execute(text(f'CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA "{pgvector.SCHEMA}"'))
        conn.execute(text(f"SET search_path TO {_ORG_SCHEMA}"))
        org_metadata.create_all(
            conn, tables=[t for t in org_metadata.sorted_tables if t is not query_audit_log]
        )
        for domain_id in ("sales", "hr"):
            conn.execute(insert(domains).values(id=domain_id))
        for role_id, caps in _ROLE_CAPS.items():
            conn.execute(
                insert(roles).values(id=role_id, capabilities=caps, description="Test role.")
            )
        for user_id, role_id, domain_id in (
            ("alice", "org_admin", "*"),
            ("sam", "sales_steward", "*"),
            ("bob", "analyst", "sales"),
            ("hal", "analyst", "hr"),
        ):
            conn.execute(
                insert(user_role_assignments).values(
                    user_id=user_id, role_id=role_id, domain_id=domain_id
                )
            )
        conn.execute(
            insert(entitlements).values(
                source="provisa",
                principal="sam",
                role_id="sales_steward",
                kind="domains",
                capability="domain_steward",
                identifiers=["sales"],
                set_by="alice",
                reason="owns sales",
            )
        )
    return engine


@pytest.fixture
def plane(monkeypatch):
    sync_engine = _prepare()
    admin_db = Database(create_engine_from_url(_URL), name="admin", search_path=_ADMIN_SCHEMA)
    org_db = Database(create_engine_from_url(_URL), name="acme", search_path=_ORG_SCHEMA)

    from provisa.api.app import state as app_state
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "config", served_config(), raising=False)
    monkeypatch.setattr("provisa.core.control_plane_id.current", lambda: "cp-test")
    registry = OrgRegistry()
    # What the runtime build loads from the entitlement store (store.load_held): sam's steward row.
    registry.set(
        "acme",
        OrgRuntime(
            org_id="acme",
            model_db=org_db,
            tenant_db=org_db,
            record_db=org_db,
            held_entitlements={"sam": HeldEntitlements(lists=(_STEWARD_ROW,))},
        ),
    )
    monkeypatch.setattr(app_state, "org_registry", registry, raising=False)
    monkeypatch.setattr(
        app_state,
        "roles",
        {rid: {"id": rid, "capabilities": caps} for rid, caps in _ROLE_CAPS.items()},
        raising=False,
    )

    async def _org_runtime(org_id: str, env: str | None = None):
        return registry.get(org_id) or SimpleNamespace(
            model_db=None, tenant_db=None, record_db=None
        )

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _org_runtime, raising=False)

    yield SimpleNamespace(admin_db=admin_db, org_db=org_db, sync=sync_engine)

    with sync_engine.begin() as conn:
        for schema in (_ADMIN_SCHEMA, _ORG_SCHEMA):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    sync_engine.dispose()


def _app(plane) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider(_TOKENS),
        admin_pool=plane.admin_db,
        db_pool=ActiveOrgPool(),
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id="acme",
    )
    app.include_router(access_review_router)
    app.include_router(users_router)

    @app.exception_handler(ApiError)
    async def _api_error(_request, exc: ApiError):  # pyright: ignore[reportUnusedFunction]
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail, "code": exc.code, "params": exc.params},
            headers=exc.headers,
        )

    return app


def _as(user: str) -> dict[str, str]:
    return {"Authorization": f"Bearer tok-{user}", "x-org-provisa": "acme"}


def _org_rows(plane, stmt):
    with plane.sync.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ORG_SCHEMA}"))
        return conn.execute(stmt).fetchall()


def test_the_domains_owner_revokes_a_grant_and_it_ends_at_once(plane):
    with TestClient(_app(plane)) as client:
        started = client.post("/admin/access-reviews", headers=_as("alice"))
        assert started.status_code == 200, started.text
        review_id = started.json()["id"]
        assert started.json()["rows"] == 4

        # A member with neither right reads no review.
        assert client.get("/admin/access-reviews", headers=_as("bob")).status_code == 403

        # The owner of sales reads the sales rows only, and may decide each.
        seen = client.get(f"/admin/access-reviews/{review_id}", headers=_as("sam"))
        assert seen.status_code == 200, seen.text
        rows = {(r["user_id"], r["domain_id"]): r for r in seen.json()["rows"]}
        assert set(rows) == {("bob", "sales")}
        assert rows[("bob", "sales")]["can_decide"] is True

        # The administrator reads every row, and decides none she does not own.
        every = client.get(f"/admin/access-reviews/{review_id}", headers=_as("alice")).json()
        by_user = {r["user_id"]: r for r in every["rows"]}
        assert set(by_user) == {"alice", "sam", "bob", "hal"}

        refused = client.post(
            f"/admin/access-reviews/rows/{by_user['hal']['id']}",
            headers=_as("sam"),
            json={"decision": "revoked", "reason": "not mine"},
        )
        assert refused.status_code == 403
        assert refused.json()["code"] == "access_review.not_owner"

        no_reason = client.post(
            f"/admin/access-reviews/rows/{by_user['bob']['id']}",
            headers=_as("sam"),
            json={"decision": "revoked", "reason": " "},
        )
        assert no_reason.status_code == 422

        revoked = client.post(
            f"/admin/access-reviews/rows/{by_user['bob']['id']}",
            headers=_as("sam"),
            json={"decision": "revoked", "reason": "moved to finance"},
        )
        assert revoked.status_code == 200, revoked.text
        body = revoked.json()
        assert (body["decision"], body["decided_by"], body["reason"], body["revoked_grant"]) == (
            "revoked",
            "sam",
            "moved to finance",
            True,
        )

        # Taken effect at once: bob holds no role now, so his next request is refused.
        again = client.get("/admin/access-reviews", headers=_as("bob"))
        assert again.status_code in (401, 403)

        exported = client.get(f"/admin/access-reviews/{review_id}/export", headers=_as("alice"))
        assert exported.status_code == 200
        lines = {r["user_id"]: r for r in csv.DictReader(io.StringIO(exported.text))}
        assert (lines["bob"]["decision"], lines["bob"]["decided_by"]) == ("revoked", "sam")
        assert lines["bob"]["reason"] == "moved to finance"

    held = _org_rows(
        plane,
        select(user_role_assignments.c.user_id).where(user_role_assignments.c.user_id == "bob"),
    )
    assert held == []
    trail = _org_rows(
        plane,
        select(admin_audit_log.c.action, admin_audit_log.c.actor_id, admin_audit_log.c.subject_id),
    )
    assert ("access_review.revoked", "sam", "bob") in [tuple(t) for t in trail]
    assert ("access_review.started", "alice", str(review_id)) in [tuple(t) for t in trail]


def test_the_schedule_is_set_cleared_and_refused_when_not_cron(plane):
    with TestClient(_app(plane)) as client:
        assert client.get("/admin/access-reviews/schedule", headers=_as("alice")).json() == {
            "cron": None
        }
        saved = client.put(
            "/admin/access-reviews/schedule", headers=_as("alice"), json={"cron": "0 6 1 * *"}
        )
        assert saved.status_code == 200, saved.text
        assert client.get("/admin/access-reviews/schedule", headers=_as("alice")).json() == {
            "cron": "0 6 1 * *"
        }
        bad = client.put(
            "/admin/access-reviews/schedule", headers=_as("alice"), json={"cron": "monthly"}
        )
        assert bad.status_code == 422
        assert bad.json()["code"] == "access_review.cron_invalid"
        # Only org_settings sets it.
        assert (
            client.put(
                "/admin/access-reviews/schedule", headers=_as("sam"), json={"cron": "0 6 * * *"}
            ).status_code
            == 403
        )
        cleared = client.put("/admin/access-reviews/schedule", headers=_as("alice"), json={})
        assert cleared.json() == {"cron": None}
