# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Offboarding and org lifecycle against the live control planes.

Covers the acts that take something away rather than grant it: removing a member (REQ-1305),
revoking org_admin (REQ-1303), the last-org_admin invariant (REQ-1302), leaving an org (REQ-1306),
the self-role-change refusal (REQ-1308), org deletion with its typed confirmation (REQ-1300),
retrying a failed provision (REQ-1315), the org creation cap (REQ-1311), and self-service account
deletion with tombstoning (REQ-1307, REQ-1312).

Each of these writes to BOTH planes, and the whole point of the tests is that neither plane is
left holding a row the other has dropped — which only a real two-plane deployment can show. As in
test_redeem_invite, DDL and row verification run on a SYNCHRONOUS psycopg2 engine so the async
control-plane engines are driven only inside the TestClient's event loop.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, text

from provisa.api.admin.orgs_router import router as orgs_router
from provisa.api.org_runtime import ActiveOrgPool
from provisa.api.auth_router import router as auth_router
from provisa.auth.middleware import AuthMiddleware
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import REGISTRY_TABLES
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_admin import (
    org_auto_join_optouts,
    org_invites,
    orgs,
    user_org_memberships,
    user_profiles,
)
from provisa.core.schema_org import (
    admin_audit_log,
    domains,
    query_audit_log,
    registered_tables,
    sources,
    table_columns,
)
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments
from tests.integration.test_auth_integration import _FirebaseLikeProvider

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_orglife_admin"
# One schema per org: the tenant plane is per-org, and an offboarding bug that writes to the wrong
# org's schema is invisible if every org shares one.
_ORG_SCHEMAS = {"root": "test_orglife_root", "acme": "test_orglife_acme"}

_TENANT_TABLES = [roles, user_role_assignments, admin_audit_log, query_audit_log]

# REQ-1301: the root org additionally carries the dataset catalog, because creating or deleting an
# org rebuilds root's org-registry view and re-registers it as a meta-domain table.
_ROOT_EXTRA_TABLES = [sources, domains, registered_tables, table_columns]

# REQ-1337: every gate on these routers reads a RIGHT, so a role row with an empty capability list
# authorizes nothing however it is named. These are the capabilities schema.sql seeds, trimmed to
# what the org-lifecycle surface asks for: cross_org is what makes platform_admin control-plane and
# lets pat administer any org, and user_management is what lets alice administer the one org she is
# a member of. org_admin holds no cross_org in either tenancy mode.
_SEEDED_ROLE_CAPS: dict[str, list[str]] = {
    "platform_admin": ["admin", "superadmin", "platform_settings", "cross_org"],
    "org_admin": ["user_management", "source_registration", "access_config", "query_development"],
    "analyst": ["usage", "ad_hoc_query", "query_development"],
}

# alice administers acme; bob is an ordinary member of it; pat is the platform administrator,
# whose platform_admin assignment lives in the root (default-org) schema.
_TOKENS = {"tok-alice": "alice", "tok-bob": "bob", "tok-pat": "pat"}


def _prepare_sync():
    """Create the admin schema and both org schemas, seed orgs, roles, memberships, assignments."""
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        for schema in (_ADMIN_SCHEMA, *_ORG_SCHEMAS.values()):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.execute(text(f"CREATE SCHEMA {schema}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)
        conn.execute(insert(orgs).values(id="root", name="Root", created_by="pat"))
        conn.execute(
            insert(orgs).values(
                id="acme",
                name="Acme",
                created_by="alice",
                # REQ-1306: acme auto-joins anyone, so a departure that does not record an opt-out
                # is undone by the very next request. That is the regression this org shape exists
                # to catch.
                auto_join=True,
                auto_join_role="analyst",
            )
        )
        for user_id, org_id in (("pat", "root"), ("alice", "acme"), ("bob", "acme")):
            conn.execute(insert(user_org_memberships).values(user_id=user_id, org_id=org_id))

        conn.execute(text(f"SET search_path TO {_ORG_SCHEMAS['root']}"))
        org_metadata.create_all(conn, tables=[*_TENANT_TABLES, *_ROOT_EXTRA_TABLES])
        conn.execute(insert(sources).values(id="provisa-admin", type="postgres"))
        conn.execute(insert(domains).values(id="meta"))
        for role_id, caps in _SEEDED_ROLE_CAPS.items():
            conn.execute(insert(roles).values(id=role_id, capabilities=caps))
        conn.execute(
            insert(user_role_assignments).values(
                user_id="pat", role_id="platform_admin", domain_id="*"
            )
        )

        conn.execute(text(f"SET search_path TO {_ORG_SCHEMAS['acme']}"))
        # REQ-1304: acme carries the whole tenant schema, because the config export offered before
        # deletion walks every catalog table the org could have configured — anything missing reads
        # as a 500 and would abort a deletion the org actually asked for.
        org_metadata.create_all(conn)
        conn.execute(insert(domains).values(id="acmeonlydomain"))
        for role_id, caps in _SEEDED_ROLE_CAPS.items():
            conn.execute(insert(roles).values(id=role_id, capabilities=caps))
        conn.execute(
            insert(user_role_assignments).values(
                user_id="alice", role_id="org_admin", domain_id="*"
            )
        )
        conn.execute(
            insert(user_role_assignments).values(user_id="bob", role_id="analyst", domain_id="*")
        )
    return engine


@pytest.fixture
def planes(monkeypatch):
    try:
        sync_engine = _prepare_sync()
    except Exception as exc:  # noqa: BLE001 — the suite provisions this PG; a miss is a config fault
        pytest.skip(f"live Postgres not reachable at {_SYNC_URL}: {exc}")

    admin_db = Database(create_engine_from_url(_ASYNC_URL), name="admin", search_path=_ADMIN_SCHEMA)
    org_dbs = {
        org_id: Database(create_engine_from_url(_ASYNC_URL), name=org_id, search_path=schema)
        for org_id, schema in _ORG_SCHEMAS.items()
    }

    from provisa.api.app import state as app_state
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime

    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "org_id", "root", raising=False)

    # The org runtime is the data plane; these tests exercise the control plane, so each org's
    # runtime carries its real tenant Database and nothing more. An org created mid-test has no
    # runtime at all — which is exactly the state a still-provisioning org is in.
    registry = OrgRegistry()
    for org_id, db in org_dbs.items():
        registry.set(org_id, OrgRuntime(org_id=org_id, tenant_db=db))
    monkeypatch.setattr(app_state, "org_registry", registry, raising=False)
    # REQ-1337: the loaded roles registry is where a role id becomes the rights it carries. In a real
    # process it comes from the schema.sql seed; these tests build their schemas by hand, so mirror
    # the same capability lists that were written into the role rows above.
    monkeypatch.setattr(
        app_state,
        "roles",
        {rid: {"id": rid, "capabilities": caps} for rid, caps in _SEEDED_ROLE_CAPS.items()},
        raising=False,
    )

    evicted: list[str] = []
    real_invalidate = registry.invalidate

    def _invalidate(org_id: str) -> None:
        evicted.append(org_id)
        real_invalidate(org_id)

    monkeypatch.setattr(registry, "invalidate", _invalidate)

    async def _org_runtime(org_id: str):
        return registry.get(org_id) or SimpleNamespace(tenant_db=None)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _org_runtime, raising=False)

    deprovisioned: list[str] = []

    async def _deprovision(_pool, org_id: str, redis_url=None):
        deprovisioned.append(org_id)

    monkeypatch.setattr("provisa.core.org_provisioning.deprovision_org", _deprovision)

    provisioned: list[tuple[str, bool, str | None, bool]] = []

    async def _provision_task(
        org_id: str, include_demo: bool, created_by: str | None, isolated_engine: bool = False
    ):
        provisioned.append((org_id, include_demo, created_by, isolated_engine))

    monkeypatch.setattr("provisa.api.admin.orgs_router._provision_org_task", _provision_task)

    yield SimpleNamespace(
        admin_db=admin_db,
        org_dbs=org_dbs,
        sync=sync_engine,
        evicted=evicted,
        deprovisioned=deprovisioned,
        provisioned=provisioned,
    )

    with sync_engine.begin() as conn:
        for schema in (_ADMIN_SCHEMA, *_ORG_SCHEMAS.values()):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    sync_engine.dispose()


def _make_app(planes) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=_FirebaseLikeProvider(_TOKENS),
        admin_pool=planes.admin_db,
        # The tenant control plane follows the org bound on the request (ActiveOrgPool), exactly as
        # production wires it — a fixed Database would pin every assignment read to org_root.
        db_pool=ActiveOrgPool(),
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        default_org_id="root",
    )
    app.include_router(orgs_router)
    app.include_router(auth_router)
    return app


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _rows(sync_engine, schema: str, stmt):
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema}"))
        return conn.execute(stmt).fetchall()


def test_remove_member_clears_both_planes(planes):  # REQ-1305, REQ-1302
    """Offboarding must drop the tenant-plane assignments with the membership: leaving them behind
    means re-adding the person silently restores everything they previously held."""
    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/admin/orgs/acme/members/bob", headers=_auth("tok-alice"))
    assert resp.status_code == 200, resp.text

    assert (
        _rows(
            planes.sync,
            _ADMIN_SCHEMA,
            select(user_org_memberships.c.user_id).where(
                user_org_memberships.c.org_id == "acme", user_org_memberships.c.user_id == "bob"
            ),
        )
        == []
    )
    assert (
        _rows(
            planes.sync,
            _ORG_SCHEMAS["acme"],
            select(user_role_assignments.c.role_id).where(
                user_role_assignments.c.user_id == "bob"
            ),
        )
        == []
    )
    audit = _rows(
        planes.sync,
        _ORG_SCHEMAS["acme"],
        select(admin_audit_log.c.action, admin_audit_log.c.actor_id, admin_audit_log.c.subject_id),
    )
    assert ("remove_member", "alice", "bob") in [tuple(r) for r in audit]


def test_member_list_names_who_holds_org_admin(planes):  # REQ-1305, REQ-1302, REQ-1303
    """The roster carries the org_admin flag, joined from the other plane.

    Without it the team page can only offer every action to everyone and let the server say no —
    it cannot know which person to show a demote control for, nor which removal REQ-1302 refuses.
    """
    with TestClient(_make_app(planes)) as client:
        resp = client.get("/admin/orgs/acme/members", headers=_auth("tok-alice"))
        assert resp.status_code == 200, resp.text
        by_user = {r["user_id"]: r for r in resp.json()}
        assert by_user["alice"]["is_org_admin"] is True
        assert by_user["bob"]["is_org_admin"] is False

        assert client.post("/admin/orgs/acme/admins/bob", headers=_auth("tok-alice")).status_code == 200
        after = client.get("/admin/orgs/acme/members", headers=_auth("tok-alice")).json()
    assert {r["user_id"] for r in after if r["is_org_admin"]} == {"alice", "bob"}


def test_config_export_hands_back_what_deletion_would_destroy(planes):  # REQ-1304
    """The download offered before deletion has to actually carry the org's configuration, and it
    is org_admin's own data — a member of another org cannot fetch it."""
    with TestClient(_make_app(planes)) as client:
        mine = client.get("/admin/orgs/acme/config-export", headers=_auth("tok-alice"))
        assert mine.status_code == 200, mine.text
        assert mine.headers["content-disposition"] == 'attachment; filename="acme-config.yaml"'
        # A domain that exists only in acme's tenant schema, proving the export was built from
        # the org the caller named rather than from whatever org the process booted with.
        assert "acmeonlydomain" in mine.text

        outsider = client.get("/admin/orgs/acme/config-export", headers=_auth("tok-bob"))
    assert outsider.status_code == 403, outsider.text


def test_remove_last_org_admin_is_refused(planes):  # REQ-1302
    """An org with no administrator can never be administered again — the only remaining way to
    act on it is to delete it, which is a different, confirmed act."""
    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/admin/orgs/acme/members/alice", headers=_auth("tok-alice"))
    assert resp.status_code == 409, resp.text
    assert "last org_admin" in resp.json()["detail"]
    assert _rows(
        planes.sync,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.user_id).where(
            user_org_memberships.c.user_id == "alice", user_org_memberships.c.org_id == "acme"
        ),
    )


def test_grant_then_revoke_org_admin_is_audited(planes):  # REQ-1303, REQ-1308
    with TestClient(_make_app(planes)) as client:
        granted = client.post("/admin/orgs/acme/admins/bob", headers=_auth("tok-alice"))
        assert granted.status_code == 200, granted.text
        assert _rows(
            planes.sync,
            _ORG_SCHEMAS["acme"],
            select(user_role_assignments.c.user_id).where(
                user_role_assignments.c.user_id == "bob",
                user_role_assignments.c.role_id == "org_admin",
            ),
        )
        revoked = client.delete("/admin/orgs/acme/admins/bob", headers=_auth("tok-alice"))
        assert revoked.status_code == 200, revoked.text

    assert (
        _rows(
            planes.sync,
            _ORG_SCHEMAS["acme"],
            select(user_role_assignments.c.role_id).where(
                user_role_assignments.c.user_id == "bob",
                user_role_assignments.c.role_id == "org_admin",
            ),
        )
        == []
    )
    # Revoking org_admin is not offboarding: bob keeps his membership and his analyst role.
    assert _rows(
        planes.sync,
        _ORG_SCHEMAS["acme"],
        select(user_role_assignments.c.role_id).where(
            user_role_assignments.c.user_id == "bob", user_role_assignments.c.role_id == "analyst"
        ),
    )
    actions = [
        r[0]
        for r in _rows(planes.sync, _ORG_SCHEMAS["acme"], select(admin_audit_log.c.action))
    ]
    assert actions == ["grant_org_admin", "revoke_org_admin"]


def test_self_role_change_is_refused(planes):  # REQ-1308
    """A user who can grant themselves a role has no role. The rule is server-side because the UI
    is not the only client."""
    with TestClient(_make_app(planes)) as client:
        granted = client.post("/admin/orgs/acme/admins/alice", headers=_auth("tok-alice"))
        revoked = client.delete("/admin/orgs/acme/admins/alice", headers=_auth("tok-alice"))
    assert granted.status_code == 403, granted.text
    assert revoked.status_code == 403, revoked.text
    assert "own role" in granted.json()["detail"]


def test_leaving_an_org_survives_the_next_request(planes):  # REQ-1306
    """acme auto-joins everyone. Without the opt-out row, bob's departure is undone by his very
    next authenticated request, which is indistinguishable from the leave button not working."""
    with TestClient(_make_app(planes)) as client:
        left = client.post("/admin/orgs/acme/leave", headers=_auth("tok-bob"))
        assert left.status_code == 200, left.text
        me = client.get("/auth/me", headers=_auth("tok-bob"))
        assert me.status_code == 200, me.text

    assert _rows(
        planes.sync,
        _ADMIN_SCHEMA,
        select(org_auto_join_optouts.c.org_id).where(
            org_auto_join_optouts.c.user_id == "bob"
        ),
    ) == [("acme",)]
    assert (
        _rows(
            planes.sync,
            _ADMIN_SCHEMA,
            select(user_org_memberships.c.org_id).where(
                user_org_memberships.c.user_id == "bob"
            ),
        )
        == []
    )


def test_being_re_added_clears_the_opt_out(planes):  # REQ-1306
    """An explicit add is an affirmative act by the org and outranks the earlier departure; a rule
    match is not."""
    with TestClient(_make_app(planes)) as client:
        assert client.post("/admin/orgs/acme/leave", headers=_auth("tok-bob")).status_code == 200
        added = client.post(
            "/admin/orgs/acme/members", json={"user_id": "bob"}, headers=_auth("tok-alice")
        )
        assert added.status_code == 200, added.text
    assert (
        _rows(
            planes.sync,
            _ADMIN_SCHEMA,
            select(org_auto_join_optouts.c.org_id).where(
                org_auto_join_optouts.c.user_id == "bob"
            ),
        )
        == []
    )


def test_org_deletion_requires_the_typed_confirmation(planes):  # REQ-1300
    with TestClient(_make_app(planes)) as client:
        bare = client.delete("/admin/orgs/acme", headers=_auth("tok-alice"))
        assert bare.status_code == 400, bare.text
        assert "permanently" in bare.json()["detail"]
        assert _rows(planes.sync, _ADMIN_SCHEMA, select(orgs.c.id).where(orgs.c.id == "acme"))

        wrong = client.delete("/admin/orgs/acme?confirm=acmee", headers=_auth("tok-alice"))
        assert wrong.status_code == 400, wrong.text

        done = client.delete("/admin/orgs/acme?confirm=acme", headers=_auth("tok-alice"))
        assert done.status_code == 200, done.text

    assert _rows(planes.sync, _ADMIN_SCHEMA, select(orgs.c.id).where(orgs.c.id == "acme")) == []
    assert (
        _rows(
            planes.sync,
            _ADMIN_SCHEMA,
            select(user_org_memberships.c.user_id).where(
                user_org_memberships.c.org_id == "acme"
            ),
        )
        == []
    )
    assert planes.deprovisioned == ["acme"]
    # The cached runtime is evicted last, after the registry row is gone, so an in-flight request
    # cannot rebuild it from a row that still exists.
    assert planes.evicted == ["acme"]


def test_root_org_cannot_be_deleted(planes):  # REQ-1300, REQ-1296
    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/admin/orgs/root?confirm=root", headers=_auth("tok-pat"))
    assert resp.status_code == 400, resp.text
    assert _rows(planes.sync, _ADMIN_SCHEMA, select(orgs.c.id).where(orgs.c.id == "root"))


def test_only_a_failed_org_can_be_retried(planes):  # REQ-1315
    with TestClient(_make_app(planes)) as client:
        ready = client.post("/admin/orgs/acme/retry", headers=_auth("tok-alice"))
        assert ready.status_code == 409, ready.text
        assert planes.provisioned == []

        with planes.sync.begin() as conn:
            conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
            conn.execute(
                orgs.update().where(orgs.c.id == "acme").values(provisioning_state="failed")
            )
        retried = client.post("/admin/orgs/acme/retry", headers=_auth("tok-alice"))
        assert retried.status_code == 200, retried.text
        assert retried.json()["provisioning_state"] == "provisioning"

    assert [p[0] for p in planes.provisioned] == ["acme"]
    # The partial schema the failure left behind is dropped before the rebuild starts.
    assert planes.deprovisioned == ["acme"]
    state_rows = _rows(
        planes.sync, _ADMIN_SCHEMA, select(orgs.c.provisioning_state).where(orgs.c.id == "acme")
    )
    assert state_rows == [("provisioning",)]


def test_a_failed_org_is_deletable_without_confirmation(planes):  # REQ-1300, REQ-1315
    """A failed org holds no data, so the ceremony guarding data loss has nothing to guard."""
    with planes.sync.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        conn.execute(orgs.update().where(orgs.c.id == "acme").values(provisioning_state="failed"))
    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/admin/orgs/acme", headers=_auth("tok-alice"))
    assert resp.status_code == 200, resp.text


def test_org_creation_cap(planes, monkeypatch):  # REQ-1311
    monkeypatch.setattr("provisa.api.admin.orgs_router._MAX_ORGS_PER_USER", 2)
    with TestClient(_make_app(planes)) as client:
        # alice already created acme (created_by=alice), so one more reaches the cap.
        first = client.post(
            "/admin/orgs/", json={"id": "beta", "name": "Beta"}, headers=_auth("tok-alice")
        )
        assert first.status_code == 200, first.text
        capped = client.post(
            "/admin/orgs/", json={"id": "gamma", "name": "Gamma"}, headers=_auth("tok-alice")
        )
        assert capped.status_code == 409, capped.text
        assert "limit reached" in capped.json()["detail"]

        # A failed org holds no data and can be retried or deleted, so it must not consume a slot.
        with planes.sync.begin() as conn:
            conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
            conn.execute(
                orgs.update().where(orgs.c.id == "beta").values(provisioning_state="failed")
            )
        after_failure = client.post(
            "/admin/orgs/", json={"id": "gamma", "name": "Gamma"}, headers=_auth("tok-alice")
        )
        assert after_failure.status_code == 200, after_failure.text


def test_isolated_engine_flag_round_trips(planes):  # REQ-1043, REQ-1067, REQ-1244
    """An org created with the "Isolated engine" checkbox persists the flag, reports it on
    status, and hands it to the provisioning task that binds the dedicated engine."""
    with TestClient(_make_app(planes)) as client:
        created = client.post(
            "/admin/orgs/",
            json={"id": "iso", "name": "Iso", "isolated_engine": True},
            headers=_auth("tok-alice"),
        )
        assert created.status_code == 200, created.text
        status = client.get("/admin/orgs/iso/status", headers=_auth("tok-alice"))
        assert status.status_code == 200, status.text
        assert status.json()["isolated_engine"] is True
        assert ("iso", False, "alice", True) in planes.provisioned

        # The default lane stays shared: no flag → False everywhere.
        pooled = client.post(
            "/admin/orgs/", json={"id": "pool", "name": "Pool"}, headers=_auth("tok-alice")
        )
        assert pooled.status_code == 200, pooled.text
        pooled_status = client.get("/admin/orgs/pool/status", headers=_auth("tok-alice"))
        assert pooled_status.json()["isolated_engine"] is False
        assert ("pool", False, "alice", False) in planes.provisioned


def test_account_deletion_is_blocked_while_last_org_admin(planes):  # REQ-1307
    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/auth/account?confirm=alice", headers=_auth("tok-alice"))
    assert resp.status_code == 409, resp.text
    # The refusal names the org to hand off, not merely that one exists.
    assert "acme" in resp.json()["detail"]
    assert _rows(
        planes.sync,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "alice"),
    )


def test_account_deletion_requires_confirmation(planes):  # REQ-1307
    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/auth/account", headers=_auth("tok-bob"))
    assert resp.status_code == 400, resp.text


def test_account_deletion_leaves_orgs_and_tombstones_references(planes):  # REQ-1307, REQ-1312
    """bob's account goes away; the org, its audit trail and its invites stay, attributed to an
    opaque token that no longer names him."""
    with planes.sync.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        conn.execute(insert(user_profiles).values(user_id="bob", email="bob@example.com"))
        conn.execute(
            insert(org_invites).values(
                token="tok-invite-bob",
                org_id="acme",
                created_by="bob",
                used_by="bob",
                expires_at=text("now() + interval '1 day'"),
            )
        )
        conn.execute(text(f"SET search_path TO {_ORG_SCHEMAS['acme']}"))
        conn.execute(
            insert(admin_audit_log).values(
                action="remove_member", actor_id="bob", subject_id="carol"
            )
        )
        conn.execute(
            insert(query_audit_log).values(
                user_id="bob",
                role_id="analyst",
                query_hash="h",
                source="test",
                status_code=200,
                duration_ms=1,
            )
        )

    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/auth/account?confirm=bob", headers=_auth("tok-bob"))
    assert resp.status_code == 200, resp.text
    tombstone = resp.json()["tombstone"]
    assert tombstone.startswith("deleted-user-")
    assert "bob" not in tombstone

    from provisa.core.org_membership import tombstone_id

    assert tombstone == tombstone_id("bob")

    assert (
        _rows(
            planes.sync,
            _ADMIN_SCHEMA,
            select(user_profiles.c.user_id).where(user_profiles.c.user_id == "bob"),
        )
        == []
    )
    assert (
        _rows(
            planes.sync,
            _ADMIN_SCHEMA,
            select(user_org_memberships.c.org_id).where(
                user_org_memberships.c.user_id == "bob"
            ),
        )
        == []
    )
    assert (
        _rows(
            planes.sync,
            _ORG_SCHEMAS["acme"],
            select(user_role_assignments.c.role_id).where(
                user_role_assignments.c.user_id == "bob"
            ),
        )
        == []
    )
    # The org itself survives its member's departure.
    assert _rows(planes.sync, _ADMIN_SCHEMA, select(orgs.c.id).where(orgs.c.id == "acme"))
    assert _rows(
        planes.sync,
        _ADMIN_SCHEMA,
        select(org_invites.c.created_by, org_invites.c.used_by).where(
            org_invites.c.token == "tok-invite-bob"
        ),
    ) == [(tombstone, tombstone)]
    # Audit entries are never deleted — a trail that erases on request is not a trail.
    assert _rows(
        planes.sync,
        _ORG_SCHEMAS["acme"],
        select(admin_audit_log.c.actor_id, admin_audit_log.c.subject_id),
    ) == [(tombstone, "carol")]
    assert _rows(
        planes.sync, _ORG_SCHEMAS["acme"], select(query_audit_log.c.user_id)
    ) == [(tombstone,)]


def test_account_deletion_is_blocked_for_the_last_platform_admin(planes):  # REQ-1307
    """A deployment with no platform administrator cannot be administered by anyone."""
    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/auth/account?confirm=pat", headers=_auth("tok-pat"))
    assert resp.status_code == 409, resp.text
    assert "platform_admin" in resp.json()["detail"]


def test_a_second_platform_admin_unblocks_deletion(planes):  # REQ-1307
    with planes.sync.begin() as conn:
        conn.execute(text(f"SET search_path TO {_ORG_SCHEMAS['root']}"))
        conn.execute(
            insert(user_role_assignments).values(
                user_id="dana", role_id="platform_admin", domain_id="*"
            )
        )
        # pat administers no org, so only the platform-admin rule can block the deletion.
        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        conn.execute(
            delete(user_org_memberships).where(user_org_memberships.c.user_id == "pat")
        )
    with TestClient(_make_app(planes)) as client:
        resp = client.delete("/auth/account?confirm=pat", headers=_auth("tok-pat"))
    assert resp.status_code == 200, resp.text
    # REQ-1312: the org pat created stays, attributed to the tombstone.
    from provisa.core.org_membership import tombstone_id

    assert _rows(
        planes.sync, _ADMIN_SCHEMA, select(orgs.c.created_by).where(orgs.c.id == "root")
    ) == [(tombstone_id("pat"),)]
