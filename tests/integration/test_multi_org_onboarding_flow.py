# Copyright (c) 2026 Kenneth Stott
# Canary: 7b2d4f19-0e6a-4c53-a8d1-2f9c6b5e04a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1266: the full multi-org SaaS onboarding lifecycle, end-to-end against live control planes.

Drives the exact flow a human would run, using the ``basic`` provider's real local-account
machinery (``local_users`` + HTTP Basic) so no test-only login shim is introduced:

  1. person 1 — first authenticated request atomically claims the sole platform superadmin
     (``superadmin_bootstrap`` singleton) and is granted ``admin``;
  2. person 2 — a member-less authenticated user self-creates an org (async provisioning:
     ``provisioning`` → poll → ``ready``), is granted org membership synchronously and
     ``org_admin`` inside the org once its schema exists, then invites a third user;
  3. person 3 — redeems the invite and becomes an ``analyst`` of person 2's org, and is
     forbidden from issuing invites (the org_admin authz boundary).

Org routing is driven by the request Host (REQ-1276): the control-plane host
``cloud.provisa.org`` carries no org, the org subdomain ``acme.provisa.org`` selects org
``acme``. The Part-1 data-plane build (``build_org_runtime``) and physical provisioning
(``provision_org``) are stubbed — this test pins the CONTROL plane (membership, role grants,
provisioning-state transitions, invite authz), which the data-plane tests
(``test_two_org_trino_isolation``) cover separately.

DDL/seeding/verification run on a SYNCHRONOUS psycopg2 engine so the async control-plane
engines are only ever driven inside the TestClient's event loop, mirroring
``test_redeem_invite``.
"""

from __future__ import annotations

import base64
import os
import time
import types

import bcrypt
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text
from sqlalchemy.exc import OperationalError

from provisa.api.admin.invites_router import router as invites_router
from provisa.api.admin.orgs_router import router as orgs_router
from provisa.api.auth_router import router as auth_router
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.providers.basic import BasicAuthProvider
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import (
    REGISTRY_TABLES,
    local_users,
    org_invites,
    user_org_memberships,
)
from provisa.core.schema_admin import metadata as admin_metadata
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import roles, user_role_assignments

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_SYNC_URL = f"postgresql+psycopg2://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ADMIN_SCHEMA = "test_req1266_admin"
_TENANT_SCHEMA = "test_req1266_tenant"

# Three seeded local accounts (id == the principal the BasicAuthProvider returns).
_ACCOUNTS = {
    "super1": "pw-super",
    "user2": "pw-two",
    "user3": "pw-three",
}
_CONTROL_HOST = "cloud.provisa.org"  # control plane — no org subdomain
_ORG_HOST = "acme.provisa.org"  # selects org "acme"


def _basic(username: str) -> dict[str, str]:
    raw = f"{username}:{_ACCOUNTS[username]}".encode()
    return {"Authorization": "Basic " + base64.b64encode(raw).decode()}


def _prepare_sync():
    """Create both schemas + tables, seed the three local accounts and the org roles — synchronously.

    No orgs/invites/memberships are pre-seeded: the flow creates them. The tenant plane seeds the
    ``org_admin`` and ``analyst`` role rows so the FK-backed ``user_role_assignments`` grants resolve
    (org_admin is normally seeded by schema.sql per org; here provisioning is stubbed)."""
    engine = create_engine(_SYNC_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {_ADMIN_SCHEMA}"))
        conn.execute(text(f"CREATE SCHEMA {_TENANT_SCHEMA}"))

        conn.execute(text(f"SET search_path TO {_ADMIN_SCHEMA}"))
        admin_metadata.create_all(conn, tables=REGISTRY_TABLES)  # includes user_profiles
        for username, password in _ACCOUNTS.items():
            pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
            conn.execute(
                insert(local_users).values(
                    id=username,
                    username=username,
                    password_hash=pw_hash,
                    email=f"{username}@example.com",
                    display_name=username,
                    is_active=True,
                )
            )

        conn.execute(text(f"SET search_path TO {_TENANT_SCHEMA}"))
        org_metadata.create_all(conn, tables=[roles, user_role_assignments])
        conn.execute(insert(roles).values(id="org_admin"))
        conn.execute(insert(roles).values(id="analyst"))
    return engine


@pytest.fixture
def planes(monkeypatch):
    try:
        sync_engine = _prepare_sync()
    except OperationalError as exc:
        pytest.skip(f"live Postgres not reachable at {_SYNC_URL}: {exc}")

    admin_db = Database(create_engine_from_url(_ASYNC_URL), name="admin", search_path=_ADMIN_SCHEMA)
    tenant_db = Database(
        create_engine_from_url(_ASYNC_URL), name="tenant", search_path=_TENANT_SCHEMA
    )

    from provisa.api.app import state as app_state

    # tenant_db/roles are AppState property shims that resolve the ContextVar-selected OrgRuntime;
    # with current_org unset they resolve the DEFAULT org's runtime. Patch that runtime's plain
    # dataclass fields directly (roles has no property setter) — monkeypatch auto-restores them.
    default_rt = app_state.org_registry.get(app_state.org_id)
    monkeypatch.setattr(app_state, "admin_db", admin_db, raising=False)
    monkeypatch.setattr(app_state, "multitenancy", True, raising=False)
    monkeypatch.setattr(default_rt, "tenant_db", tenant_db)
    monkeypatch.setattr(
        default_rt,
        "roles",
        {
            "admin": {"capabilities": ["admin", "superadmin"]},
            "org_admin": {"capabilities": ["user_management", "source_registration"]},
            "analyst": {"capabilities": ["query_development"]},
        },
    )
    # register (/auth/register) reads config.auth.provider; unused here (accounts are seeded) but the
    # app singleton must expose it consistently.
    monkeypatch.setattr(
        app_state, "config", types.SimpleNamespace(auth={"provider": "basic"}), raising=False
    )
    # REQ-1290: /auth/claim-bootstrap resolves the bootstrap flag from state.auth_config — the same
    # dict wiring.py hands the middleware — so the endpoint and the middleware can never disagree.
    monkeypatch.setattr(
        app_state, "auth_config", {"provider": "basic", "bootstrap_superadmin": True}, raising=False
    )

    # Stub the Part-1 data-plane build + physical provisioning: this test pins the control plane.
    # build_org_runtime returns the org's tenant Database so grant_org_role lands the org_admin
    # assignment we verify; provision_org (schema/PG-role/Redis) is a no-op.
    async def _fake_build(org_id, *, include_demo=False):  # noqa: ARG001
        return types.SimpleNamespace(tenant_db=tenant_db)

    async def _noop_provision(*_args, **_kwargs):
        return None

    monkeypatch.setattr("provisa.api.app.build_org_runtime", _fake_build, raising=False)
    monkeypatch.setattr(
        "provisa.core.org_provisioning.provision_org", _noop_provision, raising=False
    )

    yield admin_db, tenant_db, sync_engine

    with sync_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_ADMIN_SCHEMA} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {_TENANT_SCHEMA} CASCADE"))
    sync_engine.dispose()


def _make_app(admin_db: Database, tenant_db: Database) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        AuthMiddleware,
        provider=BasicAuthProvider(admin_db),
        admin_pool=admin_db,
        db_pool=tenant_db,
        assignments_source="provisa",
        default_assignments=[],
        multitenancy=True,
        bootstrap_superadmin=True,
        default_org_id="root",
    )
    app.include_router(auth_router)
    app.include_router(orgs_router)
    app.include_router(invites_router)

    @app.get("/whoami")
    async def _whoami(request: Request):  # noqa: RUF029 — Starlette route must be async
        return {
            "roles": request.state.identity.roles,
            "active_org_id": request.state.active_org_id,
        }

    return app


def _q(sync_engine, schema, stmt):
    with sync_engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {schema}"))
        return conn.execute(stmt).fetchall()


def _poll_ready(client: TestClient, org_id: str, headers: dict[str, str]) -> dict:
    """Poll the status endpoint until the async provisioning task flips off ``provisioning``.

    Each request drives the TestClient's event loop, letting the background task progress; the
    stubbed build makes it resolve within a turn. Bounded so a stuck task fails the test loudly."""
    for _ in range(50):
        resp = client.get(f"/admin/orgs/{org_id}/status", headers=headers)
        assert resp.status_code == 200, resp.text
        record = resp.json()
        if record["provisioning_state"] != "provisioning":
            return record
        time.sleep(0.02)
    raise AssertionError("org never left 'provisioning' state")


def test_full_multi_org_onboarding_lifecycle(planes):
    admin_db, tenant_db, sync_engine = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        # 1. person 1 claims the sole platform superadmin — REQ-1290: by explicitly posting to
        # /auth/claim-bootstrap from the first-login page, never as a side effect of authenticating.
        claim = client.post("/auth/claim-bootstrap", headers={**_basic("super1"), "host": _CONTROL_HOST})
        assert claim.status_code == 200, claim.text
        assert claim.json() == {"claimed": True, "claimed_by": "super1"}
        who = client.get("/whoami", headers={**_basic("super1"), "host": _CONTROL_HOST})
        assert who.status_code == 200, who.text
        assert who.json()["roles"] == ["admin"]

        # 2. person 2 (member-less) self-creates an org from the control-plane host.
        created = client.post(
            "/admin/orgs/",
            json={"id": "acme", "name": "Acme", "include_demo": False},
            headers={**_basic("user2"), "host": _CONTROL_HOST},
        )
        assert created.status_code == 200, created.text
        body = created.json()
        assert body["id"] == "acme"
        assert body["provisioning_state"] == "provisioning"

        # 3. provisioning completes asynchronously → ready.
        status = _poll_ready(client, "acme", {**_basic("user2"), "host": _CONTROL_HOST})
        assert status["provisioning_state"] == "ready", status
        assert status["provisioning_error"] is None

        # 4. person 2 is org_admin of acme and can now invite an analyst (acting under acme's host).
        invite = client.post(
            "/admin/invites/",
            json={"org_id": "acme", "role_id": "analyst"},
            headers={**_basic("user2"), "host": _ORG_HOST},
        )
        assert invite.status_code == 200, invite.text
        token = invite.json()["token"]
        assert invite.json()["role_id"] == "analyst"

        # 5. person 3 redeems the invite → membership + analyst assignment.
        redeem = client.post(
            "/auth/redeem-invite",
            json={"token": token},
            headers={**_basic("user3"), "host": _CONTROL_HOST},
        )
        assert redeem.status_code == 200, redeem.text
        assert redeem.json() == {"user_id": "user3", "org_id": "acme", "role_id": "analyst"}

        # 6. person 3's identity now surfaces analyst, scoped to acme.
        who3 = client.get("/whoami", headers={**_basic("user3"), "host": _ORG_HOST})
        assert who3.status_code == 200, who3.text
        assert who3.json() == {"roles": ["analyst"], "active_org_id": "acme"}

        # 7. person 3 (analyst) may NOT issue invites — the org_admin boundary.
        forbidden = client.post(
            "/admin/invites/",
            json={"org_id": "acme", "role_id": "analyst"},
            headers={**_basic("user3"), "host": _ORG_HOST},
        )
        assert forbidden.status_code == 403, forbidden.text

    # Control-plane row assertions (sync engine).
    # person 2: org membership (platform plane) granted synchronously at create.
    membership = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "user2"),
    )
    assert [r[0] for r in membership] == ["acme"]

    # person 2: org_admin assignment (tenant plane) granted by the provisioning task.
    grant2 = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id, user_role_assignments.c.domain_id).where(
            user_role_assignments.c.user_id == "user2"
        ),
    )
    assert [(r[0], r[1]) for r in grant2] == [("org_admin", "*")]

    # person 3: analyst assignment (tenant plane) + membership from redemption; invite burned.
    grant3 = _q(
        sync_engine,
        _TENANT_SCHEMA,
        select(user_role_assignments.c.role_id).where(
            user_role_assignments.c.user_id == "user3"
        ),
    )
    assert [r[0] for r in grant3] == ["analyst"]
    membership3 = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == "user3"),
    )
    assert [r[0] for r in membership3] == ["acme"]
    burned = _q(
        sync_engine,
        _ADMIN_SCHEMA,
        select(org_invites.c.used_by).where(org_invites.c.org_id == "acme"),
    )
    assert [r[0] for r in burned] == ["user3"]


def test_second_user_cannot_claim_superadmin(planes):
    # The superadmin slot is a singleton. person 1 claims it and — being a platform admin — resolves
    # an active org and reads /whoami as ["admin"]. person 2 gets NO platform grant and NO membership,
    # so the same member-only path fails closed with "Org selection required" (never silently
    # defaulted to an org, never granted admin). That contrast is the singleton guarantee.
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        claimed = client.post(
            "/auth/claim-bootstrap", headers={**_basic("super1"), "host": _CONTROL_HOST}
        )
        assert claimed.json()["claimed"] is True
        first = client.get("/whoami", headers={**_basic("super1"), "host": _CONTROL_HOST})
        assert first.status_code == 200, first.text
        assert first.json()["roles"] == ["admin"]
        # REQ-1290: person 2's own claim attempt loses the singleton race — it never displaces the
        # holder, and posting it confers nothing.
        losing = client.post(
            "/auth/claim-bootstrap", headers={**_basic("user2"), "host": _CONTROL_HOST}
        )
        assert losing.json() == {"claimed": False, "claimed_by": "super1"}
        second = client.get("/whoami", headers={**_basic("user2"), "host": _CONTROL_HOST})
    assert second.status_code == 401, second.text
    assert second.json()["detail"] == "Org selection required"


def test_duplicate_org_rejected(planes):
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        client.get("/whoami", headers={**_basic("super1"), "host": _CONTROL_HOST})
        first = client.post(
            "/admin/orgs/",
            json={"id": "acme", "name": "Acme", "include_demo": False},
            headers={**_basic("user2"), "host": _CONTROL_HOST},
        )
        assert first.status_code == 200, first.text
        dup = client.post(
            "/admin/orgs/",
            json={"id": "acme", "name": "Acme Two", "include_demo": False},
            headers={**_basic("user3"), "host": _CONTROL_HOST},
        )
    assert dup.status_code == 409, dup.text


def test_create_org_requires_authentication(planes):
    admin_db, tenant_db, _ = planes
    with TestClient(_make_app(admin_db, tenant_db), raise_server_exceptions=True) as client:
        resp = client.post(
            "/admin/orgs/",
            json={"id": "acme", "name": "Acme", "include_demo": False},
            headers={"host": _CONTROL_HOST},
        )
    assert resp.status_code == 401, resp.text
