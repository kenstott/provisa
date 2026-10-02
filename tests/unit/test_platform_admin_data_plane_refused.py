# Copyright (c) 2026 Kenneth Stott
# Canary: bd576b98-690d-4d5f-a349-1c9dea05551d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1327 / REQ-1337: platform_admin is the control plane only; org_admin owns its org's data.

Every gate names a right, and a right is held by the role the seed gives it to. The roles here are
the seed's own lists (``provisa.core.db._SEED_ROLES``), so a capability added to or removed from a
system role is tested as it ships rather than as a fixture remembers it.

Three callers:

* holds ONLY platform_admin — refused on every data-plane gate, admitted on the platform gates;
* holds platform_admin AND org_admin (the bootstrap claimant, the break-glass account) — admitted on
  both planes;
* holds a role an org defined for itself — cannot define, be invited into, or be assigned a role
  that carries a platform right or a capability outside the vocabulary.
"""

from __future__ import annotations

import types
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException

import provisa.api.app as appmod
from provisa.api.errors import ApiError
from provisa.core.db import _SEED_ROLES
from provisa.security.rights import (
    DEPLOYMENT_GRANTER,
    Capability,
    PlatformRoleGrantError,
    can_act_cross_org,
)

ROOT_ORG = "root"

# The seed, as a multitenant deployment holds it (apply_tenancy_role_grants grants
# platform_settings to org_admin only in a single-tenant one).
ROLES: dict[str, dict] = {
    role_id: {
        "id": role_id,
        "capabilities": list(caps),
        # The seed (db.py, schema.sql): the control plane reaches no data domain.
        "domain_access": [] if role_id == "platform_admin" else ["*"],
    }
    for role_id, caps in _SEED_ROLES
}
# Roles an org might try to define for itself. None is seeded; each is the shape a refused
# definition would have had, placed in the registry to show what its holder could NOT do with it.
ROLES["retired_strings"] = {
    "id": "retired_strings",
    "capabilities": ["admin", "superadmin"],
    "domain_access": ["*"],
}
ROLES["settings_only"] = {
    "id": "settings_only",
    "capabilities": ["platform_settings", "usage"],
    "domain_access": ["*"],
}

PLATFORM_ONLY = ["platform_admin"]
BOOTSTRAP = ["platform_admin", "org_admin"]
ORG_ADMIN = ["org_admin"]

DATA_RIGHTS = [
    "source_registration",
    "table_registration",
    "create_relationship",
    "create_view",
    "access_config",
    "user_management",
    "masking_config",
    "column_grant",
    "view_governance",
    "query_development",
    "glossary_read",
    "data_product_rw",
    "org_settings",
    "observability",
    "environment_switch",
]


def _request(*role_ids: str, user_id: str = "u1", org: str = ROOT_ORG) -> Any:
    identity = types.SimpleNamespace(user_id=user_id, roles=list(role_ids))
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity, active_org_id=org))


def _info(*role_ids: str) -> Any:
    return types.SimpleNamespace(context={"request": _request(*role_ids)})


@pytest.fixture(autouse=True)
def _seeded_registry(monkeypatch):
    monkeypatch.setattr(appmod.state, "roles", ROLES, raising=False)
    monkeypatch.setattr(appmod.state, "org_id", ROOT_ORG, raising=False)
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)


def test_the_seed_gives_platform_admin_the_two_platform_rights_and_nothing_else():
    assert set(ROLES["platform_admin"]["capabilities"]) == {"platform_settings", "cross_org"}
    assert ROLES["platform_admin"]["domain_access"] == []
    sql = (Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql").read_text()
    seed = sql[sql.index("'platform_admin',\n") :].split("ON CONFLICT", 1)[0]
    assert "'[]'::jsonb" in seed and "'[\"*\"]'::jsonb" not in seed
    assert "domain_access = '[]'::jsonb WHERE id = 'platform_admin'" in sql
    assert not {"platform_settings", "cross_org"} & set(ROLES["org_admin"]["capabilities"])
    for role_id, role in ROLES.items():
        if role_id not in ("retired_strings",):
            assert not {"admin", "superadmin"} & set(role["capabilities"]), role_id


# --- a caller holding ONLY platform_admin --------------------------------------------------------


@pytest.mark.parametrize("right", DATA_RIGHTS)
def test_platform_admin_alone_is_refused_every_data_right(right):
    from provisa.api.admin.capabilities import (
        allowed_domains_for_capability_request,
        has_capability,
        has_capability_request,
        require_capability,
        require_capability_request,
    )

    with pytest.raises(PermissionError, match=right):
        require_capability(_info(*PLATFORM_ONLY), right)
    assert has_capability(_info(*PLATFORM_ONLY), right) is False
    with pytest.raises(ApiError) as err:
        require_capability_request(_request(*PLATFORM_ONLY), right)
    assert (err.value.status_code, err.value.code) == (403, "auth.missing_capability")
    assert has_capability_request(_request(*PLATFORM_ONLY), right) is False
    # No role the caller holds carries the right, so it is exercisable in no domain at all.
    assert allowed_domains_for_capability_request(_request(*PLATFORM_ONLY), right) == frozenset()


def test_platform_admin_alone_is_refused_the_org_scoped_gates():
    from provisa.api.admin import _platform_guard as guard
    from provisa.api.admin.creation_requests_router import _require_capability
    from provisa.api.env_routing import may_switch
    from provisa.security.rights import capabilities_for_claims

    req = _request(*PLATFORM_ONLY)
    for gate, right in (
        (guard.require_org_settings, "org_settings"),
        (guard.require_observability, "observability"),
    ):
        with pytest.raises(ApiError) as err:
            gate(req)
        assert (err.value.status_code, err.value.code) == (403, "platform.right_required")
        assert err.value.params == {"right": right}
        assert guard.has_right(req, right) is False
    with pytest.raises(HTTPException) as exc:
        _require_capability(req, "source_registration")
    assert exc.value.status_code == 403
    assert may_switch(capabilities_for_claims(PLATFORM_ONLY, ROLES)) is False


async def test_platform_admin_alone_is_refused_the_admin_data_routes(monkeypatch):
    from provisa.api.admin import audit_query_text_router, local_users_router, roles_router

    def _reached(*_a, **_k):
        raise AssertionError("the route read its store: the gate let the caller through")

    monkeypatch.setattr(audit_query_text_router, "_tenant_pool", _reached)
    monkeypatch.setattr(roles_router, "_pool", _reached)
    monkeypatch.setattr(local_users_router, "_pool", _reached)
    monkeypatch.setattr(local_users_router, "_admin_pool", _reached)

    req = _request(*PLATFORM_ONLY)
    calls = [
        audit_query_text_router.read_statement_text(req, 1),
        roles_router.create_role(
            roles_router.CreateRoleBody(id="r", capabilities=["usage"], domain_access=["*"]),
            req,
        ),
        roles_router.update_role(
            "analyst",
            roles_router.UpdateRoleBody(capabilities=["usage"]),
            req,
        ),
        roles_router.delete_role("analyst", req),
        local_users_router.list_users(req),
        local_users_router.add_assignment(
            "u2",
            local_users_router.AssignmentBody(role_id="analyst", domain_id="*"),
            req,
        ),
    ]
    for call in calls:
        with pytest.raises(ApiError) as err:
            await call
        assert (err.value.status_code, err.value.code) == (403, "auth.missing_capability")


def test_the_role_row_itself_authorizes_no_write():
    from provisa.security.mutation_authz import authorize_mutation

    allowed, reason = authorize_mutation(ROLES["platform_admin"], ["platform_admin"])
    assert allowed is False
    assert "WRITE" in reason


def test_platform_admin_alone_keeps_the_platform_plane():
    from provisa.api.admin import _platform_guard as guard
    from provisa.api.admin.orgs_router import _require_platform_admin
    from provisa.security.rights import capabilities_for_claims

    req = _request(*PLATFORM_ONLY)
    guard.require_platform_settings(req)
    guard.require_deployment_settings(req)
    assert guard.has_platform_settings(req) is True
    assert guard.has_deployment_settings(req) is True
    _require_platform_admin(req)
    assert can_act_cross_org(capabilities_for_claims(PLATFORM_ONLY, ROLES))


def test_platform_admin_adds_no_domain_scope_to_a_role_held_beside_it(monkeypatch):
    """Scope is unioned across every role a caller holds, so the control-plane role's own scope
    is what a narrower data role held beside it would be widened by."""
    from provisa.api.admin.capabilities import (
        allowed_domains_request,
        require_domain,
        require_domain_request,
    )
    from provisa.core.request_context import current_role_claims
    from provisa.security.rights import domain_access_for_claims, effective_domain_access_role

    roles = {
        **ROLES,
        "sales_dev": {
            "id": "sales_dev",
            "capabilities": ["query_development", "create_view"],
            "domain_access": ["sales"],
        },
    }
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    held = ["platform_admin", "sales_dev"]

    assert domain_access_for_claims(PLATFORM_ONLY, roles) == set()
    assert domain_access_for_claims(held, roles) == {"sales"}
    assert allowed_domains_request(_request(*PLATFORM_ONLY)) == frozenset()
    assert allowed_domains_request(_request(*held)) == frozenset({"sales"})
    require_domain(_info(*held), "sales")
    with pytest.raises(PermissionError, match="finance"):
        require_domain(_info(*held), "finance")
    with pytest.raises(PermissionError, match="sales"):
        require_domain(_info(*PLATFORM_ONLY), "sales")
    with pytest.raises(ApiError) as err:
        require_domain_request(_request(*held), "finance")
    assert (err.value.status_code, err.value.code) == (403, "auth.domain_denied")

    # The governed query pipeline reads the acting role's scope through the same union.
    token = current_role_claims.set(tuple(held))
    try:
        assert effective_domain_access_role("sales_dev", roles)["domain_access"] == ["sales"]
    finally:
        current_role_claims.reset(token)


# --- the retired wildcard strings grant nothing --------------------------------------------------


def test_a_role_holding_the_retired_strings_passes_no_gate_on_either_plane():
    from provisa.api.admin import _platform_guard as guard
    from provisa.api.admin.capabilities import require_capability, require_capability_request
    from provisa.api.admin.orgs_router import _require_platform_admin
    from provisa.api.env_routing import may_switch
    from provisa.security.rights import capabilities_for_claims

    held = ["retired_strings"]
    req = _request(*held)
    for right in DATA_RIGHTS:
        with pytest.raises(PermissionError):
            require_capability(_info(*held), right)
        with pytest.raises(ApiError):
            require_capability_request(req, right)
    for gate in (
        guard.require_org_settings,
        guard.require_observability,
        guard.require_platform_settings,
        guard.require_deployment_settings,
        _require_platform_admin,
    ):
        with pytest.raises(ApiError) as err:
            gate(req)
        assert err.value.status_code == 403
    caps = capabilities_for_claims(held, ROLES)
    assert not can_act_cross_org(caps)
    assert may_switch(caps) is False


# --- the bootstrap administrator holds both roles and keeps both planes --------------------------


@pytest.mark.parametrize("right", DATA_RIGHTS)
def test_the_bootstrap_administrator_holds_every_data_right(right):
    from provisa.api.admin.capabilities import (
        allowed_domains_for_capability_request,
        require_capability,
        require_capability_request,
        require_domain,
    )

    require_capability(_info(*BOOTSTRAP), right)
    require_capability(_info(*BOOTSTRAP), right, domain_id="sales")
    require_domain(_info(*BOOTSTRAP), "sales")
    require_capability_request(_request(*BOOTSTRAP), right)
    assert allowed_domains_for_capability_request(_request(*BOOTSTRAP), right) is None


async def test_the_bootstrap_administrator_keeps_both_planes(monkeypatch):
    from provisa.api.admin import _platform_guard as guard
    from provisa.api.admin import audit_query_text_router
    from provisa.api.admin.creation_requests_router import _require_capability
    from provisa.api.admin.orgs_router import _require_platform_admin
    from provisa.api.env_routing import may_switch
    from provisa.security.rights import capabilities_for_claims

    req = _request(*BOOTSTRAP)
    guard.require_org_settings(req)
    guard.require_observability(req)
    guard.require_platform_settings(req)
    guard.require_deployment_settings(req)
    _require_platform_admin(req)
    _require_capability(req, "source_registration")
    caps = capabilities_for_claims(BOOTSTRAP, ROLES)
    assert can_act_cross_org(caps)
    assert may_switch(caps) is True

    def _past_the_gate():
        raise RuntimeError("past the gate")

    monkeypatch.setattr(audit_query_text_router, "_tenant_pool", _past_the_gate)
    with pytest.raises(RuntimeError, match="past the gate"):
        await audit_query_text_router.read_statement_text(req, 1)


def test_statement_text_is_read_on_view_governance():
    assert Capability.VIEW_GOVERNANCE.value in ROLES["org_admin"]["capabilities"]
    for role_id in ("analyst", "developer", "modeler", "platform_admin"):
        assert Capability.VIEW_GOVERNANCE.value not in ROLES[role_id]["capabilities"], role_id


# --- an org administrator cannot DEFINE a role carrying platform rights or unknown strings -------


class _Rows:
    def __init__(self, rows: list[dict]):
        self._rows = [types.SimpleNamespace(_mapping=r) for r in rows]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    """Answers every read with the role rows it was given and records every write."""

    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.writes: list[object] = []

    async def execute_core(self, stmt):
        if getattr(stmt, "is_select", False):
            return _Rows(self.rows)
        self.writes.append(stmt)
        return _Rows([])

    async def upsert(self, _table, values, **_kw):
        self.writes.append(values)


class _Db:
    def __init__(self, rows: list[dict]):
        self.conn: Any = _Conn(rows)

    def acquire(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *_exc):
                return False

        return _Ctx()


def _role_rows(**extra: dict) -> list[dict]:
    rows = [
        {
            "id": role_id,
            "capabilities": role["capabilities"],
            "domain_access": ["*"],
            "org_id": None,
            "parent_role_id": None,
        }
        for role_id, role in ROLES.items()
    ]
    for role_id, fields in extra.items():
        rows.append(
            {
                "id": role_id,
                "capabilities": [],
                "domain_access": ["*"],
                "org_id": "acme",
                "parent_role_id": None,
                **fields,
            }
        )
    return rows


REFUSED_DEFINITIONS = [
    (["usage", "platform_settings"], None, 403, "roles.platform_right_requires_platform_admin"),
    (["usage", "cross_org"], None, 403, "roles.platform_right_requires_platform_admin"),
    (["usage"], "platform_admin", 403, "roles.platform_right_requires_platform_admin"),
    (["usage"], "settings_only", 403, "roles.platform_right_requires_platform_admin"),
    (["usage", "everything"], None, 422, "roles.unknown_capability"),
    (["root"], None, 422, "roles.unknown_capability"),
    # The retired wildcard strings are outside the vocabulary like any other unknown word.
    (["admin"], None, 422, "roles.unknown_capability"),
    (["usage", "superadmin"], None, 422, "roles.unknown_capability"),
]


@pytest.mark.parametrize("capabilities,parent,status,code", REFUSED_DEFINITIONS)
async def test_an_org_administrator_cannot_create_such_a_role(
    monkeypatch, capabilities, parent, status, code
):
    from provisa.api.admin import roles_router

    db = _Db(_role_rows())
    monkeypatch.setattr(roles_router, "_pool", lambda _request: db)
    body = roles_router.CreateRoleBody(
        id="minted", capabilities=capabilities, domain_access=["*"], parent_role_id=parent
    )
    with pytest.raises(ApiError) as err:
        await roles_router.create_role(body, _request(*ORG_ADMIN, org="acme"))
    assert (err.value.status_code, err.value.code) == (status, code)
    assert db.conn.writes == [], "a refused definition must write nothing"


@pytest.mark.parametrize("capabilities,parent,status,code", REFUSED_DEFINITIONS)
async def test_an_org_administrator_cannot_update_a_role_into_one(
    monkeypatch, capabilities, parent, status, code
):
    from provisa.api.admin import roles_router

    rows = _role_rows(minted={"capabilities": ["usage"]})
    db = _Db(rows)
    # update_role reads the role being changed first; hand it that row, then the whole table.
    target = next(r for r in rows if r["id"] == "minted")
    reads = iter([[target]])

    async def _execute_core(stmt):
        if getattr(stmt, "is_select", False):
            return _Rows(next(reads, rows))
        db.conn.writes.append(stmt)
        return _Rows([])

    monkeypatch.setattr(db.conn, "execute_core", _execute_core)
    monkeypatch.setattr(roles_router, "_pool", lambda _request: db)
    body = roles_router.UpdateRoleBody(capabilities=capabilities, parent_role_id=parent)
    with pytest.raises(ApiError) as err:
        await roles_router.update_role("minted", body, _request(*ORG_ADMIN, org="acme"))
    assert (err.value.status_code, err.value.code) == (status, code)
    assert db.conn.writes == [], "a refused definition must write nothing"


@pytest.mark.parametrize("capabilities,parent,status,code", REFUSED_DEFINITIONS)
async def test_the_graphql_surface_refuses_the_same_definitions(
    monkeypatch, capabilities, parent, status, code
):
    """createRole (GraphQL) is an upsert, so it is the update path too."""
    from provisa.api.admin import schema_mutation
    from provisa.api.admin.types import RoleInput
    from provisa.core.repositories import role as role_repo

    db = _Db(_role_rows())

    async def _get_pool():
        return db

    written: list[object] = []

    async def _upsert(_conn, model):
        written.append(model)

    monkeypatch.setattr(schema_mutation, "_get_pool", _get_pool)
    monkeypatch.setattr(role_repo, "upsert", _upsert)
    info = types.SimpleNamespace(context={"request": _request(*ORG_ADMIN, org="acme")})
    result = await schema_mutation.Mutation().create_role(
        info,  # pyright: ignore[reportArgumentType]
        RoleInput(
            id="minted", capabilities=capabilities, domain_access=["*"], parent_role_id=parent
        ),
    )
    assert result.success is False
    assert result.code == code
    assert written == [], "a refused definition must write nothing"


def test_a_platform_administrator_may_define_a_role_carrying_a_platform_right():
    from provisa.api.admin._platform_guard import role_definition_problem

    req = _request(*BOOTSTRAP)
    assert role_definition_problem(req, ["platform_settings", "cross_org"]) is None
    assert role_definition_problem(req, ["usage"], ["cross_org"]) is None
    # ...and is held to the vocabulary like anyone else.
    problem = role_definition_problem(req, ["everything"])
    assert problem is not None and problem.code == "roles.unknown_capability"
    # platform_settings alone is not a platform administrator (a single-tenant org_admin holds it).
    single = {**ROLES, "org_admin": {**ROLES["org_admin"]}}
    single["org_admin"]["capabilities"] = [*ROLES["org_admin"]["capabilities"], "platform_settings"]
    appmod.state.roles = single
    problem = role_definition_problem(_request(*ORG_ADMIN), ["platform_settings"])
    assert problem is not None and problem.code == "roles.platform_right_requires_platform_admin"


def test_an_ordinary_definition_is_accepted():
    from provisa.api.admin._platform_guard import role_definition_problem

    req = _request(*ORG_ADMIN, org="acme")
    assert role_definition_problem(req, ["usage", "query_development", "ddl"]) is None
    assert role_definition_problem(req, ["usage"], ["create_view", "write"]) is None


# --- ...nor CONFER one: assignment, invitation, auto-join, redemption ----------------------------


@pytest.mark.parametrize("role_id", ["platform_admin", "settings_only"])
async def test_an_org_administrator_cannot_assign_a_role_carrying_a_platform_right(
    monkeypatch, role_id
):
    from provisa.api.admin import local_users_router

    def _reached(_request):
        raise AssertionError("the assignment was written")

    monkeypatch.setattr(local_users_router, "_pool", _reached)
    monkeypatch.setattr(local_users_router, "_admin_pool", _reached)
    req = _request(*ORG_ADMIN, org="acme")
    with pytest.raises(ApiError) as err:
        await local_users_router.add_assignment(
            "u2",
            local_users_router.AssignmentBody(role_id=role_id, domain_id="*"),
            req,
        )
    assert (err.value.status_code, err.value.code) == (
        403,
        "users.platform_role_requires_platform_admin",
    )


@pytest.fixture
def org_runtime(monkeypatch):
    """ensure_org_runtime answering with a tenant plane that holds the given role rows."""

    def _install(rows: list[dict]) -> Any:
        db = _Db(rows)

        async def _ensure(_org_id):
            return types.SimpleNamespace(tenant_db=db)

        monkeypatch.setattr(appmod, "ensure_org_runtime", _ensure)
        return db

    return _install


@pytest.mark.parametrize("role_id", ["platform_admin", "settings_only", "heir"])
async def test_an_invitation_into_a_tenant_org_cannot_confer_a_platform_right(org_runtime, role_id):
    from provisa.api.admin.invites_router import resolve_invite_role

    org_runtime(_role_rows(heir={"capabilities": ["usage"], "parent_role_id": "platform_admin"}))
    # Refused for EVERY inviter, a platform administrator included: platform rights are conferred
    # in the root org only.
    for granter in (set(ROLES["org_admin"]["capabilities"]), DEPLOYMENT_GRANTER):
        with pytest.raises(ApiError) as err:
            await resolve_invite_role("acme", role_id, granter_capabilities=granter)
        assert (err.value.status_code, err.value.code) == (403, "invites.platform_admin_root_only")


@pytest.mark.parametrize("role_id", ["platform_admin", "settings_only"])
async def test_an_invitation_into_root_confers_a_platform_right_only_from_its_holder(
    org_runtime, role_id
):
    from provisa.api.admin.invites_router import resolve_invite_role

    org_runtime(_role_rows())
    with pytest.raises(ApiError) as err:
        await resolve_invite_role(
            ROOT_ORG, role_id, granter_capabilities=set(ROLES["org_admin"]["capabilities"])
        )
    assert (err.value.status_code, err.value.code) == (
        403,
        "users.platform_role_requires_platform_admin",
    )
    held = set(ROLES["org_admin"]["capabilities"]) | set(ROLES["platform_admin"]["capabilities"])
    assert await resolve_invite_role(ROOT_ORG, role_id, granter_capabilities=held) == role_id


async def test_an_invitation_names_a_role_the_org_has(org_runtime):
    from provisa.api.admin.invites_router import resolve_invite_role

    org_runtime(_role_rows())
    granter = set(ROLES["org_admin"]["capabilities"])
    assert await resolve_invite_role("acme", "analyst", granter_capabilities=granter) == "analyst"
    with pytest.raises(ApiError) as err:
        await resolve_invite_role("acme", "everything", granter_capabilities=granter)
    assert (err.value.status_code, err.value.code) == (422, "invites.role_not_in_org")


@pytest.mark.parametrize("role_id", ["platform_admin", "settings_only", "heir"])
async def test_the_grant_itself_refuses_a_platform_right_nobody_holds(role_id):
    """grant_org_role is the one write every grant path ends in — auto-join, an invitation's
    redemption, the bootstrap claim, seating an org's creator."""
    from provisa.core.org_membership import grant_org_role

    db = _Db(_role_rows(heir={"capabilities": ["usage"], "parent_role_id": "platform_admin"}))
    for granter in ((), {"user_management"}, set(ROLES["org_admin"]["capabilities"])):
        with pytest.raises(PlatformRoleGrantError):
            await grant_org_role(db, "u2", role_id, granter_capabilities=granter)
    assert db.conn.writes == []
    await grant_org_role(db, "u2", role_id, granter_capabilities=DEPLOYMENT_GRANTER)
    assert db.conn.writes == [{"user_id": "u2", "role_id": role_id, "domain_id": "*"}]


async def test_a_role_carrying_no_platform_right_is_granted_by_anyone():
    from provisa.core.org_membership import grant_org_role

    db = _Db(_role_rows())
    await grant_org_role(db, "u2", "analyst", granter_capabilities=())
    assert db.conn.writes == [{"user_id": "u2", "role_id": "analyst", "domain_id": "*"}]


async def test_an_auto_join_grants_with_no_rights_at_all(monkeypatch, org_runtime):
    from provisa.api import auto_join
    from provisa.core import commerce, org_membership

    db = org_runtime(_role_rows())
    seated: list[tuple] = []

    async def _membership(_pool, user_id, org_id, **_kw):
        seated.append((user_id, org_id))

    async def _trial(*_a, **_k):
        return None

    monkeypatch.setattr(org_membership, "grant_membership", _membership)
    monkeypatch.setattr(commerce, "bind_member_to_org_trial", _trial)
    with pytest.raises(PlatformRoleGrantError):
        await auto_join.join_org_automatically(object(), "u2", "a@b.co", "acme", "settings_only")
    assert db.conn.writes == []
    await auto_join.join_org_automatically(object(), "u2", "a@b.co", "acme", "analyst")
    assert db.conn.writes == [{"user_id": "u2", "role_id": "analyst", "domain_id": "*"}]


def test_an_auto_join_rule_cannot_name_a_role_carrying_a_platform_right():
    from provisa.api.admin.orgs_router import _validate_org_policy

    for role_id in ("platform_admin", "settings_only"):
        with pytest.raises(ApiError) as err:
            _validate_org_policy(r".*@acme\.example$", True, role_id)
        assert (err.value.status_code, err.value.code) == (
            403,
            "orgs.auto_join_role_carries_platform_right",
        )
    _validate_org_policy(r".*@acme\.example$", True, "analyst")
