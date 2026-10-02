# Copyright (c) 2026 Kenneth Stott
# Canary: d0e1b18b-635e-4a57-a8fd-6f2a1ca48283
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One delete path for a role (REQ-042, REQ-1677).

The GraphQL ``deleteRole`` mutation and REST ``DELETE /admin/roles/{id}`` both call
``role_repo.delete``, so both refuse a role the deployment defines and a role other roles
inherit from, holds, or names in a grant (REQ-1918), and both rebuild the schemas when a role
goes. A role the deployment defines is
one whose row carries no org — what the seed writes; a role an administrator creates records
the org it was created in.

Run here on the SQLite control plane; ``tests/integration/test_role_delete_paths.py`` runs the
same cases on PostgreSQL through a served instance.
"""

# Requirements: REQ-042, REQ-1677, REQ-1297

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import select

import provisa.api.app as appmod
from provisa.api.admin import roles_router, schema_mutation
from provisa.api.errors import ApiError
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import Role
from provisa.core.repositories import role as role_repo
from provisa.core.schema_org import roles

ORG = "acme"


def _role(role_id: str, parent: str | None = None) -> Role:
    return Role(
        id=role_id,
        capabilities=["query_development"],
        domain_access=["*"],
        parent_role_id=parent,
    )


@pytest.fixture
async def plane(monkeypatch) -> Database:
    """A seeded SQLite control plane holding: the seeded roles; ``from_config`` (declared by the
    deployment's config); ``base`` with its heir ``derived``; and ``loose``, which nothing
    inherits from. The last three were created by an administrator of ORG."""
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="role-delete-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await role_repo.upsert(conn, _role("from_config"), org_id=None, origin="config")
        await role_repo.upsert(conn, _role("base"), org_id=ORG, origin="admin")
        await role_repo.upsert(conn, _role("derived", parent="base"), org_id=ORG, origin="admin")
        await role_repo.upsert(conn, _role("loose"), org_id=ORG, origin="admin")
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    return db


@pytest.fixture
def rebuilds(monkeypatch) -> list[int]:
    seen: list[int] = []

    async def _rebuild() -> None:
        seen.append(1)

    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    return seen


async def _ids(db: Database) -> set[str]:
    async with db.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(roles.c.id))).fetchall()}


# --- the rule ------------------------------------------------------------------------------------


async def test_the_seed_marks_its_roles_with_no_org_and_a_created_role_records_one(plane):
    async with plane.acquire() as conn:
        rows = {r["id"]: r["org_id"] for r in await role_repo.list_all(conn)}
    for seeded in ("org_admin", "analyst", "developer", "platform_admin"):
        assert rows[seeded] is None, seeded
    assert rows["from_config"] is None
    assert (rows["base"], rows["derived"], rows["loose"]) == (ORG, ORG, ORG)


async def test_every_role_records_where_it_came_from(plane):
    async with plane.acquire() as conn:
        origin = {r["id"]: r["origin"] for r in await role_repo.list_all(conn)}
    for seeded in ("org_admin", "analyst", "developer", "platform_admin"):
        assert origin[seeded] == "seed", seeded
    assert origin["from_config"] == "config"
    assert (origin["base"], origin["derived"], origin["loose"]) == ("admin", "admin", "admin")


async def test_an_admin_edit_leaves_the_origin_and_a_config_load_takes_an_admin_role_over(
    plane, caplog
):
    async with plane.acquire() as conn:
        await role_repo.upsert(conn, _role("from_config"), org_id=ORG, origin="admin")
        await role_repo.upsert(conn, _role("analyst"), org_id=None, origin="config")
        with caplog.at_level("INFO", logger="provisa.core.repositories.origin"):
            await role_repo.upsert(conn, _role("loose"), org_id=None, origin="config")
        origin = {r["id"]: r["origin"] for r in await role_repo.list_all(conn)}
    # An edit through the admin does not make a config's role the admin's, and a config that
    # declares a seeded role does not make it the config's.
    assert (origin["from_config"], origin["analyst"]) == ("config", "seed")
    # A config that declares a role made through the admin takes it over, and says so.
    assert origin["loose"] == "config"
    assert [r.getMessage() for r in caplog.records] == [
        "config load takes over role 'loose', which was made through the admin: "
        "it is now declared by the config"
    ]


async def test_an_origin_outside_the_three_is_refused(plane):
    async with plane.acquire() as conn:
        with pytest.raises(ValueError, match="origin must be one of"):
            await role_repo.upsert(conn, _role("other"), org_id=ORG, origin="system")


async def test_redefining_a_role_does_not_change_the_org_it_was_created_in(plane):
    async with plane.acquire() as conn:
        await role_repo.upsert(conn, _role("loose"), org_id=None, origin="admin")
        await role_repo.upsert(conn, _role("from_config"), org_id=ORG, origin="admin")
        rows = {r["id"]: r["org_id"] for r in await role_repo.list_all(conn)}
    assert (rows["loose"], rows["from_config"]) == (ORG, None)


@pytest.mark.parametrize("role_id", ["org_admin", "analyst", "platform_admin"])
async def test_a_role_the_deployment_seeds_is_refused(plane, role_id):
    async with plane.acquire() as conn:
        with pytest.raises(role_repo.RoleDeleteRefused) as err:
            await role_repo.delete(conn, role_id)
    assert (err.value.reason, err.value.role_id) == ("system", role_id)
    assert role_id in await _ids(plane)


async def test_a_role_a_config_declared_may_be_deleted(plane):
    """Only a seeded role is a system role. A config's role may go; the next load of a file that
    still declares it brings it back, which the delete's answer says."""
    async with plane.acquire() as conn:
        assert await role_repo.delete(conn, "from_config") is True
    assert "from_config" not in await _ids(plane)


def _named(refused: role_repo.RoleDeleteRefused) -> list[tuple[str, object, tuple[str, ...]]]:
    return [(d.ref.kind, d.ref.id, d.via) for d in refused.dependents]


async def test_a_role_others_inherit_from_is_refused_naming_them(plane):
    async with plane.acquire() as conn:
        with pytest.raises(role_repo.RoleDeleteRefused) as err:
            await role_repo.delete(conn, "base")
    assert err.value.reason == "dependents"
    assert _named(err.value) == [("role", "derived", ("roles.parent_role_id",))]
    assert "role derived" in str(err.value)
    assert "base" in await _ids(plane)


async def test_a_role_someone_holds_or_a_grant_names_is_refused_naming_each(plane):
    from provisa.core.schema_org import metrics, rls_rules, user_role_assignments

    async with plane.acquire() as conn:
        await conn.execute_core(
            user_role_assignments.insert().values(user_id="u1", role_id="loose", domain_id="*")
        )
        await conn.execute_core(
            metrics.insert().values(name="revenue", expression="SUM(o.a)", visible_to=["loose"])
        )
        await conn.execute_core(rls_rules.insert().values(role_id="loose", filter_expr=b"1=1"))
        with pytest.raises(role_repo.RoleDeleteRefused) as err:
            await role_repo.delete(conn, "loose")
        assert [(kind, via) for kind, _, via in _named(err.value)] == [
            ("metric", ("metrics.visible_to",)),
            ("role_assignment", ("user_role_assignments.role_id",)),
        ]
        assert "loose" in await _ids(plane)

        # Once nobody holds it and no grant names it, it goes, and its row filter with it.
        await conn.execute_core(user_role_assignments.delete())
        await conn.execute_core(metrics.delete())
        assert await role_repo.delete(conn, "loose") is True
        left = (await conn.execute_core(select(rls_rules.c.role_id))).fetchall()
    assert [r[0] for r in left if r[0] == "loose"] == []


async def test_a_created_role_nothing_inherits_from_is_deleted(plane):
    async with plane.acquire() as conn:
        assert await role_repo.delete(conn, "loose") is True
        assert await role_repo.delete(conn, "loose") is False
    assert "loose" not in await _ids(plane)


async def test_a_parent_goes_once_its_heir_has(plane):
    async with plane.acquire() as conn:
        assert await role_repo.delete(conn, "derived") is True
        assert await role_repo.delete(conn, "base") is True


# --- the two entry points ------------------------------------------------------------------------


def _request() -> Any:
    """The anonymous dev identity: the capability gate is not what these cases are about."""
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id=ORG))


async def _rest(role_id: str) -> dict:
    return await roles_router.delete_role(role_id, _request())


async def _graphql(role_id: str):
    info = types.SimpleNamespace(context={"request": _request()})
    return await schema_mutation.Mutation().delete_role(info, role_id)  # type: ignore[arg-type]


@pytest.fixture
def graphql_pool(plane, monkeypatch):
    async def _pool():
        return plane

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)


@pytest.mark.parametrize("role_id", ["analyst"])
async def test_rest_refuses_a_role_the_deployment_seeds(plane, rebuilds, role_id):
    with pytest.raises(ApiError) as err:
        await _rest(role_id)
    assert (err.value.status_code, err.value.code) == (400, "roles.cannot_delete_system")
    assert role_id in await _ids(plane) and rebuilds == []


@pytest.mark.parametrize("role_id", ["analyst"])
async def test_graphql_refuses_a_role_the_deployment_seeds(plane, graphql_pool, rebuilds, role_id):
    result = await _graphql(role_id)
    assert (result.success, result.code) == (False, "schema.role_is_system")
    assert role_id in await _ids(plane) and rebuilds == []


async def test_rest_refuses_a_parent_naming_its_heirs(plane, rebuilds):
    with pytest.raises(ApiError) as err:
        await _rest("base")
    assert (err.value.status_code, err.value.code) == (409, "roles.has_dependents")
    assert err.value.params == {
        "role": "base",
        "count": 1,
        "dependents": [
            {"kind": "role", "id": "derived", "name": "derived", "via": ["roles.parent_role_id"]}
        ],
    }
    assert "base" in await _ids(plane) and rebuilds == []


async def test_graphql_refuses_a_parent_naming_its_heirs(plane, graphql_pool, rebuilds):
    result = await _graphql("base")
    assert (result.success, result.code) == (False, "schema.role_has_dependents")
    assert result.params == {
        "role": "base",
        "dependents": [
            {"kind": "role", "id": "derived", "name": "derived", "via": ["roles.parent_role_id"]}
        ],
    }
    assert "base" in await _ids(plane) and rebuilds == []


async def test_rest_deletes_a_created_role_and_rebuilds(plane, rebuilds):
    assert await _rest("loose") == {"deleted": "loose", "warnings": []}
    assert "loose" not in await _ids(plane) and rebuilds == [1]


async def test_graphql_deletes_a_created_role_and_rebuilds(plane, graphql_pool, rebuilds):
    result = await _graphql("loose")
    assert (result.success, result.code) == (True, "schema.role_deleted")
    assert "loose" not in await _ids(plane) and rebuilds == [1]


async def test_rest_answers_not_found_for_a_role_that_is_not_there(plane, rebuilds):
    with pytest.raises(ApiError) as err:
        await _rest("nobody")
    assert (err.value.status_code, err.value.code) == (404, "roles.not_found")
    assert rebuilds == []


async def test_graphql_answers_not_found_for_a_role_that_is_not_there(
    plane, graphql_pool, rebuilds
):
    result = await _graphql("nobody")
    assert (result.success, result.code) == (False, "schema.role_not_found")
    assert rebuilds == []


# --- a config's role: the delete is made, and the answer says what the next load does (REQ-1919) --

_CONFIG_DELETED = {
    "code": "origin.config_object_deleted",
    "message": (
        "role 'from_config' is declared in the config: the next load of the config creates it "
        "again while the file still declares it"
    ),
    "params": {"kind": "role", "name": "from_config"},
}


async def test_rest_deletes_a_config_role_and_says_the_next_load_brings_it_back(plane, rebuilds):
    assert await _rest("from_config") == {"deleted": "from_config", "warnings": [_CONFIG_DELETED]}
    assert "from_config" not in await _ids(plane)


async def test_graphql_deletes_a_config_role_and_says_the_next_load_brings_it_back(
    plane, graphql_pool, rebuilds
):
    result = await _graphql("from_config")
    assert (result.success, result.code) == (True, "schema.role_deleted")
    assert [(w.code, w.message, w.params) for w in result.warnings] == [
        (_CONFIG_DELETED["code"], _CONFIG_DELETED["message"], _CONFIG_DELETED["params"])
    ]
    assert "from_config" not in await _ids(plane)


async def test_deleting_a_role_made_through_the_admin_carries_no_warning(
    plane, graphql_pool, rebuilds
):
    assert await _rest("loose") == {"deleted": "loose", "warnings": []}
    result = await _graphql("derived")
    assert (result.success, result.warnings) == (True, [])


async def test_rest_lists_each_roles_origin(plane):
    listed = {r["id"]: r["origin"] for r in await roles_router.list_roles(_request())}
    assert (listed["analyst"], listed["from_config"], listed["loose"]) == (
        "seed",
        "config",
        "admin",
    )
