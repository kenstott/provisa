# Copyright (c) 2026 Kenneth Stott
# Canary: 98e734ae-90b2-4e2e-b233-5e903f91cd5f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Adding a domain to a role or to a source needs the caller to reach that domain (REQ-1531).

A role's ``domain_access`` is how reach is handed out, and a source's ``allowed_domains`` is how
a source is opened to a domain. Either was changed on a right alone, so a member whose role
reaches one domain could add another. The caller must now reach each domain a change adds —
every domain, when it adds ``*`` or empties a source's list (which opens it to all). Removing a
domain needs no reach.
"""

# Requirements: REQ-1531, REQ-1530, REQ-1677

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import roles_router, schema_mutation
from provisa.api.admin.capabilities import (
    require_reach_of_added_domains,
    require_reach_of_added_domains_request,
)
from provisa.api.admin.types import RoleInput
from provisa.api.errors import ApiError
from provisa.core.database import Database
from provisa.core.schema_org import domains, roles, sources
from provisa.security.rights import domains_added

RIGHTS = ["user_management", "source_registration"]
ROLES = {
    "sales_admin": {
        "id": "sales_admin",
        "description": "Test role.",
        "capabilities": RIGHTS,
        "domain_access": ["sales"],
    },
    "everywhere": {
        "id": "everywhere",
        "description": "Test role.",
        "capabilities": RIGHTS,
        "domain_access": ["*"],
    },
}


# --- what a change adds ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("before", "after", "added"),
    [
        (None, ["sales"], {"sales"}),
        (["sales"], ["sales", "finance"], {"finance"}),
        (["sales", "finance"], ["sales"], set()),
        (["sales"], ["*"], {"*"}),
        (["sales"], ["*", "finance"], {"*"}),
        (["*"], ["sales"], set()),
        (["*"], ["*", "finance"], set()),
        ([], [], set()),
    ],
)
def test_the_domains_a_change_adds_to_a_list(before, after, added):
    assert domains_added(before, after) == added


@pytest.mark.parametrize(
    ("before", "after", "added"),
    [
        (None, [], {"*"}),  # a new source with no list is open to every domain
        (None, ["sales"], {"sales"}),
        (["sales"], [], {"*"}),  # emptying the list opens the source to every domain
        ([], ["sales"], set()),  # naming a domain on an open source narrows it
        ([], [], set()),
        (["sales"], ["sales", "finance"], {"finance"}),
    ],
)
def test_the_domains_a_change_adds_when_an_empty_list_means_all(before, after, added):
    assert domains_added(before, after, empty_is_all=True) == added


# --- the gate --------------------------------------------------------------------------------------


def _request(role_id: str | None) -> Any:
    identity = (
        None
        if role_id is None
        else types.SimpleNamespace(user_id="u1", roles=[role_id], acting_roles=[role_id])
    )
    return types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme")
    )


def _info(role_id: str | None) -> Any:
    return types.SimpleNamespace(context={"request": _request(role_id)})


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        for domain_id in ("sales", "finance"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        await conn.execute_core(
            insert(roles).values(
                id="seller",
                capabilities=["usage"],
                domain_access=["sales"],
                org_id="acme",
            )
        )
        await conn.execute_core(
            insert(roles).values(
                id="auditor",
                capabilities=["usage"],
                domain_access=["finance"],
                org_id="acme",
            )
        )
        await conn.execute_core(
            insert(sources).values(id="warehouse", type="postgresql", allowed_domains=["sales"])
        )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", appmod.state.tenant_db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", ROLES, raising=False)
    monkeypatch.setattr(appmod.state, "source_allowed_domains", {}, raising=False)
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _pool():
        return db

    async def _rebuild() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    # The REST role routes rebuild too; a real rebuild would replace the callers' test roles.
    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    return db


def test_the_gate_asks_for_each_added_domain_and_for_nothing_removed(plane):
    require_reach_of_added_domains(_info("sales_admin"), ["finance", "sales"], ["sales"])
    require_reach_of_added_domains(_info("sales_admin"), None, ["sales"])
    with pytest.raises(PermissionError, match="No access to domain 'finance'"):
        require_reach_of_added_domains(_info("sales_admin"), ["sales"], ["sales", "finance"])
    with pytest.raises(PermissionError, match=r"No access to domain '\*'"):
        require_reach_of_added_domains(_info("sales_admin"), ["sales"], ["*"])
    require_reach_of_added_domains(_info("everywhere"), ["sales"], ["*"])
    require_reach_of_added_domains(_info(None), ["sales"], ["*"])  # no auth provider: no gate
    with pytest.raises(ApiError) as err:
        require_reach_of_added_domains_request(_request("sales_admin"), ["sales"], ["finance"])
    assert (err.value.status_code, err.value.code) == (403, "auth.domain_denied")


async def _role_row(db: Database, role_id: str) -> dict | None:
    async with db.acquire() as conn:
        row = (await conn.execute_core(select(roles).where(roles.c.id == role_id))).fetchone()
    return None if row is None else dict(row._mapping)


# --- a role, through REST ------------------------------------------------------------------------


async def test_rest_creates_a_role_in_the_callers_domain_and_refuses_one_outside_it(plane):
    body = roles_router.CreateRoleBody(
        id="r1", description="Test role.", capabilities=["usage"], domain_access=["sales"]
    )
    await roles_router.create_role(body, _request("sales_admin"))
    assert (await _role_row(plane, "r1"))["domain_access"] == ["sales"]

    for listed in (["finance"], ["sales", "finance"], ["*"]):
        body = roles_router.CreateRoleBody(
            id="r2", description="Test role.", capabilities=["usage"], domain_access=listed
        )
        with pytest.raises(ApiError) as err:
            await roles_router.create_role(body, _request("sales_admin"))
        assert (err.value.status_code, err.value.code) == (403, "auth.domain_denied")
    assert await _role_row(plane, "r2") is None


async def test_rest_a_child_of_a_role_elsewhere_reaches_only_its_own_domains(plane):
    """REQ-2155: reach is never inherited, so a child of the finance auditor listing sales reaches
    sales alone, and a sales administrator may define it."""
    body = roles_router.CreateRoleBody(
        id="r3",
        description="Test role.",
        capabilities=["usage"],
        domain_access=["sales"],
        parents=["auditor"],
    )
    await roles_router.create_role(body, _request("sales_admin"))
    row = await _role_row(plane, "r3")
    assert row is not None and row["domain_access"] == ["sales"]


async def test_rest_update_needs_every_domain_the_role_reaches_before_and_after(plane):
    with pytest.raises(ApiError) as err:
        await roles_router.update_role(
            "seller",
            roles_router.UpdateRoleBody(domain_access=["sales", "finance"]),
            _request("sales_admin"),
        )
    assert err.value.code == "auth.domain_denied"
    assert (await _role_row(plane, "seller"))["domain_access"] == ["sales"]

    await roles_router.update_role(
        "seller",
        roles_router.UpdateRoleBody(domain_access=["sales", "finance"]),
        _request("everywhere"),
    )
    # REQ-1531 (amended 2026-10-07): taking finance away again is an act in finance too -- the
    # role reaches it -- so a sales administrator is refused, and one reaching it may.
    with pytest.raises(ApiError) as err:
        await roles_router.update_role(
            "seller", roles_router.UpdateRoleBody(domain_access=["sales"]), _request("sales_admin")
        )
    assert err.value.code == "auth.domain_denied"
    assert (await _role_row(plane, "seller"))["domain_access"] == ["sales", "finance"]
    await roles_router.update_role(
        "seller", roles_router.UpdateRoleBody(domain_access=["sales"]), _request("everywhere")
    )
    assert (await _role_row(plane, "seller"))["domain_access"] == ["sales"]


# --- a role, through GraphQL ---------------------------------------------------------------------


async def _create_role(caller: str, role_id: str, listed: list[str], parent: str | None = None):
    return await schema_mutation.Mutation().create_role(
        _info(caller),  # type: ignore[arg-type]
        RoleInput(
            id=role_id,
            description="Test role.",
            capabilities=["usage"],
            domain_access=listed,
            parents=[parent] if parent else [],
        ),
    )


async def _update_role(caller: str, role_id: str, listed: list[str]):
    return await schema_mutation.Mutation().update_role(
        _info(caller),  # type: ignore[arg-type]
        RoleInput(
            id=role_id, description="Test role.", capabilities=["usage"], domain_access=listed
        ),
    )


async def test_graphql_creates_a_role_in_the_callers_domain_and_refuses_one_outside_it(plane):
    assert (await _create_role("sales_admin", "g1", ["sales"])).success is True
    for listed in (["finance"], ["*"]):
        with pytest.raises(PermissionError, match="No access to domain"):
            await _create_role("sales_admin", "g2", listed)
    assert await _role_row(plane, "g2") is None
    # REQ-2155: a parent elsewhere hands down capabilities, never reach.
    assert (await _create_role("sales_admin", "g3", ["sales"], "auditor")).success is True
    assert (await _create_role("everywhere", "g2", ["*"])).success is True


async def test_graphql_redefining_a_role_adds_only_what_the_caller_reaches(plane):
    with pytest.raises(PermissionError, match="No access to domain 'finance'"):
        await _update_role("sales_admin", "seller", ["sales", "finance"])
    assert (await _role_row(plane, "seller"))["domain_access"] == ["sales"]
    # REQ-1531 (amended 2026-10-07): the auditor role reaches finance, so redefining it is an act
    # in finance even when finance stays on it.
    with pytest.raises(PermissionError, match="No access to domain 'finance'"):
        await _update_role("sales_admin", "auditor", ["finance"])
    assert (await _update_role("everywhere", "auditor", ["finance"])).success is True


async def test_graphql_create_refuses_a_role_that_exists_and_update_refuses_one_that_does_not(
    plane,
):
    """A create makes a new role, an update changes one: each refuses the other's id by name,
    and writes nothing."""
    before = await _role_row(plane, "auditor")
    taken = await _create_role("everywhere", "auditor", ["*"])
    assert (taken.success, taken.code, taken.params) == (
        False,
        "schema.role_id_taken",
        {"role": "auditor"},
    )
    assert await _role_row(plane, "auditor") == before
    missing = await _update_role("everywhere", "nobody", ["sales"])
    assert (missing.success, missing.code) == (False, "schema.role_not_found")
    assert await _role_row(plane, "nobody") is None


# --- a source ------------------------------------------------------------------------------------


async def _allowed(db: Database) -> list[str]:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(sources.c.allowed_domains).where(sources.c.id == "warehouse")
            )
        ).scalar_one()


async def _set_allowed(caller: str, listed: list[str]):
    return await schema_mutation.Mutation().update_source_allowed_domains(
        _info(caller),  # type: ignore[arg-type]
        "warehouse",
        listed,
    )


async def test_a_source_is_opened_only_to_a_domain_the_caller_reaches(plane):
    with pytest.raises(PermissionError, match="No access to domain 'finance'"):
        await _set_allowed("sales_admin", ["sales", "finance"])
    assert await _allowed(plane) == ["sales"]
    assert (await _set_allowed("everywhere", ["sales", "finance"])).success is True
    # Closing it to finance again needs no reach of finance.
    assert (await _set_allowed("sales_admin", ["sales"])).success is True
    assert await _allowed(plane) == ["sales"]


async def test_emptying_a_sources_list_opens_it_to_every_domain_and_needs_all_of_them(plane):
    with pytest.raises(PermissionError, match=r"No access to domain '\*'"):
        await _set_allowed("sales_admin", [])
    assert await _allowed(plane) == ["sales"]
    assert (await _set_allowed("everywhere", [])).success is True
    assert await _allowed(plane) == []
    # An open source narrowed to the caller's domain: nothing is added.
    assert (await _set_allowed("sales_admin", ["sales"])).success is True
