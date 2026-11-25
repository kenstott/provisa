# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e0c61-2f4d-4a7e-9c15-7d2e6a91b4f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A domain's stewards are people holding domain_steward rows (REQ-609, REQ-2219 amended
2026-11-23).

The Steward field names people; saving it writes or ends their domain_steward rows. A domain is
stewarded when a person's row names it or a domain above it; a role holding domain_steward with no
list stewards nothing by it. System domains are exempt from the steward gate and none is seeded a
steward. These run through the admin API on a model store."""

# Requirements: REQ-609, REQ-2219

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation, schema_query
from provisa.api.admin.types import DomainInput
from provisa.core.models import Domain
from provisa.core.repositories import domain as domain_repo
from provisa.core.schema_org import object_owners, roles, user_role_assignments
from provisa.entitlements.owners import (
    DOMAINS,
    holders,
    set_domain_stewards,
    unstewarded_domains,
    write_object_owners,
)

BU, UK, NOBODY = "bu", "uk", "nobody"


@pytest.fixture
async def plane(monkeypatch, control_plane):
    """``retail`` holds ``retail_emea``, which holds ``retail_emea_uk``; ``finance`` stands alone.
    ``bu`` and ``uk`` hold a role carrying domain_steward with no list; ``nobody`` holds a role
    without it."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await domain_repo.upsert(conn, Domain(id="retail"))
        await domain_repo.upsert(conn, Domain(id="retail_emea", parent_domain_id="retail"))
        await domain_repo.upsert(conn, Domain(id="retail_emea_uk", parent_domain_id="retail_emea"))
        await domain_repo.upsert(conn, Domain(id="finance"))
        for role, capabilities in (("stewards", ["domain_steward"]), ("readers", [])):
            await conn.execute_core(
                insert(roles).values(
                    id=role, capabilities=capabilities, domain_access=["*"], parents=[]
                )
            )
        for user, role in ((BU, "stewards"), (UK, "stewards"), (NOBODY, "readers")):
            await conn.execute_core(
                insert(user_role_assignments).values(user_id=user, role_id=role, domain_id="*")
            )

    async def _pool():
        return db

    async def _rebuild():
        return None

    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "names_table", None, raising=False)
    monkeypatch.setattr(appmod.state, "held_entitlements", {}, raising=False)
    monkeypatch.setattr(appmod.state, "data_product_refs", (), raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    return db


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


async def _stewards(db, *, exact: bool = False) -> dict[str, tuple[str, ...]]:
    async with db.acquire() as conn:
        return await holders(conn, DOMAINS, exact=exact)


async def _unstewarded(db) -> list[str]:
    async with db.acquire() as conn:
        return await unstewarded_domains(conn)


async def _set(db, domain_id: str, people: list[str]) -> None:
    async with db.acquire() as conn:
        await set_domain_stewards(conn, domain_id, people, set_by=None)


async def test_naming_a_steward_writes_their_row_and_the_domain_is_stewarded(plane):
    result = await schema_mutation.Mutation().set_domain_stewards(_info(), "finance", [BU])
    assert (result.success, result.code) == (True, "schema.domain_stewards_saved")
    assert (await _stewards(plane, exact=True))["finance"] == (BU,)
    assert "finance" not in await _unstewarded(plane)


async def test_unnaming_a_steward_ends_their_row_and_the_domain_is_pending_again(plane):
    await _set(plane, "finance", [BU])
    result = await schema_mutation.Mutation().set_domain_stewards(_info(), "finance", [])
    assert result.success
    assert "finance" not in await _stewards(plane)
    assert "finance" in await _unstewarded(plane)


async def test_a_person_without_the_capability_is_refused_and_nothing_is_written(plane):
    result = await schema_mutation.Mutation().set_domain_stewards(_info(), "finance", [BU, NOBODY])
    assert (result.success, result.code) == (False, "domain.steward_not_eligible")
    assert result.params == {"domain": "finance", "steward": NOBODY}
    assert "finance" not in await _stewards(plane)


async def test_an_unknown_domain_is_refused_by_name(plane):
    result = await schema_mutation.Mutation().set_domain_stewards(_info(), "ghost", [BU])
    assert (result.success, result.code) == (False, "schema.domain_not_found")


async def test_a_role_holding_the_capability_with_no_list_stewards_nothing(plane):
    # bu and uk both hold domain_steward on a role with no list: no domain is stewarded by it.
    assert await _stewards(plane) == {}
    assert await _unstewarded(plane) == ["finance", "retail", "retail_emea", "retail_emea_uk"]


async def test_a_domain_with_no_steward_answers_to_its_nearest_ancestors(plane):
    await _set(plane, "retail", [BU])
    covering = await _stewards(plane)
    assert covering["retail_emea"] == (BU,)
    assert covering["retail_emea_uk"] == (BU,)
    assert "retail_emea" not in await _stewards(plane, exact=True)


async def test_a_domain_keeps_its_own_steward_beside_its_ancestors(plane):
    await _set(plane, "retail", [BU])
    await _set(plane, "retail_emea_uk", [UK])
    assert (await _stewards(plane, exact=True))["retail_emea_uk"] == (UK,)
    assert (await _stewards(plane))["retail_emea_uk"] == (BU, UK)


async def test_no_steward_up_the_tree_leaves_the_domain_pending(plane):
    await _set(plane, "retail", [BU])
    assert await _unstewarded(plane) == ["finance"]


async def test_naming_a_child_steward_leaves_the_parents_rows_alone(plane):
    await _set(plane, "retail", [BU])
    await _set(plane, "retail_emea", [UK])
    await _set(plane, "retail_emea", [])
    assert (await _stewards(plane, exact=True)) == {"retail": (BU,)}


async def test_system_domains_are_exempt_and_none_is_seeded_a_steward(plane):
    async with plane.acquire() as conn:
        listed = {r.id for r in (await conn.execute_core(select(domain_repo.domains.c.id)))}
    assert {"ops", "meta", "ontology"} <= listed
    unstewarded = await _unstewarded(plane)
    assert not {"", "ops", "meta", "ontology"} & set(unstewarded)
    assert not {"", "ops", "meta", "ontology"} & set(await _stewards(plane))


async def test_a_domain_created_with_stewards_names_them(plane):
    created = await schema_mutation.Mutation().create_domain(
        _info(), DomainInput(id="sales", stewards=[UK])
    )
    assert created.success
    assert (await _stewards(plane, exact=True))["sales"] == (UK,)


async def test_a_domain_created_with_an_ineligible_steward_is_not_created(plane):
    created = await schema_mutation.Mutation().create_domain(
        _info(), DomainInput(id="sales", stewards=[NOBODY])
    )
    assert (created.success, created.code) == (False, "domain.steward_not_eligible")
    async with plane.acquire() as conn:
        assert await domain_repo.get(conn, "sales") is None


async def test_the_domains_query_reports_own_and_inherited_stewards(plane, monkeypatch):
    await _set(plane, "retail", [BU])
    scope = types.SimpleNamespace(lists_domain=lambda _domain, _holding: True)

    async def _pool():
        return plane

    async def _no_tables(_conn, _scope):
        return []

    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "catalog_scope", lambda _info: scope)
    monkeypatch.setattr(schema_query, "_listed_tables", _no_tables)
    monkeypatch.setattr(schema_query, "_resolve_admin_context", lambda _info: None)
    listed = {d.id: d for d in await schema_query.Query().domains(_info())}
    assert (listed["retail"].stewards, listed["retail"].effective_stewards) == ([BU], [BU])
    assert (listed["retail_emea"].stewards, listed["retail_emea"].effective_stewards) == ([], [BU])
    assert (listed["finance"].stewards, listed["finance"].effective_stewards) == ([], [])


async def test_the_meta_views_read_what_the_build_derives(plane):
    await _set(plane, "retail", [BU])
    async with plane.acquire() as conn:
        await write_object_owners(conn)
        rows = (
            await conn.execute_core(
                select(object_owners.c.object_id, object_owners.c.principal).where(
                    object_owners.c.object_kind == "domain"
                )
            )
        ).fetchall()
    assert sorted((r.object_id, r.principal) for r in rows) == [
        ("retail", BU),
        ("retail_emea", BU),
        ("retail_emea_uk", BU),
    ]


def test_a_config_naming_a_steward_is_refused():
    with pytest.raises(ValueError, match="steward is no longer a setting"):
        Domain.model_validate({"id": "sales", "steward": "ana"})
