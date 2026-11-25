# Copyright (c) 2026 Kenneth Stott
# Canary: 0ebead0f-6c35-4904-b6dc-bd0ff9f8a885
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The domain tree in the model store (REQ-1363): a domain is created under its parent, which
must exist and may not sit under it; a domain with children is not deleted; a config states the
tree whole, and one that does not hold fails the load by name."""

# Requirements: REQ-1363

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import DomainInput
from provisa.core.database import Database
from provisa.core.models import Domain, ProvisaConfig
from provisa.core.repositories import domain as domain_repo
from provisa.core.schema_org import domains


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    """``retail`` holds ``retail_emea``; ``finance`` stands alone."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await domain_repo.upsert(conn, Domain(id="retail"))
        await domain_repo.upsert(conn, Domain(id="retail_emea", parent_domain_id="retail"))
        await domain_repo.upsert(conn, Domain(id="finance"))

    async def _pool():
        return db

    rebuilds: list[int] = []

    async def _rebuild():
        rebuilds.append(1)

    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    db.rebuilds = rebuilds  # type: ignore[attr-defined]
    return db


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


async def _parent_of(db: Database, domain_id: str) -> str | None:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(domains.c.parent_domain_id).where(domains.c.id == domain_id)
            )
        ).scalar_one()


async def test_the_parent_is_stored_with_the_domain(plane):
    assert await _parent_of(plane, "retail_emea") == "retail"
    assert await _parent_of(plane, "retail") is None


async def test_a_domain_with_children_is_not_deleted_and_the_children_are_named(plane):
    async with plane.acquire() as conn:
        with pytest.raises(domain_repo.DomainDeleteRefused) as err:
            await domain_repo.delete(conn, "retail")
        assert [(d.ref.kind, d.ref.id) for d in err.value.dependents] == [("domain", "retail_emea")]
        assert await domain_repo.delete(conn, "retail_emea") is True
        assert await domain_repo.delete(conn, "retail") is True


async def test_an_unknown_parent_is_refused_by_name(plane):
    result = await schema_mutation.Mutation().create_domain(
        _info(), DomainInput(id="retail_apac", parent_domain_id="ghost")
    )
    assert (result.success, result.code) == (False, "schema.domain_parent_invalid")
    assert result.message == "Parent domain 'ghost' not found"
    async with plane.acquire() as conn:
        assert await domain_repo.get(conn, "retail_apac") is None
    assert plane.rebuilds == []


async def test_a_domain_created_under_a_parent_rebuilds_what_roles_reach(plane):
    created = await schema_mutation.Mutation().create_domain(
        _info(), DomainInput(id="retail_apac", parent_domain_id="retail")
    )
    assert created.success and plane.rebuilds == [1]
    assert await _parent_of(plane, "retail_apac") == "retail"


async def test_a_domain_created_with_no_parent_does_not_rebuild(plane):
    created = await schema_mutation.Mutation().create_domain(_info(), DomainInput(id="standalone"))
    assert created.success and plane.rebuilds == []


async def test_the_domains_query_reports_the_parent(plane, monkeypatch):
    # Its stewards, own and inherited down the tree: test_domain_steward_gate (REQ-2219).
    from provisa.api.admin import schema_query

    async with plane.acquire() as conn:
        await domain_repo.upsert(conn, Domain(id="retail"))
        await domain_repo.upsert(conn, Domain(id="retail_emea", parent_domain_id="retail"))
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
    assert listed["retail_emea"].parent_domain_id == "retail"
    assert listed["finance"].parent_domain_id is None


def _config(domain_rows: list[dict]) -> dict:
    return {"sources": [], "domains": domain_rows, "tables": [], "relationships": [], "roles": []}


def test_a_config_states_the_tree():
    config = ProvisaConfig.model_validate(
        _config([{"id": "retail"}, {"id": "retail_emea", "parent_domain_id": "retail"}])
    )
    assert {d.id: d.parent_domain_id for d in config.domains} == {
        "retail": None,
        "retail_emea": "retail",
    }


def test_a_config_naming_a_parent_that_is_no_domain_fails_the_load():
    with pytest.raises(ValueError, match="names the parent 'ghost', which is not a domain"):
        ProvisaConfig.model_validate(_config([{"id": "a", "parent_domain_id": "ghost"}]))


def test_a_config_with_a_cycle_fails_the_load_naming_it():
    with pytest.raises(ValueError, match="domain hierarchy cycle: a -> b -> a"):
        ProvisaConfig.model_validate(
            _config([{"id": "a", "parent_domain_id": "b"}, {"id": "b", "parent_domain_id": "a"}])
        )
