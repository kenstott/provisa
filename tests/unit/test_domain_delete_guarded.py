# Copyright (c) 2026 Kenneth Stott
# Canary: d2ceb54f-9e57-4a23-a960-8055f042ba1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A domain is deleted only when nothing refers to it (REQ-1917).

The model store's ``domain.delete`` asks the dependency guard: while a table sits in the
domain, a role lists it, a source allows it, or anything else refers to it, the delete is
refused naming each dependent and nothing is removed. ``deleteDomain`` carries that list.
Run here on the SQLite control plane, where no foreign key would have stopped or cascaded
anything; ``tests/integration/test_domain_delete_guarded.py`` runs it on both planes through a
served instance.
"""

# Requirements: REQ-1917, REQ-1918, REQ-1919

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.repositories import domain as domain_repo
from provisa.core.schema_org import (
    domains,
    registered_tables,
    roles,
    sources,
    table_columns,
    user_role_assignments,
)


@pytest.fixture
async def plane(monkeypatch) -> Database:
    """``sales`` holds a table with a column, is listed by a role someone is assigned on it, and
    is allowed by the source. ``empty`` is referred to by nothing."""
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="domain-delete-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        for domain_id in ("sales", "empty"):
            await conn.execute_core(insert(domains).values(id=domain_id, origin="admin"))
        await conn.execute_core(
            insert(sources).values(
                id="pg", type="postgresql", allowed_domains=["sales"], origin="admin"
            )
        )
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                origin="admin",
            )
        )
        table_id = (await conn.execute_core(select(registered_tables.c.id))).scalar_one()
        await conn.execute_core(
            insert(table_columns).values(table_id=table_id, column_name="id", domain_id="sales")
        )
        await conn.execute_core(
            insert(roles).values(
                id="seller", capabilities=[], domain_access=["sales"], org_id="a", origin="admin"
            )
        )
        await conn.execute_core(
            insert(user_role_assignments).values(user_id="u1", role_id="seller", domain_id="sales")
        )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    return db


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


async def _domain_ids(db: Database) -> set[str]:
    async with db.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(domains.c.id))).fetchall()}


# --- the model store's delete --------------------------------------------------------------------


async def test_a_domain_something_refers_to_is_refused_naming_each_and_nothing_is_removed(plane):
    before = {t.name: await _count(plane, t) for t in (registered_tables, table_columns, roles)}
    async with plane.acquire() as conn:
        with pytest.raises(domain_repo.DomainDeleteRefused) as err:
            await domain_repo.delete(conn, "sales")
    assert err.value.reason == "dependents"
    assert [(d.ref.kind, d.via) for d in err.value.dependents] == [
        ("role", ("roles.domain_access",)),
        ("role_assignment", ("user_role_assignments.domain_id",)),
        ("source", ("sources.allowed_domains",)),
        ("table", ("registered_tables.domain_id", "table_columns.domain_id")),
    ]
    for named in ("role seller", "source pg"):
        assert named in str(err.value)
    assert "sales" in await _domain_ids(plane)
    assert before == {
        t.name: await _count(plane, t) for t in (registered_tables, table_columns, roles)
    }


async def test_a_domain_nothing_refers_to_is_deleted(plane):
    async with plane.acquire() as conn:
        assert await domain_repo.delete(conn, "empty") is True
        assert await domain_repo.delete(conn, "empty") is False
    assert await _domain_ids(plane) >= {"sales"} and "empty" not in await _domain_ids(plane)


async def test_the_domain_goes_once_everything_that_referred_to_it_has(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(user_role_assignments.delete())
        await conn.execute_core(roles.delete().where(roles.c.id == "seller"))
        await conn.execute_core(table_columns.delete())
        await conn.execute_core(registered_tables.delete())
        await conn.execute_core(sources.update().values(allowed_domains=[]))
        assert await domain_repo.delete(conn, "sales") is True


async def test_a_domain_the_deployment_keeps_is_refused(plane, monkeypatch):
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "system_domain_ids", lambda: ["empty"])
    async with plane.acquire() as conn:
        with pytest.raises(domain_repo.DomainDeleteRefused) as err:
            await domain_repo.delete(conn, "empty")
    assert err.value.reason == "system" and "empty" in await _domain_ids(plane)


# --- the mutation --------------------------------------------------------------------------------


@pytest.fixture
def served(plane, monkeypatch) -> list[int]:
    rebuilds: list[int] = []

    async def _pool():
        return plane

    async def _rebuild() -> None:
        rebuilds.append(1)

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    return rebuilds


async def _delete_domain(domain_id: str) -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    info = types.SimpleNamespace(context={"request": request})
    return await schema_mutation.Mutation().delete_domain(info, domain_id)  # type: ignore[arg-type]


async def test_the_mutation_refuses_with_the_list_and_does_not_rebuild(plane, served):
    result = await _delete_domain("sales")
    assert (result.success, result.code) == (False, "schema.domain_has_dependents")
    assert result.params["domain"] == "sales"
    assert {(d["kind"], d["id"]) for d in result.params["dependents"]} >= {
        ("role", "seller"),
        ("source", "pg"),
    }
    assert [d["via"] for d in result.params["dependents"] if d["kind"] == "table"] == [
        ["registered_tables.domain_id", "table_columns.domain_id"]
    ]
    assert "sales" in await _domain_ids(plane) and served == []


async def test_the_mutation_deletes_an_unreferred_domain_and_rebuilds(plane, served):
    result = await _delete_domain("empty")
    assert (result.success, result.code) == (True, "schema.domain_deleted")
    assert "empty" not in await _domain_ids(plane) and served == [1]


async def test_the_mutation_answers_not_found(plane, served):
    result = await _delete_domain("nowhere")
    assert (result.success, result.code) == (False, "schema.domain_not_found") and served == []
