# Copyright (c) 2026 Kenneth Stott
# Canary: 0a4ae16c-17ac-45ef-a731-f002e1ed907f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source is deleted only when no table is registered against it (REQ-1918).

The model store's ``source.delete`` used to delete the source's registered tables with it —
and, through them on PostgreSQL, every relationship, row filter and tag that referred to those
tables. It now asks the dependency guard: while a table is registered against the source or a
command is bound to it, the delete is refused naming each one and nothing is removed. When it
may go, its remote-registration rows and its tag assignments go with it.
"""

# Requirements: REQ-1918, REQ-1919, REQ-014

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.repositories import source as source_repo
from provisa.core.schema_org import (
    api_endpoints,
    api_sources,
    domains,
    registered_tables,
    sources,
    tag_assignments,
    tracked_functions,
)


@pytest.fixture
async def plane(monkeypatch) -> Database:
    """``crm`` has a registered table and a command bound to it; ``api`` has neither, and an API
    registration with an endpoint and a tag."""
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="source-delete-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales", origin="admin"))
        for source_id, kind in (("crm", "postgresql"), ("api", "openapi")):
            await conn.execute_core(insert(sources).values(id=source_id, type=kind, origin="admin"))
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="crm",
                domain_id="sales",
                schema_name="public",
                table_name="customers",
                origin="admin",
            )
        )
        await conn.execute_core(
            insert(tracked_functions).values(
                name="refund", source_id="crm", domain_id="sales", origin="admin"
            )
        )
        await conn.execute_core(
            insert(api_sources).values(id="api", type="openapi", base_url="http://x")
        )
        await conn.execute_core(
            insert(api_endpoints).values(source_id="api", path="/p", table_name="t", columns=[])
        )
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="pii",
                base_tag_id="pii",
                object_type="source",
                object_key="api",
                source_id="api",
                origin="admin",
            )
        )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    return db


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


async def _source_ids(db: Database) -> set[str]:
    async with db.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(sources.c.id))).fetchall()}


async def test_a_source_with_a_table_or_a_command_is_refused_naming_each(plane):
    async with plane.acquire() as conn:
        table_id = (await conn.execute_core(select(registered_tables.c.id))).scalar_one()
        with pytest.raises(source_repo.SourceDeleteRefused) as err:
            await source_repo.delete(conn, "crm")
    assert err.value.reason == "dependents"
    assert [(d.ref.kind, d.ref.id, d.via) for d in err.value.dependents] == [
        ("command", "refund", ("tracked_functions.source_id",)),
        ("table", table_id, ("registered_tables.source_id",)),
    ]
    assert "crm" in await _source_ids(plane)
    assert await _count(plane, registered_tables) == 1  # its table was NOT taken with it


async def test_a_source_nothing_is_registered_against_goes_with_its_registration_rows(plane):
    async with plane.acquire() as conn:
        assert await source_repo.delete(conn, "api") is True
        assert await source_repo.delete(conn, "api") is False
    assert "api" not in await _source_ids(plane)
    assert await _count(plane, api_sources) == 0 and await _count(plane, api_endpoints) == 0
    assert await _count(plane, tag_assignments) == 0


async def test_the_source_goes_once_its_table_and_command_have(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(registered_tables.delete())
        await conn.execute_core(tracked_functions.delete())
        assert await source_repo.delete(conn, "crm") is True


async def test_a_source_the_deployment_keeps_is_refused(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(sources).values(id=DERIVED_SOURCE_ID, type="duckdb", origin="admin")
        )
        with pytest.raises(source_repo.SourceDeleteRefused) as err:
            await source_repo.delete(conn, DERIVED_SOURCE_ID)
    assert err.value.reason == "system" and DERIVED_SOURCE_ID in await _source_ids(plane)


async def test_a_declared_set_is_removed_without_the_guard(plane):
    async with plane.acquire() as conn:
        await source_repo.remove_where(conn, sources.c.id.not_in(["api"]))
    assert await _source_ids(plane) == {"api"}


# --- the mutation --------------------------------------------------------------------------------


@pytest.fixture
def served(plane, monkeypatch) -> dict[str, list]:
    seen: dict[str, list] = {"rebuilds": [], "dropped": [], "forgotten": []}

    async def _pool():
        return plane

    async def _rebuild() -> None:
        seen["rebuilds"].append(1)

    async def _forget(source_id, _ref) -> None:
        seen["forgotten"].append(source_id)

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(schema_mutation, "forget_source_password", _forget)
    monkeypatch.setattr(
        schema_mutation, "_drop_source_on_engine", lambda _state, sid: seen["dropped"].append(sid)
    )
    monkeypatch.setattr(appmod.state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(appmod.state, "source_catalogs", {}, raising=False)
    return seen


async def _delete_source(source_id: str) -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    info = types.SimpleNamespace(context={"request": request})
    return await schema_mutation.Mutation().delete_source(info, source_id)  # type: ignore[arg-type]


async def test_the_mutation_refuses_with_the_list_and_leaves_the_engine_alone(plane, served):
    result = await _delete_source("crm")
    assert (result.success, result.code) == (False, "schema.source_has_dependents")
    assert {(d["kind"], d["via"][0]) for d in result.params["dependents"]} == {
        ("command", "tracked_functions.source_id"),
        ("table", "registered_tables.source_id"),
    }
    assert "crm" in await _source_ids(plane)
    assert served == {"rebuilds": [], "dropped": [], "forgotten": []}


async def test_the_mutation_deletes_drops_the_catalog_and_rebuilds(plane, served):
    result = await _delete_source("api")
    assert (result.success, result.code) == (True, "schema.source_deleted")
    assert served == {"rebuilds": [1], "dropped": ["api"], "forgotten": ["api"]}


async def test_the_mutation_answers_not_found(plane, served):
    result = await _delete_source("nowhere")
    assert (result.success, result.code) == (False, "schema.source_not_found")
