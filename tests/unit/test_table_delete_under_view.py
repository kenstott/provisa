# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f0d3e-4c2a-4e8b-9a71-d5e3c8f20b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read table is not deleted under its view (REQ-1986, amended 2026-11-23).

A derived table (source ``__derived__``) whose SQL reads a table blocks that table's delete, and
the refusal names it. A code resolver's bridge reads its two tables only as the string arguments
of its resolver call (``codesource/resolver.call_sql``), never in FROM position: those are reads
too. Once the derived table is retired the table goes. A code resolver set's retire removes its
bridge and its relationships together, and then its two tables may go.
"""

# Requirements: REQ-1986, REQ-1918, REQ-2248, REQ-2249

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import code_resolver, schema_helpers, schema_mutation
from provisa.core.database import Database
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.repositories import integrity
from provisa.core.schema_org import domains, registered_tables, relationships, sources
from tests.unit.gate_identity import serve_every_column

SET_ID = "kept_1"
# What codesource/resolver.call_sql writes for a bridge.
BRIDGE_SQL = "SELECT * FROM match_1('pet_store.stores_1', 'pet_store.regions_1') AS m"


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which dependents a caller may be told of is REQ-2152's own
    tests (test_refusal_read_rights)."""
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="py", type="python"))
        await conn.execute_core(insert(sources).values(id=DERIVED_SOURCE_ID, type="derived"))
        await conn.execute_core(insert(domains).values(id="pet-store"))
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)

    async def _pool():
        return db

    async def _rebuild() -> None:
        return None

    async def _invalidate(_state, _table_ids) -> int:
        return 0

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(schema_helpers, "_get_pool", _pool)
    monkeypatch.setattr(schema_helpers, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr("provisa.cache.tenancy.invalidate_tables", _invalidate)
    monkeypatch.setattr("provisa.dq.scan.reschedule_org_checkers", _rebuild)
    monkeypatch.setattr(appmod.state, "hot_manager", None, raising=False)
    return db


async def _table(db: Database, name: str, **more: Any) -> int:
    values = {"source_id": "py", "domain_id": "pet-store", "schema_name": "public", **more}
    async with db.acquire() as conn:
        await conn.execute_core(insert(registered_tables).values(table_name=name, **values))
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def _bridge(db: Database, **more: Any) -> int:
    return await _table(
        db,
        SET_ID,
        source_id=DERIVED_SOURCE_ID,
        schema_name="views",
        view_sql=BRIDGE_SQL,
        materialize=True,
        **more,
    )


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


async def _delete_table(table_id: int) -> Any:
    return await schema_mutation.Mutation().delete_table(_info(), table_id)  # type: ignore[arg-type]


async def _retired(db: Database, table_id: int) -> bool:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(registered_tables.c.retired).where(registered_tables.c.id == table_id)
            )
        ).scalar_one()


def test_the_tables_a_call_is_given_by_name_are_read():
    relations, _ = integrity.names_in_sql(BRIDGE_SQL)
    assert relations == {"stores_1", "regions_1"}


def test_a_string_argument_that_is_not_a_table_name_names_no_table():
    relations, _ = integrity.names_in_sql("SELECT * FROM search('red shoes', 10) AS s")
    assert relations == set()


async def test_a_table_a_derived_table_reads_is_refused_naming_it(plane):
    stores = await _table(plane, "stores_1")
    await _table(plane, "regions_1")
    bridge = await _bridge(plane)
    result = await _delete_table(stores)
    assert (result.success, result.code) == (False, "schema.table_has_dependents")
    assert result.params["name"] == "stores_1"
    assert result.params["dependents"] == [
        {
            "kind": "table",
            "id": bridge,
            "name": SET_ID,
            "label": SET_ID,
            "via": ["registered_tables.view_sql"],
        }
    ]
    assert SET_ID in result.message
    assert await _retired(plane, stores) is False


async def test_the_table_is_deleted_once_its_derived_table_is_retired(plane):
    stores = await _table(plane, "stores_1")
    await _table(plane, "regions_1")
    bridge = await _bridge(plane)
    assert (await _delete_table(bridge)).success is True
    result = await _delete_table(stores)
    assert (result.success, result.code) == (True, "schema.table_deleted")
    assert await _retired(plane, stores) is True


async def test_a_code_resolver_set_retires_together_and_then_its_tables_go(plane):
    stores = await _table(plane, "stores_1")
    regions = await _table(plane, "regions_1")
    bridge = await _bridge(plane, code_resolver=SET_ID)
    set_names = code_resolver.names(SET_ID)
    async with plane.acquire() as conn:
        for rel_id, source, target, via in (
            (set_names.edge, stores, regions, bridge),
            (set_names.matches, stores, bridge, None),
            (set_names.target, bridge, regions, None),
        ):
            await conn.execute_core(
                insert(relationships).values(
                    id=rel_id,
                    source_table_id=source,
                    target_table_id=target,
                    source_column="id",
                    target_column="id",
                    cardinality="many-to-one",
                    via_table_id=via,
                    code_resolver=SET_ID,
                )
            )
    # The set's bridge cannot be deleted apart from it, and its tables are held under it.
    assert (await _delete_table(bridge)).code == "schema.code_resolver_part"
    assert (await _delete_table(stores)).code == "schema.table_has_dependents"

    retired = await code_resolver.retire(_info(), SET_ID)  # type: ignore[arg-type]
    assert (retired.success, retired.code) == (True, "schema.code_resolver_retired")
    assert await _retired(plane, bridge) is True

    for table_id in (stores, regions):
        result = await _delete_table(table_id)
        assert (result.success, result.code) == (True, "schema.table_deleted")
