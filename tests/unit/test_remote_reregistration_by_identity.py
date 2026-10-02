# Copyright (c) 2026 Kenneth Stott
# Canary: 6c184dcd-d4a9-41f8-bdd8-529cd93d3668
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Re-registering a remote source keeps its tables' identity (REQ-1918).

A remote source's tables are generated from its schema. Re-registering the source used to
delete every one of them and insert them again under new ids, so every relationship, row filter,
tag and glossary reference to them was lost on each refresh (cascaded away on PostgreSQL, left
dangling on SQLite). Each table is now upserted by its identity — source, schema, name — so what
is still in the remote schema keeps its id and what refers to it. A table the remote no longer
has is deleted through the model store; when something depends on it, it is kept and reported.
"""

# Requirements: REQ-1918, REQ-1919, REQ-314, REQ-308

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    sources,
    tag_assignments,
)
from provisa.openapi.register import auto_register_openapi_source


def _spec(*operations: str) -> dict:
    """An OpenAPI document with one GET list operation per name."""
    return {
        "openapi": "3.0.0",
        "info": {"title": "t", "version": "1"},
        "paths": {
            f"/{op}": {
                "get": {
                    "operationId": op,
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {"id": {"type": "integer"}},
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            }
            for op in operations
        },
    }


@pytest.fixture
async def plane() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="reregister-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="crm", type="openapi"))
        await conn.execute_core(insert(domains).values(id="sales"))
    return db


async def _register(db: Database, *operations: str) -> list[dict]:
    async with db.acquire() as conn:
        _, _, kept = await auto_register_openapi_source(
            "crm", _spec(*operations), conn, "sales", base_url="http://crm.test"
        )
    return kept


async def _ids(db: Database) -> dict[str, int]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            select(registered_tables.c.table_name, registered_tables.c.id).where(
                registered_tables.c.source_id == "crm"
            )
        )
        return {r[0]: r[1] for r in rows.fetchall()}


async def _relate(db: Database, ids: dict[str, int], source: str, target: str) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(relationships).values(
                id=f"{source}_{target}",
                source_table_id=ids[source],
                target_table_id=ids[target],
                source_column="id",
                target_column="id",
                cardinality="many-to-one",
            )
        )
        await conn.execute_core(
            insert(tag_assignments).values(
                tag_id="pii",
                base_tag_id="pii",
                object_type="table",
                object_key=target,
                table_id=ids[target],
            )
        )


async def _relationship_ends(db: Database) -> list[tuple[int, int]]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            select(relationships.c.source_table_id, relationships.c.target_table_id)
        )
        return [tuple(r) for r in rows.fetchall()]


async def test_a_table_still_in_the_remote_schema_keeps_its_id_and_what_refers_to_it(plane):
    assert await _register(plane, "listPets", "listOwners") == []
    first = await _ids(plane)
    assert len(first) == 2
    pets, owners = sorted(first)[1], sorted(first)[0]
    await _relate(plane, first, pets, owners)

    assert await _register(plane, "listPets", "listOwners") == []

    assert await _ids(plane) == first
    assert await _relationship_ends(plane) == [(first[pets], first[owners])]
    async with plane.acquire() as conn:
        tagged = (await conn.execute_core(select(tag_assignments.c.table_id))).fetchall()
    assert [t[0] for t in tagged] == [first[owners]]


async def test_a_table_the_remote_no_longer_has_is_kept_and_reported_while_something_refers_to_it(
    plane,
):
    await _register(plane, "listPets", "listOwners")
    first = await _ids(plane)
    pets, owners = sorted(first)[1], sorted(first)[0]
    await _relate(plane, first, pets, owners)

    kept = await _register(plane, "listPets")

    assert kept == [
        {
            "id": first[owners],
            "name": owners,
            "dependents": [
                {
                    "kind": "relationship",
                    "id": f"{pets}_{owners}",
                    "via": ["relationships.target_table_id"],
                }
            ],
        }
    ]
    assert await _ids(plane) == first  # nothing was removed
    assert await _relationship_ends(plane) == [(first[pets], first[owners])]


async def test_a_table_the_remote_no_longer_has_goes_once_nothing_refers_to_it(plane):
    await _register(plane, "listPets", "listOwners")
    first = await _ids(plane)
    pets, owners = sorted(first)[1], sorted(first)[0]
    await _relate(plane, first, pets, owners)
    async with plane.acquire() as conn:
        await conn.execute_core(relationships.delete())

    assert await _register(plane, "listPets") == []

    assert await _ids(plane) == {pets: first[pets]}
    async with plane.acquire() as conn:
        # Its tag assignment was its part and went with it.
        assert (await conn.execute_core(select(tag_assignments.c.id))).fetchall() == []


async def test_retiring_touches_only_that_sources_generated_schema(plane):
    await _register(plane, "listPets")
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="crm", domain_id="sales", schema_name="manual", table_name="notes"
            )
        )
        await conn.execute_core(insert(sources).values(id="erp", type="openapi"))
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="erp", domain_id="sales", schema_name="openapi", table_name="invoices"
            )
        )
        assert await table_repo.retire_generated(conn, "crm", "openapi", set()) == []
        left = (await conn.execute_core(select(registered_tables.c.table_name))).fetchall()
    assert sorted(r[0] for r in left) == ["invoices", "notes"]


# --- a GraphQL remote source ---------------------------------------------------------------------


def _gql_table(name: str) -> dict:
    return {"name": name, "columns": [{"name": "id", "type": "integer"}]}


async def test_a_graphql_remote_reregistration_keeps_ids_and_reports_what_it_kept(plane):
    from provisa.api.admin.graphql_remote_router import _upsert_tables_to_semantic_layer

    async with plane.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="shop", type="graphql_remote"))

    async def ids() -> dict[str, int]:
        async with plane.acquire() as conn:
            rows = await conn.execute_core(
                select(registered_tables.c.table_name, registered_tables.c.id).where(
                    registered_tables.c.source_id == "shop"
                )
            )
            return {r[0]: r[1] for r in rows.fetchall()}

    both = [_gql_table("pets"), _gql_table("owners")]
    assert await _upsert_tables_to_semantic_layer("shop", "sales", both, plane) == []
    first = await ids()
    assert set(first) == {"pets", "owners"}
    await _relate(plane, first, "pets", "owners")

    assert await _upsert_tables_to_semantic_layer("shop", "sales", both, plane) == []
    assert await ids() == first
    assert await _relationship_ends(plane) == [(first["pets"], first["owners"])]

    kept = await _upsert_tables_to_semantic_layer("shop", "sales", [_gql_table("pets")], plane)
    assert [(k["name"], [d["kind"] for d in k["dependents"]]) for k in kept] == [
        ("owners", ["relationship"])
    ]
    assert await ids() == first
