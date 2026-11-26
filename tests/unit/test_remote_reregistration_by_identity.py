# Copyright (c) 2026 Kenneth Stott
# Canary: 6c184dcd-d4a9-41f8-bdd8-529cd93d3668
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Refreshing a remote source keeps its registered tables' identity (REQ-1918).

A remote source's registered tables are brought up to date from its schema on each refresh.
That used to delete every one of them and insert them again under new ids, so every
relationship, row filter, tag and glossary reference to them was lost on each refresh (cascaded
away on PostgreSQL, left dangling on SQLite). Each table is now upserted by its identity --
source, schema, name -- so what is still in the remote schema keeps its id and what refers to
it. A table the remote no longer has is deleted through the model store; when something depends
on it, it is kept and reported.
"""

# Requirements: REQ-1918, REQ-1919, REQ-308, REQ-311, REQ-2248

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.api.admin.graphql_remote_router import _upsert_tables_to_semantic_layer
from provisa.api.admin.read_rights import SERVICE_READER
from provisa.core.database import Database
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    sources,
    tag_assignments,
)


def _gql_table(name: str) -> dict:
    return {"name": name, "columns": [{"name": "id", "type": "integer"}]}


@pytest.fixture
async def plane(control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="crm", type="graphql_remote"))
        await conn.execute_core(insert(domains).values(id="sales"))
    return db


async def _register(db: Database, *tables: str) -> list[dict]:
    """A refresh of the crm source whose schema now maps to ``tables``: what it keeps."""
    return await _upsert_tables_to_semantic_layer(
        "crm", "sales", [_gql_table(t) for t in tables], db, SERVICE_READER
    )


async def _ids(db: Database) -> dict[str, int]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            # The tables in service: one the remote no longer has is retired (REQ-2248).
            select(registered_tables.c.table_name, registered_tables.c.id).where(
                registered_tables.c.source_id == "crm", registered_tables.c.retired.is_(False)
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
    assert await _register(plane, "pets", "owners") == []
    first = await _ids(plane)
    assert len(first) == 2
    pets, owners = sorted(first)[1], sorted(first)[0]
    await _relate(plane, first, pets, owners)

    assert await _register(plane, "pets", "owners") == []

    assert await _ids(plane) == first
    assert await _relationship_ends(plane) == [(first[pets], first[owners])]
    async with plane.acquire() as conn:
        tagged = (await conn.execute_core(select(tag_assignments.c.table_id))).fetchall()
    assert [t[0] for t in tagged] == [first[owners]]


async def test_a_table_the_remote_no_longer_has_is_kept_and_reported_while_something_refers_to_it(
    plane,
):
    await _register(plane, "pets", "owners")
    first = await _ids(plane)
    pets, owners = sorted(first)[1], sorted(first)[0]
    await _relate(plane, first, pets, owners)

    kept = await _register(plane, "pets")

    assert kept == [
        {
            "id": first[owners],
            "name": owners,
            "dependents": [
                {
                    "kind": "relationship",
                    "id": f"{pets}_{owners}",
                    "name": f"{pets}_{owners}",
                    "label": f"{pets} -> {owners}",
                    "via": ["relationships.target_table_id"],
                }
            ],
        }
    ]
    assert await _ids(plane) == first  # nothing was removed
    assert await _relationship_ends(plane) == [(first[pets], first[owners])]


async def test_a_table_the_remote_no_longer_has_goes_once_nothing_refers_to_it(plane):
    await _register(plane, "pets", "owners")
    first = await _ids(plane)
    pets, owners = sorted(first)[1], sorted(first)[0]
    await _relate(plane, first, pets, owners)
    async with plane.acquire() as conn:
        await conn.execute_core(relationships.delete())

    assert await _register(plane, "pets") == []

    assert await _ids(plane) == {pets: first[pets]}
    async with plane.acquire() as conn:
        # REQ-2248: retired, not removed: its tag assignment is kept with it.
        assert len((await conn.execute_core(select(tag_assignments.c.id))).fetchall()) == 1


async def test_retiring_touches_only_that_sources_generated_schema(plane):
    await _register(plane, "pets")
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="crm",
                domain_id="sales",
                schema_name="manual",
                table_name="notes",
            )
        )
        await conn.execute_core(insert(sources).values(id="erp", type="graphql_remote"))
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="erp",
                domain_id="sales",
                schema_name="graphql",
                table_name="invoices",
            )
        )
        assert await table_repo.retire_generated(conn, "crm", "graphql", set()) == []
        left = (
            await conn.execute_core(
                select(registered_tables.c.table_name).where(registered_tables.c.retired.is_(False))
            )
        ).fetchall()
    assert sorted(r[0] for r in left) == ["invoices", "notes"]


# --- a second source ------------------------------------------------------------------------------


async def test_a_graphql_remote_reregistration_keeps_ids_and_reports_what_it_kept(plane):
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
    assert (
        await _upsert_tables_to_semantic_layer("shop", "sales", both, plane, SERVICE_READER) == []
    )
    first = await ids()
    assert set(first) == {"pets", "owners"}
    await _relate(plane, first, "pets", "owners")

    assert (
        await _upsert_tables_to_semantic_layer("shop", "sales", both, plane, SERVICE_READER) == []
    )
    assert await ids() == first
    assert await _relationship_ends(plane) == [(first["pets"], first["owners"])]

    kept = await _upsert_tables_to_semantic_layer(
        "shop", "sales", [_gql_table("pets")], plane, SERVICE_READER
    )
    assert [(k["name"], [d["kind"] for d in k["dependents"]]) for k in kept] == [
        ("owners", ["relationship"])
    ]
    assert await ids() == first


async def test_a_column_registered_with_its_selection_outlives_a_refresh_from_the_endpoint(plane):
    """The demo registers ``schedules.employee_id`` read as ``employee_id: employee { id }``; the
    ``Schedule`` type holds ``employee`` and no ``employee_id``. The boot's refresh from the
    endpoint wrote the endpoint's columns alone, so the registered column went, and every read of
    it was refused (``[V003] Column 'employee_id' is not visible to this role``)."""
    from provisa.core.models import Column, Table

    async with plane.acquire() as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="crm",
                domain_id="sales",
                schema_name="graphql",
                table_name="schedules",
                columns=[
                    Column(name="id", data_type="integer", visible_to=["analyst"]),
                    Column(
                        name="employee_id",
                        data_type="integer",
                        visible_to=["analyst"],
                        description="Employee the shift belongs to",
                        gql_selection="employee_id: employee { id }",
                    ),
                ],
            ),
        )
    endpoint = {
        "name": "schedules",
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "employee", "type": "jsonb", "gql_object_type": "Employee"},
        ],
    }
    await _upsert_tables_to_semantic_layer("crm", "sales", [endpoint], plane, SERVICE_READER)
    async with plane.acquire() as conn:
        held = await table_repo.get_by_name(conn, "crm", "graphql", "schedules")
    assert held is not None
    columns = {c["column_name"]: c for c in held["columns"]}
    assert set(columns) == {"id", "employee", "employee_id"}, columns
    kept = columns["employee_id"]
    assert kept["gql_selection"] == "employee_id: employee { id }"
    assert (kept["data_type"], list(kept["visible_to"])) == ("integer", ["analyst"])
    assert kept["description"] == "Employee the shift belongs to"


# --- what the model holds of a column the endpoint lists -------------------------------------------


async def _register_contacts(db: Database, *columns) -> None:
    from provisa.core.models import Table

    async with db.acquire() as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="crm",
                domain_id="sales",
                schema_name="graphql",
                table_name="contacts",
                columns=list(columns),
            ),
        )


async def _contacts_columns(db: Database) -> dict[str, dict]:
    async with db.acquire() as conn:
        held = await table_repo.get_by_name(conn, "crm", "graphql", "contacts")
    assert held is not None
    return {c["column_name"]: c for c in held["columns"]}


async def test_a_listed_column_keeps_its_mask_alias_scope_and_description_across_a_refresh(plane):
    """The endpoint owns a column's existence, name and type. A refresh used to rebuild each
    listed column from the endpoint alone, keeping only ``visible_to`` and
    ``required_by_steward``, so a mask, alias, scope or description the config or an admin set
    was overwritten on every boot."""
    from provisa.core.models import Column

    await _register_contacts(
        plane,
        Column(name="id", data_type="integer", visible_to=["analyst"]),
        Column(
            name="email",
            data_type="varchar",
            visible_to=["analyst"],
            masks=[{"to": None, "mask": {"type": "constant", "value": "hidden"}}],
            alias="contact_email",
            scope="public",
            description="Where the contact is written to",
            fake="email()",
        ),
    )
    masked = [(e["to"], e["mask"]) for e in (await _contacts_columns(plane))["email"]["masks"]]
    assert [(to, mask["type"], mask["value"]) for to, mask in masked] == [
        (None, "constant", "hidden")
    ]
    endpoint = {
        "name": "contacts",
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "email", "type": "text", "description": "The endpoint's own wording"},
        ],
    }
    await _upsert_tables_to_semantic_layer("crm", "sales", [endpoint], plane, SERVICE_READER)

    email = (await _contacts_columns(plane))["email"]
    assert [(e["to"], e["mask"]) for e in email["masks"]] == masked, email["masks"]
    assert email["alias"] == "contact_email"
    assert email["scope"] == "public"
    assert email["description"] == "Where the contact is written to"
    assert email["fake"] == "email()"
    assert list(email["visible_to"]) == ["analyst"]


async def test_a_listed_column_takes_the_endpoints_type_and_keeps_the_rest(plane):
    from provisa.core.models import Column

    await _register_contacts(
        plane,
        Column(name="id", data_type="integer", visible_to=[]),
        Column(
            name="score",
            data_type="varchar",
            visible_to=["analyst"],
            alias="rating",
            description="How the contact is rated",
        ),
    )
    endpoint = {
        "name": "contacts",
        "columns": [{"name": "id", "type": "integer"}, {"name": "score", "type": "integer"}],
    }
    await _upsert_tables_to_semantic_layer("crm", "sales", [endpoint], plane, SERVICE_READER)

    score = (await _contacts_columns(plane))["score"]
    assert score["data_type"] == "integer"
    assert (score["alias"], score["description"]) == ("rating", "How the contact is rated")
    assert list(score["visible_to"]) == ["analyst"]


async def test_a_column_the_endpoint_drops_leaves_unless_the_model_selects_it(plane):
    from provisa.core.models import Column

    await _register_contacts(
        plane,
        Column(name="id", data_type="integer", visible_to=[]),
        Column(name="fax", data_type="varchar", visible_to=["analyst"], alias="fax_number"),
    )
    endpoint = {"name": "contacts", "columns": [{"name": "id", "type": "integer"}]}
    await _upsert_tables_to_semantic_layer("crm", "sales", [endpoint], plane, SERVICE_READER)

    assert set(await _contacts_columns(plane)) == {"id"}


async def test_a_column_the_endpoint_newly_lists_takes_the_endpoints_description(plane):
    from provisa.core.models import Column

    await _register_contacts(plane, Column(name="id", data_type="integer", visible_to=[]))
    endpoint = {
        "name": "contacts",
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "phone", "type": "text", "description": "Where the contact is called"},
        ],
    }
    await _upsert_tables_to_semantic_layer("crm", "sales", [endpoint], plane, SERVICE_READER)

    phone = (await _contacts_columns(plane))["phone"]
    assert (phone["data_type"], phone["description"]) == ("varchar", "Where the contact is called")
    assert list(phone["visible_to"]) == []
