# Copyright (c) 2026 Kenneth Stott
# Canary: 5d71c0e2-9f3a-4b86-a2e4-c18f6b07d935
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""R510: the meta seed writes its relationships as a SET, and removes the ones it no longer
declares.

REQ-1634 made data-product membership many-to-many and renamed its seeded edges
(meta:data_products:registered_tables -> meta:data_products:member_tables, both alias
MEMBER_TABLES on data_products), REQ-2155 moved a role's parent to roles_parents. The seed upserted
by id only, so a control plane seeded before that kept the old rows, and the old row still held
the edge's (source_table_id, alias): the new id's insert was refused
(relationships_source_alias_unique) and every boot of the instance stopped (alpha.509, cloud-dev).
A fresh install never holds the old rows, which is why CI did not see it.
"""

# Requirements: REQ-2163, REQ-1634

from __future__ import annotations

import pytest
from sqlalchemy import delete, insert, select

from provisa.api._meta_seed import (
    meta_relationship_seed_rows,
    seed_relationship_id_prefixes,
    undeclared_seed_relationship_ids,
)
from provisa.core.schema_org import registered_tables, relationships, sources

# What alpha.508 seeded and alpha.509 no longer declares: (id, source table, target table,
# source column, target column, cardinality, alias), as the older _meta_seed.py declared them.
_OLD_ROWS = [
    (
        "meta:data_products:registered_tables",
        "data_products",
        "registered_tables",
        "id",
        "product_id",
        "one-to-many",
        "MEMBER_TABLES",
    ),
    (
        "meta:registered_tables:data_products",
        "registered_tables",
        "data_products",
        "product_id",
        "id",
        "many-to-one",
        "PRODUCT",
    ),
    (
        "meta:data_products:tracked_functions",
        "data_products",
        "tracked_functions",
        "id",
        "product_id",
        "one-to-many",
        "COMMANDS",
    ),
    (
        "meta:tracked_functions:data_products",
        "tracked_functions",
        "data_products",
        "product_id",
        "id",
        "many-to-one",
        "PRODUCT",
    ),
    (
        "meta:roles:parent_role",
        "roles",
        "roles",
        "parent_role_id",
        "id",
        "many-to-one",
        "PARENT_ROLE",
    ),
]
_OLD_IDS = [row[0] for row in _OLD_ROWS]
# The declared edges the old rows were renamed to, absent from an alpha.508 control plane.
_RENAMED_TO = [
    "meta:data_products:member_tables",
    "meta:registered_tables:products",
    "meta:data_products:commands",
    "meta:tracked_functions:products",
]
_PERSON_REL = "data_products_steward"


def test_the_seed_removes_only_built_in_ids_it_no_longer_declares():
    declared = [cols["id"] for _src, _tgt, _via, cols in meta_relationship_seed_rows()]
    held = [*declared, *_OLD_IDS, _PERSON_REL, "gql_auto__orders__customers", "metadata:x"]

    assert seed_relationship_id_prefixes() == ("meta:", "ontology:")
    assert undeclared_seed_relationship_ids(held) == sorted(_OLD_IDS)
    assert undeclared_seed_relationship_ids(declared) == []


async def _table_id(conn, table_name: str) -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(
                registered_tables.c.source_id == "provisa-admin",
                registered_tables.c.table_name == table_name,
            )
        )
    ).scalar_one()


async def _all_relationships(conn) -> list[dict]:
    rows = (await conn.execute_core(select(relationships).order_by(relationships.c.id))).fetchall()
    return [dict(r._mapping) for r in rows]


@pytest.mark.asyncio
async def test_a_reseed_over_renamed_built_in_relationships_completes(control_plane):
    from provisa.api.startup_seed import (
        _seed_meta_domain,
        _seed_meta_relationships,
        _seed_ontology_domain,
    )

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        await _seed_meta_domain(conn, org_id="default")
        await _seed_ontology_domain(conn, org_id="default")
        await _seed_meta_relationships(conn)
        fresh = {r["id"] for r in await _all_relationships(conn)}

        # Make it the control plane alpha.508 left: the renamed edges under their old ids, on the
        # same source tables and aliases, and the new ids not there yet.
        await conn.execute_core(delete(relationships).where(relationships.c.id.in_(_RENAMED_TO)))
        for rid, src, tgt, src_col, tgt_col, card, alias in _OLD_ROWS:
            await conn.execute_core(
                insert(relationships).values(
                    id=rid,
                    source_table_id=await _table_id(conn, src),
                    target_table_id=await _table_id(conn, tgt),
                    source_column=src_col,
                    target_column=tgt_col,
                    cardinality=card,
                    alias=alias,
                )
            )
        # A relationship a person made on the same source table.
        await conn.execute_core(
            insert(relationships).values(
                id=_PERSON_REL,
                source_table_id=await _table_id(conn, "data_products"),
                target_table_id=await _table_id(conn, "roles"),
                source_column="steward_role",
                target_column="id",
                cardinality="many-to-one",
                alias="STEWARD",
                graphql_alias="steward",
                owner="someone",
            )
        )
        person_before = [r for r in await _all_relationships(conn) if r["id"] == _PERSON_REL]

        await _seed_meta_relationships(conn)
        after = await _all_relationships(conn)

        await _seed_meta_relationships(conn)
        again = await _all_relationships(conn)

    held = {r["id"] for r in after}
    assert held & set(_OLD_IDS) == set()
    assert set(_RENAMED_TO) <= fresh
    # Every edge a fresh install seeds is there, and nothing else but the person's.
    assert held == fresh | {_PERSON_REL}
    assert [r for r in after if r["id"] == _PERSON_REL] == person_before
    assert again == after
