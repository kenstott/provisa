# Copyright (c) 2026 Kenneth Stott
# Canary: 4f7b2c19-8e3d-4a61-b5c0-9d2e6f1a3b87
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every relationship the admin API lists names its model type and its properties (REQ-1586,
amended 2026-11-22).

The type is ``direct`` (a foreign key), ``junction`` (through a junction table, whose two keys
are two of its columns) or ``shared_dimension`` (both ends join the dimension's one key). The
properties are what Cypher serves on the edge: the middle table's columns that are no key of the
join and no discriminator. A junction holding only its two keys has none.
"""

# Requirements: REQ-1586

from __future__ import annotations

from sqlalchemy import insert

import provisa.core.schema_org as so
from provisa.api.admin._row_mappers import _rel_from_row
from provisa.core.database import Database, create_engine_from_url


def _row(**via: object) -> dict:
    return {
        "id": "r",
        "source_table_id": 1,
        "target_table_id": 2,
        "source_table_name": "person",
        "source_domain_id": "hr",
        "target_table_name": "project",
        "source_column": "id",
        "target_column": "id",
        "cardinality": "one-to-many",
        "alias": None,
        "graphql_alias": None,
        "graphql_field": "projects",
        "cypher_type": "WORKS_ON",
        "same_entity": False,
        "parameters": {},
        "via_table_id": None,
        "via_table_name": None,
        "via_source_column": None,
        "via_target_column": None,
        "via_type_column": None,
        **via,
    }


def _junction(**more: object) -> dict:
    return _row(
        **{
            "via_table_id": 3,
            "via_table_name": "assignment",
            "via_source_column": "person_id",
            "via_target_column": "project_id",
            **more,
        }
    )


def test_a_foreign_key_is_direct_and_has_no_properties():
    rel = _rel_from_row(_row(), ())
    assert (rel.shape, rel.properties) == ("direct", [])


def test_a_junction_is_a_junction_whose_other_columns_are_its_properties():
    rel = _rel_from_row(_junction(), ("person_id", "project_id", "role", "since"))
    assert (rel.shape, rel.properties) == ("junction", ["role", "since"])


def test_a_junction_holding_only_its_two_keys_has_no_properties():
    rel = _rel_from_row(_junction(), ("person_id", "project_id"))
    assert (rel.shape, rel.properties) == ("junction", [])


def test_a_junctions_discriminator_and_composite_keys_are_no_properties():
    rel = _rel_from_row(
        _junction(
            via_source_column="person_id, person_org",
            via_target_column="project_id",
            via_type_column="kind",
        ),
        ("person_id", "person_org", "project_id", "kind", "hours"),
    )
    assert (rel.shape, rel.properties) == ("junction", ["hours"])


def test_both_ends_joining_one_key_is_a_shared_dimension():
    rel = _rel_from_row(
        _row(
            via_table_id=4,
            via_table_name="neighborhood",
            via_source_column="neighborhood",
            via_target_column="neighborhood",
        ),
        ("neighborhood", "location", "description"),
    )
    assert (rel.shape, rel.properties) == ("shared_dimension", ["location", "description"])


async def test_the_listing_reads_each_middle_tables_columns_once():
    from provisa.api.admin.schema_query import _via_columns

    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="shape")
    tables = ("sources", "domains", "registered_tables", "table_columns")
    with db.engine.begin() as conn:
        so.metadata.create_all(conn, tables=[so.metadata.tables[n] for n in tables])
    async with db.acquire() as conn:
        await conn.execute_core(insert(so.sources).values(id="s", type="files"))
        await conn.execute_core(insert(so.domains).values(id="hr"))
        for tid, name in ((3, "assignment"), (5, "bare")):
            await conn.execute_core(
                insert(so.registered_tables).values(
                    id=tid, source_id="s", domain_id="hr", schema_name="main", table_name=name
                )
            )
        for column in ("person_id", "project_id", "role"):
            await conn.execute_core(
                insert(so.table_columns).values(
                    table_id=3, domain_id="hr", column_name=column, data_type="text"
                )
            )
        found = await _via_columns(conn, [_junction(), _row(), _row(via_table_id=5)])
    assert {k: sorted(v) for k, v in found.items()} == {
        3: ["person_id", "project_id", "role"],
        5: [],
    }
