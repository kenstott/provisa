# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2d71-9c4a-4f36-a1e0-7d3c6b9f2a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A many-to-many pair accepted as a shared dimension (REQ-413), on the demo's neighborhood shape:
the budget, the permits and the complaints files share neighborhood values, a key of none.

Accepting one such pair plans a derived table of the pair's distinct values, named for the
concept the proposal names, and a many-to-one relationship from each side to it. A second pair
on the same values reuses the dimension one of its sides already joins.
"""

# Requirements: REQ-413

from __future__ import annotations

import pytest
import sqlglot
from sqlalchemy import insert

import provisa.core.schema_org as so
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.discovery import dimension

_TABLES = ("sources", "domains", "registered_tables", "table_columns", "relationships")


async def _store() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="dimension")
    with db.engine.begin() as conn:
        so.metadata.create_all(conn, tables=[so.metadata.tables[n] for n in _TABLES])
    async with db.acquire() as conn:
        for sid in ("drop", DERIVED_SOURCE_ID):
            await conn.execute_core(insert(so.sources).values(id=sid, type="files"))
        await conn.execute_core(insert(so.domains).values(id="city-files"))
        for tid, name in (
            (1, "budget_2026__budget"),
            (2, "building_permits"),
            (3, "complaints_2026_08"),
        ):
            await conn.execute_core(
                insert(so.registered_tables).values(
                    id=tid,
                    source_id="drop",
                    domain_id="city-files",
                    schema_name="main",
                    table_name=name,
                )
            )
            await conn.execute_core(
                insert(so.table_columns).values(
                    table_id=tid,
                    domain_id="city-files",
                    column_name="neighborhood",
                    data_type="varchar",
                    is_primary_key=False,
                )
            )
    return db


def test_the_dimension_is_named_for_the_concept_the_proposal_names():
    assert dimension.concept("neighborhood", "neighborhood") == "neighborhood"
    assert dimension.concept("city-neighborhood", "hood") == "city_neighborhood"
    assert dimension.concept("Permit Neighborhood", "hood") == "permit_neighborhood"


def test_a_proposal_naming_no_concept_takes_the_name_its_values_are_held_under():
    assert dimension.concept("", "neighborhood") == "neighborhood"
    assert dimension.concept(None, "Hood") == "hood"


async def test_a_new_dimension_holds_the_distinct_values_of_both_columns_and_joins_each_side():
    db = await _store()
    async with db.acquire() as conn:
        planned = await dimension.plan(
            conn,
            (1, "neighborhood"),
            (2, "neighborhood"),
            suggested_name="neighborhood",
            name=None,
        )
    assert planned.name == "neighborhood"
    assert planned.column == "neighborhood"
    assert planned.table_id is None  # made by accepting
    assert planned.domain_id == "city-files" and planned.data_type == "varchar"
    assert [(s.table_name, s.column) for s in planned.joining] == [
        ("budget_2026__budget", "neighborhood"),
        ("building_permits", "neighborhood"),
    ]
    # One row per value either side holds: a UNION of the two columns, nulls left out.
    tree = sqlglot.parse_one(planned.view_sql or "", read="postgres")
    assert isinstance(tree, sqlglot.exp.Union) and tree.args.get("distinct") is not False
    assert tree.named_selects == ["neighborhood"]
    read = sorted(t.sql() for t in tree.find_all(sqlglot.exp.Table))
    assert read == ["city_files.budget_2026__budget", "city_files.building_permits"]
    assert planned.view_sql.count("IS NOT NULL") == 2


async def test_the_reviewers_name_names_a_new_dimension():
    db = await _store()
    async with db.acquire() as conn:
        planned = await dimension.plan(
            conn,
            (1, "neighborhood"),
            (2, "neighborhood"),
            suggested_name="neighborhood",
            name="neighborhoods",
        )
    assert planned.name == "neighborhoods" and planned.column == "neighborhood"


async def test_each_side_joins_the_dimension_many_to_one_under_a_field_of_its_own():
    db = await _store()
    async with db.acquire() as conn:
        planned = await dimension.plan(
            conn, (1, "neighborhood"), (2, "neighborhood"), suggested_name="neighborhood", name=None
        )
    edges = [dimension.edge(planned, side) for side in planned.joining]
    assert [e.id for e in edges] == [
        "budget_2026__budget-neighborhood-to-neighborhood",
        "building_permits-neighborhood-to-neighborhood",
    ]
    assert {e.cardinality for e in edges} == {"many-to-one"}
    assert {(e.target_table, e.target_column) for e in edges} == {("neighborhood", "neighborhood")}
    # The side's own column is named for the concept too: the edge's field must not take it.
    assert {e.graphql_alias for e in edges} == {"neighborhood_dimension"}


async def _joined_to_dimension(db: Database) -> None:
    """The dimension the budget-permits pair made: its view and its two relationships."""
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.registered_tables).values(
                id=10,
                source_id=DERIVED_SOURCE_ID,
                domain_id="city-files",
                schema_name="views",
                table_name="neighborhood",
                view_sql="SELECT 1",
            )
        )
        await conn.execute_core(
            insert(so.table_columns).values(
                table_id=10,
                domain_id="city-files",
                column_name="neighborhood",
                data_type="varchar",
                is_primary_key=True,
            )
        )
        for source in (1, 2):
            await conn.execute_core(
                insert(so.relationships).values(
                    id=f"edge-{source}",
                    source_table_id=source,
                    source_column="neighborhood",
                    target_table_id=10,
                    target_column="neighborhood",
                    cardinality="many-to-one",
                )
            )


async def test_a_second_pair_on_the_same_values_reuses_the_dimension_one_side_joins():
    db = await _store()
    await _joined_to_dimension(db)
    async with db.acquire() as conn:
        planned = await dimension.plan(
            conn,
            (3, "neighborhood"),
            (1, "neighborhood"),
            suggested_name="complaint-neighborhood",
            name="anything",
        )
    assert planned.table_id == 10 and planned.name == "neighborhood"
    assert planned.view_sql is None  # nothing is made: the dimension is there
    assert [(s.table_name, s.column) for s in planned.joining] == [
        ("complaints_2026_08", "neighborhood")
    ]


async def test_a_pair_both_of_whose_sides_join_the_dimension_creates_nothing():
    db = await _store()
    await _joined_to_dimension(db)
    async with db.acquire() as conn:
        planned = await dimension.plan(
            conn, (2, "neighborhood"), (1, "neighborhood"), suggested_name="x", name=None
        )
    assert planned.table_id == 10 and planned.joining == ()


async def test_a_side_joining_a_table_that_is_no_derived_table_is_not_joined_to_a_dimension():
    db = await _store()
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationships).values(
                id="to-permits",
                source_table_id=3,
                source_column="neighborhood",
                target_table_id=2,
                target_column="neighborhood",
                cardinality="many-to-one",
            )
        )
        planned = await dimension.plan(
            conn, (3, "neighborhood"), (1, "neighborhood"), suggested_name="hood", name=None
        )
    assert planned.table_id is None and planned.name == "hood"
    assert len(planned.joining) == 2


async def test_a_pair_whose_sides_join_two_different_dimensions_is_refused_naming_both():
    db = await _store()
    await _joined_to_dimension(db)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.registered_tables).values(
                id=11,
                source_id=DERIVED_SOURCE_ID,
                domain_id="city-files",
                schema_name="views",
                table_name="district",
                view_sql="SELECT 1",
            )
        )
        await conn.execute_core(
            insert(so.relationships).values(
                id="edge-3",
                source_table_id=3,
                source_column="neighborhood",
                target_table_id=11,
                target_column="district",
                cardinality="many-to-one",
            )
        )
        with pytest.raises(dimension.TwoDimensions, match="district and neighborhood"):
            await dimension.plan(
                conn, (3, "neighborhood"), (1, "neighborhood"), suggested_name="x", name=None
            )


async def test_the_dimension_a_pair_would_join_is_named_before_it_is_accepted():
    db = await _store()
    async with db.acquire() as conn:
        assert await dimension.named(
            conn, (1, "neighborhood"), (2, "neighborhood"), "neighborhood"
        ) == ("neighborhood", False)
    await _joined_to_dimension(db)
    async with db.acquire() as conn:
        assert await dimension.named(
            conn, (3, "neighborhood"), (1, "neighborhood"), "complaint-neighborhood"
        ) == ("neighborhood", True)


async def test_a_side_joining_a_registered_dimension_table_reuses_it():
    """REQ-1586 (amended 2026-11-22): a registered table marked a dimension (a reference table
    with its own details) is reused as a derived dimension is, whose key holds the values."""
    db = await _store()
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.registered_tables).values(
                id=20,
                source_id="drop",
                domain_id="city-files",
                schema_name="main",
                table_name="neighborhoods",
                modeling_role="dimension",
            )
        )
        await conn.execute_core(
            insert(so.relationships).values(
                id="to-neighborhoods",
                source_table_id=1,
                source_column="neighborhood",
                target_table_id=20,
                target_column="name",
                cardinality="many-to-one",
            )
        )
        planned = await dimension.plan(
            conn, (3, "neighborhood"), (1, "neighborhood"), suggested_name="hood", name=None
        )
    assert (planned.table_id, planned.name, planned.column) == (20, "neighborhoods", "name")
    assert planned.view_sql is None
    assert [(s.table_name, s.column) for s in planned.joining] == [
        ("complaints_2026_08", "neighborhood")
    ]
