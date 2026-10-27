# Copyright (c) 2026 Kenneth Stott
# Canary: 63135e14-3f5b-4b22-845a-0d2fdbe42622
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship's effective names -- its Cypher type and its GraphQL field -- are stored when it is
saved, by the one derivation, so the meta domain (a product's panel, the knowledge graph, SPARQL)
reads the very names the admin API shows: an alias a steward pinned, the names derived from a
foreign key's column and the cardinality, or a junction's nominated label."""

# Requirements: REQ-351, REQ-1586

from __future__ import annotations

from sqlalchemy import insert, select

from provisa.api.admin.db_queries import fetch_relationships
from provisa.core.models import Cardinality, Relationship
from provisa.core.repositories import relationship as rel_repo
from provisa.core.schema_org import domains, registered_tables, relationships, sources


async def _plane(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="shop-db", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="shop"))
        for table in ("customers", "orders", "order_links", "line_items"):
            await conn.execute_core(
                insert(registered_tables).values(
                    source_id="shop-db", domain_id="shop", schema_name="shop", table_name=table
                )
            )
        for rel in (
            # Pinned: the steward's alias is the type.
            Relationship(
                id="orders-to-customer",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality("many-to-one"),
                alias="PLACED_BY",
            ),
            # Derived: from the column and the cardinality.
            Relationship(
                id="customer-to-orders",
                source_table_id="customers",
                target_table_id="orders",
                source_column="order_id",
                target_column="id",
                cardinality=Cardinality("one-to-many"),
            ),
            # Derived, many-to-one.
            Relationship(
                id="order-to-customer",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality("many-to-one"),
            ),
            # A two-word target, whose GraphQL field the naming convention spells.
            Relationship(
                id="order-to-line-items",
                source_table_id="orders",
                target_table_id="line_items",
                source_column="line_item_id",
                target_column="id",
                cardinality=Cardinality("one-to-many"),
            ),
            # A junction naming its type by its discriminator value.
            Relationship(
                id="order-links",
                source_table_id="orders",
                target_table_id="orders",
                source_column="id",
                target_column="id",
                cardinality=Cardinality("many-to-one"),
                via_table="order_links",
                via_source_column="from_id",
                via_target_column="to_id",
                via_type_column="kind",
                via_type_value="replaces",
                via_label_source="column",
            ),
        ):
            await rel_repo.upsert(conn, rel)
    return db


async def test_the_stored_names_are_the_names_the_admin_api_shows(control_plane):
    db = await _plane(control_plane)
    async with db.acquire() as conn:
        stored = {
            r.id: (r.cypher_type, r.graphql_field)
            for r in (
                await conn.execute_core(
                    select(
                        relationships.c.id,
                        relationships.c.cypher_type,
                        relationships.c.graphql_field,
                    )
                )
            ).fetchall()
        }
        shown = {r["id"]: r for r in await fetch_relationships(conn)}
    assert stored["orders-to-customer"] == (
        "PLACED_BY",
        shown["orders-to-customer"]["graphql_alias"],
    )
    assert stored["customer-to-orders"] == (
        shown["customer-to-orders"]["computed_cypher_alias"],
        shown["customer-to-orders"]["graphql_alias"],
    )
    assert stored["customer-to-orders"][0] == "HAS_ORDER"
    assert stored["order-links"][0] == "REPLACES"
    assert stored["order-links"][1] == shown["order-links"]["graphql_alias"]


async def test_the_admin_api_the_label_map_and_meta_agree_on_an_unaliased_type(control_plane):
    """The admin API once derived an unaliased type its own way (IS_CUSTOMER where Cypher served
    BELONGS_TO_CUSTOMER): every reader now reads the one stored type."""
    from provisa.api.admin._row_mappers import _rel_from_row

    db = await _plane(control_plane)
    async with db.acquire() as conn:
        rows = {
            r.id: dict(r._mapping)
            for r in (
                await conn.execute_core(
                    select(
                        relationships,
                        registered_tables.c.table_name.label("target_table_name"),
                    ).select_from(
                        relationships.join(
                            registered_tables,
                            registered_tables.c.id == relationships.c.target_table_id,
                        )
                    )
                )
            ).fetchall()
        }
        served = {r["id"]: r for r in await fetch_relationships(conn)}
    for rel_id, expected in (
        ("order-to-customer", "BELONGS_TO_CUSTOMER"),
        ("customer-to-orders", "HAS_ORDER"),
    ):
        assert rows[rel_id]["cypher_type"] == expected  # meta reads the row
        assert served[rel_id]["computed_cypher_alias"] == expected  # the label map's input
        assert _rel_from_row(rows[rel_id]).computed_cypher_alias == expected  # the admin API


async def test_a_change_of_naming_convention_respells_the_stored_graphql_field(control_plane):
    from provisa.compiler import naming
    from provisa.core.relationship_names import store_effective_names

    db = await _plane(control_plane)
    field = select(relationships.c.graphql_field).where(relationships.c.id == "order-to-line-items")
    try:
        async with db.acquire() as conn:
            assert (await conn.execute_core(field)).scalar() == "lineItems"
            naming.configure(gql="hasura_graphql")
            await store_effective_names(conn)
            assert (await conn.execute_core(field)).scalar() == "line_items"
            served = {r["id"]: r for r in await fetch_relationships(conn)}
            assert served["order-to-line-items"]["graphql_alias"] == "line_items"
    finally:
        naming.configure()


async def test_a_build_writes_no_name_already_stored(control_plane):
    """REQ-351, REQ-1524: a worker reloading another's change runs the build outside any model
    change scope, where a model write is refused (ModelChangeOutsideScope). Names that are already
    stored are not written again, so the reload reaches the stored stamp."""
    from sqlalchemy.sql.dml import Update

    from provisa.core.relationship_names import store_effective_names

    db = await _plane(control_plane)
    async with db.acquire() as conn:
        written: list[object] = []
        execute = conn.execute_core

        async def _recording(stmt, *args, **kwargs):
            if isinstance(stmt, Update):
                written.append(stmt)
            return await execute(stmt, *args, **kwargs)

        conn.execute_core = _recording  # type: ignore[method-assign]
        await store_effective_names(conn)
    assert written == []


async def test_names_a_change_of_convention_respells_are_written_in_a_model_change(
    control_plane, monkeypatch
):
    """REQ-351, REQ-1524: a build that finds the stored names in another naming convention writes
    them as a model change of its own -- or as part of the change it runs inside -- never outside
    one, where the write is refused (ModelChangeOutsideScope: a schema build right after a change
    of naming convention, run 40190966276). Names already stored open no change at all."""
    from provisa.compiler import naming
    from provisa.core import model_change
    from provisa.core.relationship_names import store_effective_names

    db = await _plane(control_plane)
    flushed: list[str] = []
    flush = model_change.flush

    async def _flush(current):
        flushed.append(current.label)
        await flush(current)

    monkeypatch.setattr(model_change, "flush", _flush)
    try:
        async with db.acquire() as conn:
            await store_effective_names(conn)
            assert flushed == []  # nothing to respell: no change opened
            naming.configure(gql="hasura_graphql")
            await store_effective_names(conn)
        assert len(flushed) == 1
    finally:
        naming.configure()


async def test_the_admin_api_names_every_edge_by_the_type_cypher_serves(control_plane):
    """REQ-1586: every client reads an edge's Cypher type from the admin API's cypher_type, the
    stored type the label map serves; none derives it (an aliased edge, an unaliased one and a
    junction-backed one alike)."""
    from provisa.api.admin._row_mappers import _rel_from_row

    db = await _plane(control_plane)
    async with db.acquire() as conn:
        rows = {
            r.id: dict(r._mapping)
            for r in (
                await conn.execute_core(
                    select(
                        relationships,
                        registered_tables.c.table_name.label("target_table_name"),
                    ).select_from(
                        relationships.join(
                            registered_tables,
                            registered_tables.c.id == relationships.c.target_table_id,
                        )
                    )
                )
            ).fetchall()
        }
    for rel_id, expected in (
        ("orders-to-customer", "PLACED_BY"),
        ("customer-to-orders", "HAS_ORDER"),
        ("order-links", "REPLACES"),
    ):
        assert _rel_from_row(rows[rel_id]).cypher_type == expected
