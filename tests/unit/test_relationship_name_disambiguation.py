# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e6c2a-7d41-4f3e-9a58-1c2e8f4b6d93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-415 (amended 2026-11-16): two generated relationships that would carry the same name (its
id, its GraphQL field or its Cypher type, where that name must be unique) but join different
columns are told apart automatically, as foreign-key aliases are: ``_by_<column>``, then the
table where the column alone does not separate them. Declared foreign keys, accepted discovery
suggestions and several accepted at once, judged against the relationships already registered. A
name a person typed is never changed; a clash with one is refused by name. On a real model store
(SQLite)."""

# Requirements: REQ-415

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.core import schema_org as so
from provisa.core.database import Database, create_engine_from_url
from provisa.core.relationship_naming import RelationshipNameTaken, disambiguated
from provisa.discovery import candidates, fk_introspect
from provisa.discovery.analyzer import RelationshipCandidate

_TABLES = (
    "sources",
    "domains",
    "registered_tables",
    "table_columns",
    "tag_assignments",
    "relationships",
    "relationship_candidates",
    "glossary_terms",
    "glossary_term_refs",
    "glossary_term_domains",
    "glossary_term_edges",
    "glossary_term_experts",
    "profiler_classifications",
    "rls_rules",
    "roles",
    "data_products",
)

_LISTINGS, _NEIGHBOURHOODS, _USERS, _ORDERS = 1, 2, 3, 4

_SHAPES: dict[int, tuple[str, list[tuple[str, bool]]]] = {
    _LISTINGS: (
        "listings",
        [("id", True), ("neighbourhood_id", False), ("neighbourhood", False)],
    ),
    _NEIGHBOURHOODS: ("neighbourhoods", [("id", True), ("neighbourhood", False)]),
    _USERS: ("users", [("id", True), ("name", False)]),
    _ORDERS: ("orders", [("id", True), ("buyer_id", False), ("seller_id", False)]),
}


async def _store(tmp_path) -> Database:
    db = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'store.db'}"), name="naming"
    )
    with db.engine.begin() as conn:
        so.metadata.create_all(conn, tables=[so.metadata.tables[n] for n in _TABLES])
    async with db.acquire() as conn:
        await conn.execute_core(insert(so.sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(so.domains).values(id="city"))
        for tid, (name, columns) in _SHAPES.items():
            await conn.execute_core(
                insert(so.registered_tables).values(
                    id=tid,
                    source_id="pg",
                    domain_id="city",
                    schema_name="public",
                    table_name=name,
                )
            )
            for col, pk in columns:
                await conn.execute_core(
                    insert(so.table_columns).values(
                        table_id=tid,
                        domain_id="city",
                        column_name=col,
                        data_type="integer",
                        is_primary_key=pk,
                    )
                )
    return db


def _suggestion(source_column: str, target_column: str) -> RelationshipCandidate:
    """listings → neighbourhoods on the given columns, named as the model named both."""
    return RelationshipCandidate(
        source_table_id=_LISTINGS,
        source_column=source_column,
        target_table_id=_NEIGHBOURHOODS,
        target_column=target_column,
        cardinality="many-to-one",
        confidence=0.9,
        reasoning="",
        suggested_name="neighbourhood",
    )


async def _suggest(conn: Any) -> tuple[int, int]:
    first, second = await candidates.store_candidates(
        conn,
        [_suggestion("neighbourhood_id", "id"), _suggestion("neighbourhood", "neighbourhood")],
        "run:1",
    )
    return first, second


async def _names(conn: Any) -> dict[str, dict[str, Any]]:
    rows = (
        await conn.execute_core(
            select(
                so.relationships.c.id,
                so.relationships.c.source_table_id,
                so.relationships.c.alias,
                so.relationships.c.graphql_alias,
                so.relationships.c.graphql_field,
                so.relationships.c.cypher_type,
            )
        )
    ).fetchall()
    return {r.id: dict(r._mapping) for r in rows}


def test_the_rule_is_the_foreign_key_rule_then_the_table():
    assert disambiguated("users", "buyer_id", "users", set()) == "users"
    assert disambiguated("users", "seller_id", "users", {"users"}) == "users_by_seller_id"
    assert (
        disambiguated("users", "seller_id", "people", {"users", "users_by_seller_id"})
        == "users_by_people_seller_id"
    )


@pytest.mark.asyncio
async def test_two_suggestions_named_alike_on_different_columns_get_different_names(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        first, second = await _suggest(conn)
        one = await candidates.accept(conn, first)
        two = await candidates.accept(conn, second)
        names = await _names(conn)

    assert one["relationship_id"] == "neighbourhood"
    assert two["relationship_id"] == "neighbourhood_by_neighbourhood"
    a, b = names["neighbourhood"], names["neighbourhood_by_neighbourhood"]
    assert a["graphql_field"] != b["graphql_field"]
    assert a["cypher_type"] != b["cypher_type"]
    # The first keeps the names derived for it; only the second is told apart.
    assert a["alias"] is None and a["graphql_alias"] is None
    assert b["cypher_type"] == f"{a['cypher_type']}_BY_NEIGHBOURHOOD"


@pytest.mark.asyncio
async def test_accepting_both_at_once_with_the_names_shown_tells_them_apart(tmp_path):
    """The run's result sends each proposal's shown name, its suggested name, with the accept."""
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        first, second = await _suggest(conn)
        ids = [
            (await candidates.accept(conn, c, "neighbourhood"))["relationship_id"]
            for c in (first, second)
        ]
        names = await _names(conn)

    assert ids == ["neighbourhood", "neighbourhood_by_neighbourhood"]
    assert len({n["graphql_field"] for n in names.values()}) == 2
    assert len({n["cypher_type"] for n in names.values()}) == 2


@pytest.mark.asyncio
async def test_a_suggestion_named_as_a_registered_relationship_is_told_apart(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(so.relationships).values(
                id="neighbourhood",
                source_table_id=_LISTINGS,
                target_table_id=_NEIGHBOURHOODS,
                source_column="neighbourhood_id",
                target_column="id",
                cardinality="many-to-one",
                graphql_field="neighbourhood",
                cypher_type="BELONGS_TO_NEIGHBOURHOOD",
            )
        )
        _, second = await _suggest(conn)
        accepted = await candidates.accept(conn, second)
        names = await _names(conn)

    assert accepted["relationship_id"] == "neighbourhood_by_neighbourhood"
    held, new = names["neighbourhood"], names["neighbourhood_by_neighbourhood"]
    assert held["graphql_field"] == "neighbourhood"  # the registered one is never renamed
    assert new["graphql_field"] != "neighbourhood"
    assert new["cypher_type"] != "BELONGS_TO_NEIGHBOURHOOD"


@pytest.mark.asyncio
async def test_a_typed_name_that_clashes_is_refused_by_name(tmp_path):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        first, second = await _suggest(conn)
        await candidates.accept(conn, first, "hoods")
        with pytest.raises(RelationshipNameTaken) as refused:
            await candidates.accept(conn, second, "hoods")
        names = await _names(conn)

    assert refused.value.code == "relationship.name_taken"
    assert refused.value.params["name"] == "hoods"
    assert set(names) == {"hoods"}


class _Pools:
    def has(self, _source_id: str) -> bool:
        return False


async def _register_orders_keys(conn: Any, monkeypatch) -> int:
    async def _driver(*_a: Any) -> tuple[object, None]:
        return object(), None

    async def _fks(*_a: Any) -> list[dict[str, str]]:
        return [
            {"fk_table": "orders", "fk_column": col, "ref_table": "users", "ref_column": "id"}
            for col in ("buyer_id", "seller_id")
        ]

    monkeypatch.setattr(fk_introspect, "_constraint_driver", _driver)
    monkeypatch.setattr(fk_introspect, "_pg_fks", _fks)
    return await fk_introspect.auto_register_fk_relationships(
        _Pools(), "postgresql", "pg", "public", "orders", conn
    )


@pytest.mark.asyncio
async def test_foreign_key_aliases_are_unchanged_and_their_fields_told_apart(tmp_path, monkeypatch):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        assert await _register_orders_keys(conn, monkeypatch) == 4
        names = await _names(conn)

    aliases = {i: n["alias"] for i, n in names.items()}
    assert aliases == {
        "fk__orders__buyer_id__to__users": "users",
        "fk__orders__seller_id__to__users": "users_by_seller_id",
        "fk__users__from__orders__buyer_id": "orders",
        "fk__users__from__orders__seller_id": "orders_by_seller_id",
    }
    # Two keys to one table would otherwise be one GraphQL field of each type.
    for table in (_ORDERS, _USERS):
        fields = [n["graphql_field"] for n in names.values() if n["source_table_id"] == table]
        assert len(set(fields)) == 2, fields


@pytest.mark.asyncio
async def test_registering_foreign_keys_again_adds_nothing(tmp_path, monkeypatch):
    db = await _store(tmp_path)
    async with db.acquire() as conn:
        await _register_orders_keys(conn, monkeypatch)
        before = await _names(conn)
        assert await _register_orders_keys(conn, monkeypatch) == 0
        assert await _names(conn) == before


@pytest.mark.asyncio
async def test_two_sides_of_one_table_joining_one_dimension_get_different_fields(tmp_path):
    """A shared dimension's edges carry a generated field, ``<dimension>_dimension``: two columns
    of one table joining it are told apart by their columns."""
    from provisa.core.relationship_naming import Generated, unique_names

    def side(column: str) -> Generated:
        return Generated(
            rel_id=f"listings-{column}-to-neighbourhoods",
            id_typed=False,
            source_table_id=_LISTINGS,
            target_table_id=_NEIGHBOURHOODS,
            source_column=column,
            target_column="neighbourhood",
            cardinality="many-to-one",
            target_name="neighbourhoods",
            target_table="neighbourhoods",
            graphql_alias="neighbourhood_dimension",
        )

    db = await _store(tmp_path)
    async with db.acquire() as conn:
        one, two = await unique_names(conn, [side("neighbourhood"), side("neighbourhood_id")])

    assert one.graphql_alias == "neighbourhood_dimension"
    assert two.graphql_alias == "neighbourhood_dimension_by_neighbourhood_id"
    assert one.rel_id != two.rel_id
