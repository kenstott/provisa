# Copyright (c) 2026 Kenneth Stott
# Canary: 2baf1816-c2d1-44af-ba10-775a530ce1ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MATCH ... WITH ... MATCH of a new node (#147): the rows the WITH carries are read by the
statement that follows it, so the two are combined as two MATCH clauses are -- the lowered SQL
runs on an engine and returns the pairs the two-MATCH form returns."""

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.label_map import CypherLabelMap, NodeMapping
from provisa.cypher.parser import parse_cypher
from provisa.cypher.translator import cypher_to_sql


def _node(label: str, table_id: int, table: str, columns: list[str]) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=table_id,
        source_id="pg",
        id_column="id",
        pk_columns=[],
        catalog_name="pg",
        schema_name="public",
        table_name=table,
        properties={c: c for c in columns},
    )


def _label_map() -> CypherLabelMap:
    return CypherLabelMap(
        nodes={
            "Orders": _node("Orders", 1, "orders", ["id", "region"]),
            "Customers": _node("Customers", 2, "customers", ["id", "name", "region"]),
        },
        relationships={},
    )


@pytest.fixture
def engine():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute("CREATE TABLE pg.public.orders (id INTEGER, region VARCHAR)")
    con.execute("CREATE TABLE pg.public.customers (id INTEGER, name VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO pg.public.orders VALUES (1, 'east'), (2, 'west'), (3, 'north')")
    con.execute(
        "INSERT INTO pg.public.customers VALUES (10, 'ann', 'east'), (20, 'bo', 'west'), "
        "(30, 'cy', 'east')"
    )
    return con


def _rows(engine, cypher: str) -> list[tuple]:
    sql_tree, _, _ = cypher_to_sql(parse_cypher(cypher), _label_map(), {})
    return sorted(engine.execute(sql_tree.sql(dialect="duckdb")).fetchall())


_TWO_MATCH = "MATCH (o:Orders) MATCH (c:Customers) WHERE o.region = c.region RETURN o.id, c.name"


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (o:Orders) WITH o MATCH (c:Customers) WHERE o.region = c.region RETURN o.id, c.name",
        "MATCH (c:Customers) WITH c MATCH (o:Orders) WHERE o.region = c.region RETURN o.id, c.name",
        "MATCH (o:Orders) WITH o WHERE o.id < 3 MATCH (c:Customers) WHERE o.region = c.region "
        "RETURN o.id, c.name",
    ],
)
def test_a_match_after_a_with_reads_the_rows_the_with_carries(cypher, engine):
    expected = _rows(engine, _TWO_MATCH)
    assert expected == [(1, "ann"), (1, "cy"), (2, "bo")]
    assert _rows(engine, cypher) == expected


def test_a_with_that_carries_a_value_is_read_beside_the_new_match(engine):
    cypher = (
        "MATCH (o:Orders) WITH o.region AS r MATCH (c:Customers) WHERE c.region = r "
        "RETURN r, c.name"
    )
    assert _rows(engine, cypher) == [("east", "ann"), ("east", "cy"), ("west", "bo")]


@pytest.mark.parametrize(
    ("cypher", "expected"),
    [
        ("MATCH (o:Orders) WITH o WHERE o.id < 3 RETURN o.id", [(1,), (2,)]),
        ("MATCH (o:Orders) WITH o.region AS r WHERE r = 'east' RETURN r", [("east",)]),
    ],
)
def test_a_withs_where_reads_what_the_with_carries(cypher, expected, engine):
    assert _rows(engine, cypher) == expected


def test_each_with_in_a_chain_goes_on_from_the_one_before(engine):
    cypher = (
        "MATCH (o:Orders) WITH o.region AS r MATCH (c:Customers) WHERE c.region = r "
        "WITH r, c.name AS name MATCH (p:Orders) WHERE p.region = r RETURN p.id, name"
    )
    assert _rows(engine, cypher) == [(1, "ann"), (1, "cy"), (2, "bo")]


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (o:Orders) WHERE EXISTS { MATCH (o)-[:NOPE]->(c:Customers) } RETURN o.id",
        "MATCH (o:Orders) WHERE COUNT { MATCH (o)-[:NOPE]->(c:Customers) } > 0 RETURN o.id",
        "MATCH (o:Orders) RETURN o.id, COLLECT { MATCH (o)-[:NOPE]->(c:Customers) RETURN c.id }",
        "MATCH (o:Orders) RETURN o.id, [(o)-[:NOPE]->(c:Customers) | c.id] AS ids",
        "MATCH (o:Orders) CALL { WITH o MATCH (o)-[:NOPE]->(c:Customers) RETURN c.id AS cid } "
        "RETURN o.id, cid",
    ],
)
def test_an_unregistered_type_inside_a_statement_is_refused_as_in_its_match(cypher):
    """REQ-603: a statement inside the statement and a pattern comprehension name relationship
    types too; an unregistered one refuses the whole statement, never lowers to no rows or to
    some other relationship."""
    from provisa.cypher.translator_types import UnregisteredRelationshipType

    with pytest.raises(UnregisteredRelationshipType, match=r"type\(s\): NOPE"):
        cypher_to_sql(parse_cypher(cypher), _label_map(), {})
