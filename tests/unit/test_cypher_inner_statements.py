# Copyright (c) 2026 Kenneth Stott
# Canary: 7232e432-bb94-4fc0-aea4-c1df316b038f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement inside a statement goes on from the outer row (#160): EXISTS { }, COUNT { },
COLLECT { } and CALL { WITH x } over a node the outer statement holds are answered for that
row, not for every row of its table. The lowered SQL is run on an engine."""

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
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
    placed_by = RelationshipMapping(
        rel_type="PLACED_BY",
        source_label="Orders",
        target_label="Customers",
        join_source_column="customer_code",
        join_target_column="code",
        field_name="customer",
    )
    return CypherLabelMap(
        nodes={
            "Orders": _node("Orders", 1, "orders", ["id", "customer_code"]),
            "Customers": _node("Customers", 2, "customers", ["id", "code", "name"]),
        },
        relationships={"PLACED_BY": placed_by},
        aliases={"PLACED_BY": [placed_by]},
    )


@pytest.fixture
def engine():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute("CREATE TABLE pg.public.orders (id INTEGER, customer_code VARCHAR)")
    con.execute("CREATE TABLE pg.public.customers (id INTEGER, code VARCHAR, name VARCHAR)")
    # Orders 1 and 3 are ann's; order 2 is nobody's; bo has no order.
    con.execute("INSERT INTO pg.public.orders VALUES (1, 'a1'), (2, 'zz'), (3, 'a1')")
    con.execute("INSERT INTO pg.public.customers VALUES (10, 'a1', 'ann'), (20, 'b2', 'bo')")
    return con


_CASES = {
    "exists": (
        "MATCH (o:Orders) WHERE EXISTS { MATCH (o)-[:PLACED_BY]->(c:Customers) } RETURN o.id",
        [(1,), (3,)],
    ),
    "exists_backward": (
        "MATCH (c:Customers) WHERE EXISTS { MATCH (c)<-[:PLACED_BY]-(o:Orders) } RETURN c.id",
        [(10,)],
    ),
    "exists_with_its_own_where": (
        "MATCH (o:Orders) WHERE EXISTS { MATCH (o)-[:PLACED_BY]->(c:Customers) "
        "WHERE c.name = 'bo' } RETURN o.id",
        [],
    ),
    "not_exists": (
        "MATCH (c:Customers) WHERE NOT EXISTS { MATCH (c)<-[:PLACED_BY]-(o:Orders) } RETURN c.id",
        [(20,)],
    ),
    "exists_of_the_outer_row_alone": (
        "MATCH (o:Orders) WHERE EXISTS { MATCH (o) WHERE o.id > 1 } RETURN o.id",
        [(2,), (3,)],
    ),
    "exists_of_a_node_of_its_own": (
        "MATCH (o:Orders) WHERE EXISTS { MATCH (c:Customers) WHERE c.code = o.customer_code } "
        "RETURN o.id",
        [(1,), (3,)],
    ),
    "count": (
        "MATCH (c:Customers) RETURN c.id, COUNT { MATCH (c)<-[:PLACED_BY]-(o:Orders) } AS n",
        [(10, 2), (20, 0)],
    ),
    "call": (
        "MATCH (o:Orders) CALL { WITH o MATCH (o)-[:PLACED_BY]->(c:Customers) "
        "RETURN c.id AS cid } RETURN o.id, cid",
        [(1, 10), (3, 10)],
    ),
}


@pytest.mark.parametrize("case", list(_CASES))
def test_an_inner_statement_is_answered_for_the_outer_row(case, engine):
    cypher, expected = _CASES[case]
    sql_tree, _, _ = cypher_to_sql(parse_cypher(cypher), _label_map(), {})
    lowered = sql_tree.sql(dialect="duckdb")
    assert sorted(engine.execute(lowered).fetchall()) == expected, lowered


def test_collect_gathers_the_outer_rows_own(engine):
    cypher = (
        "MATCH (c:Customers) RETURN c.id, "
        "COLLECT { MATCH (c)<-[:PLACED_BY]-(o:Orders) RETURN o.id } AS ids"
    )
    sql_tree, _, _ = cypher_to_sql(parse_cypher(cypher), _label_map(), {})
    rows = engine.execute(sql_tree.sql(dialect="duckdb")).fetchall()
    assert sorted((cid, sorted(ids)) for cid, ids in rows) == [(10, [1, 3]), (20, [])]
