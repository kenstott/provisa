# Copyright (c) 2026 Kenneth Stott
# Canary: b79a1baf-a6c0-40dd-9efe-c64b61850534
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A variable-length path keeps its lower bound (#283): [*3..5] returns only pairs three to five
hops apart, and [*0..n] pairs each node with itself too. The recursive form runs on DuckDB over a
chain of people, so the rows it returns are checked, not only its SQL."""

# Requirements: REQ-348, REQ-2135

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CypherTranslateError
from tests.unit.table_ids import table_id


def _node(label: str, table: str) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=table_id(label),
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="pg",
        schema_name="public",
        table_name=table,
        properties={"name": "name"},
    )


def _rel(rel_type: str, src: str, tgt: str, col: str) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type=rel_type,
        source_label=src,
        target_label=tgt,
        join_source_column=col,
        join_target_column="id",
        field_name=rel_type.lower(),
    )


def _people() -> CypherLabelMap:
    return CypherLabelMap(
        nodes={"Person": _node("Person", "persons")},
        relationships={"KNOWS": _rel("KNOWS", "Person", "Person", "knows_id")},
    )


def _orders() -> CypherLabelMap:
    """Order to City in one hop (SHIPS_TO) and in two (PLACED_BY, then LIVES_IN)."""
    return CypherLabelMap(
        nodes={
            "Order": _node("Order", "orders"),
            "Customer": _node("Customer", "customers"),
            "City": _node("City", "cities"),
        },
        relationships={
            "SHIPS_TO": _rel("SHIPS_TO", "Order", "City", "ship_city_id"),
            "PLACED_BY": _rel("PLACED_BY", "Order", "Customer", "customer_id"),
            "LIVES_IN": _rel("LIVES_IN", "Customer", "City", "city_id"),
        },
    )


def _sql(cypher: str, lm: CypherLabelMap) -> str:
    return cypher_to_sql(parse_cypher(cypher), lm, {})[0].sql(dialect="duckdb")


@pytest.fixture
def chain() -> duckdb.DuckDBPyConnection:
    """p1 knows p2 knows p3 knows p4 knows p5; p5 knows no one."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute("CREATE SCHEMA pg.public")
    con.execute("CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR, knows_id INTEGER)")
    con.execute(
        "INSERT INTO pg.public.persons VALUES "
        "(1, 'p1', 2), (2, 'p2', 3), (3, 'p3', 4), (4, 'p4', 5), (5, 'p5', NULL)"
    )
    return con


def _pairs(con: duckdb.DuckDBPyConnection, cypher: str) -> set[tuple[str, str]]:
    return {(a, b) for a, b in con.execute(_sql(cypher, _people())).fetchall()}


def test_a_recursive_path_keeps_its_lower_bound(chain):
    pairs = _pairs(chain, "MATCH (a:Person)-[:KNOWS*3..5]->(b:Person) RETURN a.name, b.name")
    assert pairs == {("p1", "p4"), ("p1", "p5"), ("p2", "p5")}


def test_a_recursive_path_from_one_hop_is_unchanged(chain):
    pairs = _pairs(chain, "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name, b.name")
    assert pairs == {
        ("p1", "p2"),
        ("p2", "p3"),
        ("p3", "p4"),
        ("p4", "p5"),
        ("p1", "p3"),
        ("p2", "p4"),
        ("p3", "p5"),
    }
    assert "hops >=" not in _sql(
        "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name, b.name", _people()
    )


def test_a_zero_length_path_pairs_each_node_with_itself(chain):
    pairs = _pairs(chain, "MATCH (a:Person)-[:KNOWS*0..1]->(b:Person) RETURN a.name, b.name")
    assert pairs == {(f"p{i}", f"p{i}") for i in range(1, 6)} | {
        ("p1", "p2"),
        ("p2", "p3"),
        ("p3", "p4"),
        ("p4", "p5"),
    }


def test_a_flat_path_keeps_only_schema_paths_at_least_its_lower_bound():
    sql = _sql("MATCH (o:Order)-[*2..2]->(c:City) RETURN o.name, c.name", _orders())
    assert '"customer_id"' in sql and '"city_id"' in sql
    assert '"ship_city_id"' not in sql
    one_or_two = _sql("MATCH (o:Order)-[*1..2]->(c:City) RETURN o.name, c.name", _orders())
    assert '"ship_city_id"' in one_or_two


def test_a_flat_path_with_no_schema_path_that_long_is_refused():
    with pytest.raises(CypherTranslateError, match="No schema path"):
        _sql("MATCH (o:Order)-[:SHIPS_TO*2..3]->(c:City) RETURN o.name, c.name", _orders())
