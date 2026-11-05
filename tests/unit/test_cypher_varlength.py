# Copyright (c) 2026 Kenneth Stott
# Canary: 0e83eca5-196d-4d34-a71e-6a6361d88e72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Variable-length patterns answered on DuckDB (#309, #310). The model: Ann knows Bo, Bo knows
Cy, and Cy and Di know no one."""

# Requirements: REQ-345, REQ-348

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from tests.unit import test_sparql_compiler as _sparql_tests
from tests.unit.test_sparql_compiler import _model, public_url  # noqa: F401 -- fixtures

con = _sparql_tests.con  # the model's DuckDB fixture


def _cypher(con, cypher: str) -> list[tuple]:
    lm = _model()
    sql, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    return sorted(con.execute(sql.sql(dialect="duckdb")).fetchall(), key=repr)


def test_the_length_of_a_recursive_path_is_its_hop_count(con):  # 309
    assert _cypher(
        con,
        "MATCH p = (n:Person {name: 'Ann'})-[:KNOWS*1..2]->(m:Person) RETURN m.name, length(p)",
    ) == [("Bo", 1), ("Cy", 2)]


def test_an_undirected_variable_length_pattern_walks_both_ways(con):  # 310
    # From Bo: Cy one hop out, Ann one hop in -- and no further, since Cy and Ann go on to no one.
    assert _cypher(
        con, "MATCH (n:Person {name: 'Bo'})-[:KNOWS*1..2]-(m:Person) RETURN DISTINCT m.name"
    ) == [("Ann",), ("Cy",)]
    # From Ann: Bo, then Cy beyond him -- two hops, the second the other way from the first.
    assert _cypher(
        con, "MATCH (n:Person {name: 'Ann'})-[:KNOWS*1..2]-(m:Person) RETURN DISTINCT m.name"
    ) == [("Bo",), ("Cy",)]


def test_an_undirected_walk_takes_no_edge_twice(con):  # 310
    # Two hops from Bo: Ann and Cy are each one edge away, and no second edge leads on from
    # either. Going back over the edge just taken would return Bo; a path repeats no edge.
    assert (
        _cypher(con, "MATCH (n:Person {name: 'Bo'})-[:KNOWS*2..2]-(m:Person) RETURN m.name") == []
    )


def test_the_length_of_an_undirected_walk_is_its_hop_count(con):  # 309, 310
    assert _cypher(
        con,
        "MATCH p = (n:Person {name: 'Ann'})-[:KNOWS*1..2]-(m:Person) RETURN m.name, length(p)",
    ) == [("Bo", 1), ("Cy", 2)]
