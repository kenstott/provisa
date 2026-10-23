# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A WITH carries each node it names as that node (REQ-345, #293): two nodes whose tables share
column names keep their own values after the WITH, a node a WITH carries is null where an
OPTIONAL MATCH found none (the anti-join), and a MATCH after the WITH goes on from it. Each is
answered on DuckDB."""

# Requirements: REQ-345

import json

from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from tests.unit import test_sparql_compiler as _sparql_tests
from tests.unit.test_sparql_compiler import _model, public_url  # noqa: F401 -- fixture

con = _sparql_tests.con  # the model's DuckDB fixture


def _rows(con, cypher: str) -> list[tuple]:
    lm = _model()
    sql, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    return sorted(con.execute(sql.sql(dialect="duckdb")).fetchall(), key=repr)


def test_the_anti_join_keeps_the_rows_an_optional_match_found_nothing_for(con):
    assert _rows(
        con,
        "MATCH (s:Person) OPTIONAL MATCH (s)-[:WORKS_AT]->(c:Company) WITH s, c "
        "WHERE c IS NULL RETURN s.name",
    ) == [("Di",)]


def test_two_carried_nodes_keep_their_own_properties(con):
    assert _rows(
        con,
        "MATCH (s:Person)-[:WORKS_AT]->(c:Company) WITH s, c RETURN s.name, c.name, c.id",
    ) == [("Ann", "Acme", 1), ("Bo", "Acme", 1), ("Cy", "Beta", 2)]


def test_a_match_after_the_with_goes_on_from_a_carried_node(con):
    assert _rows(
        con,
        "MATCH (s:Person)-[:WORKS_AT]->(c:Company) WITH s, c "
        "MATCH (c)-[:PARENT]->(p:Company) RETURN s.name, p.name",
    ) == [("Ann", "Beta"), ("Bo", "Beta"), ("Cy", "Corp")]


def test_a_carried_node_with_a_where_on_its_property(con):
    assert _rows(
        con,
        "MATCH (s:Person)-[:WORKS_AT]->(c:Company) WITH s, c WHERE c.name = 'Beta' "
        "RETURN s.name, s.age",
    ) == [("Cy", 29)]


def test_a_carried_node_returned_whole_is_its_own(con):
    lm = _model()
    cypher = (
        "MATCH (s:Person) OPTIONAL MATCH (s)-[:WORKS_AT]->(c:Company) WITH s, c RETURN s.name, c"
    )
    sql, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    rows = {
        name: (json.loads(node) if isinstance(node, str) else node)
        for name, node in con.execute(sql.sql(dialect="duckdb")).fetchall()
    }
    assert rows["Di"] is None  # an OPTIONAL MATCH that found none carries no node
    assert rows["Cy"]["name"] == "Beta" and rows["Cy"]["label"] == "Company"
