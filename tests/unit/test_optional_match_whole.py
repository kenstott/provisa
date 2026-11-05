# Copyright (c) 2026 Kenneth Stott
# Canary: 73576f77-bda3-40c4-9059-00d865b16e71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OPTIONAL MATCH's pattern matches whole or not at all (REQ-345), answered on DuckDB: every
hop, every comma-separated part and its own WHERE, or none of its variables is bound.

The model: Ann and Bo work at Acme, whose parent is Beta, whose parent is Corp; Cy works at Beta;
Di works nowhere."""

# Requirements: REQ-345

from provisa.cypher.parser import parse_cypher
from tests.unit import test_sparql_compiler as _sparql_tests
from tests.unit.test_graph_where_scope import _cypher
from tests.unit.test_sparql_compiler import _model, public_url  # noqa: F401 -- fixtures

con = _sparql_tests.con  # the model's DuckDB fixture

_NONE = (None, None, None)


def test_a_path_that_matches_only_part_way_binds_none_of_its_variables(con):
    # Cy's Beta has a parent (Corp), which has none: the three-hop pattern does not match.
    assert _cypher(
        con,
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company)-[:PARENT]->(p:Company)"
        "-[:PARENT]->(q:Company) RETURN n.name, c.name, p.name, q.name",
    ) == [
        ("Ann", "Acme", "Beta", "Corp"),
        ("Bo", "Acme", "Beta", "Corp"),
        ("Cy", *_NONE),
        ("Di", *_NONE),
    ]


def test_comma_separated_parts_are_one_pattern(con):
    assert _cypher(
        con,
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company), "
        "(c)-[:PARENT]->(p:Company)-[:PARENT]->(q:Company) RETURN n.name, c.name, p.name, q.name",
    ) == [
        ("Ann", "Acme", "Beta", "Corp"),
        ("Bo", "Acme", "Beta", "Corp"),
        ("Cy", *_NONE),
        ("Di", *_NONE),
    ]


def test_the_where_of_the_match_is_part_of_the_whole(con):
    # Acme's parent is Beta, not Corp: Ann and Bo keep neither their company nor its parent.
    assert _cypher(
        con,
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:WORKS_AT]->(c:Company)-[:PARENT]->(p:Company) "
        "WHERE p.name = 'Corp' RETURN n.name, c.name, p.name",
    ) == [("Ann", None, None), ("Bo", None, None), ("Cy", "Beta", "Corp"), ("Di", None, None)]


def test_an_optional_match_takes_comma_separated_patterns():
    ast = parse_cypher("MATCH (n) OPTIONAL MATCH (n)-[:R]->(m), (m)-[:S]->(o) RETURN n")
    assert [m.optional for m in ast.match_clauses] == [False, True, True]
    assert ast.match_clauses[2].pattern.nodes[0].variable == "m"


# --- a statement that opens with an OPTIONAL MATCH ------------------------------------------------


def test_an_opening_optional_match_that_matches_nothing_answers_one_null_row(con):
    assert _cypher(con, "OPTIONAL MATCH (n:Person) WHERE n.age > 100 RETURN n.name") == [(None,)]
    assert _cypher(con, "OPTIONAL MATCH (n:Person {name: 'Zed'}) RETURN n.name") == [(None,)]


def test_an_opening_optional_match_answers_what_it_matches(con):
    assert _cypher(con, "OPTIONAL MATCH (n:Person) WHERE n.age > 40 RETURN n.name") == [("Ann",)]


def test_an_opening_optional_pattern_matches_whole_or_not_at_all(con):
    # Nobody works at Corp: not the people without it, one row with neither.
    assert _cypher(
        con,
        "OPTIONAL MATCH (n:Person)-[:WORKS_AT]->(c:Company {name: 'Corp'}) RETURN n.name, c.name",
    ) == [(None, None)]


def test_a_count_over_an_opening_optional_match_that_matches_nothing_is_zero(con):
    assert _cypher(con, "OPTIONAL MATCH (n:Person) WHERE n.age > 100 RETURN count(n)") == [(0,)]
