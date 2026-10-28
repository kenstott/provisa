# Copyright (c) 2026 Kenneth Stott
# Canary: f69e7461-26bd-45e9-996f-e00b778b7ab3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Graph writes in the graph IR (REQ-2176, REQ-2135): CREATE, MERGE, SET, REMOVE and DELETE parse
into write clauses of the IR, each a step of the statement's pipeline in the order written, every
value an expression tree. The read entry still takes no write."""

# Requirements: REQ-2176, REQ-2135

from __future__ import annotations

import pytest

from provisa.cypher.parser import CypherParseError, parse_cypher, parse_cypher_statement
from provisa.gql.expr_ast import Binary, Literal, MapLiteral, Parameter, Property, Variable
from provisa.gql.ir import (
    CreateClause,
    DeleteClause,
    MatchStep,
    MergeClause,
    RemoveClause,
    SetClause,
    UnwindClause,
)

pytestmark = pytest.mark.unit


def test_a_create_is_a_write_clause_of_patterns_whose_values_are_expressions():
    ast = parse_cypher_statement(
        "CREATE (a:Person {id: 1, name: $n})-[:KNOWS {since: 2 * 3}]->(b:Temp)"
    )
    [create] = ast.pipeline
    assert isinstance(create, CreateClause)
    [path] = create.patterns
    a, b = path.nodes
    assert (a.variable, a.labels, b.variable, b.labels) == ("a", ["Person"], "b", ["Temp"])
    assert a.properties == {"id": Literal(1, "number"), "name": Parameter("n")}
    [rel] = path.rels
    assert (rel.types, rel.direction) == (["KNOWS"], "right")
    assert isinstance(rel.properties["since"], Binary)
    assert ast.return_clause is None
    assert ast.writes == [create]


def test_a_create_of_several_patterns_and_anonymous_nodes():
    [create] = parse_cypher_statement("CREATE (), (:A:B {k: x.y}), (c)<-[:R]-(d)").pipeline
    assert isinstance(create, CreateClause)
    assert [p.nodes[0].labels for p in create.patterns] == [[], ["A", "B"], []]
    assert create.patterns[0].nodes[0].variable is None
    assert create.patterns[1].nodes[0].properties == {"k": Property(Variable("x"), "y")}
    assert create.patterns[2].rels[0].direction == "left"


def test_leading_clauses_bind_what_a_create_reaches_back_to():
    ast = parse_cypher_statement("UNWIND $xs AS x MATCH (p:Person) CREATE (p)-[:SAW]->(:T {v: x})")
    unwind, match, create = ast.pipeline
    assert isinstance(unwind, UnwindClause) and isinstance(match, MatchStep)
    assert isinstance(create, CreateClause)
    assert create.patterns[0].nodes[1].properties == {"v": Variable("x")}


def test_set_items_labels_properties_and_maps():
    ast = parse_cypher_statement(
        "MATCH (n:P) WHERE n.id = 1 SET n:A:B, n.x = n.x + 1, n += {y: 2}, n = {z: 3}"
    )
    match, set_clause = ast.pipeline
    assert isinstance(match, MatchStep) and match.where is not None
    assert isinstance(set_clause, SetClause)
    labels, prop, merged, replaced = set_clause.items
    assert (labels.variable, labels.labels) == ("n", ["A", "B"])
    assert (prop.variable, prop.property, isinstance(prop.value, Binary)) == ("n", "x", True)
    assert (merged.property, merged.merge, merged.value) == (
        None,
        True,
        MapLiteral([("y", Literal(2, "number"))]),
    )
    assert (replaced.property, replaced.merge) == (None, False)


def test_remove_items_labels_and_properties():
    [_, remove] = parse_cypher_statement("MATCH (n) REMOVE n:A:B, n.x").pipeline
    assert isinstance(remove, RemoveClause)
    assert [(i.variable, i.labels, i.property) for i in remove.items] == [
        ("n", ["A", "B"], None),
        ("n", [], "x"),
    ]


@pytest.mark.parametrize(
    ("statement", "detach", "deleted"),
    [
        ("MATCH (n) DELETE n", False, [Variable("n")]),
        ("MATCH (n)-[r]->() DELETE r, n", False, [Variable("r"), Variable("n")]),
        ("MATCH (n) DETACH DELETE n", True, [Variable("n")]),
    ],
)
def test_delete_and_detach_delete(statement, detach, deleted):
    delete = parse_cypher_statement(statement).pipeline[-1]
    assert isinstance(delete, DeleteClause)
    assert (delete.detach, delete.expressions) == (detach, deleted)


def test_merge_with_its_on_create_and_on_match_sets():
    [merge] = parse_cypher_statement(
        "MERGE (n:P {id: 1}) ON CREATE SET n.c = 1 ON MATCH SET n.m = 2, n:Seen"
    ).pipeline
    assert isinstance(merge, MergeClause)
    assert merge.pattern.nodes[0].properties == {"id": Literal(1, "number")}
    assert [(i.property, i.value) for i in merge.on_create] == [("c", Literal(1, "number"))]
    assert [(i.property, i.labels) for i in merge.on_match] == [("m", []), (None, ["Seen"])]


def test_a_write_followed_by_return_keeps_both():
    ast = parse_cypher_statement("CREATE (n:Person {id: 1}) RETURN n, id(n) AS i")
    assert isinstance(ast.pipeline[0], CreateClause)
    assert ast.return_clause is not None and len(ast.return_clause.items) == 2


def test_a_write_then_more_clauses_in_order():
    ast = parse_cypher_statement("MATCH (a) CREATE (b:T) WITH a, b SET b:U RETURN b")
    kinds = [type(step).__name__ for step in ast.pipeline]
    assert kinds == ["MatchStep", "CreateClause", "WithClause", "SetClause"]


def test_a_statement_with_neither_a_write_nor_a_return_is_refused():
    with pytest.raises(CypherParseError, match="Missing RETURN clause"):
        parse_cypher_statement("MATCH (n)")


def test_the_read_entry_takes_no_write():
    with pytest.raises(CypherParseError):
        parse_cypher("CREATE (n:Temp {k: 1})")


def test_a_read_parses_as_it_does_through_the_read_entry():
    statement = "MATCH (n:P) WHERE n.k > 1 RETURN n.k AS k ORDER BY k LIMIT 3"
    assert parse_cypher_statement(statement) == parse_cypher(statement)
    assert parse_cypher_statement(statement).writes == []
