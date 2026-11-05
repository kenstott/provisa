# Copyright (c) 2026 Kenneth Stott
# Canary: 55cdda0c-ea9f-43e7-b73a-28d03cb8b9d0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``[r:A|B]`` on a single hop (REQ-1586, REQ-345): types that are rows of one junction, told apart
by its type column, are one join of that junction whose type column holds any of their values;
type(r) is the type its row names. OPTIONAL keeps one row for a node with none of them. An
alternation over types that are not one junction's rows is refused by name. Each case runs on
DuckDB against the rows it must answer."""

# Requirements: REQ-1586, REQ-345

import json

import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import CypherTranslateError
from tests.unit.test_cypher_junction_relationships import _fk_edge, _term_node
from tests.unit.test_cypher_translator_correctness import _rows, terms  # noqa: F401  (fixture)
from tests.unit.test_sql_to_cypher_shapes import _edge


def test_an_alternation_answers_each_of_its_types_rows(terms):  # noqa: F811
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm)-[r:KIND_OF|PART_OF]->(b:GlossaryTerm) "
        "RETURN a.name, type(r), b.name",
    ) == sorted(
        [
            ("customer", "KIND_OF", "party"),
            ("ledger", "PART_OF", "party"),
            ("ledger", "KIND_OF", "customer"),
            ("ledger", "KIND_OF", "party"),
        ],
        key=repr,
    )


def test_an_optional_alternation_keeps_one_row_for_a_node_with_none(terms):  # noqa: F811
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) OPTIONAL MATCH (a)-[r:KIND_OF|PART_OF]->(b:GlossaryTerm) "
        "RETURN a.name, type(r), b.name",
    ) == sorted(
        [
            ("party", None, None),
            ("customer", "KIND_OF", "party"),
            ("ledger", "PART_OF", "party"),
            ("ledger", "KIND_OF", "customer"),
            ("ledger", "KIND_OF", "party"),
        ],
        key=repr,
    )


def test_an_alternation_s_type_is_filterable_and_its_arrow_holds(terms):  # noqa: F811
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm {name: 'party'})<-[r:KIND_OF|PART_OF]-(b:GlossaryTerm) "
        "WHERE type(r) = 'PART_OF' RETURN b.name",
    ) == [("ledger",)]


def test_an_alternation_s_edge_names_its_own_type(terms):  # noqa: F811
    rows = _rows(terms, "MATCH (a:GlossaryTerm)-[r:KIND_OF|PART_OF]->(b:GlossaryTerm) RETURN r")
    edges = [json.loads(r) for (r,) in rows]
    assert sorted((e["type"], e["identity"]) for e in edges) == [
        ("KIND_OF", "KIND_OF:2-1"),
        ("KIND_OF", "KIND_OF:3-1"),
        ("KIND_OF", "KIND_OF:3-2"),
        ("PART_OF", "PART_OF:3-1"),
    ]


def test_an_alternation_in_a_pattern_predicate(terms):  # noqa: F811
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) WHERE NOT EXISTS { MATCH (a)-[:KIND_OF|PART_OF]->(:GlossaryTerm) } "
        "RETURN a.name",
    ) == [("party",)]


@pytest.mark.parametrize(
    "pattern",
    [
        "(a:GlossaryTerm)-[r:KIND_OF|OWNER]->(b:GlossaryTerm)",  # a junction's type and an FK's
        "(a:GlossaryTerm)-[r:KIND_OF|ANTONYM_OF]->(b:GlossaryTerm)",  # a type not declared
        "(a:GlossaryTerm)-[r:KIND_OF|PART_OF]-(b:GlossaryTerm)",  # both ways round
    ],
)
def test_an_alternation_that_is_not_one_junction_s_rows_is_refused(pattern):
    kind, part = _edge("KIND_OF", "kind_of"), _edge("PART_OF", "part_of")
    owner = _fk_edge("OWNER", "GlossaryTerm", "GlossaryTerm")
    lm = CypherLabelMap(
        nodes={"GlossaryTerm": _term_node()},
        relationships={"KIND_OF": kind, "PART_OF": part, "OWNER": owner},
        aliases={"KIND_OF": [kind], "PART_OF": [part], "OWNER": [owner]},
    )
    with pytest.raises(CypherTranslateError, match="one pattern per type"):
        cypher_to_sql(parse_cypher(f"MATCH {pattern} RETURN a.name"), lm, {})


def test_an_untyped_hop_over_a_junction_reads_each_of_its_rows_of_any_type(terms):  # noqa: F811
    # A row of a type no relationship declares is a relationship of an untyped hop all the same:
    # the hop is every row of the junction, never the declared types' rows only. Its type is its
    # stored value, as it has no declared name.
    terms[1].execute(
        "INSERT INTO provisa_admin.public.glossary_term_edges VALUES (11, 1, 2, 'other', NULL)"
    )
    assert _rows(
        terms, "MATCH (a:GlossaryTerm)-[r]->(b:GlossaryTerm) RETURN a.name, b.name, r.id, type(r)"
    ) == sorted(
        [
            ("customer", "party", 7, "KIND_OF"),
            ("ledger", "party", 8, "PART_OF"),
            ("ledger", "customer", 9, "KIND_OF"),
            ("ledger", "party", 10, "KIND_OF"),
            ("party", "customer", 11, "other"),
        ],
        key=repr,
    )


def test_a_match_s_where_holds_beside_an_optional_match_s_own(terms):  # noqa: F811
    # Each WHERE is its clause's: the MATCH's filters the rows, the OPTIONAL MATCH's the pattern.
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) WHERE a.name = 'ledger' "
        "OPTIONAL MATCH (a)-[r:KIND_OF]->(b:GlossaryTerm) WHERE b.name = 'party' "
        "RETURN a.name, b.name",
    ) == [("ledger", "party")]
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) WHERE a.name = 'party' "
        "OPTIONAL MATCH (a)-[r:KIND_OF]->(b:GlossaryTerm) WHERE b.name = 'zz' "
        "RETURN a.name, b.name",
    ) == [("party", None)]


def test_an_optional_junction_hop_constrained_at_its_far_end_keeps_no_refused_row(terms):  # noqa: F811
    # ledger KIND_OF customer and party: the customer edge is no match, not a row with no node.
    assert _rows(
        terms,
        "MATCH (a:GlossaryTerm) OPTIONAL MATCH (a)-[r:KIND_OF]->(b:GlossaryTerm) "
        "WHERE b.name = 'party' RETURN a.name, b.name",
    ) == sorted([("customer", "party"), ("ledger", "party"), ("party", None)], key=repr)


def test_an_optional_relationship_that_matched_none_is_null(terms):  # noqa: F811
    rows = _rows(
        terms,
        "MATCH (a:GlossaryTerm) WHERE a.name = 'party' "
        "OPTIONAL MATCH (a)-[r:KIND_OF]->(b:GlossaryTerm) RETURN r",
    )
    assert rows == [(None,)]
