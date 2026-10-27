# Copyright (c) 2026 Kenneth Stott
# Canary: 83f6da1e-ff90-4a63-80ef-9779ccd012ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The governing rule (REQ-2176): a CREATE naming exactly one model table and no free label is
the governed write; any other makes an ephemeral node carrying every label, anchored in the first
ephemeral table it names, else the first free label's, else the session's default. Model and
domain names resolve first; a domain alone is sealed."""

# Requirements: REQ-2176, REQ-661, REQ-572

from __future__ import annotations

import pytest

from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import pattern
from provisa.ephemeral.classify import classify_create, resolve
from provisa.ephemeral.store import Graph
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping

pytestmark = pytest.mark.unit


def _node(type_name: str, table_label: str, domain: str | None) -> NodeMapping:
    return NodeMapping(
        label=f"{domain}:{table_label}" if domain else table_label,
        type_name=type_name,
        domain_label=domain,
        table_label=table_label,
        table_id=1,
        source_id="s",
        id_column="id",
        pk_columns=["id"],
        catalog_name="c",
        schema_name="s",
        table_name=table_label.lower(),
        properties={"id": "id"},
    )


def _model() -> CypherLabelMap:
    nodes = {
        "Sales_Orders": _node("Sales_Orders", "Orders", "Sales"),
        "Sales_Customers": _node("Sales_Customers", "Customers", "Sales"),
        "Person": _node("Person", "Person", None),
    }
    knows = RelationshipMapping("KNOWS", "Person", "Person", "id", "id", "knows")
    return CypherLabelMap(
        nodes=nodes,
        relationships={"KNOWS": knows},
        domains={"Sales": ["Sales_Orders", "Sales_Customers"]},
        nodes_by_table={
            "Orders": ["Sales_Orders"],
            "Customers": ["Sales_Customers"],
            "Person": ["Person"],
        },
        aliases={"KNOWS": [knows]},
    )


@pytest.mark.parametrize(
    ("labels", "models", "free"),
    [
        (["Person"], ("Person",), ()),
        (["Orders"], ("Sales_Orders",), ()),
        (["Sales", "Orders"], ("Sales_Orders",), ()),
        (["Sales", "Orders", "Temp"], ("Sales_Orders",), ("Temp",)),
        (["Person", "Temp"], ("Person",), ("Temp",)),
        (["Person", "Orders"], ("Person", "Sales_Orders"), ()),
        (["Sales", "Orders", "Person"], ("Sales_Orders", "Person"), ()),
        (["Temp", "Scratch"], (), ("Temp", "Scratch")),
        ([], (), ()),
    ],
)
def test_a_label_resolves_as_the_models_before_the_sessions(labels, models, free):
    resolved = resolve(labels, _model())
    assert (resolved.models, resolved.free) == (models, free)


@pytest.mark.parametrize("labels", [["Sales"], ["Sales", "Temp"]])
def test_a_domain_is_sealed(labels):
    with pytest.raises(CypherParseError, match="domains are sealed"):
        resolve(labels, _model())


def _classify(statement: str, graph: Graph | None = None):
    prefix, patterns = pattern.create_clause(statement)
    return classify_create(
        pattern.parse_patterns(patterns), pattern.bound(prefix), _model(), graph or Graph()
    )


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE (:Person {id: 1})",
        "CREATE (:Sales:Orders {id: 1})",
        "MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)",
    ],
)
def test_one_exact_model_target_is_the_governed_write(statement):
    assert _classify(statement).governed


@pytest.mark.parametrize(
    ("statement", "anchors"),
    [
        ("CREATE (:Temp {k: 1})", ["Temp"]),
        ("CREATE (:Person:Temp)", ["Temp"]),
        ("CREATE (:Person:Orders)", [None]),
        ("CREATE (:Sales:Orders:Person:Temp)", ["Temp"]),
        ("CREATE ()", [None]),
        ("CREATE (:Temp)-[:LINK]->(:Scratch)", ["Temp", "Scratch"]),
        ("MATCH (a:Person), (b:Person) CREATE (a)-[:LIKES]->(b)", []),
        ("UNWIND [1, 2] AS i CREATE (:N {i: i})", ["N"]),
    ],
)
def test_anything_else_is_ephemeral(statement, anchors):
    kind = _classify(statement)
    assert not kind.governed
    assert [c.anchor for c in kind.created] == anchors


def test_an_existing_ephemeral_table_anchors_before_a_new_free_label():
    graph = Graph(anchors={"Scratch": "cypher__anchor__Scratch"})
    assert [c.anchor for c in _classify("CREATE (:Temp:Scratch)", graph).created] == ["Scratch"]


def test_a_node_carries_every_label_given():
    [created] = _classify("CREATE (:Person:Temp:Other)").created
    assert created.labels.written == ("Person", "Temp", "Other")


def test_a_node_naming_two_model_tables_carries_both():
    # No single model row can be both, so the node is the session's (REQ-2176).
    [created] = _classify("CREATE (:Person:Orders)").created
    assert created.labels.keys == ("Person", "Sales_Orders")


@pytest.mark.parametrize(
    ("statement", "match"),
    [
        ("CREATE (:Person), (:Temp)", "model rows or session nodes, not both"),
        (
            "MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b), (a)-[:LIKES]->(b)",
            "model relationships or session relationships, not both",
        ),
        ("MATCH (a:Person) CREATE (a:Temp)", "already declared"),
    ],
)
def test_what_has_no_one_reading_is_refused_by_name(statement, match):
    with pytest.raises(CypherParseError, match=match):
        _classify(statement)
