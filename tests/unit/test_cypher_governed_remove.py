# Copyright (c) 2026 Kenneth Stott
# Canary: 46aac4cd-5a1e-4a9a-bd48-c542e70cd9da
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REMOVE on model nodes (REQ-2176): "SET n:L and REMOVE n:L write node_labels rows on ephemeral or
model nodes; a node never moves table." A model node's session tag is removed; its own label and
its ontology Class (REQ-2163, a read-only schema) are not; a REMOVE of a property has no write
form and is refused by name."""

# Requirements: REQ-2176, REQ-2163

from __future__ import annotations

from dataclasses import replace

import pytest

from provisa.cypher.parser import CypherParseError
from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_MODEL = {
    "Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')"),
    "Customer": ({"id": "integer", "name": "text"}, "(1, 'Ann')"),
}


def _names(rows) -> list[str]:
    return sorted(r["n.name"] for r in rows)


def _with_class(s: Session, label: str) -> None:
    """Make ``label``'s table an ontology Class table (a negative table id, REQ-2163)."""
    s.model.nodes[label] = replace(s.model.nodes[label], table_id=-7)


def test_remove_takes_a_session_tag_off_a_model_node(monkeypatch):
    s = Session(monkeypatch, _MODEL)
    s.run("MATCH (p:Person) SET p:Vip")
    s.run("MATCH (p:Person) WHERE p.name = 'Ann' REMOVE p:Vip")
    assert _names(s.run("MATCH (n:Vip) RETURN n.name")) == ["Bo"]
    assert _names(s.run("MATCH (n:Person) RETURN n.name")) == ["Ann", "Bo"]


def test_remove_of_a_label_a_model_node_does_not_carry_changes_nothing(monkeypatch):
    s = Session(monkeypatch, _MODEL)
    s.run("MATCH (p:Person) REMOVE p:Vip")
    assert _names(s.run("MATCH (n:Person) RETURN n.name")) == ["Ann", "Bo"]


def test_remove_of_a_model_nodes_own_label_is_refused_by_name(monkeypatch):
    s = Session(monkeypatch, _MODEL)
    with pytest.raises(CypherParseError, match="Person is a model node's own label"):
        s.run("MATCH (p:Person) REMOVE p:Person")


def test_remove_of_an_ontology_class_from_a_model_node_is_refused_by_name(monkeypatch):
    s = Session(monkeypatch, _MODEL)
    _with_class(s, "Customer")
    with pytest.raises(CypherParseError, match="Customer is an ontology Class") as err:
        s.run("MATCH (p:Person) REMOVE p:Customer")
    assert "read-only ontology schema" in str(err.value)
    assert "REQ-" not in str(err.value)


def test_remove_of_a_property_is_refused_by_name():
    with pytest.raises(CypherWriteParseError, match="A REMOVE of a property is not written") as err:
        parse_cypher_write("MATCH (p:Person) REMOVE p.name")
    assert "SET, of a column its table declares" in str(err.value)
    assert "REQ-" not in str(err.value)


def test_remove_of_a_property_beside_a_label_is_refused_as_a_propertys():
    with pytest.raises(CypherWriteParseError, match="A REMOVE of a property is not written"):
        parse_cypher_write("MATCH (p:Person) REMOVE p:Vip, p.name")


def test_a_label_remove_the_governed_writer_meets_says_where_it_is_written():
    # The graph session takes a label REMOVE ending its statement; one followed by a RETURN
    # reaches the governed writer, which says so.
    with pytest.raises(CypherWriteParseError, match="in a statement of its own") as err:
        parse_cypher_write("MATCH (p:Person) REMOVE p:Vip RETURN p.name")
    assert "A REMOVE of a property" not in str(err.value)
    assert "REQ-" not in str(err.value)
