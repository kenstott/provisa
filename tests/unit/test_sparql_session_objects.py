# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7c9e14-6b3d-4f81-9d05-c8e1b4a7f362
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPARQL Update's session objects in the session tables Cypher uses (REQ-2135, amended
2026-11-23; REQ-2176): a blank node is a session node, a class outside the model a free label, a
literal predicate a property (many values one list), an IRI predicate a relationship type, a
named graph a graph tag; each IRI's name is a row of the session's IRI table. Cypher reads what
SPARQL wrote. Graph management acts on session graphs; the model's own graph is refused by name.

The session and the model sit in one DuckDB (tests/unit/ephemeral_harness.py); Cypher reads run
through the seam exactly as a surface runs them."""

# Requirements: REQ-2135, REQ-2176

from __future__ import annotations

import asyncio
import dataclasses

import pytest

from provisa.cypher.counters import counting
from provisa.ephemeral import store
from provisa.sparql_query.compiler import SparqlRefused
from provisa.sparql_query.session_graph import IRIS, SessionWrite, names_of
from provisa.sparql_query.update import (
    ground_graphs,
    model_identity,
    parse_update,
    split_session,
)
from provisa.sparql_query.vocabulary import Grounding, build_vocabulary
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

BASE = "https://data.example.com"
P = f"{BASE}/id/hr/person"
V = f"{BASE}/vocab/hr"
EX = "http://ex.org/"
PREFIXES = f"PREFIX ex: <{EX}> PREFIX p: <{V}/person#> PREFIX hr: <{V}/> "

_PERSON = {"id": "integer", "name": "text", "age": "integer"}
_MODEL = {"Person": (_PERSON, "(1, 'Ann', 30), (2, 'Bo', 40)")}


def _session(monkeypatch) -> Session:
    s = Session(monkeypatch, _MODEL)
    for label, nm in list(s.model.nodes.items()):
        s.model.nodes[label] = dataclasses.replace(nm, domain_id="hr")
    from provisa.core import settings_registry

    held = settings_registry.value
    monkeypatch.setattr(
        settings_registry, "value", lambda key: BASE if key == "server.public_url" else held(key)
    )
    return s


def _vocab(s: Session):
    types = {("Person", prop): kind for prop, kind in _PERSON.items()}
    return build_vocabulary(s.model, BASE, types, Grounding())


def _update(s: Session, text: str):
    """The update's session objects as /data/sparql writes them (sparql_router._updated); the
    model's own triples are tests/unit/test_sparql_update.py's. Returns the counters."""
    vocab = _vocab(s)

    async def run():
        names = await names_of(s.state, s.session, s.model)
        objects = SessionWrite(
            s.state, s.session, s.model, vocab, names, lambda t: model_identity(t, vocab, s.model)
        )
        for op in parse_update(PREFIXES + text, BASE):
            if op.manage is not None:
                await objects.manage(op.manage)
                continue
            deletes, inserts = op.delete_graphs, op.insert_graphs
            if op.where is not None:
                deletes, inserts = ground_graphs(deletes, [{}]), ground_graphs(inserts, [{}])
            model_deletes, deletes[""] = split_session(deletes.get("", []), BASE)
            model_inserts, inserts[""] = split_session(inserts.get("", []), BASE)
            assert not model_deletes and not model_inserts, "a session object test writes none"
            await objects.delete(deletes)
            await objects.insert(inserts)
        return objects

    with counting() as counted:
        objects = asyncio.run(run())
    return counted, objects


def _names(s: Session) -> dict[tuple[str, str, str], str]:
    rows = s.duck.execute(f'SELECT * FROM "{IRIS}"').fetchall()
    return {(iri, graph, kind): name for iri, graph, kind, name in rows}


def _rels(s: Session) -> list[tuple]:
    return s.duck.execute(
        f'SELECT type, from_id, to_id, graph FROM "{store.RELS}" ORDER BY ALL'
    ).fetchall()


# --- INSERT DATA ---------------------------------------------------------------------------------


def test_a_blank_node_is_a_session_node_cypher_reads(monkeypatch):
    s = _session(monkeypatch)
    counted, objects = _update(s, 'INSERT DATA { _:b a ex:Thing ; ex:title "One" }')
    assert objects.wrote
    assert (counted.nodes_created, counted.labels_added, counted.properties_set) == (1, 1, 1)
    assert s.run("MATCH (n:Thing) RETURN n.title AS t") == [{"t": "One"}]
    names = _names(s)
    assert names[(f"{EX}Thing", "", "class")] == "Thing"
    assert names[(f"{EX}title", "", "property")] == "title"


def test_a_blank_node_is_a_new_node_each_operation(monkeypatch):
    s = _session(monkeypatch)
    _update(s, "INSERT DATA { _:b a ex:Thing } ; INSERT DATA { _:c a ex:Thing }")
    assert s.run("MATCH (n:Thing) RETURN count(n) AS c") == [{"c": 2}]


def test_a_blank_node_named_by_two_operations_is_no_sparql(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="is not SPARQL this endpoint reads"):
        _update(s, "INSERT DATA { _:b ex:p ex:o } ; INSERT DATA { _:b ex:p ex:o }")


def test_an_iri_outside_the_base_is_one_session_node_its_values_a_list(monkeypatch):
    s = _session(monkeypatch)
    _update(s, 'INSERT DATA { ex:s ex:tag "a", "b" }')
    _update(s, 'INSERT DATA { ex:s ex:tag "b", "c" }')
    [row] = s.run("MATCH (n) WHERE n.tag IS NOT NULL RETURN n.tag AS t")
    assert sorted(row["t"]) == ["a", "b", "c"]
    names = _names(s)
    assert names[(f"{EX}s", "", "node")] in store.graph(s.session).node_keys


def test_an_iri_predicate_is_a_relationship_type(monkeypatch):
    s = _session(monkeypatch)
    counted, _ = _update(s, "INSERT DATA { ex:a ex:knows ex:b }")
    assert (counted.nodes_created, counted.relationships_created) == (2, 1)
    assert s.run("MATCH ()-[r:knows]->() RETURN count(r) AS c") == [{"c": 1}]
    # RDF holds a triple once.
    counted, _ = _update(s, "INSERT DATA { ex:a ex:knows ex:b }")
    assert counted.relationships_created == 0
    assert len(_rels(s)) == 1


def test_a_session_relationship_ends_on_the_model_row_its_iri_names(monkeypatch):
    s = _session(monkeypatch)
    _update(s, f"INSERT DATA {{ _:b a ex:Badge ; ex:heldBy <{P}/1> }}")
    rows = s.run("MATCH (:Badge)-[:heldBy]->(p:Person) RETURN p.name AS n")
    assert rows == [{"n": "Ann"}]


def test_a_class_named_like_a_model_label_is_given_its_own_name(monkeypatch):
    s = _session(monkeypatch)
    _update(s, "INSERT DATA { _:b a ex:Person }")
    assert _names(s)[(f"{EX}Person", "", "class")] == "Person_2"
    assert s.run("MATCH (n:Person_2) RETURN count(n) AS c") == [{"c": 1}]


# --- refusals ------------------------------------------------------------------------------------


def test_a_blank_node_typed_as_a_model_class_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="is a class of the model"):
        _update(s, "INSERT DATA { _:b a hr:person }")


def test_a_predicate_under_the_base_the_role_does_not_read_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="is not a predicate this role reads"):
        _update(s, f"INSERT DATA {{ _:b <{V}/person#salary> 9 }}")


@pytest.mark.parametrize(
    "text",
    [
        f"INSERT DATA {{ GRAPH <{BASE}/g> {{ ex:a ex:p ex:b }} }}",
        f"CLEAR GRAPH <{BASE}/g>",
        "DROP DEFAULT",
        "CLEAR ALL",
        "COPY <http://g> TO DEFAULT",
    ],
)
def test_the_models_own_graph_is_refused_by_name(monkeypatch, text):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="names the model's own graph"):
        _update(s, text)


def test_load_is_refused_by_name():
    with pytest.raises(SparqlRefused, match="LOAD reads a document from outside"):
        parse_update("LOAD <http://x/y>", BASE)


# --- DELETE DATA ---------------------------------------------------------------------------------


def test_delete_data_takes_one_value_a_label_and_a_relationship(monkeypatch):
    s = _session(monkeypatch)
    _update(s, 'INSERT DATA { ex:s a ex:Thing ; ex:tag "a", "b" ; ex:knows ex:o }')
    counted, _ = _update(s, 'DELETE DATA { ex:s ex:tag "a" ; ex:knows ex:o }')
    assert counted.relationships_deleted == 1
    # The column widened to a list holds a list from then on (a column only widens).
    assert s.run("MATCH (n:Thing) RETURN n.tag AS t") == [{"t": ["b"]}]
    assert _rels(s) == []
    # ex:o held only the relationship: with it gone it holds no triple, and is gone.
    assert (f"{EX}o", "", "node") not in _names(s)
    _update(s, 'DELETE DATA { ex:s a ex:Thing ; ex:tag "b" }')
    assert store.graph(s.session).node_keys == {}
    assert (f"{EX}s", "", "node") not in _names(s)


def test_delete_data_of_a_triple_the_session_does_not_hold_changes_nothing(monkeypatch):
    s = _session(monkeypatch)
    counted, objects = _update(s, 'DELETE DATA { ex:nobody ex:tag "a" }')
    assert not objects.wrote
    assert counted.stats() == counting().__enter__().stats()


# --- named graphs --------------------------------------------------------------------------------


def test_a_named_graph_is_a_graph_tag_on_its_nodes_and_its_relationships(monkeypatch):
    s = _session(monkeypatch)
    _update(s, "INSERT DATA { GRAPH <http://g1> { ex:a ex:knows ex:b } ex:a ex:knows ex:b }")
    names = _names(s)
    tag = names[("http://g1", "", "graph")]
    # One node per IRI in each graph.
    in_g1 = names[(f"{EX}a", "http://g1", "node")]
    in_default = names[(f"{EX}a", "", "node")]
    assert in_g1 != in_default
    assert tag in store.graph(s.session).node_keys[in_g1]
    assert tag not in store.graph(s.session).node_keys[in_default]
    assert sorted(g or "" for *_, g in _rels(s)) == ["", "http://g1"]


def test_clear_drop_and_create_act_on_session_graphs(monkeypatch):
    s = _session(monkeypatch)
    _update(s, "INSERT DATA { GRAPH <http://g1> { ex:a ex:knows ex:b } ex:c ex:knows ex:d }")
    _update(s, "CLEAR GRAPH <http://g1>")
    assert ("http://g1", "", "graph") in _names(s)  # the graph stays, empty
    assert [g for *_, g in _rels(s)] == [None]
    assert (f"{EX}a", "http://g1", "node") not in _names(s)
    with pytest.raises(SparqlRefused, match="holds it already"):
        _update(s, "CREATE GRAPH <http://g1>")
    _update(s, "CREATE SILENT GRAPH <http://g1>")
    _update(s, "DROP GRAPH <http://g1>")
    assert ("http://g1", "", "graph") not in _names(s)
    with pytest.raises(SparqlRefused, match="holds no such graph"):
        _update(s, "DROP GRAPH <http://g1>")
    _update(s, "DROP SILENT GRAPH <http://g1>")
    _update(s, "CREATE GRAPH <http://g2>")
    assert ("http://g2", "", "graph") in _names(s)


def test_drop_named_drops_every_session_graph_and_leaves_the_default_one(monkeypatch):
    s = _session(monkeypatch)
    _update(
        s,
        "INSERT DATA { GRAPH <http://g1> { ex:a ex:p ex:b } GRAPH <http://g2> { ex:a ex:p ex:b } "
        "ex:a ex:p ex:b }",
    )
    _update(s, "DROP NAMED")
    assert not [k for k in _names(s) if k[2] == "graph"]
    assert [g for *_, g in _rels(s)] == [None]


def test_add_copy_and_move_copy_a_graphs_triples(monkeypatch):
    s = _session(monkeypatch)
    _update(
        s,
        'INSERT DATA { GRAPH <http://g1> { ex:a a ex:Thing ; ex:tag "x" ; ex:knows _:b } '
        'GRAPH <http://g2> { ex:z ex:tag "z" } }',
    )
    _update(s, "ADD <http://g1> TO <http://g2>")
    names = _names(s)
    a2 = names[(f"{EX}a", "http://g2", "node")]
    assert (f"{EX}z", "http://g2", "node") in names  # ADD keeps what the target held
    assert "Thing" in store.graph(s.session).node_keys[a2]
    assert sorted(g for *_, g in _rels(s)) == ["http://g1", "http://g2"]
    _update(s, "COPY <http://g1> TO <http://g3>")
    _update(s, "COPY <http://g3> TO <http://g2>")
    assert (f"{EX}z", "http://g2", "node") not in _names(s)  # COPY empties the target first
    _update(s, "MOVE <http://g1> TO <http://g4>")
    names = _names(s)
    assert ("http://g1", "", "graph") not in names
    assert (f"{EX}a", "http://g4", "node") in names
    rows = s.run("MATCH (n:Thing) RETURN n.tag AS t")
    assert rows == [{"t": "x"}] * 3  # in g2, g3 and g4


def test_a_template_blank_node_is_a_new_session_node_for_each_solution():
    [op] = parse_update(PREFIXES + "INSERT { _:b ex:of ?p } WHERE { ?p p:name ?n }", BASE)
    from rdflib import URIRef

    grounded = ground_graphs(op.insert_graphs, [{"p": URIRef(f"{P}/1")}, {"p": URIRef(f"{P}/2")}])
    subjects = {t[0] for t in grounded[""]}
    assert len(subjects) == 2
