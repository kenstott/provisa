# Copyright (c) 2026 Kenneth Stott
# Canary: 7e4b1c92-5d3a-4f68-b0e1-9a2c6d8f4b17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPARQL reads of the session objects SPARQL Update writes (REQ-2135, amended 2026-11-23;
REQ-2176): the vocabulary takes the session's classes, properties and edges from its IRI table,
a query compiles over the session's overlay on the one SQL pipeline, a session node answers as
its IRI or a blank node, and a GRAPH pattern reads its graph's objects alone. An update's WITH,
USING, DELETE WHERE and GRAPH pattern read a session graph's objects.

The session and the model sit in one DuckDB (tests/unit/ephemeral_harness.py); a query runs
through :func:`provisa.sparql_query.session_graph.read_over` as /data/sparql runs it."""

# Requirements: REQ-2135, REQ-2176

from __future__ import annotations

import asyncio

import pytest
from rdflib import XSD, BNode, Literal, URIRef

from provisa.gql.assembler import assemble_rows
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.compiler import SparqlRefused, compile_sparql, compile_where
from provisa.sparql_query.results import rdf_term
from provisa.sparql_query.session_graph import SessionWrite, names_of, read_over
from provisa.sparql_query.update import (
    ground_graphs,
    model_identity,
    parse_update,
    split_session,
    where_variables,
)
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_sparql_session_objects import BASE, EX, P, PREFIXES, _session, _vocab

pytestmark = pytest.mark.unit


def _rows(s, compiled, read) -> list[dict]:
    if compiled.empty:
        return [compiled.empty_row] if compiled.empty_row is not None else []
    ast, ordered, graph_vars = cypher_to_sql(compiled.ast, read.graph, {})
    assert not ordered
    ast = apply_graph_rewrites(ast, graph_vars, read.graph)
    ast = read.expand(ast)
    result = s.duck.execute(transpile_to_duckdb(ast.sql(dialect="postgres")))
    columns = [d[0] for d in result.description]
    return assemble_rows([dict(zip(columns, r)) for r in result.fetchall()], graph_vars)


def _read(s):
    return asyncio.run(read_over(s.state, s.session, s.model, _vocab(s)))


def select(s, text: str) -> list[dict]:
    """A SELECT's solutions as /data/sparql answers them: each variable's RDF term."""
    read = _read(s)
    compiled = compile_sparql(PREFIXES + text, read.vocab, read.graph)
    return [
        {
            v: t
            for v in compiled.variables
            if (t := rdf_term(row.get(v), BASE, read.terms)) is not None
        }
        for row in _rows(s, compiled, read)
    ]


def update(s, text: str) -> None:
    """An update's session objects as /data/sparql writes them (sparql_router._updated), each
    WHERE read over the session as :func:`select` reads."""

    async def run():
        vocab = _vocab(s)
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
                read = await read_over(s.state, s.session, s.model, vocab)
                variables = where_variables(op)
                compiled = compile_where(op.where, variables, read.vocab, read.graph)
                solutions = [
                    {
                        v: t
                        for v in variables
                        if (t := rdf_term(row.get(v), BASE, read.terms, update=True)) is not None
                    }
                    for row in _rows(s, compiled, read)
                ]
                deletes = ground_graphs(deletes, solutions)
                inserts = ground_graphs(inserts, solutions)
            model_deletes, deletes[""] = split_session(deletes.get("", []), BASE)
            model_inserts, inserts[""] = split_session(inserts.get("", []), BASE)
            assert not model_deletes and not model_inserts, "a session object test writes none"
            await objects.delete(deletes)
            await objects.insert(inserts)

    asyncio.run(run())


def _s(value: str) -> Literal:
    """A text property's value as the answer gives it: an xsd:string literal, as a model text
    column's is."""
    return Literal(value, datatype=XSD.string)


def _values(rows: list[dict], var: str) -> list:
    return sorted(r[var].toPython() for r in rows if var in r)


# --- reads ---------------------------------------------------------------------------------------


def test_a_blank_node_answers_as_a_blank_node_with_its_class_and_property(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { _:b a ex:Thing ; ex:title "One" }')
    [row] = select(s, "SELECT ?s ?t WHERE { ?s a ex:Thing ; ex:title ?t }")
    assert isinstance(row["s"], BNode)
    assert row["t"] == _s("One")


def test_an_iri_subject_answers_as_its_iri_and_is_read_by_it(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { ex:alice ex:name "Alice" . ex:bob ex:name "Bob" }')
    assert select(s, 'SELECT ?s WHERE { ?s ex:name "Alice" }') == [{"s": URIRef(f"{EX}alice")}]
    assert select(s, "SELECT ?n WHERE { ex:bob ex:name ?n }") == [{"n": _s("Bob")}]


def test_an_edge_between_session_nodes_is_walked(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { ex:a ex:knows ex:b . ex:b ex:name "B" }')
    assert select(s, "SELECT ?n WHERE { ex:a ex:knows ?x . ?x ex:name ?n }") == [{"n": _s("B")}]
    assert select(s, "SELECT ?x WHERE { ?x ex:knows ex:b }") == [{"x": URIRef(f"{EX}a")}]


def test_an_edge_to_a_model_row_reads_the_row_and_the_model_still_reads(monkeypatch):
    s = _session(monkeypatch)
    update(s, f"INSERT DATA {{ ex:a ex:likes <{P}/1> }}")
    assert select(s, "SELECT ?n WHERE { ex:a ex:likes ?p . ?p p:name ?n }") == [{"n": _s("Ann")}]
    assert select(s, "SELECT ?p WHERE { ex:a ex:likes ?p }") == [{"p": URIRef(f"{P}/1")}]
    rows = select(s, "SELECT ?n WHERE { ?p a hr:person ; p:name ?n }")
    assert _values(rows, "n") == ["Ann", "Bo"]
    assert select(s, f"SELECT ?n WHERE {{ <{P}/2> p:name ?n }}") == [{"n": _s("Bo")}]


def test_a_node_of_two_classes_answers_once(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { _:b a ex:A, ex:B ; ex:title "T" }')
    assert select(s, "SELECT ?t WHERE { ?s ex:title ?t }") == [{"t": _s("T")}]
    assert select(s, "SELECT ?t WHERE { ?s a ex:B ; ex:title ?t }") == [{"t": _s("T")}]


def test_a_graph_pattern_reads_its_graph_alone_and_the_default_graph_none(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { GRAPH ex:g { ex:a ex:name "G" } ex:a ex:name "D" }')
    assert select(s, "SELECT ?n WHERE { GRAPH ex:g { ?s ex:name ?n } }") == [{"n": _s("G")}]
    assert select(s, "SELECT ?n WHERE { ?s ex:name ?n }") == [{"n": _s("D")}]
    assert select(s, "SELECT ?n WHERE { GRAPH ex:other { ?s ex:name ?n } }") == []


def test_a_graph_pattern_naming_the_models_graph_or_a_variable_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { ex:a ex:name "D" }')
    with pytest.raises(SparqlRefused, match="model's own graph"):
        select(s, f"SELECT ?n WHERE {{ GRAPH <{BASE}/g> {{ ?s ex:name ?n }} }}")
    with pytest.raises(SparqlRefused, match="by a variable"):
        select(s, "SELECT ?n WHERE { GRAPH ?g { ?s ex:name ?n } }")


def test_a_session_holding_nothing_reads_the_model_as_before(monkeypatch):
    s = _session(monkeypatch)
    rows = select(s, "SELECT ?n WHERE { ?p a hr:person ; p:name ?n }")
    assert _values(rows, "n") == ["Ann", "Bo"]


# --- update WHERE over session objects ------------------------------------------------------------


def test_with_reads_and_writes_its_session_graph(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { GRAPH ex:g { ex:a ex:name "G" } ex:a ex:name "D" }')
    update(
        s, "WITH ex:g DELETE { ?s ex:name ?n } INSERT { ?s ex:label ?n } WHERE { ?s ex:name ?n }"
    )
    assert select(s, "SELECT ?n WHERE { GRAPH ex:g { ?s ex:label ?n } }") == [{"n": _s("G")}]
    assert select(s, "SELECT ?n WHERE { GRAPH ex:g { ?s ex:name ?n } }") == []
    assert select(s, "SELECT ?n WHERE { ?s ex:name ?n }") == [{"n": _s("D")}]


def test_using_reads_its_session_graph_into_the_default_one(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { GRAPH ex:g { ex:a ex:name "G" } }')
    update(s, "INSERT { ?s ex:copied ?n } USING ex:g WHERE { ?s ex:name ?n }")
    assert select(s, "SELECT ?s ?n WHERE { ?s ex:copied ?n }") == [
        {"s": URIRef(f"{EX}a"), "n": _s("G")}
    ]


def test_delete_where_on_a_session_graph(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { GRAPH ex:g { ex:a ex:name "G" . _:b ex:name "H" } }')
    update(s, "DELETE WHERE { GRAPH ex:g { ?s ex:name ?n } }")
    assert select(s, "SELECT ?n WHERE { GRAPH ex:g { ?s ex:name ?n } }") == []


def test_delete_where_takes_a_blank_nodes_triples_in_the_default_graph(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { _:b a ex:Thing ; ex:title "One" }')
    update(s, "DELETE WHERE { ?s ex:title ?t }")
    assert select(s, "SELECT ?t WHERE { ?s ex:title ?t }") == []


def test_a_graph_pattern_inside_where_reads_its_graph(monkeypatch):
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { GRAPH ex:g { ex:a ex:name "G" } }')
    update(s, "INSERT { ?s ex:seen ?n } WHERE { GRAPH ex:g { ?s ex:name ?n } }")
    assert select(s, "SELECT ?n WHERE { ?s ex:seen ?n }") == [{"n": _s("G")}]


# --- many values; one variable in several graphs ---------------------------------------------------


def test_a_property_of_many_values_answers_one_solution_per_value(monkeypatch):
    """REQ-2135 (amended 2026-11-23): many values of one predicate widen to a list in the
    session; a read answers each value as a solution of its own, as a triple of each."""
    s = _session(monkeypatch)
    update(s, 'INSERT DATA { ex:a ex:tag "x", "y" . ex:b ex:tag "z" }')
    rows = select(s, "SELECT ?s ?t WHERE { ?s ex:tag ?t }")
    assert sorted((str(r["s"]), str(r["t"])) for r in rows) == [
        (f"{EX}a", "x"),
        (f"{EX}a", "y"),
        (f"{EX}b", "z"),
    ]
    assert select(s, 'SELECT ?s WHERE { ?s ex:tag "y" }') == [{"s": URIRef(f"{EX}a")}]
    assert _values(select(s, "SELECT ?t WHERE { ex:a ex:tag ?t }"), "t") == ["x", "y"]


def test_a_variable_in_two_graph_patterns_joins_across_both(monkeypatch):
    """SPARQL 18.6: a variable shared by two GRAPH patterns is one term in each graph; an IRI
    subject is a session node of its own in each graph, so the two are joined by their IRI."""
    s = _session(monkeypatch)
    update(
        s,
        'INSERT DATA { GRAPH ex:g1 { ex:a ex:name "A1" . ex:b ex:name "B1" } '
        'GRAPH ex:g2 { ex:a ex:label "A2" } }',
    )
    rows = select(
        s,
        "SELECT ?s ?n ?l WHERE { GRAPH ex:g1 { ?s ex:name ?n } GRAPH ex:g2 { ?s ex:label ?l } }",
    )
    assert rows == [{"s": URIRef(f"{EX}a"), "n": _s("A1"), "l": _s("A2")}]
    nested = select(
        s,
        "SELECT ?n ?l WHERE { GRAPH ex:g1 { ?s ex:name ?n GRAPH ex:g2 { ?s ex:label ?l } } }",
    )
    assert nested == [{"n": _s("A1"), "l": _s("A2")}]
    constant = select(
        s, "SELECT ?n ?l WHERE { GRAPH ex:g1 { ex:a ex:name ?n } GRAPH ex:g2 { ex:a ex:label ?l } }"
    )
    assert constant == [{"n": _s("A1"), "l": _s("A2")}]


def test_a_variable_in_a_graph_pattern_and_the_default_graph_joins_across_both(monkeypatch):
    s = _session(monkeypatch)
    update(
        s,
        'INSERT DATA { ex:a ex:name "D" . ex:c ex:name "C" GRAPH ex:g { ex:a ex:label "G" } }',
    )
    rows = select(s, "SELECT ?n ?l WHERE { ?s ex:name ?n GRAPH ex:g { ?s ex:label ?l } }")
    assert rows == [{"n": _s("D"), "l": _s("G")}]


def test_cypher_unwinds_a_property_of_many_values_to_its_items(monkeypatch):
    """REQ-2176: Cypher sees SPARQL's session objects; UNWIND of a property held as JSON (a list
    on one node, one value on another) is its items, a value that is no list itself."""
    from provisa.cypher.parser import parse_cypher
    from provisa.ephemeral.then import UNLABELED

    s = _session(monkeypatch)
    update(s, 'INSERT DATA { ex:a ex:tag "x", "y" . ex:b ex:tag "z" }')
    read = _read(s)
    ast = parse_cypher(f"MATCH (n:{UNLABELED}) UNWIND n.tag AS t RETURN t")
    sql, _, graph_vars = cypher_to_sql(ast, read.graph, {})
    sql = read.expand(apply_graph_rewrites(sql, graph_vars, read.graph))
    rows = s.duck.execute(transpile_to_duckdb(sql.sql(dialect="postgres"))).fetchall()
    assert sorted(r[0] for r in rows) == ['"x"', '"y"', '"z"']
