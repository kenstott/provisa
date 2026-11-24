# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8a1c37-2d9f-4b60-8e14-a7c3f0d9b261
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPARQL Update as the governed table writes Cypher's writes make (REQ-2135, amended
2026-11-22): INSERT DATA, DELETE DATA, DELETE WHERE and DELETE/INSERT ... WHERE become the write
ASTs of the one write planner (written), so each statement they send is the one a Cypher write
sends and is admitted as any write (the write right, write grants and their columns, row rules).
A triple of a session object (a blank node, an IRI outside the base, a session graph) is
tests/unit/test_sparql_session_objects.py's; graph IRIs under the base and LOAD are refused by
name.

The model sits in one DuckDB (tests/unit/ephemeral_harness.py); a WHERE is read as the SPARQL
SELECT of its template's variables, compiled by the SPARQL front end."""

# Requirements: REQ-2135, REQ-670, REQ-2176

from __future__ import annotations

import asyncio
import dataclasses
import json

import pytest
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib import URIRef

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.gql.assembler import assemble_rows
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import JunctionMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql
from provisa.security.mutation_authz import ColumnNotWritable
from provisa.sparql_query.compiler import SparqlRefused, compile_where
from provisa.sparql_query.results import rdf_term
from provisa.sparql_query.update import (
    delete_writes,
    ground,
    insert_write,
    parse_update,
    split_session,
    update_refusal,
    where_variables,
)
from provisa.sparql_query.vocabulary import Grounding, build_vocabulary
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

BASE = "https://data.example.com"
P = f"{BASE}/id/hr/person"
C = f"{BASE}/id/hr/company"
V = f"{BASE}/vocab/hr"
PREFIXES = (
    f"PREFIX p: <{V}/person#> PREFIX hr: <{V}/> PREFIX rel: <{BASE}/vocab/rel/> "
    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
)

_PERSON = {"id": "integer", "name": "text", "age": "integer"}
_COMPANY = {"id": "integer", "name": "text"}
_MODEL = {
    "Person": (_PERSON, "(1, 'Ann', 30), (2, 'Bo', 40), (3, 'Cy', 50)"),
    "Company": (_COMPANY, "(10, 'Acme')"),
}


def _junction(rel_type: str, source: str, target: str, table: str) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type=rel_type,
        source_label=source,
        target_label=target,
        join_source_column="id",
        join_target_column="id",
        field_name=table,
        via=JunctionMapping(
            catalog_name="memory",
            schema_name="s",
            table_name=table,
            type_name=table,
            source_columns=("from_id",),
            target_columns=("to_id",),
            attributes={},
        ),
    )


def _session(monkeypatch) -> Session:
    relationships = [
        _junction("KNOWS", "Person", "Person", "knows"),
        _junction("WORKS_FOR", "Person", "Company", "works_for"),
        # A relationship a foreign key derives: no junction stores it.
        RelationshipMapping(
            rel_type="OWNS",
            source_label="Company",
            target_label="Person",
            join_source_column="id",
            join_target_column="id",
            field_name="owns",
        ),
    ]
    s = Session(monkeypatch, _MODEL, relationships=relationships)
    for label, nm in list(s.model.nodes.items()):
        s.model.nodes[label] = dataclasses.replace(nm, domain_id="hr")
    s.duck.execute("CREATE TABLE s.knows (from_id integer, to_id integer)")
    s.duck.execute("INSERT INTO s.knows VALUES (1, 2)")
    s.duck.execute("CREATE TABLE s.works_for (from_id integer, to_id integer)")
    from provisa.core import settings_registry

    held = settings_registry.value
    monkeypatch.setattr(
        settings_registry,
        "value",
        lambda key: BASE if key == "server.public_url" else held(key),
    )
    return s


def _vocab(s: Session):
    types = {
        (label, prop): kind
        for label, (columns, _) in _MODEL.items()
        for prop, kind in columns.items()
    }
    return build_vocabulary(s.model, BASE, types, Grounding())


def _solutions(s: Session, op, vocab) -> list[dict]:
    """An operation's WHERE, read as the SPARQL SELECT of its template's variables."""
    variables = where_variables(op)
    compiled = compile_where(op.where, variables, vocab, s.model)
    if compiled.empty:
        return []
    ast, ordered, graph_vars = cypher_to_sql(compiled.ast, s.model, {})
    ast = apply_graph_rewrites(ast, graph_vars, s.model)
    assert not ordered
    result = s.duck.execute(transpile_to_duckdb(ast.sql(dialect="postgres")).replace("memory.", ""))
    columns = [d[0] for d in result.description]
    raw = [
        {c: json.loads(v) if isinstance(v, str) and v.startswith("{") else v for c, v in r}
        for r in (zip(columns, row) for row in result.fetchall())
    ]
    rows = assemble_rows(raw, graph_vars)
    return [
        {v: t for v in variables if (t := rdf_term(row.get(v), BASE)) is not None} for row in rows
    ]


def _update(s: Session, text: str) -> None:
    """The update as /data/sparql runs it: each operation's WHERE read, then its deletes and its
    inserts through the write planner."""
    vocab = _vocab(s)
    for op in parse_update(PREFIXES + text, BASE):
        deletes, inserts = op.delete, op.insert
        if op.where is not None:
            solutions = _solutions(s, op, vocab)
            deletes, inserts = ground(op.delete, solutions), ground(op.insert, solutions)
        asts, params = delete_writes(deletes, vocab, s.model)
        inserted = insert_write(inserts, vocab, s.model)
        if inserted is not None:
            asts.append(inserted)
        for ast in asts:
            asyncio.run(s.written(ast, params))


def _rows(s: Session, table: str) -> list[tuple]:
    return s.duck.execute(f"SELECT * FROM s.{table} ORDER BY ALL").fetchall()


# --- INSERT DATA ---------------------------------------------------------------------------------


def test_insert_data_writes_the_row_its_iri_names(monkeypatch):
    s = _session(monkeypatch)
    _update(s, f'INSERT DATA {{ <{P}/7> a hr:person ; p:name "Di" ; p:age 22 }}')
    assert (7, "Di", 22) in _rows(s, "person")
    assert any(sql.startswith('INSERT INTO "memory"."s"."person"') for sql in s.sent)


def test_insert_data_of_a_row_held_sets_its_properties_and_writes_no_second_row(monkeypatch):
    s = _session(monkeypatch)
    _update(s, f"INSERT DATA {{ <{P}/1> p:age 31 }}")
    _update(s, f"INSERT DATA {{ <{P}/1> p:age 31 }}")
    assert [r for r in _rows(s, "person") if r[0] == 1] == [(1, "Ann", 31)]
    assert not any(sql.startswith("INSERT") for sql in s.sent)


def test_insert_data_of_an_edge_writes_its_junction_row_once(monkeypatch):
    s = _session(monkeypatch)
    _update(s, f"INSERT DATA {{ <{P}/3> rel:WORKS_FOR <{C}/10> }}")
    _update(s, f"INSERT DATA {{ <{P}/3> rel:WORKS_FOR <{C}/10> }}")
    assert _rows(s, "works_for") == [(3, 10)]


def test_insert_data_of_a_foreign_key_relationship_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    from provisa.cypher.write_translator import CypherWriteParseError

    with pytest.raises(CypherWriteParseError, match="OWNS is derived from a foreign key"):
        _update(s, f"INSERT DATA {{ <{C}/10> rel:OWNS <{P}/1> }}")
    assert s.sent == []


def test_two_values_of_one_property_for_one_row_are_refused(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="a column holds one"):
        _update(s, f'INSERT DATA {{ <{P}/1> p:name "A", "B" }}')


def test_a_predicate_the_role_does_not_read_is_refused_by_name(monkeypatch):
    """A column hidden from the role is in neither its label map nor its vocabulary: a write of
    it is refused by name, never dropped."""
    s = _session(monkeypatch)
    nm = s.model.nodes["Person"]
    s.model.nodes["Person"] = dataclasses.replace(
        nm,
        properties={k: v for k, v in nm.properties.items() if k != "age"},
        physical_properties={k: v for k, v in nm.physical_properties.items() if k != "age"},
    )
    with pytest.raises(SparqlRefused, match="person#age> is not a predicate of Person"):
        _update(s, f"INSERT DATA {{ <{P}/1> p:age 9 }}")
    assert s.sent == []


def test_a_subject_under_the_base_that_names_no_row_is_refused(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="names no row"):
        _update(s, f"INSERT DATA {{ <{BASE}/id/hr> <urn:b> <urn:c> }}")
    assert s.sent == []


def test_a_blank_node_or_an_iri_outside_the_base_is_a_session_objects_triple():
    """REQ-2135 (amended 2026-11-23): such a subject is a session object, never a model row's
    write (tests/unit/test_sparql_session_objects.py)."""
    from rdflib import BNode, Literal, URIRef

    triples = [
        (URIRef("urn:a"), URIRef("urn:b"), URIRef("urn:c")),
        (BNode("b"), URIRef(f"{V}/person#name"), Literal("x")),
        (URIRef(f"{P}/1"), URIRef(f"{V}/person#name"), Literal("A")),
    ]
    model, session = split_session(triples, BASE)
    assert model == triples[2:]
    assert session == triples[:2]


def test_a_class_that_does_not_type_the_row_is_refused(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="does not type"):
        _update(s, f"INSERT DATA {{ <{P}/8> a hr:company }}")


# --- DELETE DATA ---------------------------------------------------------------------------------


def test_delete_data_of_a_property_nulls_it_only_where_it_holds_that_value(monkeypatch):
    s = _session(monkeypatch)
    _update(s, f"DELETE DATA {{ <{P}/1> p:age 99 }}")
    assert (1, "Ann", 30) in _rows(s, "person")
    _update(s, f"DELETE DATA {{ <{P}/1> p:age 30 }}")
    assert (1, "Ann", None) in _rows(s, "person")


def test_delete_data_of_the_type_deletes_the_row_and_its_other_triples(monkeypatch):
    s = _session(monkeypatch)
    _update(s, f'DELETE DATA {{ <{P}/3> a hr:person ; p:name "Cy" ; p:id 3 }}')
    assert [r[0] for r in _rows(s, "person")] == [1, 2]
    assert [sql.split(" WHERE")[0] for sql in s.sent] == ['DELETE FROM "memory"."s"."person"']


def test_delete_data_of_an_edge_deletes_its_junction_row(monkeypatch):
    s = _session(monkeypatch)
    _update(s, f"DELETE DATA {{ <{P}/1> rel:KNOWS <{P}/2> }}")
    assert _rows(s, "knows") == []


def test_delete_data_of_a_key_alone_is_refused(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="is its key, the row's identity"):
        _update(s, f"DELETE DATA {{ <{P}/1> p:id 1 }}")


# --- DELETE WHERE and Modify ---------------------------------------------------------------------


def test_delete_where_deletes_each_matched_triple(monkeypatch):
    s = _session(monkeypatch)
    _update(s, "DELETE WHERE { ?p p:age ?a }")
    assert [r[2] for r in _rows(s, "person")] == [None, None, None]


def test_delete_where_of_the_type_deletes_the_matched_rows(monkeypatch):
    s = _session(monkeypatch)
    _update(s, 'DELETE WHERE { ?p a hr:person ; p:name "Bo" }')
    assert [r[0] for r in _rows(s, "person")] == [1, 3]


def test_modify_deletes_then_inserts_from_each_solution(monkeypatch):
    s = _session(monkeypatch)
    _update(
        s,
        'DELETE { ?p p:name ?n } INSERT { ?p p:name "Zed" ; rel:WORKS_FOR <'
        + C
        + "/10> } WHERE { ?p p:name ?n ; p:age ?a FILTER(?a >= 40) }",
    )
    assert _rows(s, "person") == [(1, "Ann", 30), (2, "Zed", 40), (3, "Zed", 50)]
    assert _rows(s, "works_for") == [(2, 10), (3, 10)]


def test_a_template_triple_with_an_unbound_variable_is_no_triple(monkeypatch):
    s = _session(monkeypatch)
    _update(s, 'INSERT { ?p p:name ?missing } WHERE { ?p p:name "Ann" }')
    assert (1, "Ann", 30) in _rows(s, "person")
    assert s.sent == []


# --- named graphs --------------------------------------------------------------------------------


def test_load_alone_of_graph_management_is_refused_before_the_update_is_read():
    refused = update_refusal("LOAD <http://x/y>")
    assert refused is not None
    code, said = refused
    assert code == "sparql.update_refused"
    assert "LOAD reads a document from outside the session" in said
    for text in ("DROP NAMED", "CLEAR GRAPH <http://g>", "CREATE GRAPH <http://g>"):
        assert update_refusal(text) is None
        [op] = parse_update(text, BASE)
        assert op.manage is not None


@pytest.mark.parametrize(
    "text",
    [
        "DROP ALL",
        "CLEAR DEFAULT",
        f"CREATE GRAPH <{BASE}/g>",
        f"INSERT DATA {{ GRAPH <{BASE}/g> {{ <http://a> <http://p> 1 }} }}",
        f"WITH <{BASE}/g> DELETE {{ ?s <http://p> ?o }} WHERE {{ ?s <http://p> ?o }}",
    ],
)
def test_the_models_own_graph_is_refused_by_name(monkeypatch, text):
    s = _session(monkeypatch)
    with pytest.raises(SparqlRefused, match="names the model's own graph"):
        _update(s, text)


def test_a_graph_block_names_a_session_graph():
    [op] = parse_update("INSERT DATA { GRAPH <http://g> { <http://a> <http://p> 1 } }", BASE)
    assert list(op.insert_graphs) == ["http://g"]


@pytest.mark.parametrize(
    "text, what",
    [
        ("WITH <http://g> DELETE { ?s <http://p> ?o } WHERE { ?s <http://p> ?o }", "WITH"),
        ("DELETE { ?s <http://p> ?o } USING <http://g> WHERE { ?s <http://p> ?o }", "USING"),
        ("DELETE WHERE { GRAPH <http://g> { ?s <http://p> ?o } }", "DELETE WHERE"),
    ],
)
def test_a_where_reading_a_session_graph_reads_it_in_a_graph_pattern(text, what):
    """REQ-2135 (amended 2026-11-23): WITH, USING and DELETE WHERE on a session graph read its
    objects; the WHERE is a GRAPH pattern of that graph."""
    [op] = parse_update(text, BASE)
    assert op.where is not None
    graphs = [n.term for n in _patterns(op.where) if n.name == "Graph"]
    assert graphs == [URIRef("http://g")], what


def _patterns(node):
    if isinstance(node, CompValue):
        yield node
        for value in node.values():
            yield from _patterns(value)
    elif isinstance(node, (list, tuple)):
        for item in node:
            yield from _patterns(item)


@pytest.mark.parametrize(
    "text, said",
    [
        (
            f"WITH <{BASE}/g> DELETE {{ ?s <http://p> ?o }} WHERE {{ ?s <http://p> ?o }}",
            "model's own",
        ),
        (
            "DELETE { ?s <http://p> ?o } USING <http://g> USING <http://h> WHERE { ?s <http://p> ?o }",
            "USING names several graphs",
        ),
    ],
)
def test_a_where_of_the_models_graph_or_of_several_graphs_is_refused_by_name(text, said):
    with pytest.raises(SparqlRefused, match=said):
        parse_update(text, BASE)


def test_an_insert_template_blank_node_is_a_new_node_for_each_solution():
    [op] = parse_update('INSERT { ?s <http://p> _:b } WHERE { ?s <http://q> "x" }', BASE)
    made = ground(op.insert, [{"s": URIRef("http://a")}, {"s": URIRef("http://c")}])
    assert len({o for _, _, o in made}) == 2


# --- governance ----------------------------------------------------------------------------------


def _gov(*operations: str, columns: dict[int, list[str]] | None = None, rows: str = "TRUE"):
    """A role reading the model's tables, with the write right and a grant per operation."""
    tables = {"person": 1, "knows": 2, "works_for": 3}
    gov = GovernanceContext()
    gov.role_id = "clerk"
    gov.can_write = True
    gov.table_map = {
        name: tid
        for table, tid in tables.items()
        for name in (table, f"s.{table}", f"memory.s.{table}")
    }
    gov.all_columns = {
        1: [("id", "integer"), ("name", "varchar"), ("age", "integer")],
        2: [("from_id", "integer"), ("to_id", "integer")],
        3: [("from_id", "integer"), ("to_id", "integer")],
    }
    gov.write_grants = {
        tid: {
            op: {
                "role": "clerk",
                "operation": op,
                "rows": rows,
                "columns": (columns or {}).get(tid, [c for c, _ in gov.all_columns[tid]]),
            }
            for op in operations
        }
        for tid in tables.values()
    }
    gov.write_ops = {tid: frozenset({"insert", "update", "delete"}) for tid in tables.values()}
    gov.write_returns_rows = {tid: True for tid in tables.values()}
    gov.write_refused_forms = {tid: frozenset() for tid in tables.values()}
    return gov


def _every_kind_sent(monkeypatch) -> list[str]:
    s = _session(monkeypatch)
    _update(s, f'INSERT DATA {{ <{P}/7> p:name "Di" . <{P}/1> p:age 31 }}')
    _update(
        s, f"DELETE DATA {{ <{P}/1> rel:KNOWS <{P}/2> . <{P}/2> p:age 40 . <{P}/3> a hr:person }}"
    )
    kinds = {sql.split()[0] for sql in s.sent}
    assert kinds == {"INSERT", "UPDATE", "DELETE"}
    return s.sent


def test_each_statement_an_update_sends_is_admitted_as_any_write(monkeypatch):
    for sql in _every_kind_sent(monkeypatch):
        apply_governance(sql, _gov("insert", "update", "delete"), session_vars={})


def test_a_role_without_the_write_right_is_refused_by_name(monkeypatch):
    gov = _gov("insert", "update", "delete")
    gov.can_write = False
    for sql in _every_kind_sent(monkeypatch):
        with pytest.raises(WriteNotAdmitted, match="does not hold the 'write' right"):
            apply_governance(sql, gov, session_vars={})


def test_a_role_without_a_grant_for_the_operation_is_refused(monkeypatch):
    for sql in _every_kind_sent(monkeypatch):
        with pytest.raises(WriteNotAdmitted, match="holds no (insert|update|delete) grant"):
            apply_governance(sql, _gov(), session_vars={})


def test_a_column_the_grant_does_not_name_is_not_written(monkeypatch):
    """A masked column the role reads but its write grant does not name is refused by name."""
    s = _session(monkeypatch)
    _update(s, f"INSERT DATA {{ <{P}/1> p:age 31 }}")
    [update] = [sql for sql in s.sent if sql.startswith("UPDATE")]
    with pytest.raises(ColumnNotWritable, match="'age'"):
        apply_governance(
            update, _gov("insert", "update", columns={1: ["id", "name"]}), session_vars={}
        )


def test_a_row_rule_applies_to_what_an_update_writes(monkeypatch):
    s = _session(monkeypatch)
    _update(s, f"DELETE DATA {{ <{P}/2> p:age 40 }}")
    [update] = s.sent
    governed = apply_governance(update, _gov("update", rows="age > 35"), session_vars={})
    assert "age > 35" in governed.replace('"', "")
