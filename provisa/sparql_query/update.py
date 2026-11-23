# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b2d8e-91c4-4a7e-b5d0-6e2f8c1a9b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPARQL 1.1 Update as the governed table writes Cypher's writes make (REQ-2135, amended
2026-11-22).

An update's triples are read against the role's :class:`Vocabulary` and become the write ASTs of
the shared write planner (:mod:`provisa.cypher.write_translator`, :func:`written`), which turns
them into the same governed INSERTs, UPDATEs and DELETEs a Cypher write makes, under the same
rights, row rules, masks and audit:

- a subject is the row its IRI names (:mod:`provisa.gql.iri`): its table's label and its key;
- ``rdf:type`` of one of the classes typing that table says the row exists;
- a column predicate of the label is that column, its literal object the column's value;
- an edge predicate (``vocab/rel/<type>``) is a junction row between the two rows its subject
  and object name; a relationship a foreign key derives is refused by name, as Cypher refuses it.

The operations:

- INSERT DATA is a MERGE of each row on its key, setting each property it states whether the row
  was there or not, then a MERGE of each relationship between the two rows: a triple already held
  is not written twice, as RDF holds a triple once. A column holds one value, so two values of one
  property for one row are refused by name.
- DELETE DATA deletes each relationship's junction row; a property's triple sets that column to
  null where it holds that value; an ``rdf:type`` triple deletes the row, and with it every other
  triple of that row the operation deletes. A key column's triple is the row's identity: it is
  deleted only with the row.
- DELETE WHERE and DELETE/INSERT ... WHERE (Modify) read their pattern as a SELECT through the
  one governed read, then delete, then insert, the triples their templates make of each solution;
  a template triple with an unbound variable is no triple.

Session objects (REQ-2135, amended 2026-11-23; REQ-2176): a triple of a blank node, of an IRI
outside the model's base IRI, or in a session graph (a GRAPH block naming an IRI outside the base) is
a session object's, written to the principal's graph session by
:mod:`provisa.sparql_query.session_graph`; graph management (DROP, CLEAR, CREATE, COPY, MOVE, ADD)
acts on session graphs. A graph IRI under the base, and DEFAULT, are the model's own graph and are
refused by name; LOAD reads from outside and is refused by name.
"""

# Requirements: REQ-2135, REQ-670, REQ-2176

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from rdflib import BNode, Literal as RdfLiteral, URIRef, Variable as RdfVariable
from rdflib.plugins.sparql.algebra import translateUpdate
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.plugins.sparql.parserutils import CompValue

from provisa.cypher.write_translator import CreatedLink, CreatedNode, MergePart, WriteAST
from provisa.gql.iri import parse_subject_iri
from provisa.gql.label_map import CypherLabelMap, NodeMapping
from provisa.gql.terms import subject_key
from provisa.sparql_query.compiler import SparqlRefused
from provisa.sparql_query.vocabulary import RDF_TYPE, XSD, Vocabulary

Triple = tuple[Any, Any, Any]

#: The update operations whose whole subject is a graph (rdflib's parse names).
GRAPH_MANAGEMENT = {"Load", "Clear", "Drop", "Create", "Add", "Move", "Copy"}

#: The parameters an update's own reads bind (an edge's two ends' keys).
_PARAM = "provisa_sparql_"

#: An operation's triples by graph: "" the default graph, else a session graph's IRI.
ByGraph = dict[str, list[Triple]]


@dataclass
class UpdateOperation:
    """One operation of an update request: the triples it deletes and inserts by graph -- ground
    for INSERT DATA and DELETE DATA, templates for DELETE WHERE and Modify -- and the pattern a
    template is instantiated from (None for the DATA forms); or, for graph management, the
    operation itself (``manage``)."""

    name: str
    delete_graphs: ByGraph = field(default_factory=dict)
    insert_graphs: ByGraph = field(default_factory=dict)
    where: CompValue | None = None
    manage: CompValue | None = None

    @property
    def delete(self) -> list[Triple]:
        """The default graph's triples it deletes."""
        return self.delete_graphs.get("", [])

    @property
    def insert(self) -> list[Triple]:
        """The default graph's triples it inserts."""
        return self.insert_graphs.get("", [])


def update_refusal(text: str) -> tuple[str, str] | None:
    """(error code, what is said) when ``text`` is no SPARQL Update, or one of its operations
    LOADs from outside the session; None for any other update."""
    try:
        parsed = parseUpdate(text)
    except Exception as exc:  # allow-ble: rdflib raises pyparsing's and its own errors alike
        return "sparql.refused", f"The update is not SPARQL this endpoint reads: {exc}"
    if any(getattr(op, "name", "") == "Load" for op in parsed.request):
        return (
            "sparql.update_refused",
            "LOAD reads a document from outside the session; insert its triples with INSERT DATA",
        )
    return None


def _by_graph(clause: Any, base: str, what: str, into: str = "") -> ByGraph:
    """A clause's triples by graph: its plain triples in ``into`` (the default graph, or WITH's),
    each GRAPH block's in the session graph it names."""
    from provisa.sparql_query.session_graph import session_graph

    out: ByGraph = {}
    if clause is None:
        return out
    if clause.triples:
        out[into] = list(clause.triples)
    for graph, triples in clause.quads.items():
        out.setdefault(session_graph(graph, base, f"A GRAPH block in {what}"), []).extend(triples)
    return out


def _in_graph(graph: str, pattern: CompValue) -> CompValue:
    """``pattern`` read in the session graph ``graph``: a GRAPH pattern of it."""
    return CompValue("Graph", term=URIRef(graph), p=pattern)


def _joined(patterns: list[CompValue]) -> CompValue:
    """The join of ``patterns`` (none: the empty pattern, one solution)."""
    if not patterns:
        return CompValue("BGP", triples=[])
    out = patterns[0]
    for other in patterns[1:]:
        out = CompValue("Join", p1=out, p2=other)
    return out


def _using_graph(op: CompValue, base: str) -> str | None:
    """The session graph a Modify's USING makes the WHERE's default graph; None for none. A
    WHERE reads one graph as its default: USING NAMED alone names graphs its GRAPH patterns read
    by IRI anyway, and two USING graphs are refused by name."""
    from provisa.sparql_query.session_graph import session_graph

    defaults = []
    for using in op.using or []:
        if "named" in using:
            session_graph(using["named"], base, "USING NAMED")
        else:
            defaults.append(session_graph(using["default"], base, "USING"))
    if len(defaults) > 1:
        raise SparqlRefused(
            "USING names several graphs, whose merge is the WHERE's default graph: read each "
            "by a GRAPH pattern"
        )
    return defaults[0] if defaults else None


def managed_graph(term: Any, base: str, op: str) -> str | None:
    """The session graph a graph management operation's ``term`` names; None for NAMED (every
    session graph). DEFAULT and ALL hold the model's own graph, as does an IRI under ``base``:
    refused by name."""
    from provisa.sparql_query.compiler import model_graph_refusal
    from provisa.sparql_query.session_graph import session_graph

    if term == "NAMED":
        return None
    if term in ("DEFAULT", "ALL"):
        raise model_graph_refusal(f"{op} {term}", base)
    return session_graph(term, base, op)


def _managed_graphs(op: CompValue, base: str) -> None:
    """Refuse by name a graph management operation naming the model's own graph."""
    terms = op.graph if op.name in ("Add", "Copy", "Move") else [op.graphiri]
    for term in terms:
        managed_graph(term, base, op.name.upper())


def parse_update(text: str, base: str) -> list[UpdateOperation]:
    """The operations of the update ``text``, in order; a graph under ``base`` (the model's own)
    and LOAD are refused by name."""
    from provisa.sparql_query.session_graph import session_graph

    refusal = update_refusal(text)
    if refusal is not None:
        raise SparqlRefused(refusal[1])
    parsed = translateUpdate(parseUpdate(text))
    out = []
    for op in parsed.algebra:
        if op.name in GRAPH_MANAGEMENT:
            _managed_graphs(op, base)
            out.append(UpdateOperation(op.name, manage=op))
        elif op.name == "InsertData":
            out.append(UpdateOperation(op.name, insert_graphs=_by_graph(op, base, "INSERT DATA")))
        elif op.name == "DeleteData":
            out.append(UpdateOperation(op.name, delete_graphs=_by_graph(op, base, "DELETE DATA")))
        elif op.name == "DeleteWhere":
            # Its pattern is its template: each graph's triples read in that graph.
            graphs = _by_graph(op, base, "DELETE WHERE")
            where = _joined(
                [
                    CompValue("BGP", triples=triples)
                    if graph == ""
                    else _in_graph(graph, CompValue("BGP", triples=triples))
                    for graph, triples in graphs.items()
                ]
            )
            out.append(UpdateOperation(op.name, delete_graphs=graphs, where=where))
        elif op.name == "Modify":
            # WITH names the graph its templates write and, without USING, its WHERE reads; USING
            # names the graph its WHERE reads as its default (SPARQL 1.1 Update, 3.1.3).
            into = ""
            if op.withClause is not None:
                into = session_graph(op.withClause, base, "WITH")
            reads = _using_graph(op, base) if op.using else (into or None)
            where = op.where if reads is None else _in_graph(reads, op.where)
            out.append(
                UpdateOperation(
                    op.name,
                    delete_graphs=_by_graph(op.delete, base, "DELETE", into),
                    insert_graphs=_by_graph(op.insert, base, "INSERT", into),
                    where=where,
                )
            )
        else:
            raise SparqlRefused(f"A {op.name} is not served")
    return out


def where_variables(op: UpdateOperation) -> list[str]:
    """The variables of ``op``'s templates that its pattern can bind, in order: what its SELECT
    projects. A template variable the pattern never binds makes no triple."""
    bound: set[str] = set()

    def visit(node: Any) -> None:
        if isinstance(node, CompValue):
            if node.name == "Extend" and isinstance(node.var, RdfVariable):
                bound.add(str(node.var))
            for value in node.values():
                visit(value)
        elif isinstance(node, (list, tuple)):
            for item in node:
                visit(item)
        elif isinstance(node, RdfVariable):
            bound.add(str(node))

    visit(op.where)
    out: list[str] = []
    templates = [*op.delete_graphs.values(), *op.insert_graphs.values()]
    for triple in (t for triples in templates for t in triples):
        for term in triple:
            if isinstance(term, RdfVariable) and str(term) in bound and str(term) not in out:
                out.append(str(term))
    return out


def ground(template: list[Triple], solutions: list[dict[str, Any]]) -> list[Triple]:
    """The triples ``template`` makes of each solution (variable name -> RDF term), each once: a
    triple naming a variable the solution leaves unbound is none (SPARQL 1.1 Update, 3.1.3). A
    blank node is a new one for each solution: a session object (REQ-2135, amended
    2026-11-23)."""
    out: list[Triple] = []
    for i, solution in enumerate(solutions):
        for triple in template:
            made = []
            for term in triple:
                if isinstance(term, RdfVariable):
                    term = solution.get(str(term))
                    if term is None:
                        break
                elif isinstance(term, BNode):
                    term = BNode(f"{term}_{i}")
                made.append(term)
            else:
                if tuple(made) not in out:
                    out.append((made[0], made[1], made[2]))
    return out


def ground_graphs(templates: ByGraph, solutions: list[dict[str, Any]]) -> ByGraph:
    """:func:`ground` of each graph's template."""
    return {graph: ground(template, solutions) for graph, template in templates.items()}


def split_session(triples: list[Triple], base: str) -> tuple[list[Triple], list[Triple]]:
    """The default graph's ``triples`` as (the model's, the session objects'): a triple whose
    subject is a blank node or an IRI outside ``base`` is a session object's."""
    from provisa.sparql_query.session_graph import is_session_subject

    model: list[Triple] = []
    session: list[Triple] = []
    for triple in triples:
        (session if is_session_subject(triple[0], "", base) else model).append(triple)
    return model, session


def model_identity(term: Any, vocab: Vocabulary, lm: CypherLabelMap) -> str:
    """The graph identity (``Label|key``) of the model row the IRI ``term`` names: a session
    relationship's end on it."""
    from provisa.ephemeral.write import _model_identity

    row = _Rows(vocab, lm).row(term)
    return _model_identity(row.node, row.key)


@dataclass
class _Row:
    """A row an update's triples name: its label, its node and its key, the properties stated
    of it, whether it is typed, and the relationships from it (type, the end row's IRI)."""

    iri: str
    label: str
    node: NodeMapping
    key: dict[str, Any]
    props: dict[str, Any] = field(default_factory=dict)
    typed: bool = False
    edges: list[tuple[str, str]] = field(default_factory=list)
    key_stated: list[str] = field(default_factory=list)


class _Rows:
    """The rows a set of ground triples names, read against the role's vocabulary."""

    def __init__(self, vocab: Vocabulary, lm: CypherLabelMap) -> None:
        self.vocab = vocab
        self.lm = lm
        self.rows: dict[str, _Row] = {}

    def row(self, term: Any) -> _Row:
        if isinstance(term, BNode):
            raise SparqlRefused("A blank node names no row: name each subject by its IRI")
        if not isinstance(term, URIRef):
            raise SparqlRefused(f"{term.n3()} is no subject: a subject is a row's IRI")
        iri = str(term)
        if iri in self.rows:
            return self.rows[iri]
        ref = parse_subject_iri(self.vocab.base, iri)
        if ref is None:
            raise SparqlRefused(
                f"<{iri}> names no row: a row's IRI is {self.vocab.base}/id/<domain>/<table>/<key>"
            )
        labels = [
            label
            for label in self.vocab.classes_of
            if self.lm.nodes[label].domain_id == ref.domain
            and self.lm.nodes[label].table_name == ref.table
        ]
        if len(labels) != 1:
            raise SparqlRefused(
                f"<{iri}> names {ref.domain}.{ref.table}, which is no table this role reads"
                if not labels
                else f"<{iri}> names {ref.domain}.{ref.table}, which several labels read: "
                f"{sorted(labels)}"
            )
        label = labels[0]
        nm = self.lm.nodes[label]
        columns = subject_key(nm)
        if len(columns) != len(ref.keys):
            raise SparqlRefused(
                f"<{iri}> gives {len(ref.keys)} key values; {nm.table_name}'s key is "
                f"{', '.join(columns)}"
            )
        by_column = {col: prop for prop, col in nm.properties.items()}
        key: dict[str, Any] = {}
        for column, value in zip(columns, ref.keys, strict=True):
            prop = by_column.get(column)
            if prop is None:
                raise SparqlRefused(f"{nm.table_name}'s key column {column} is not readable")
            datatype = next(
                (cp.datatype for _, cp in self.vocab.columns_of[label] if cp.prop == prop), None
            )
            key[prop] = (
                value
                if datatype is None or datatype == f"{XSD}string"
                else _value(RdfLiteral(value, datatype=URIRef(datatype)))
            )
        made = _Row(iri, label, nm, key)
        self.rows[iri] = made
        return made

    def add(self, triple: Triple) -> None:
        s, p, o = triple
        row = self.row(s)
        if isinstance(p, BNode) or not isinstance(p, URIRef):
            raise SparqlRefused(f"{p.n3()} is no predicate")
        pred = str(p)
        if pred == RDF_TYPE:
            if not isinstance(o, URIRef) or str(o) not in self.vocab.classes_of[row.label]:
                raise SparqlRefused(
                    f"{o.n3()} does not type <{row.iri}>: its classes are "
                    f"{', '.join(self.vocab.classes_of[row.label])}"
                )
            row.typed = True
            return
        column = next(
            (cp for cp in self.vocab.columns.get(pred, []) if cp.label == row.label), None
        )
        if column is not None:
            if not isinstance(o, RdfLiteral):
                raise SparqlRefused(
                    f"<{pred}> is a property of {row.label}: its object is a literal"
                )
            value = _value(o)
            if column.prop in row.key:
                if value != row.key[column.prop]:
                    raise SparqlRefused(
                        f"<{row.iri}>'s {column.prop} is its key, {row.key[column.prop]!r}, "
                        "named by its IRI"
                    )
                row.key_stated.append(column.prop)
                return
            if column.prop in row.props and row.props[column.prop] != value:
                raise SparqlRefused(
                    f"<{row.iri}> is given two values of <{pred}>; a column holds one"
                )
            row.props[column.prop] = value
            return
        if pred in self.vocab.edges:
            end = self.row(o)
            if not any(
                ep.source == row.label and ep.target == end.label for ep in self.vocab.edges[pred]
            ):
                raise SparqlRefused(f"<{pred}> does not relate {row.label} to {end.label}")
            rel_type = next(
                ep.rel_type
                for ep in self.vocab.edges[pred]
                if ep.source == row.label and ep.target == end.label
            )
            if (rel_type, end.iri) not in row.edges:
                row.edges.append((rel_type, end.iri))
            return
        raise SparqlRefused(f"<{pred}> is not a predicate of {row.label} this role reads")


def _value(literal: RdfLiteral) -> Any:
    """A literal as the value its column is written with."""
    value = literal.toPython()
    return str(literal) if isinstance(value, RdfLiteral) else value


def _rows_of(triples: list[Triple], vocab: Vocabulary, lm: CypherLabelMap) -> _Rows:
    rows = _Rows(vocab, lm)
    for triple in triples:
        rows.add(triple)
    return rows


def insert_write(triples: list[Triple], vocab: Vocabulary, lm: CypherLabelMap) -> WriteAST | None:
    """The write of the ground ``triples`` an insert states: a MERGE of each row they name on its
    key, setting each property stated of it, then a MERGE of each relationship. None for none."""
    if not triples:
        return None
    rows = _rows_of(triples, vocab, lm)
    names = {iri: f"r{i}" for i, iri in enumerate(rows.rows)}
    merges: list[MergePart] = []
    for iri, row in rows.rows.items():
        var = names[iri]
        sets = [(var, prop, value) for prop, value in row.props.items()]
        merges.append(
            MergePart(
                nodes=[CreatedNode(var, row.label, dict(row.key))],
                links=[],
                on_create=sets,
                on_match=list(sets),
            )
        )
    for iri, row in rows.rows.items():
        for rel_type, end in row.edges:
            merges.append(
                MergePart(
                    nodes=[CreatedNode(names[iri], "", {}), CreatedNode(names[end], "", {})],
                    links=[CreatedLink("", rel_type, {}, 0, 1)],
                )
            )
    return WriteAST(kind="merge", label="", variable="", merges=merges)


def delete_writes(
    triples: list[Triple], vocab: Vocabulary, lm: CypherLabelMap
) -> tuple[list[WriteAST], dict[str, Any]]:
    """The writes of the ground ``triples`` a delete states, and the parameters their reads bind:
    each relationship's junction row, then each property set to null where it holds the value
    stated, then each typed row deleted (and with it each other triple of it stated here)."""
    rows = _rows_of(triples, vocab, lm)
    params: dict[str, Any] = {}

    def held(row: _Row) -> str:
        pairs = []
        for prop, value in row.key.items():
            name = f"{_PARAM}{len(params)}"
            params[name] = value
            pairs.append(f"{prop}: ${name}")
        return "{" + ", ".join(pairs) + "}"

    erased: list[WriteAST] = []
    updated: list[WriteAST] = []
    deleted: list[WriteAST] = []
    for row in rows.rows.values():
        for rel_type, end_iri in row.edges:
            end = rows.rows[end_iri]
            reads = f"MATCH (a:{row.label} {held(row)})-[r:{rel_type}]->(b:{end.label} {held(end)})"
            erased.append(WriteAST(kind="erase", label="", variable="", reads=reads, deleted=["r"]))
        if row.typed:
            deleted.append(
                WriteAST(kind="delete", label=row.label, variable="n", match_props=dict(row.key))
            )
            continue
        if row.key_stated:
            raise SparqlRefused(
                f"<{row.iri}>'s {', '.join(row.key_stated)} is its key, the row's identity: "
                "delete the row with its rdf:type triple"
            )
        for prop, value in row.props.items():
            updated.append(
                WriteAST(
                    kind="update",
                    label=row.label,
                    variable="n",
                    match_props={**row.key, prop: value},
                    set_assignments=[(prop, None)],
                )
            )
    return [*erased, *updated, *deleted], params
