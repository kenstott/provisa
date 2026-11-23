# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1f0b7a-3c92-4e58-a1d4-8b7e2f9c0a63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPARQL Update's session objects, in the session tables Cypher's ephemeral objects use
(REQ-2135, amended 2026-11-23; REQ-2176).

A triple whose subject is a blank node, or an IRI outside the model's base IRI, or any triple of
a session graph, is a session object's, written to the principal's graph session
(:mod:`provisa.ephemeral.store`) as a Cypher CREATE writes one:

- its subject is a session node: a blank node a new one each time an operation names it, an IRI
  the one node the session holds for it in that graph (the IRI's ``node`` name);
- an ``rdf:type`` of a class outside the model is a free label; one of the model's is refused by
  name, since a session node is no row of a model table;
- a literal predicate is a property, its many values one list (the store's widening rule);
- an IRI predicate is a relationship type, to the node its object names: a session node, or, in
  the default graph, the model row its IRI names;
- a named graph is a graph tag: a label every node of the graph carries, and the ``graph`` column
  of each relationship in it. A graph IRI under the model's base IRI names the model's own graph,
  refused by name, as is DEFAULT.

The name each IRI is given -- a label, a property, a relationship type, a graph tag, or the node
an IRI subject is in a graph -- is a row of one more session table, :data:`IRIS`; Cypher reads
the same objects under those names.

Graph management (CLEAR, DROP, CREATE, COPY, MOVE, ADD) acts on the session's graphs alone.
"""

# Requirements: REQ-2135, REQ-2176

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from rdflib import BNode, Literal as RdfLiteral, URIRef

from provisa.compiler.temp_tables import TempSession
from provisa.cypher import counters
from provisa.ephemeral import store
from provisa.ephemeral.store import NewLabel, NewNode, NewRel, Writes
from provisa.sparql_query.compiler import SparqlRefused, model_graph_refusal
from provisa.sparql_query.vocabulary import RDF_TYPE, under_base

Triple = tuple[Any, Any, Any]

#: The session table naming each IRI a session object uses: (iri, graph, kind, name). ``graph``
#: is the graph a ``node`` is in ("" for the default graph) and "" for every other kind.
IRIS = store.SPARQL_IRIS
_IRIS_COLUMNS = (("iri", "text"), ("graph", "text"), ("kind", "text"), ("name", "text"))

CLASS, PROPERTY, REL, GRAPH, NODE = "class", "property", "rel", "graph", "node"
#: The kinds whose names are labels: told apart from each other and from the model's labels.
_LABEL_KINDS = (CLASS, GRAPH)

DEFAULT = ""


def session_graph(term: Any, base: str, what: str) -> str:
    """The session graph ``term`` names: an IRI outside the model's base IRI. One under the base
    (the model's own graph) is refused by name."""
    if not isinstance(term, URIRef):
        raise SparqlRefused(f"{what} names its graph by a variable; name it by its IRI")
    iri = str(term)
    if under_base(iri, base):
        raise model_graph_refusal(f"<{iri}>", base)
    return iri


def is_session_subject(term: Any, graph: str, base: str) -> bool:
    """Whether a triple of ``graph`` with subject ``term`` is a session object's: every triple of
    a session graph; in the default graph, a blank node's and an IRI's outside the base."""
    if graph != DEFAULT:
        return True
    if isinstance(term, BNode):
        return True
    return isinstance(term, URIRef) and not under_base(str(term), base)


# --- the IRI names -------------------------------------------------------------------------------


_WORD = re.compile(r"[^A-Za-z0-9_]+")


def _local(iri: str) -> str:
    """The word an IRI's last segment gives a name: after its last ``#``, ``/`` or ``:``."""
    tail = re.split(r"[#/:]", iri.rstrip("#/"))[-1]
    word = _WORD.sub("_", tail).strip("_")
    if not word or word[0].isdigit():
        word = f"n_{word}" if word else "n"
    return word


@dataclass
class Names:
    """The session's IRI names, as :data:`IRIS` holds them, and the rows a write adds."""

    held: dict[tuple[str, str, str], str] = field(default_factory=dict)
    added: list[dict[str, str]] = field(default_factory=list)
    removed: list[tuple[str, str, str]] = field(default_factory=list)
    #: Names no kind of label may take: the model's labels and relationship types.
    reserved: frozenset[str] = frozenset()

    def get(self, kind: str, iri: str, graph: str = DEFAULT) -> str | None:
        return self.held.get((iri, graph, kind))

    def _taken(self, kind: str) -> set[str]:
        kinds = _LABEL_KINDS if kind in _LABEL_KINDS else (kind,)
        taken = {name for (_, _, k), name in self.held.items() if k in kinds}
        if kind != PROPERTY:
            taken |= set(self.reserved)
        else:
            taken |= {store.NODE_ID, "_id"}
        return taken

    def name(self, kind: str, iri: str, graph: str = DEFAULT, given: str | None = None) -> str:
        """The name of ``iri`` as ``kind``: the one held, else a new one -- ``given``, or the IRI's
        last word, made unique among its kind's names (labels among labels and the model's)."""
        held = self.get(kind, iri, graph)
        if held is not None:
            return held
        if given is None:
            word = _local(iri)
            taken = self._taken(kind)
            given, n = word, 1
            while given in taken:
                n += 1
                given = f"{word}_{n}"
        self.held[(iri, graph, kind)] = given
        self.added.append({"iri": iri, "graph": graph, "kind": kind, "name": given})
        return given

    def drop(self, kind: str, iri: str, graph: str = DEFAULT) -> None:
        if self.held.pop((iri, graph, kind), None) is not None:
            self.removed.append((iri, graph, kind))
            self.added = [
                a for a in self.added if (a["iri"], a["graph"], a["kind"]) != (iri, graph, kind)
            ]

    def graphs(self) -> dict[str, str]:
        """Each session graph's IRI -> its tag."""
        return {iri: name for (iri, _, kind), name in self.held.items() if kind == GRAPH}

    def by_name(self, kind: str) -> dict[str, str]:
        """Each name of ``kind`` -> its IRI."""
        return {name: iri for (iri, _, k), name in self.held.items() if k == kind}


async def names_of(state: Any, session: TempSession, model: Any) -> Names:
    """The session's IRI names, read from :data:`IRIS`."""
    from provisa.pgwire import temp_exec

    reserved = frozenset(
        {nm.label for nm in model.nodes.values()}
        | {nm.type_name for nm in model.nodes.values()}
        | {r.rel_type for r in model.relationships.values()}
    )
    names = Names(reserved=reserved)
    if IRIS in session.tables:
        for iri, graph, kind, name in await temp_exec.held_rows(state, session, IRIS):
            names.held[(str(iri), str(graph or ""), str(kind))] = str(name)
    return names


async def save_names(state: Any, session: TempSession, names: Names) -> None:
    """Land the rows a write added to and removed from :data:`IRIS`."""
    if names.removed:
        await store.remove_rows(state, session, IRIS, ("iri", "graph", "kind"), names.removed)
    if names.added:
        await store.add_rows(state, session, IRIS, _IRIS_COLUMNS, names.added)
    names.added, names.removed = [], []


# --- what the session holds of a node ------------------------------------------------------------


async def _props_of(state: Any, session: TempSession, node_ids: list[str]) -> dict[str, dict]:
    """Each of ``node_ids``' properties as its anchor holds them (None-valued ones left out)."""
    from provisa.pgwire import temp_exec

    g = store.graph(session)
    by_anchor: dict[str, list[str]] = {}
    for node_id in node_ids:
        by_anchor.setdefault(g.node_anchors[node_id], []).append(node_id)
    out: dict[str, dict] = {n: {} for n in node_ids}
    for anchor, ids in by_anchor.items():
        if anchor not in session.tables:
            continue
        kinds = dict(session.tables[anchor].columns)
        columns = tuple(kinds)
        for row in await temp_exec.keyed_rows(state, session, anchor, store.NODE_ID, ids, columns):
            held = dict(zip(columns, row, strict=True))
            node_id = str(held.pop(store.NODE_ID))
            out[node_id] = {
                k: json.loads(v) if kinds[k] == "json" and isinstance(v, str) else v
                for k, v in held.items()
                if v is not None
            }
    return out


@dataclass(frozen=True)
class HeldRel:
    rel_id: str
    rel_type: str
    from_id: str
    to_id: str
    graph: str


async def _rels_of(state: Any, session: TempSession, node_ids: list[str]) -> list[HeldRel]:
    """The session relationships either end of which is one of ``node_ids``, each once."""
    from provisa.pgwire import temp_exec

    if not node_ids or store.RELS not in session.tables:
        return []
    columns = (store.REL_ID, "type", "from_id", "to_id", "graph")
    found: dict[str, HeldRel] = {}
    for end in ("from_id", "to_id"):
        for rel_id, rel_type, a, b, graph in await temp_exec.keyed_rows(
            state, session, store.RELS, end, sorted(node_ids), columns
        ):
            found[str(rel_id)] = HeldRel(str(rel_id), str(rel_type), str(a), str(b), graph or "")
    return list(found.values())


async def _rels_in(state: Any, session: TempSession, graph: str) -> list[HeldRel]:
    """The session relationships of the named ``graph`` (its tag)."""
    from provisa.pgwire import temp_exec

    if store.RELS not in session.tables:
        return []
    columns = (store.REL_ID, "type", "from_id", "to_id", "graph")
    return [
        HeldRel(str(r), str(t), str(a), str(b), str(g))
        for r, t, a, b, g in await temp_exec.keyed_rows(
            state, session, store.RELS, "graph", [graph], columns
        )
    ]


def _values(held: Any) -> list[Any]:
    """A property's values: a list's members, else the one value."""
    return list(held) if isinstance(held, (list, tuple)) else [held]


def _value_of(values: list[Any], listed: bool = False) -> Any:
    """Values as one property: one alone, several a list, none None (the property cleared). A
    column widened to a list (``listed``) holds a list from then on, one value or several: a
    column only widens (REQ-2176)."""
    if not values:
        return None
    return values[0] if len(values) == 1 and not listed else list(values)


def _listed(session: TempSession, anchor: str, prop: str) -> bool:
    """Whether ``anchor``'s column ``prop`` was widened to hold lists (a list, or JSON)."""
    held = session.tables.get(anchor)
    kind = dict(held.columns).get(prop) if held is not None else None
    return kind is not None and (kind.startswith("list<") or kind == "json")


def _literal(o: RdfLiteral) -> Any:
    value = o.toPython()
    return str(o) if isinstance(value, RdfLiteral) else value


# --- writing -------------------------------------------------------------------------------------


@dataclass
class _Node:
    """A session node an operation names: held already (``node_id`` known), or new."""

    node_id: str
    new: bool
    labels: list[str] = field(default_factory=list)
    props: dict[str, list[Any]] = field(default_factory=dict)


class SessionWrite:
    """The session objects one update writes, in the principal's graph session."""

    def __init__(
        self, state: Any, session: TempSession, model: Any, vocab: Any, names: Names, rows: Any
    ) -> None:
        self.state = state
        self.session = session
        self.model = model
        self.vocab = vocab
        self.names = names
        self.base = vocab.base
        #: A model row's IRI term -> its graph identity (``Label|key``).
        self.rows = rows
        self.g = store.graph(session)
        #: Blank node labels already used by an earlier operation of the request.
        self.blank_used: set[str] = set()
        self.wrote = False

    # -- what a term names --

    def _predicate(self, p: Any) -> str:
        """``p`` as a session object's predicate: any IRI outside the base; one under it only
        when the role reads it."""
        if not isinstance(p, URIRef):
            raise SparqlRefused(f"{p.n3()} is no predicate")
        pred = str(p)
        if (
            pred != RDF_TYPE
            and under_base(pred, self.base)
            and pred not in self.vocab.columns
            and pred not in self.vocab.edges
        ):
            raise SparqlRefused(f"<{pred}> is not a predicate this role reads")
        return pred

    def _tag(self, graph: str) -> str | None:
        return None if graph == DEFAULT else self.names.name(GRAPH, graph)

    def _held_node(
        self, term: Any, graph: str, blanks: dict, pending: dict | None = None
    ) -> str | None:
        """The node ``term`` names in ``graph``: one the session holds, or one this operation is
        making (``pending``)."""
        if isinstance(term, BNode):
            return blanks.get((graph, str(term)))
        node_id = self.names.get(NODE, str(term), graph)
        if node_id in self.g.node_keys or (pending is not None and node_id in pending):
            return node_id
        return None

    def _node(self, term: Any, graph: str, blanks: dict, nodes: dict[str, _Node]) -> _Node:
        """The session node ``term`` names in ``graph``, made when the session holds none."""
        node_id = self._held_node(term, graph, blanks, nodes)
        if node_id is None:
            node_id = self.g.new_id()
            if isinstance(term, BNode):
                blanks[(graph, str(term))] = node_id
            else:
                self.names.drop(NODE, str(term), graph)
                self.names.name(NODE, str(term), graph, given=node_id)
            nodes[node_id] = _Node(node_id, new=True)
        elif node_id not in nodes:
            nodes[node_id] = _Node(node_id, new=False)
        return nodes[node_id]

    def _subject(self, term: Any) -> None:
        if not isinstance(term, (BNode, URIRef)):
            raise SparqlRefused(f"{term.n3()} is no subject")
        if isinstance(term, BNode) and str(term) in self.blank_used:
            raise SparqlRefused(
                f"The update is not SPARQL this endpoint reads: the blank node _:{term} is "
                "named by two operations of one request"
            )

    # -- INSERT --

    async def insert(self, by_graph: dict[str, list[Triple]]) -> None:
        """Write the session objects of ``by_graph`` (a graph's IRI, "" for the default graph ->
        its ground triples)."""
        if not any(by_graph.values()):
            for graph in by_graph:
                if graph != DEFAULT:
                    self._tag(graph)
            await self._save()
            return
        blanks: dict[tuple[str, str], str] = {}
        nodes: dict[str, _Node] = {}
        edges: list[tuple[str, str, str, str]] = []  # (from node, type, to identity, graph)
        used: set[str] = set()
        for graph, triples in by_graph.items():
            tag = self._tag(graph)
            for s, p, o in triples:
                self._subject(s)
                if isinstance(s, BNode):
                    used.add(str(s))
                node = self._node(s, graph, blanks, nodes)
                if tag is not None and tag not in node.labels:
                    node.labels.append(tag)
                pred = self._predicate(p)
                if pred == RDF_TYPE:
                    if not isinstance(o, URIRef):
                        raise SparqlRefused(f"{o.n3()} is no class: a class is an IRI")
                    if under_base(str(o), self.base):
                        raise SparqlRefused(
                            f"{s.n3()} is a session object, and {o.n3()} is a class of the "
                            "model: a model row is named by its IRI, in the default graph"
                        )
                    label = self.names.name(CLASS, str(o))
                    if label not in node.labels:
                        node.labels.append(label)
                elif isinstance(o, RdfLiteral):
                    prop = self.names.name(PROPERTY, pred)
                    values = node.props.setdefault(prop, [])
                    value = _literal(o)
                    if value not in values:
                        values.append(value)
                else:
                    rel_type = self.names.name(REL, pred)
                    if isinstance(o, BNode):
                        used.add(str(o))
                    end = self._end(o, graph, blanks, nodes, tag)
                    edges.append((node.node_id, rel_type, end, graph))
        self.blank_used |= used
        await self._write(nodes, edges)

    def _end(
        self, o: Any, graph: str, blanks: dict, nodes: dict[str, _Node], tag: str | None
    ) -> str:
        """The graph identity of a relationship's end: a model row's (in the default graph, an
        IRI under the base), else a session node's."""
        if graph == DEFAULT and isinstance(o, URIRef) and under_base(str(o), self.base):
            return self.rows(o)
        end = self._node(o, graph, blanks, nodes)
        if tag is not None and tag not in end.labels:
            end.labels.append(tag)
        return end.node_id

    def _labels_of(self, identity: str, nodes: dict[str, _Node]) -> tuple[str, ...]:
        if identity in nodes and nodes[identity].new:
            return tuple(nodes[identity].labels) or ("",)
        if identity in self.g.node_keys:
            held = tuple(k for k in self.g.node_keys[identity] if k)
            added = (
                tuple(x for x in nodes[identity].labels if x not in held)
                if identity in nodes
                else ()
            )
            return (*held, *added) or ("",)
        type_name = identity.split("|", 1)[0]
        named = [nm.type_name for nm in self.model.nodes.values() if nm.label == type_name]
        return (named[0] if named else type_name,)

    async def _write(self, nodes: dict[str, _Node], edges: list[tuple[str, str, str, str]]) -> None:
        held_ids = [n.node_id for n in nodes.values() if not n.new]
        held_props = await _props_of(self.state, self.session, held_ids) if held_ids else {}
        held_rels = await _rels_of(self.state, self.session, held_ids)
        writes = Writes()
        updates: dict[str, dict[str, Any]] = {}
        labels_added = props_set = 0
        for node in nodes.values():
            if node.new:
                anchor = self._anchor(node.labels)
                props = {
                    k: _value_of(v, _listed(self.session, anchor, k)) for k, v in node.props.items()
                }
                writes.nodes.append(NewNode(node.node_id, anchor, tuple(node.labels), props))
                labels_added += len(node.labels)
                props_set += len(props)
                continue
            carried = set(self.g.node_keys[node.node_id])
            for label in node.labels:
                if label not in carried:
                    writes.labels.append(NewLabel(node.node_id, label))
                    labels_added += 1
            held = held_props.get(node.node_id, {})
            changed: dict[str, Any] = {}
            for prop, values in node.props.items():
                before = _values(held[prop]) if prop in held else []
                after = before + [v for v in values if v not in before]
                if after != before:
                    anchor = self.g.node_anchors[node.node_id]
                    changed[prop] = _value_of(after, _listed(self.session, anchor, prop))
                    props_set += 1
            if changed:
                updates[node.node_id] = changed
        seen = {(r.from_id, r.rel_type, r.to_id, r.graph) for r in held_rels}
        for from_id, rel_type, to_id, graph in edges:
            if (from_id, rel_type, to_id, graph) in seen:
                continue  # RDF holds a triple once
            seen.add((from_id, rel_type, to_id, graph))
            writes.rels.append(
                NewRel(
                    self.g.new_id("r"),
                    rel_type,
                    from_id,
                    to_id,
                    {},
                    self._labels_of(from_id, nodes),
                    self._labels_of(to_id, nodes),
                    None if graph == DEFAULT else graph,
                )
            )
        if writes.nodes or writes.labels or writes.rels:
            await store.write(self.state, self.session, writes)
        if updates:
            await store.set_properties(self.state, self.session, updates, {})
        counters.count(
            nodes_created=len(writes.nodes),
            relationships_created=len(writes.rels),
            labels_added=labels_added,
            properties_set=props_set,
        )
        self.wrote = self.wrote or bool(writes.nodes or writes.labels or writes.rels or updates)
        await self._save()

    def _anchor(self, labels: list[str]) -> str:
        """A new node's anchor table: its first class label's, else the default anchor."""
        graph_tags = set(self.names.graphs().values())
        first = next((label for label in labels if label not in graph_tags), None)
        if first is None:
            return store.DEFAULT_ANCHOR
        return self.g.anchors.setdefault(first, store.anchor_of(self.g, first))

    async def _save(self) -> None:
        if self.names.added or self.names.removed:
            self.wrote = True
        await save_names(self.state, self.session, self.names)

    # -- DELETE --

    async def delete(self, by_graph: dict[str, list[Triple]]) -> None:
        """Delete the session objects' triples of ``by_graph``: a label, a property's value, a
        relationship. A triple the session does not hold is no change; a node left with no
        triple is gone."""
        removed_labels: set[tuple[str, str]] = set()
        updates: dict[str, dict[str, Any]] = {}
        gone_rels: set[str] = set()
        touched: set[str] = set()
        for graph, triples in by_graph.items():
            subjects = {
                s: node_id
                for s, _, _ in triples
                if (node_id := self._held_node(s, graph, {})) is not None
            }
            ids = sorted(set(subjects.values()))
            touched.update(ids)
            held_props = await _props_of(self.state, self.session, ids) if ids else {}
            held_rels = await _rels_of(self.state, self.session, ids)
            for s, p, o in triples:
                if isinstance(s, BNode):
                    raise SparqlRefused("A blank node in DELETE DATA names nothing to delete")
                node_id = subjects.get(s)
                if node_id is None:
                    continue
                pred = str(p)
                if pred == RDF_TYPE:
                    label = self.names.get(CLASS, str(o))
                    if label is not None and label in self.g.node_keys[node_id]:
                        removed_labels.add((node_id, label))
                elif isinstance(o, RdfLiteral):
                    prop = self.names.get(PROPERTY, pred)
                    if prop is None:
                        continue
                    current = updates.get(node_id, {}).get(prop, held_props[node_id].get(prop))
                    if current is None:
                        continue
                    left = [v for v in _values(current) if v != _literal(o)]
                    if len(left) != len(_values(current)):
                        anchor = self.g.node_anchors[node_id]
                        listed = _listed(self.session, anchor, prop)
                        updates.setdefault(node_id, {})[prop] = _value_of(left, listed)
                else:
                    rel_type = self.names.get(REL, pred)
                    if graph == DEFAULT and isinstance(o, URIRef) and under_base(str(o), self.base):
                        end: str | None = self.rows(o)
                    else:
                        end = self._held_node(o, graph, {})
                    for r in held_rels:
                        if (r.from_id, r.rel_type, r.to_id, r.graph) == (
                            node_id,
                            rel_type,
                            end,
                            graph,
                        ):
                            gone_rels.add(r.rel_id)
                            if r.to_id in self.g.node_keys:
                                touched.add(r.to_id)
        if gone_rels:
            await store.delete_rels(self.state, self.session, sorted(gone_rels))
        if removed_labels:
            await store.remove_labels(self.state, self.session, removed_labels)
        if updates:
            await store.set_properties(self.state, self.session, updates, {})
        counters.count(
            relationships_deleted=len(gone_rels),
            labels_removed=len(removed_labels),
            properties_set=sum(len(u) for u in updates.values()),
        )
        self.wrote = self.wrote or bool(gone_rels or removed_labels or updates)
        await self._vanish(sorted(touched))
        await self._save()

    async def _vanish(self, node_ids: list[str]) -> None:
        """Delete each of ``node_ids`` that holds no triple any more: no label but its graph
        tag, no property, no relationship. An IRI's node goes with its name."""
        if not node_ids:
            return
        graph_tags = set(self.names.graphs().values())
        props = await _props_of(self.state, self.session, node_ids)
        rels = await _rels_of(self.state, self.session, node_ids)
        ended = {r.from_id for r in rels} | {r.to_id for r in rels}
        empty = [
            n
            for n in node_ids
            if not props[n]
            and n not in ended
            and not [x for x in self.g.node_keys[n] if x and x not in graph_tags]
        ]
        await self._delete_nodes(empty)

    async def _delete_nodes(self, node_ids: list[str]) -> None:
        if not node_ids:
            return
        gone = set(node_ids)
        rels = await _rels_of(self.state, self.session, node_ids)
        if rels:
            await store.delete_rels(self.state, self.session, [r.rel_id for r in rels])
        await store.delete_nodes(self.state, self.session, node_ids)
        for (iri, graph, kind), name in list(self.names.held.items()):
            if kind == NODE and name in gone:
                self.names.drop(NODE, iri, graph)
        counters.count(nodes_deleted=len(node_ids), relationships_deleted=len(rels))
        self.wrote = True

    # -- graph management --

    def _graph_nodes(self, iri: str) -> list[str]:
        tag = self.names.get(GRAPH, iri)
        if tag is None:
            return []
        return sorted(n for n, labels in self.g.node_keys.items() if tag in labels)

    def _exists(self, iri: str) -> bool:
        return self.names.get(GRAPH, iri) is not None

    def _graph_of(self, term: Any, op: str) -> str | None:
        """The session graph an operation's ``term`` names; None for NAMED (every one)."""
        from provisa.sparql_query.update import managed_graph

        return managed_graph(term, self.base, op)

    async def clear(self, iri: str) -> None:
        """Delete every triple of the session graph ``iri``; the graph stays, empty."""
        await self._delete_nodes(self._graph_nodes(iri))
        tag = self.names.get(GRAPH, iri)
        if tag is not None:
            rels = await _rels_in(self.state, self.session, iri)
            if rels:
                await store.delete_rels(self.state, self.session, [r.rel_id for r in rels])
                counters.count(relationships_deleted=len(rels))
                self.wrote = True

    async def manage(self, op: Any) -> None:
        """One graph management operation (rdflib's algebra) on the session's graphs."""
        silent = bool(getattr(op, "silent", None))
        name = op.name.upper()
        if op.name in ("Clear", "Drop"):
            graph = self._graph_of(op.graphiri, name)
            targets = sorted(self.names.graphs()) if graph is None else [graph]
            if graph is not None and not self._exists(graph) and not silent:
                raise SparqlRefused(f"{name} GRAPH <{graph}>: the session holds no such graph")
            for iri in targets:
                if not self._exists(iri):
                    continue
                await self.clear(iri)
                if op.name == "Drop":
                    self.names.drop(GRAPH, iri)
        elif op.name == "Create":
            graph = self._graph_of(op.graphiri, name)
            assert graph is not None, "CREATE names one graph"
            if self._exists(graph):
                if not silent:
                    raise SparqlRefused(f"CREATE GRAPH <{graph}>: the session holds it already")
            else:
                self._tag(graph)
        elif op.name in ("Add", "Copy", "Move"):
            source, target = (self._graph_of(t, name) for t in op.graph)
            if source is None or target is None:
                raise SparqlRefused(f"{name} names one graph on each side, not NAMED")
            if not self._exists(source):
                if silent:
                    return
                raise SparqlRefused(f"{name} <{source}>: the session holds no such graph")
            if source != target:
                if op.name != "Add" and self._exists(target):
                    await self.clear(target)
                await self._copy(source, target)
                if op.name == "Move":
                    await self.clear(source)
                    self.names.drop(GRAPH, source)
        else:
            raise SparqlRefused(f"{name} is not served: it loads from outside the session")
        await self._save()

    async def _copy(self, source: str, target: str) -> None:
        """Add every triple of the session graph ``source`` to ``target``."""
        source_tag = self.names.get(GRAPH, source)
        node_ids = self._graph_nodes(source)
        props = await _props_of(self.state, self.session, node_ids) if node_ids else {}
        iri_of = {
            name: iri
            for (iri, graph, kind), name in self.names.held.items()
            if kind == NODE and graph == source
        }
        by_name = {kind: self.names.by_name(kind) for kind in (CLASS, PROPERTY)}
        blank_terms = {n: BNode(f"copy{n}") for n in node_ids if n not in iri_of}

        def term(node_id: str) -> Any:
            return URIRef(iri_of[node_id]) if node_id in iri_of else blank_terms[node_id]

        triples: list[Triple] = []
        for node_id in node_ids:
            for label in self.g.node_keys[node_id]:
                if label and label != source_tag and label in by_name[CLASS]:
                    triples.append((term(node_id), URIRef(RDF_TYPE), URIRef(by_name[CLASS][label])))
            for prop, value in props[node_id].items():
                for v in _values(value):
                    triples.append((term(node_id), URIRef(by_name[PROPERTY][prop]), RdfLiteral(v)))
        rel_iris = self.names.by_name(REL)
        for r in await _rels_in(self.state, self.session, source):
            end = term(r.to_id) if r.to_id in self.g.node_keys else None
            if end is None:
                raise SparqlRefused(f"<{source}> holds a relationship to {r.to_id}, no node of it")
            triples.append((term(r.from_id), URIRef(rel_iris[r.rel_type]), end))
        # The copies' blank nodes are this copy's own, never a request's.
        used, self.blank_used = self.blank_used, set()
        try:
            await self.insert({target: triples})
        finally:
            self.blank_used = used


# --- reading -------------------------------------------------------------------------------------


class SessionNode(BNode):
    """A session node an update's WHERE solution names that has no IRI (a blank node's): its
    templates name the very node again, by its id, never a new one."""

    node_id: str

    def __new__(cls, node_id: str) -> SessionNode:
        made = BNode.__new__(cls, _blank_label(node_id))
        made.node_id = node_id
        return made


def _blank_label(node_id: str) -> str:
    """A session node's id as a blank node's label (``e:1`` -> ``e1``)."""
    return _WORD.sub("", node_id)


def _model_keys(key: str) -> list[str]:
    """A model row's key text (provisa.gql.select_builder.node_key_text) as its key values: a
    compound key's columns split on each ``|`` no backslash escapes, each unescaped."""
    values, held, escaped = [], [], False
    for ch in key:
        if escaped:
            held.append(ch)
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == "|":
            values.append("".join(held))
            held = []
        else:
            held.append(ch)
    values.append("".join(held))
    return values


@dataclass(frozen=True)
class SessionTerms:
    """The RDF term each node a read over the session answers with (its graph identity) is:
    a session node's IRI, or a blank node where it has none; a model row's IRI."""

    #: Each session node an IRI names -> that IRI.
    iris: dict[str, str]
    #: Each model label -> (its domain, its table), which its rows' IRIs name.
    models: dict[str, tuple[str, str]]
    base: str

    def term(self, graph_id: str, update: bool = False) -> Any:
        """``graph_id`` as an RDF term; a blank node of an update's WHERE is a
        :class:`SessionNode`, which its templates name the node by."""
        from provisa.gql.iri import subject_iri

        if graph_id in self.iris:
            return URIRef(self.iris[graph_id])
        label, sep, key = graph_id.partition("|")
        if sep:
            domain, table = self.models[label]
            return URIRef(subject_iri(self.base, domain, table, _model_keys(key)))
        return SessionNode(graph_id) if update else BNode(_blank_label(graph_id))


@dataclass
class SessionRead:
    """What a SPARQL read compiles over in the principal's graph session (REQ-2135, amended
    2026-11-23): the vocabulary with the session's classes, properties and edges; the label map
    (the session's overlay of the model, :mod:`provisa.ephemeral.seam`); and the terms its
    session nodes answer as. Outside a session, or in one holding nothing, the model's own."""

    vocab: Any
    graph: Any
    model: Any
    session: TempSession | None
    terms: SessionTerms | None = None

    def expand(self, sql: Any) -> Any:
        """``sql`` with the session's relations in place (seam.expand)."""
        from provisa.ephemeral import seam

        return seam.expand(sql, self.graph, self.model, self.session)


def _keyed(overlay: Any) -> Any:
    """``overlay`` with each session relation's node named by its graph identity: a subject of
    it is told apart, compared and answered by ``__graph_id``."""
    import copy
    import dataclasses

    from provisa.gql.label_map import GRAPH_ID, is_session_node

    out = copy.copy(overlay)
    out.nodes = {
        key: (
            dataclasses.replace(
                nm,
                pk_columns=[GRAPH_ID],
                properties={**nm.properties, GRAPH_ID: GRAPH_ID},
                physical_properties={**nm.physical_properties, GRAPH_ID: GRAPH_ID},
                property_types=(*nm.property_types, (GRAPH_ID, "text")),
            )
            if is_session_node(nm)
            else nm
        )
        for key, nm in overlay.nodes.items()
    }
    if "" in out.nodes:
        # The session's nodes of no label are read by a name of their own, as a read after a
        # write reads them (provisa.ephemeral.then.UNLABELED): a node pattern of label "" is
        # one of no label, which reads every node.
        from provisa.ephemeral.then import UNLABELED

        out.nodes[UNLABELED] = out.nodes[""]
    return out


def session_vocabulary(vocab: Any, graph: Any, model: Any, names: Names) -> Any:
    """``vocab`` (the role's) with the session's: each class IRI types the nodes of its label,
    each property IRI is read from every session label holding it, and each relationship IRI is
    an edge along every session relationship of its type."""
    import dataclasses

    from provisa.gql.label_map import is_session_node
    from provisa.sparql_query.vocabulary import (
        ColumnPredicate,
        EdgePredicate,
        SessionPart,
        xsd_of,
    )

    session_nodes = {k: nm for k, nm in graph.nodes.items() if is_session_node(nm)}
    labels = frozenset(k for k in session_nodes if k and k not in model.nodes)
    classes = {k: list(v) for k, v in vocab.classes.items()}
    classes_of = {k: list(v) for k, v in vocab.classes_of.items()}
    columns = {k: list(v) for k, v in vocab.columns.items()}
    columns_of = {k: list(v) for k, v in vocab.columns_of.items()}
    edges = {k: list(v) for k, v in vocab.edges.items()}
    edges_of = {k: list(v) for k, v in vocab.edges_of.items()}
    for (iri, _, kind), name in sorted(names.held.items()):
        if kind == CLASS and name in labels:
            classes.setdefault(iri, []).append(name)
            classes_of.setdefault(name, []).append(iri)
        elif kind == PROPERTY:
            for label in sorted(labels):
                nm = session_nodes[label]
                if name not in nm.properties:
                    continue
                cp = ColumnPredicate(label, name, xsd_of(dict(nm.property_types)[name]))
                columns.setdefault(iri, []).append(cp)
                columns_of.setdefault(label, []).append((iri, cp))
        elif kind == REL:
            for rm in graph.relationships.values():
                if rm.rel_type != name or not is_session_node_rel(rm):
                    continue
                ep = EdgePredicate(name, rm.source_label, rm.target_label, rm.join)
                if ep not in edges.setdefault(iri, []):
                    edges[iri].append(ep)
                    edges_of.setdefault(rm.source_label, []).append((iri, ep))
    part = SessionPart(
        labels=labels,
        tags=names.graphs(),
        nodes={
            (iri, graph): name for (iri, graph, kind), name in names.held.items() if kind == NODE
        },
        model_tables={k: model.nodes[k].table_name for k in session_nodes if k in model.nodes},
    )
    return dataclasses.replace(
        vocab,
        classes=classes,
        classes_of=classes_of,
        columns=columns,
        columns_of=columns_of,
        edges=edges,
        edges_of=edges_of,
        session=part,
    )


def is_session_node_rel(rm: Any) -> bool:
    """Whether ``rm`` is a session relationship (the overlay's, provisa.ephemeral.read)."""
    from provisa.gql.label_map import SESSION_CATALOG

    return getattr(rm.via, "catalog_name", None) == SESSION_CATALOG


async def read_over(
    state: Any, session: TempSession | None, label_map: Any, vocab: Any
) -> SessionRead:
    """What a SPARQL read compiles over for the role whose label map is ``label_map`` and whose
    vocabulary is ``vocab``, in ``session`` (the principal's graph session, None outside one)."""
    from provisa.ephemeral import read, seam

    model = label_map.with_sql_only()
    if session is None or not read.involved(session):
        return SessionRead(vocab, model, model, session)
    names = await names_of(state, session, label_map)
    graph = _keyed(seam.label_map(model, session))
    terms = SessionTerms(
        iris={name: iri for (iri, _, kind), name in names.held.items() if kind == NODE},
        models={
            nm.label: (nm.domain_id, nm.table_name)
            for nm in label_map.nodes.values()
            if nm.domain_id
        },
        base=vocab.base,
    )
    return SessionRead(
        session_vocabulary(vocab, graph, label_map, names), graph, model, session, terms
    )
