# Copyright (c) 2026 Kenneth Stott
# Canary: 94695c83-88ca-47d3-baae-cc30f0b6fc22
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SPARQL front end (REQ-2135): a SELECT, ASK or CONSTRUCT, parsed by rdflib into its SPARQL
algebra and lowered to the graph IR the Cypher front end lowers to, read against the role's
:class:`Vocabulary`. Nothing below the IR is SPARQL's own, and SPARQL is never translated through
Cypher text.

How a query's parts become the IR:

- A group (a basic graph pattern with its FILTERs, BINDs and OPTIONALs) becomes one MATCH. UNION
  gives several groups, each a UNION ALL part of one statement.
- Every variable in a subject position, or the object of an edge or a path, is a NODE, and is a
  node of one label in each INTERPRETATION of the group: the labels its triples allow (a class it
  is typed by, the label of a property read from it, an edge's ends) are intersected, and each
  combination of one label per node that every triple holds in is one interpretation, a UNION ALL
  part. The parts are bounded by graph.union_limit.
- A property triple binds its object variable to the property, or compares it; an edge triple is a
  relationship; a property path is a path expression under REACH; ``?s ?p ?o`` is a variable
  predicate over the subject label's predicates.
- The answer: SELECT returns each projected variable as an RDF term (provisa.gql.terms), ASK one
  row when there is a solution, CONSTRUCT the template's triples.

A subject named by its IRI is parsed back to its table and keys (provisa.gql.iri) and matched on
the keys. What the front end does not lower is refused by name.
"""

# Requirements: REQ-2135

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any

from rdflib import BNode, Literal as RdfLiteral, URIRef, Variable as RdfVariable
from rdflib.paths import AlternativePath, InvPath, MulPath, NegatedPath, Path, SequencePath
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue

from provisa.core import settings_registry
from provisa.gql.expr_ast import (
    Binary,
    Case,
    CypherExpr,
    FunctionCall,
    IriConst,
    IsNull,
    ListLiteral,
    Literal,
    Property,
    Term,
    TypedLiteral,
    Unary,
    Variable,
)
from provisa.gql.ir import (
    AltPath,
    CypherAST,
    EdgeStep,
    MatchClause,
    MatchStep,
    NegatedEdges,
    NodePattern,
    OrderItem,
    PathExpr,
    PathMode,
    PathPattern,
    Predicate,
    RelPattern,
    RepeatPath,
    ReturnClause,
    ReturnItem,
    SeqPath,
    TriplePattern,
    TripleTemplate,
    WhereClause,
)
from provisa.gql.iri import parse_subject_iri
from provisa.gql.translator_types import CypherTranslateError
from provisa.sparql_query.vocabulary import RDF_TYPE, XSD, ColumnPredicate, Vocabulary

_ORDER_PREFIX = "__ord"


class SparqlRefused(CypherTranslateError):
    """A SPARQL query the front end does not lower; the message names what."""


@dataclass
class CompiledSparql:
    """The IR of one SPARQL query. ``form`` is SELECT, ASK or CONSTRUCT; ``variables`` are a
    SELECT's projected variables, in order (its answer's other columns are not part of it).
    ``empty`` is a query no interpretation can match: it is answered with no solution, unrun."""

    form: str
    ast: CypherAST | None
    variables: list[str]
    empty: bool = False
    #: The one solution an empty query still has: an aggregate over no solutions (COUNT is 0).
    empty_row: dict[str, Any] | None = None


# --- groups ------------------------------------------------------------------------------------


@dataclass
class _Group:
    """A basic graph pattern with what applies to it."""

    triples: list[tuple[Any, Any, Any]] = field(default_factory=list)
    filters: list[Any] = field(default_factory=list)
    binds: list[tuple[str, Any]] = field(default_factory=list)
    optionals: list["_Group"] = field(default_factory=list)

    def joined(self, other: "_Group") -> "_Group":
        return _Group(
            [*self.triples, *other.triples],
            [*self.filters, *other.filters],
            [*self.binds, *other.binds],
            [*self.optionals, *other.optionals],
        )


def _alternatives(p: CompValue) -> list[_Group]:
    """``p`` as the groups whose solutions together are its own (a UNION's sides apart)."""
    name = p.name
    if name == "BGP":
        return [_Group(triples=list(p.triples))]
    if name == "Join":
        return [a.joined(b) for a in _alternatives(p.p1) for b in _alternatives(p.p2)]
    if name == "Union":
        return [*_alternatives(p.p1), *_alternatives(p.p2)]
    if name == "Filter":
        groups = _alternatives(p.p)
        for g in groups:
            g.filters.append(p.expr)
        return groups
    if name == "Extend":
        groups = _alternatives(p.p)
        for g in groups:
            g.binds.append((str(p.var), p.expr))
        return groups
    if name == "LeftJoin":
        optional = _alternatives(p.p2)
        if len(optional) != 1:
            raise SparqlRefused("An OPTIONAL holding a UNION is not supported")
        if p.expr is not None and getattr(p.expr, "name", None) != "TrueFilter":
            optional[0].filters.append(p.expr)
        groups = _alternatives(p.p1)
        for g in groups:
            g.optionals.append(optional[0])
        return groups
    if name in ("ToMultiSet", "values"):
        raise SparqlRefused("VALUES and sub-SELECTs are not supported")
    if name in ("Group", "AggregateJoin"):
        raise SparqlRefused("GROUP BY and aggregates are supported only at the top of a query")
    raise SparqlRefused(f"The SPARQL operator {name} is not supported")


@dataclass
class _Aggregation:
    """A query's GROUP BY: its keys (variables; empty for one group of every solution), its
    aggregates by the variable each result is, the projection's rebindings over them (in the
    order they apply) and its HAVING conditions."""

    keys: list[str]
    aggregates: dict[str, Any]
    rebinds: list[tuple[str, Any]]
    having: list[Any]


def _aggregation(p: CompValue) -> tuple[CompValue, _Aggregation | None]:
    """Peel a GROUP BY's rebindings and HAVING off ``p``: the pattern it groups, and it."""
    rebinds: list[tuple[str, Any]] = []
    having: list[Any] = []
    top = p
    while p.name in ("Extend", "Filter"):
        if p.name == "Extend":
            rebinds.append((str(p.var), p.expr))
        else:
            having.append(p.expr)
        p = p.p
    if p.name != "AggregateJoin":
        return top, None
    group = p.p
    keys = []
    for key in group.expr or []:
        if not isinstance(key, RdfVariable):
            raise SparqlRefused("GROUP BY groups by variables; bind an expression to one first")
        keys.append(str(key))
    aggregates = {str(a.res): a for a in p.A}
    return group.p, _Aggregation(keys, aggregates, list(reversed(rebinds)), having)


_AGGREGATE_NAMES = {
    "Aggregate_Count": "count",
    "Aggregate_Sum": "sum",
    "Aggregate_Avg": "avg",
    "Aggregate_Min": "min",
    "Aggregate_Max": "max",
    "Aggregate_Sample": "min",
    "Aggregate_GroupConcat": "group_concat",
}
_NUMERIC = {f"{XSD}{t}" for t in ("integer", "decimal", "double", "float")}


def _solution_modifiers(p: CompValue) -> tuple[CompValue, dict[str, Any]]:
    """Peel Slice, Distinct, Project and OrderBy off ``p``: the pattern under them, and them."""
    mods: dict[str, Any] = {}
    while p.name in ("Slice", "Distinct", "Reduced", "Project", "OrderBy"):
        if p.name == "Slice":
            mods["offset"] = p.start or None
            mods["limit"] = p.length
        elif p.name == "Distinct":
            mods["distinct"] = True
        elif p.name == "Project":
            mods["project"] = [str(v) for v in p.PV]
        elif p.name == "OrderBy":
            mods["order"] = list(p.expr)
        p = p.p
    return p, mods


# --- paths -------------------------------------------------------------------------------------


def _path_expr(path: Any, vocab: Vocabulary) -> PathExpr:
    if isinstance(path, URIRef):
        return EdgeStep(_edge_type(str(path), vocab))
    if isinstance(path, InvPath):
        return _inverse(_path_expr(path.arg, vocab))
    if isinstance(path, SequencePath):
        return SeqPath(tuple(_path_expr(a, vocab) for a in path.args))
    if isinstance(path, AlternativePath):
        return AltPath(tuple(_path_expr(a, vocab) for a in path.args))
    if isinstance(path, MulPath):
        lo, hi = {"*": (0, None), "+": (1, None), "?": (0, 1)}[path.mod]
        return RepeatPath(_path_expr(path.path, vocab), lo, hi)
    if isinstance(path, NegatedPath):
        forward = [a for a in path.args if isinstance(a, URIRef)]
        backward = [a.arg for a in path.args if isinstance(a, InvPath)]
        if forward and backward:
            raise SparqlRefused("A negated set of both forward and inverse edges is not supported")
        names = tuple(
            _edge_type(str(a), vocab) for a in (forward or backward) if str(a) in vocab.edges
        )
        return NegatedEdges(names, inverse=bool(backward))
    raise SparqlRefused(f"The property path {path} is not supported")


def _edge_type(iri: str, vocab: Vocabulary) -> str:
    edges = vocab.edges.get(iri)
    if not edges:
        raise SparqlRefused(f"<{iri}> is no edge predicate, and a property path walks edges")
    return edges[0].rel_type


def _inverse(path: PathExpr) -> PathExpr:
    if isinstance(path, EdgeStep):
        return EdgeStep(path.rel_type, not path.inverse)
    if isinstance(path, NegatedEdges):
        return NegatedEdges(path.rel_types, not path.inverse)
    if isinstance(path, SeqPath):
        return SeqPath(tuple(_inverse(p) for p in reversed(path.parts)))
    if isinstance(path, AltPath):
        return AltPath(tuple(_inverse(p) for p in path.options))
    return RepeatPath(_inverse(path.body), path.min, path.max)


def _ends(path: PathExpr, vocab: Vocabulary) -> tuple[set[str], set[str]]:
    """The labels a path can start and end at."""
    if isinstance(path, EdgeStep | NegatedEdges):
        every = [ep for eps in vocab.edges.values() for ep in eps]
        if isinstance(path, EdgeStep):
            eps = [ep for ep in every if ep.rel_type == path.rel_type]
        else:
            eps = [ep for ep in every if ep.rel_type not in path.rel_types]
        starts, ends = {ep.source for ep in eps}, {ep.target for ep in eps}
        return (ends, starts) if path.inverse else (starts, ends)
    if isinstance(path, SeqPath):
        return _ends(path.parts[0], vocab)[0], _ends(path.parts[-1], vocab)[1]
    if isinstance(path, AltPath):
        pairs = [_ends(o, vocab) for o in path.options]
        return set().union(*(a for a, _ in pairs)), set().union(*(b for _, b in pairs))
    starts, ends = _ends(path.body, vocab)
    if path.min == 0:
        return starts | ends, starts | ends
    return starts, ends


# --- the compiler ------------------------------------------------------------------------------


def _is_var(t: Any) -> bool:
    return isinstance(t, RdfVariable)


class _Lowering:
    """One SPARQL query's lowering against ``vocab``."""

    def __init__(self, vocab: Vocabulary, lm: Any) -> None:
        self.vocab = vocab
        self._lm = lm
        self._placed: set[str] = set()

    # -- node labels ----------------------------------------------------------------------

    def _subject_node(self, term: Any, named: dict[str, str], keys: dict[str, tuple]) -> str:
        """The node variable a subject or object position names; an IRI gets its own."""
        if _is_var(term):
            return str(term)
        if isinstance(term, BNode):
            return named.setdefault(str(term), f"_b{len(named)}")
        if isinstance(term, URIRef):
            # Named by its place among the group's constants, so both passes over the group
            # (its interpretations, then its MATCH) name it alike.
            iri = str(term)
            if iri not in named:
                named[iri] = f"_n{len(named)}"
                keys[named[iri]] = (iri,)
            return named[iri]
        raise SparqlRefused(f"A literal {term} can't be a subject")

    def interpretations(self, group: _Group, outer: dict[str, str]) -> list[dict[str, str]]:
        """Every assignment of one label to each node variable of ``group`` its triples allow.
        ``outer`` holds the labels of variables already assigned (an OPTIONAL's enclosing group)."""
        allowed: dict[str, set[str] | None] = {}
        named: dict[str, str] = {}
        keys: dict[str, tuple] = {}

        def allow(var: str, labels: set[str] | None) -> None:
            current = allowed.get(var)
            if labels is None:
                allowed.setdefault(var, None)
                return
            allowed[var] = labels if current is None else current & labels

        constraints: list[tuple[str, str, str]] = []  # (kind, s, o/iri)
        for s, p, o in group.triples:
            sv = self._subject_node(s, named, keys)
            if isinstance(p, Path):
                path = _path_expr(p, self.vocab)
                starts, ends = _ends(path, self.vocab)
                allow(sv, starts)
                allow(self._subject_node(o, named, keys), ends)
                continue
            if _is_var(p):
                allow(sv, None)
                continue
            iri = str(p)
            if iri == RDF_TYPE:
                if _is_var(o):
                    allow(sv, None)
                else:
                    allow(sv, set(self.vocab.labels_of_class(str(o))))
                continue
            if iri in self.vocab.columns:
                allow(sv, {cp.label for cp in self.vocab.columns[iri]})
                continue
            if iri in self.vocab.edges:
                ov = self._subject_node(o, named, keys)
                allow(sv, {ep.source for ep in self.vocab.edges[iri]})
                allow(ov, {ep.target for ep in self.vocab.edges[iri]})
                constraints.append(("edge", sv, iri))
                constraints.append(("edge-target", ov, iri))
                continue
            allow(sv, set())  # a predicate the vocabulary does not have: no solution
        for var, iri_tuple in keys.items():
            ref = parse_subject_iri(self.vocab.base, iri_tuple[0])
            labels = {
                label
                for label in self.vocab.classes_of
                if ref is not None and self._is_table(label, ref.domain, ref.table)
            }
            allow(var, labels)
        everything = set(self.vocab.classes_of)
        options = []
        names = []
        for var, labels in allowed.items():
            if var in outer:
                labels = {outer[var]} if labels is None or outer[var] in labels else set()
            names.append(var)
            options.append(sorted(everything if labels is None else labels))
        found = []
        for combo in itertools.product(*options):
            assignment = dict(zip(names, combo))
            if self._holds(group, assignment, named):
                found.append(assignment)
        return found

    def _is_table(self, label: str, domain: str, table: str) -> bool:
        nm = self._node(label)
        return nm.domain_id == domain and nm.table_name == table

    def _node(self, label: str) -> Any:
        return self._lm.nodes[label]

    def _holds(self, group: _Group, labels: dict[str, str], named: dict[str, str]) -> bool:
        """Whether every edge triple of ``group`` joins its two labels as assigned."""
        for s, p, o in group.triples:
            if isinstance(p, Path) or _is_var(p) or str(p) not in self.vocab.edges:
                continue
            sv = str(s) if _is_var(s) else named.get(str(s), "")
            ov = str(o) if _is_var(o) else named.get(str(o), "")
            if not any(
                ep.source == labels.get(sv) and ep.target == labels.get(ov)
                for ep in self.vocab.edges[str(p)]
            ):
                return False
        return True

    # -- one group, one interpretation ----------------------------------------------------

    def lower_group(
        self,
        group: _Group,
        labels: dict[str, str],
        bindings: dict[str, tuple[CypherExpr, str | None]],
        optional: bool,
    ) -> MatchStep:
        """The MATCH of ``group`` with its nodes labelled ``labels``. ``bindings`` gains each
        variable the group binds to a value: (its expression, its datatype)."""
        named: dict[str, str] = {}
        keys: dict[str, tuple] = {}
        conditions: list[CypherExpr] = []
        edges: list[tuple[str, RelPattern, str]] = []
        triples: list[TriplePattern] = []
        nodes: list[str] = []

        def node(term: Any) -> str:
            var = self._subject_node(term, named, keys)
            if var not in nodes:
                nodes.append(var)
            return var

        for s, p, o in group.triples:
            sv = node(s)
            if isinstance(p, Path):
                path = _path_expr(p, self.vocab)
                rel = RelPattern(None, [], None, None, "right", path=path, mode=PathMode.REACH)
                edges.append((sv, rel, node(o)))
                continue
            if _is_var(p):
                if not _is_var(o):
                    raise SparqlRefused(
                        "A variable predicate with a constant object is not supported"
                    )
                triples.append(TriplePattern(sv, str(p), str(o), self._predicates_of(labels[sv])))
                continue
            iri = str(p)
            if iri == RDF_TYPE:
                if _is_var(o):
                    bindings.setdefault(
                        str(o), (IriConst(self.vocab.classes_of[labels[sv]][0]), None)
                    )
                continue
            if iri in self.vocab.edges:
                rel_type = self.vocab.edges[iri][0].rel_type
                edges.append((sv, RelPattern(None, [rel_type], None, None, "right"), node(o)))
                continue
            cp = self._column(iri, labels[sv])
            value: CypherExpr = Property(Variable(sv), cp.prop)
            if _is_var(o):
                if str(o) in bindings:
                    conditions.append(Binary("=", value, bindings[str(o)][0]))
                else:
                    bindings[str(o)] = (value, cp.datatype)
                    if not optional:
                        conditions.append(IsNull(value, negated=True))
            else:
                conditions.append(Binary("=", value, _literal(o)))

        for var, (iri,) in keys.items():
            conditions.extend(self._key_conditions(var, labels[var], iri))
        for var, expr in group.binds:
            bindings[var] = (self.expr(expr, bindings, labels), None)
        for f in group.filters:
            conditions.append(self.expr(f, bindings, labels))

        matches = self._clauses(nodes, edges, labels, optional)
        matches.extend(MatchClause(t, None, optional=optional) for t in triples)
        where = None
        if conditions:
            cond = conditions[0]
            for c in conditions[1:]:
                cond = Binary("AND", cond, c)
            where = WhereClause(cond)
        return MatchStep(matches, where)

    def _clauses(
        self,
        nodes: list[str],
        edges: list[tuple[str, RelPattern, str]],
        labels: dict[str, str],
        optional: bool,
    ) -> list[MatchClause]:
        """The group's patterns, each joining the statement through a node already in it."""

        def n(var: str) -> NodePattern:
            return NodePattern(var, [labels[var]], {})

        placed: set[str] = set(self._placed)
        clauses: list[MatchClause] = []
        pending = list(edges)
        while pending:
            # The next pattern is one that joins through a node already placed, when one does.
            i = next((i for i, (s, _, o) in enumerate(pending) if s in placed or o in placed), 0)
            s, rel, o = pending.pop(i)
            if o in placed and s not in placed:
                # Written from the placed end: an edge the other way round, a path inverted.
                if rel.path is not None:
                    back = RelPattern(
                        None, [], None, None, "right", path=_inverse(rel.path), mode=rel.mode
                    )
                else:
                    back = RelPattern(None, rel.types, None, None, "left")
                pattern = PathPattern([n(o), n(s)], [back])
            else:
                pattern = PathPattern([n(s), n(o)], [rel])
            clauses.append(MatchClause(pattern, None, optional=optional))
            placed.update((s, o))
        for var in nodes:
            if var not in placed:
                clauses.append(MatchClause(PathPattern([n(var)], []), None, optional=optional))
                placed.add(var)
        self._placed |= placed
        return clauses

    def _column(self, iri: str, label: str) -> ColumnPredicate:
        for cp in self.vocab.columns.get(iri, []):
            if cp.label == label:
                return cp
        raise SparqlRefused(f"<{iri}> is no predicate of {label}")

    def _predicates_of(self, label: str) -> tuple[Predicate, ...]:
        out = [Predicate(RDF_TYPE, constant=c) for c in self.vocab.classes_of[label]]
        out += [
            Predicate(iri, property=cp.prop, datatype=cp.datatype)
            for iri, cp in self.vocab.columns_of.get(label, [])
        ]
        out += [
            Predicate(iri, rel_type=ep.rel_type) for iri, ep in self.vocab.edges_of.get(label, [])
        ]
        return tuple(out)

    def _key_conditions(self, var: str, label: str, iri: str) -> list[CypherExpr]:
        """``var`` is the row its IRI names: its key columns equal the key values."""
        ref = parse_subject_iri(self.vocab.base, iri)
        nm = self._node(label)
        assert ref is not None  # the interpretation only labels it with the table it names
        key_columns = nm.pk_columns or [nm.id_column]
        if len(key_columns) != len(ref.keys):
            return [Literal(False, "boolean")]
        by_column = {col: prop for prop, col in nm.properties.items()}
        out: list[CypherExpr] = []
        for col, value in zip(key_columns, ref.keys, strict=True):
            prop = by_column.get(col)
            if prop is None:
                raise SparqlRefused(f"{nm.table_name}'s key column {col} is not readable")
            datatype = next(
                (cp.datatype for _, cp in self.vocab.columns_of[label] if cp.prop == prop), None
            )
            out.append(Binary("=", Property(Variable(var), prop), _typed(value, datatype)))
        return out

    # -- expressions ----------------------------------------------------------------------

    def expr(
        self,
        e: Any,
        bindings: dict[str, tuple[CypherExpr, str | None]],
        labels: dict[str, str],
    ) -> CypherExpr:
        if isinstance(e, RdfVariable):
            name = str(e)
            if name in bindings:
                return bindings[name][0]
            if name in labels:
                raise SparqlRefused(
                    f"?{name} is a subject; compare it with an IRI, or read one of its properties"
                )
            return Literal(None, "null")
        if isinstance(e, RdfLiteral):
            return _literal(e)
        if isinstance(e, URIRef):
            return IriConst(str(e))
        name = e.name
        if name == "RelationalExpression":
            return self._relational(e, bindings, labels)
        if name in ("ConditionalAndExpression", "ConditionalOrExpression"):
            op = "AND" if name == "ConditionalAndExpression" else "OR"
            out = self.expr(e.expr, bindings, labels)
            for other in e.other or []:
                out = Binary(op, out, self.expr(other, bindings, labels))
            return out
        if name == "UnaryNot":
            return Unary("NOT", self.expr(e.expr, bindings, labels))
        if name == "UnaryMinus":
            return Unary("-", self.expr(e.expr, bindings, labels))
        if name == "UnaryPlus":
            return self.expr(e.expr, bindings, labels)
        if name in ("AdditiveExpression", "MultiplicativeExpression"):
            out = self.expr(e.expr, bindings, labels)
            for op, other in zip(e.op or [], e.other or [], strict=True):
                out = Binary(op, out, self.expr(other, bindings, labels))
            return out
        return self._builtin(e, bindings, labels)

    def _relational(self, e: Any, bindings, labels) -> CypherExpr:
        op: str = {"!=": "<>"}.get(str(e.op), str(e.op))
        left_var = str(e.expr) if isinstance(e.expr, RdfVariable) else None
        right_var = str(e.other) if isinstance(e.other, RdfVariable) else None
        if left_var in labels and right_var in labels and op in ("=", "<>"):
            assert left_var is not None and right_var is not None
            cond = self._same_node(left_var, right_var, labels)
            return cond if op == "=" else Unary("NOT", cond)
        if (
            left_var is not None
            and left_var in labels
            and isinstance(e.other, URIRef)
            and op in ("=", "<>")
        ):
            cond = self._iri_match(left_var, str(e.other), labels)
            return cond if op == "=" else Unary("NOT", cond)
        if op in ("IN", "NOT IN"):
            items = ListLiteral([self.expr(x, bindings, labels) for x in e.other])
            cond = Binary("IN", self.expr(e.expr, bindings, labels), items)
            return cond if op == "IN" else Unary("NOT", cond)
        return Binary(op, self.expr(e.expr, bindings, labels), self.expr(e.other, bindings, labels))

    def _same_node(self, a: str, b: str, labels: dict[str, str]) -> CypherExpr:
        """Whether two subjects are the same: rows of one table with equal keys."""
        if labels[a] != labels[b]:
            return Literal(False, "boolean")
        nm = self._node(labels[a])
        by_column = {col: prop for prop, col in nm.properties.items()}
        out: CypherExpr | None = None
        for col in nm.pk_columns or [nm.id_column]:
            prop = by_column.get(col)
            if prop is None:
                raise SparqlRefused(f"{nm.table_name}'s key column {col} is not readable")
            eq = Binary("=", Property(Variable(a), prop), Property(Variable(b), prop))
            out = eq if out is None else Binary("AND", out, eq)
        assert out is not None
        return out

    def _iri_match(self, var: str, iri: str, labels: dict[str, str]) -> CypherExpr:
        ref = parse_subject_iri(self.vocab.base, iri)
        if ref is None or not self._is_table(labels[var], ref.domain, ref.table):
            return Literal(False, "boolean")
        conds = self._key_conditions(var, labels[var], iri)
        out = conds[0]
        for c in conds[1:]:
            out = Binary("AND", out, c)
        return out

    def _builtin(self, e: Any, bindings, labels) -> CypherExpr:
        name = e.name
        args = lambda *keys: [self.expr(getattr(e, k), bindings, labels) for k in keys]  # noqa: E731
        if name == "Builtin_BOUND":
            var = str(e.arg)
            if var in labels:
                return Literal(True, "boolean")
            return IsNull(self.expr(e.arg, bindings, labels), negated=True)
        if name == "Builtin_REGEX":
            if e.flags is not None:
                raise SparqlRefused("REGEX flags are not supported")
            text, pattern = args("text", "pattern")
            return Binary("=~", text, pattern)
        if name == "Builtin_CONTAINS":
            return Binary("CONTAINS", *args("arg1", "arg2"))
        if name == "Builtin_STRSTARTS":
            return Binary("STARTS WITH", *args("arg1", "arg2"))
        if name == "Builtin_STRENDS":
            return Binary("ENDS WITH", *args("arg1", "arg2"))
        if name == "Builtin_LCASE":
            return FunctionCall("toLower", args("arg"))
        if name == "Builtin_UCASE":
            return FunctionCall("toUpper", args("arg"))
        if name == "Builtin_STR":
            return FunctionCall("toString", args("arg"))
        if name == "Builtin_ABS":
            return FunctionCall("abs", args("arg"))
        if name == "Builtin_COALESCE":
            return FunctionCall("coalesce", [self.expr(a, bindings, labels) for a in e.arg])
        if name == "Builtin_IF":
            cond, then, other = args("arg1", "arg2", "arg3")
            return Case(None, [(cond, then)], other)
        if name == "Builtin_sameTerm":
            return Binary("=", *args("arg1", "arg2"))
        if name in ("Builtin_isIRI", "Builtin_isURI", "Builtin_isLITERAL", "Builtin_isBLANK"):
            is_node = isinstance(e.arg, RdfVariable) and str(e.arg) in labels
            if name == "Builtin_isBLANK":
                return Literal(False, "boolean")
            wants_node = name != "Builtin_isLITERAL"
            if isinstance(e.arg, RdfVariable) and str(e.arg) not in labels:
                bound = bindings.get(str(e.arg))
                if bound is not None and isinstance(bound[0], IriConst):
                    is_node = True
            return Literal(is_node == wants_node, "boolean")
        raise SparqlRefused(f"The SPARQL function {name.removeprefix('Builtin_')} is not supported")

    # -- the statement --------------------------------------------------------------------

    def _aggregate(self, a: Any, bindings, labels) -> tuple[CypherExpr, str | None]:
        """One SPARQL aggregate as the IR's call, with the datatype of its result."""
        name = _AGGREGATE_NAMES.get(a.name)
        if name is None:
            raise SparqlRefused(
                f"The aggregate {a.name.removeprefix('Aggregate_')} is not supported"
            )
        distinct = a.get("distinct") == "DISTINCT"
        if str(a.vars) == "*":
            return FunctionCall("count", [], distinct=distinct, star=True), f"{XSD}integer"
        operand = a.vars
        var = str(operand) if isinstance(operand, RdfVariable) else None
        if var is not None and var in labels:
            if name not in ("count",):
                raise SparqlRefused(
                    f"?{var} is a subject; COUNT it, or aggregate one of its properties"
                )
            arg: CypherExpr = self._key_of(var, labels)
            datatype = None
        else:
            arg = self.expr(operand, bindings, labels)
            datatype = bindings[var][1] if var is not None and var in bindings else None
        if name == "count":
            return FunctionCall("count", [arg], distinct=distinct), f"{XSD}integer"
        if name == "group_concat":
            separator = a.get("separator")
            sep = Literal(" " if separator is None else str(separator), "string")
            return FunctionCall("group_concat", [arg, sep], distinct=distinct), f"{XSD}string"
        if name == "avg":
            return FunctionCall("avg", [arg], distinct=distinct), (
                f"{XSD}double" if datatype in (f"{XSD}double", f"{XSD}float") else f"{XSD}decimal"
            )
        if name == "sum":
            kept = datatype if datatype in _NUMERIC else f"{XSD}decimal"
            return FunctionCall("sum", [arg], distinct=distinct), kept
        return FunctionCall(name, [arg], distinct=distinct), datatype

    def _key_of(self, var: str, labels: dict[str, str]) -> CypherExpr:
        """A subject's first key column, which tells one subject of its table from another."""
        nm = self._node(labels[var])
        by_column = {col: prop for prop, col in nm.properties.items()}
        column = (nm.pk_columns or [nm.id_column])[0]
        if column not in by_column:
            raise SparqlRefused(f"{nm.table_name}'s key column {column} is not readable")
        return Property(Variable(var), by_column[column])

    def _group_keys(self, keys: list[str], bindings, labels) -> list[CypherExpr]:
        out: list[CypherExpr] = []
        for key in keys:
            if key in labels:
                nm = self._node(labels[key])
                by_column = {col: prop for prop, col in nm.properties.items()}
                for column in nm.pk_columns or [nm.id_column]:
                    if column not in by_column:
                        raise SparqlRefused(
                            f"{nm.table_name}'s key column {column} is not readable"
                        )
                    out.append(Property(Variable(key), by_column[column]))
            elif key in bindings:
                out.append(bindings[key][0])
            else:
                raise SparqlRefused(f"GROUP BY ?{key}: the pattern binds no ?{key}")
        return out

    def statement(
        self,
        group: _Group,
        labels: dict[str, str],
        mods: dict,
        form: str,
        template,
        aggregation: "_Aggregation | None" = None,
    ):
        """One UNION ALL part: ``group`` under one interpretation, answered as ``form``."""
        self._placed = set()
        bindings: dict[str, tuple[CypherExpr, str | None]] = {}
        steps = [self.lower_group(group, labels, bindings, optional=False)]
        for opt in group.optionals:
            opt_labels = self.interpretations(opt, labels)
            if len(opt_labels) != 1:
                raise SparqlRefused(
                    "An OPTIONAL whose nodes could be of several classes is not supported; "
                    "give them a class"
                )
            steps.append(
                self.lower_group(opt, {**labels, **opt_labels[0]}, bindings, optional=True)
            )
            labels = {**labels, **opt_labels[0]}

        group_keys: list[CypherExpr] | None = None
        having: CypherExpr | None = None
        if aggregation is not None:
            group_keys = self._group_keys(aggregation.keys, bindings, labels)
            for res, a in aggregation.aggregates.items():
                # A SAMPLE of a key is the key itself: the projection's way of naming it.
                if a.name == "Aggregate_Sample" and str(a.vars) in aggregation.keys:
                    continue
                bindings[res] = self._aggregate(a, bindings, labels)
            for var, expr in aggregation.rebinds:
                source = (
                    aggregation.aggregates.get(str(expr)) if isinstance(expr, RdfVariable) else None
                )
                if (
                    source is not None
                    and source.name == "Aggregate_Sample"
                    and str(source.vars) == var
                ):
                    continue
                dt = (
                    bindings[str(expr)][1]
                    if isinstance(expr, RdfVariable) and str(expr) in bindings
                    else None
                )
                bindings[var] = (self.expr(expr, bindings, labels), dt)
            conds = [self.expr(h, bindings, labels) for h in aggregation.having]
            for c in conds:
                having = c if having is None else Binary("AND", having, c)
            # A subject that is not a key is no value of the group.
            labels = {v: lab for v, lab in labels.items() if v in aggregation.keys}

        def term(var: str) -> CypherExpr:
            if var in labels:
                return Term(Variable(var))
            if var in self._term_vars(steps):
                return Variable(var)
            bound = bindings.get(var)
            if bound is None:
                return Term(Literal(None, "null"))
            return Term(bound[0], datatype=bound[1])

        ast = CypherAST(
            pipeline=list(steps), return_clause=None, order_by=[], skip=None, limit=None
        )
        if form == "CONSTRUCT":
            ast.construct = [
                TripleTemplate(
                    *(self._template_term(x, term) for x in triple)  # type: ignore[arg-type]
                )
                for triple in template
            ]
            return ast
        if form == "ASK":
            ast.return_clause = ReturnClause([ReturnItem(Literal(1, "number"), "ask", name="ask")])
            return ast
        items = [ReturnItem(term(v), v, name=v) for v in mods["project"]]
        for i, cond in enumerate(mods.get("order", [])):
            items.append(
                ReturnItem(self.expr(cond.expr, bindings, labels), f"{_ORDER_PREFIX}{i}", name="")
            )
        ast.return_clause = ReturnClause(
            items, distinct=bool(mods.get("distinct")), group_by=group_keys, having=having
        )
        return ast

    def _template_term(self, x: Any, term) -> CypherExpr:
        if isinstance(x, RdfVariable):
            return term(str(x))
        if isinstance(x, URIRef):
            return Term(IriConst(str(x)))
        if isinstance(x, RdfLiteral):
            return Term(
                _literal(x), datatype=str(x.datatype) if x.datatype else None, lang=x.language
            )
        raise SparqlRefused("A blank node in a CONSTRUCT template is not supported")

    @staticmethod
    def _term_vars(steps: list[MatchStep]) -> set[str]:
        return {
            v
            for s in steps
            for m in s.matches
            if isinstance(m.pattern, TriplePattern)
            for v in (m.pattern.predicate, m.pattern.object)
        }


def _literal(lit: Any) -> CypherExpr:
    """An RDF literal as the IR's value of it."""
    if lit.language is not None:
        return TypedLiteral(str(lit), None, lit.language)
    dt = str(lit.datatype) if lit.datatype is not None else None
    if dt is None or dt == f"{XSD}string":
        return Literal(str(lit), "string")
    if dt == f"{XSD}boolean":
        return Literal(bool(lit.toPython()), "boolean")
    return TypedLiteral(str(lit), dt)


def _typed(value: str, datatype: str | None) -> CypherExpr:
    """A key value from an IRI as the value its column holds."""
    if datatype is None or datatype == f"{XSD}string":
        return Literal(value, "string")
    return TypedLiteral(value, datatype)


def compile_sparql(text: str, vocab: Vocabulary, lm: Any) -> CompiledSparql:
    """``text``, a SPARQL SELECT, ASK or CONSTRUCT, as the graph IR, read against ``vocab``
    (built from the label map ``lm``)."""
    try:
        query = translateQuery(parseQuery(text))
    except Exception as exc:  # rdflib raises pyparsing's and its own errors alike
        raise SparqlRefused(f"The query is not SPARQL this endpoint reads: {exc}") from exc
    algebra = query.algebra
    form = {"SelectQuery": "SELECT", "AskQuery": "ASK", "ConstructQuery": "CONSTRUCT"}.get(
        algebra.name
    )
    if form is None:
        raise SparqlRefused(f"A {algebra.name} is not supported; SELECT, ASK and CONSTRUCT are")
    if query.prologue is not None and getattr(algebra, "datasetClause", None):
        raise SparqlRefused("FROM and FROM NAMED are not supported")
    pattern, mods = _solution_modifiers(algebra.p)
    pattern, aggregation = _aggregation(pattern)
    if aggregation is not None and form != "SELECT":
        raise SparqlRefused(f"GROUP BY and aggregates in a {form} are not supported")
    if form == "SELECT" and "project" not in mods:
        mods["project"] = [str(v) for v in algebra.PV]
    template = list(algebra.template) if form == "CONSTRUCT" else None

    lowering = _Lowering(vocab, lm)
    parts: list[CypherAST] = []
    for group in _alternatives(pattern):
        for labels in lowering.interpretations(group, {}):
            parts.append(lowering.statement(group, labels, mods, form, template, aggregation))
    if aggregation is not None and len(parts) > 1:
        raise SparqlRefused(
            "An aggregate over subjects of several classes (or a UNION) is not supported yet; "
            "give the subjects one class"
        )
    limit = settings_registry.value("graph.union_limit")
    if len(parts) > limit:
        raise SparqlRefused(
            f"The query matches {len(parts)} combinations of classes, past the graph union limit "
            f"of {limit}; give its subjects classes, or raise the limit (graph.union_limit)"
        )
    variables = mods.get("project", []) if form == "SELECT" else []
    if not parts:
        empty_row = None
        if aggregation is not None and not aggregation.keys:
            # One group of no solutions: its COUNTs are 0, a GROUP_CONCAT the empty string, and
            # every other aggregate unbound -- answered without running anything.
            empty_row = {}
            for var, expr in aggregation.rebinds:
                a = aggregation.aggregates.get(str(expr)) if isinstance(expr, RdfVariable) else None
                if a is not None and a.name == "Aggregate_Count":
                    empty_row[var] = {"literal": 0, "datatype": f"{XSD}integer"}
                elif a is not None and a.name == "Aggregate_GroupConcat":
                    empty_row[var] = {"literal": "", "datatype": f"{XSD}string"}
        return CompiledSparql(form, None, variables, empty=True, empty_row=empty_row)
    head = parts[0]
    head.union_parts = [(p, True) for p in parts[1:]]
    head.order_by = [
        OrderItem(Variable(f"{_ORDER_PREFIX}{i}"), "DESC" if c.order == "DESC" else "ASC")
        for i, c in enumerate(mods.get("order", []))
    ]
    head.skip = mods.get("offset")
    head.limit = 1 if form == "ASK" else mods.get("limit")
    head.modifiers_of_whole = True
    return CompiledSparql(form, head, variables)
