# Copyright (c) 2026 Kenneth Stott
# Canary: 16571769-20f7-4aab-8a21-90b35dab49e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The graph query IR (REQ-2135): what a graph language front end parses a statement into, and what
the back end in this package lowers to semantic SQL.

Today Cypher's front end (provisa.cypher.parser) is the one that produces it. A clause's
expressions (WHERE, RETURN, WITH, UNWIND, ORDER BY) are expression trees (expr_ast), parsed by the
front end; the back end walks the trees and reads no language's syntax.
"""

# Requirements: REQ-2135, REQ-347, REQ-348, REQ-571

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from provisa.gql.expr_ast import CypherExpr


@dataclass(frozen=True)
class LabelName:
    """A label expression naming one label (GQL ``:A``)."""

    name: str


@dataclass(frozen=True)
class LabelOr:
    """A node with any of these labels (GQL ``:A|B``)."""

    options: tuple[LabelName, ...]


@dataclass(frozen=True)
class LabelAnd:
    """A node with every one of these labels (GQL ``:A&B``, Cypher ``:A:B``)."""

    parts: tuple[LabelName, ...]


@dataclass(frozen=True)
class LabelAny:
    """A node with any label at all (GQL ``%``, an unlabelled node)."""


LabelExpr = LabelName | LabelOr | LabelAnd | LabelAny


@dataclass
class NodePattern:
    variable: str | None
    labels: list[str]
    properties: dict[str, Any]
    label_alternation: bool = False
    #: Each inline property's value as the statement writes it (``'x'``, ``42``, ``$p``), which a
    #: read matches on as ``WHERE var.prop = <value>`` (:func:`with_inline_conditions`).
    property_text: dict[str, str] = field(default_factory=dict)

    @property
    def label_expr(self) -> LabelExpr:
        """The node's labels as a label expression. ``labels`` and ``label_alternation`` hold it:
        the label expressions the back end lowers are one level deep (REQ-2135)."""
        if not self.labels:
            return LabelAny()
        if len(self.labels) == 1:
            return LabelName(self.labels[0])
        names = tuple(LabelName(n) for n in self.labels)
        return LabelOr(names) if self.label_alternation else LabelAnd(names)

    @classmethod
    def with_label_expr(
        cls, variable: str | None, expr: LabelExpr, properties: dict[str, Any] | None = None
    ) -> "NodePattern":
        """A node pattern from a label expression; a front end builds one this way."""
        props = properties or {}
        if isinstance(expr, LabelAny):
            return cls(variable, [], props)
        if isinstance(expr, LabelName):
            return cls(variable, [expr.name], props)
        if isinstance(expr, LabelOr):
            return cls(variable, [o.name for o in expr.options], props, label_alternation=True)
        return cls(variable, [p.name for p in expr.parts], props)


class PathMode(str, Enum):
    """What a quantified path may repeat (GQL's path modes, REQ-2135). WALK repeats nodes and
    edges; REACH is SPARQL's property-path meaning, each pair of ends once however many paths join
    them. TRAIL, ACYCLIC and SIMPLE are GQL's and are refused until a front end needs them."""

    WALK = "WALK"
    TRAIL = "TRAIL"
    ACYCLIC = "ACYCLIC"
    SIMPLE = "SIMPLE"
    REACH = "REACH"


@dataclass(frozen=True)
class EdgeStep:
    """One edge of a relationship type; ``inverse`` walks it target to source (SPARQL ``^p``)."""

    rel_type: str
    inverse: bool = False


@dataclass(frozen=True)
class SeqPath:
    """Each part after the one before (SPARQL ``p/q``)."""

    parts: tuple["PathExpr", ...]


@dataclass(frozen=True)
class AltPath:
    """Any one of the options (SPARQL ``p|q``)."""

    options: tuple["PathExpr", ...]


@dataclass(frozen=True)
class RepeatPath:
    """The body ``min`` to ``max`` times (GQL ``{m,n}``; SPARQL ``p*``, ``p+``, ``p?``). With no
    ``max`` it repeats up to the operator's depth limit, and a result cut there says so."""

    body: "PathExpr"
    min: int
    max: int | None


@dataclass(frozen=True)
class NegatedEdges:
    """One edge of any relationship type but these (SPARQL ``!(p|q)``); ``inverse`` as EdgeStep."""

    rel_types: tuple[str, ...]
    inverse: bool = False


PathExpr = EdgeStep | SeqPath | AltPath | RepeatPath | NegatedEdges


@dataclass
class RelPattern:
    variable: str | None
    types: list[str]
    min_hops: int | None  # None = exactly 1 unless variable-length
    max_hops: int | None
    direction: str  # "right" | "left" | "none"
    variable_length: bool = False
    #: An inline property map on the relationship, as its node's is (``property_text``).
    properties: dict[str, Any] = field(default_factory=dict)
    property_text: dict[str, str] = field(default_factory=dict)
    #: A path expression between the two nodes in place of ``types`` and the hop bounds (REQ-2135):
    #: what a SPARQL property path or a GQL quantified path pattern parses into.
    path: PathExpr | None = None
    mode: PathMode = PathMode.WALK


@dataclass
class PathPattern:
    nodes: list[NodePattern]
    rels: list[RelPattern]


@dataclass
class PathFunction:
    func_name: str  # "shortestPath" | "allShortestPaths"
    pattern: PathPattern


@dataclass(frozen=True)
class Predicate:
    """One mapped predicate a variable predicate may stand for (REQ-2135): ``iri`` names it in the
    answer, and it reads a property of the subject's label (``property``, a literal of
    ``datatype``), an edge of ``rel_type`` from it (its object the edge's target node), or holds
    for every subject of the label with one object, the IRI ``constant`` (``rdf:type`` a class)."""

    iri: str
    property: str | None = None
    rel_type: str | None = None
    datatype: str | None = None
    constant: str | None = None


@dataclass
class TriplePattern:
    """``subject ?predicate ?object`` (SPARQL's ``?s ?p ?o``): a compile-time union over the
    mapped predicates the front end found for the subject's label, bounded by graph.union_limit.
    ``subject`` is a node variable a node pattern binds earlier in the statement; ``predicate`` and
    ``object`` are the variables the triple binds, each an RDF term."""

    subject: str
    predicate: str
    object: str
    candidates: tuple[Predicate, ...]


@dataclass
class TripleTemplate:
    """One triple of a CONSTRUCT's template: three expressions, each an RDF term of the answer."""

    subject: CypherExpr
    predicate: CypherExpr
    object: CypherExpr


@dataclass
class MatchClause:
    pattern: PathPattern | PathFunction | TriplePattern
    variable: str | None  # path variable (p = shortestPath(...))
    optional: bool = False


@dataclass
class WhereClause:
    expression: CypherExpr


@dataclass
class ReturnItem:
    expression: CypherExpr
    alias: str | None = None
    #: The item as the statement writes it, which names an unaliased item.
    name: str = ""


@dataclass
class ReturnClause:
    items: list[ReturnItem]
    distinct: bool = False
    #: The grouping keys, when the statement names them (SPARQL's GROUP BY, REQ-2135); a key need
    #: not be returned. None groups as Cypher does, by the items that aggregate nothing; an empty
    #: list is one group of every row.
    group_by: list[CypherExpr] | None = None
    #: A condition on each group, over its aggregates (SPARQL's HAVING).
    having: CypherExpr | None = None


@dataclass
class WithClause:
    items: list[ReturnItem]
    where: WhereClause | None = None
    # A WITH's own ORDER BY, SKIP and LIMIT pick the rows it carries on, not the statement's.
    order_by: list[OrderItem] = field(default_factory=list)
    skip: int | None = None
    limit: int | None = None


@dataclass
class UnwindClause:
    expression: CypherExpr  # a list literal, a parameter, or a property
    variable: str  # AS variable name


@dataclass
class ProcedureCall:  # REQ-2168
    """``CALL ns.name(args) YIELD col [AS var], … [WHERE …]`` composed in a statement: a command
    called by its Cypher command binding, its rows a source as an UNWIND's are."""

    name: str
    args: list[CypherExpr]
    yields: list[tuple[str, str]]  # (column the command returns, variable it is bound to)
    where: "WhereClause | None" = None


@dataclass
class MatchStep:
    """One or more MATCH/OPTIONAL MATCH clauses and their associated WHERE."""

    matches: list[MatchClause]
    where: WhereClause | None = None


@dataclass
class CallSubquery:
    body: "CypherAST"
    imported_vars: list[str] = field(default_factory=list)  # vars from outer scope via WITH


@dataclass
class OrderItem:
    expression: CypherExpr
    direction: str = "ASC"


@dataclass
class CypherAST:  # REQ-347, REQ-348, REQ-571
    pipeline: list["MatchStep | WithClause"]
    return_clause: ReturnClause | None
    order_by: list[OrderItem]
    skip: int | None
    limit: int | None
    call_subqueries: list[CallSubquery] = field(default_factory=list)
    # UNION / UNION ALL: list of (sub_ast, is_all)
    union_parts: list[tuple["CypherAST", bool]] = field(default_factory=list)
    #: A CONSTRUCT's template (REQ-2135), in place of the RETURN: the answer is its triples.
    construct: list[TripleTemplate] | None = None
    #: The ORDER BY, SKIP and LIMIT apply to the whole UNION (SPARQL's solution modifiers), not to
    #: its first part as a Cypher statement's own do (REQ-2135).
    modifiers_of_whole: bool = False
    comments: list[str] = field(
        default_factory=list
    )  # // comment text (without //) in source order

    @property
    def match_clauses(self) -> list[MatchClause]:
        return [m for s in self.pipeline if isinstance(s, MatchStep) for m in s.matches]

    @property
    def with_clauses(self) -> list[WithClause]:
        return [s for s in self.pipeline if isinstance(s, WithClause)]

    @property
    def where(self) -> WhereClause | None:
        for s in self.pipeline:
            if isinstance(s, MatchStep) and s.where is not None:
                return s.where
        return None
