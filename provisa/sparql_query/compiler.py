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
  is typed by, the labels of a property read from it -- every class whose column binds it, for a
  Property term's (REQ-2163) -- an edge's ends) are intersected, and each
  combination of one label per node that every triple holds in is one interpretation, a UNION ALL
  part. The parts are bounded by graph.union_limit.
- A property triple binds its object variable to the property, or compares it; an edge triple is a
  relationship; a property path is a path expression under REACH; ``?s ?p ?o`` is each of the
  subject label's predicates in turn, a UNION ALL part each, read as that predicate named is, so
  governed as it is (:meth:`_Lowering.predicate_reads`). In a part ?p is an IRI and ?o the value
  or node of that predicate, so a FILTER, BIND or ORDER BY reads them typed, and a value of the
  wrong kind is SPARQL's error, not the engine's. An OPTIONAL over one is a part per predicate
  and one where none matched. An aggregate reads the parts' rows as one.
- A subject of a table with a registered key is one row, so each part reads that row, with no join
  of a table to itself. A subject of a table with none can be several rows: past the lone triple
  each row answers for itself, its other triples are read through a join of its rows on the
  column that names it, which the join guard refuses (V002) by the table's name.
- An open subject is of the classes the role can name a subject of: a table whose key it is not
  served is left out, and so is an edge into one. A query naming one and asking for its subject is
  refused by the table's name.
- The answer: SELECT returns each projected variable as an RDF term (provisa.gql.terms), ASK one
  row when there is a solution, CONSTRUCT the template's triples.

A subject named by its IRI is parsed back to its table and keys (provisa.gql.iri) and matched on
the keys. What the front end does not lower is refused by name.
"""

# Requirements: REQ-2135, REQ-2163

from __future__ import annotations

import copy
import collections
import dataclasses
import itertools
import re
from urllib.parse import unquote
from dataclasses import dataclass, field
from typing import Any

from pyparsing import MatchFirst, Suppress
from rdflib import BNode, Literal as RdfLiteral, URIRef, Variable as RdfVariable
from rdflib.paths import AlternativePath, InvPath, MulPath, NegatedPath, Path, SequencePath
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql import parser as _rdf_parser
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import Comp, CompValue, Param

from provisa.gql.expr_ast import (
    AxiomObject,
    Binary,
    Case,
    CypherExpr,
    FunctionCall,
    IriConst,
    IsNull,
    ListLiteral,
    Literal,
    Property,
    SubqueryExpr,
    Term,
    TermIri,
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
    TripleTemplate,
    WhereClause,
)
from provisa.gql.iri import parse_subject_iri
from provisa.gql.terms import subject_key, union_allowance, union_refusal
from provisa.gql.translator_types import CypherTranslateError
from provisa.security import universal_regex
from provisa.security.masking import UNIVERSAL_REGEX_REPLACE
from provisa.security.rights import ONTOLOGY_DOMAIN_ID
from provisa.sparql_query.vocabulary import (
    OWL,
    RDF,
    RDF_TYPE,
    RDFS,
    SKOS,
    XSD,
    ColumnPredicate,
    Vocabulary,
)

_ORDER_PREFIX = "__ord"
_ORDER_LANES = ("k", "n", "t")  # the kind, number and text an ORDER BY over a variable predicate
_GROUP_PREFIX = "__g"  # an aggregate over a variable predicate: a part's key
_INPUT_PREFIX = "__a"  # ...and an aggregate's input

# REQ-2163: the ontology schema as RDFS/OWL/SKOS, read from ontology._axioms. A triple is an axiom
# when its predicate is one of these, or it is rdf:type with an OWL class; its object is a term
# (TERM), an IRI or term (EITHER), or a literal (LITERAL).
_TERM, _EITHER, _LITERAL = "term", "either", "literal"
_AXIOM_OBJECTS = {
    f"{RDFS}subClassOf": _TERM,
    f"{RDFS}subPropertyOf": _TERM,
    f"{RDFS}domain": _EITHER,  # a Class term, or a table class a term-bound column is of
    f"{RDFS}range": _EITHER,
    f"{SKOS}prefLabel": _LITERAL,
    f"{SKOS}definition": _LITERAL,
}
_OWL_CLASSES = frozenset({f"{OWL}Class", f"{OWL}ObjectProperty", f"{OWL}DatatypeProperty"})


_KIND_OF = frozenset({f"{RDFS}subClassOf", f"{RDFS}subPropertyOf"})


def _either(a: CypherExpr, b: CypherExpr) -> CypherExpr:
    """``a`` or ``b``, as one value that no AND beside it can split (an OR is no atom of a list of
    conditions, which are ANDed unparenthesized)."""
    return Case(
        None,
        [(a, Literal(True, "boolean")), (b, Literal(True, "boolean"))],
        Literal(False, "boolean"),
    )


def _any_of(value: CypherExpr, ids: list[int]) -> CypherExpr:
    """``value`` is one of ``ids``."""
    out: CypherExpr = Binary("=", value, Literal(ids[0], "number"))
    for i in ids[1:]:
        out = Binary("OR", out, Binary("=", value, Literal(i, "number")))
    return out


def _closure_path(p: Any) -> tuple[str, bool] | None:
    """``p`` as a read of ontology._closure -- rdfs:subClassOf or rdfs:subPropertyOf, zero or more
    (``*``) or one or more (``+``) times -- as (its predicate, one or more); None for any other."""
    if (
        isinstance(p, MulPath)
        and isinstance(p.path, URIRef)
        and str(p.path) in _KIND_OF
        and p.mod in ("*", "+")
    ):
        return str(p.path), p.mod == "+"
    return None


def _is_axiom(p: Any, o: Any) -> bool:
    if isinstance(p, Path):
        return _closure_path(p) is not None
    if _is_var(p):
        return False
    iri = str(p)
    return iri in _AXIOM_OBJECTS or (
        iri == RDF_TYPE and isinstance(o, URIRef) and str(o) in _OWL_CLASSES
    )


RDF_NIL = URIRef(f"{RDF}nil")

# The functions the back end lowers by the name given here, each of the arguments named.
_SHARED_SCALARS = {
    "Builtin_STRLEN": "char_length",
    "Builtin_CEIL": "ceil",
    "Builtin_FLOOR": "floor",
    "Builtin_STRBEFORE": "strbefore",
    "Builtin_STRAFTER": "strafter",
    "Builtin_YEAR": "year",
    "Builtin_MONTH": "month",
    "Builtin_DAY": "day",
    "Builtin_HOURS": "hours",
    "Builtin_MINUTES": "minutes",
    "Builtin_SECONDS": "seconds",
    "Builtin_NOW": "now",
}
_SHARED_SCALAR_ARGS = {
    "Builtin_STRBEFORE": ("arg1", "arg2"),
    "Builtin_STRAFTER": ("arg1", "arg2"),
    "Builtin_NOW": (),
}
_XSD_CAST_FNS = {
    f"{XSD}integer": "toInteger",
    f"{XSD}double": "toFloat",
    f"{XSD}float": "toFloat",
    f"{XSD}string": "toString",
    f"{XSD}boolean": "toBoolean",
    f"{XSD}decimal": "toDecimal",
    f"{XSD}date": "toDate",
    f"{XSD}dateTime": "toDateTime",
}


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
    #: OPTIONALs with a variable predicate that did not match: this group holds where none of
    #: their predicates does.
    absent: list["_Group"] = field(default_factory=list)

    def joined(self, other: "_Group") -> "_Group":
        return _Group(
            [*self.triples, *other.triples],
            [*self.filters, *other.filters],
            [*self.binds, *other.binds],
            [*self.optionals, *other.optionals],
            [*self.absent, *other.absent],
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
class _Deferred:
    """A group's BINDs and FILTERs read after its OPTIONALs are lowered."""

    binds: list[tuple[str, Any]] = field(default_factory=list)
    filters: list[Any] = field(default_factory=list)


def _free_vars(expr: Any) -> set[str]:
    """The variables an expression reads."""
    if isinstance(expr, RdfVariable):
        return {str(expr)}
    if isinstance(expr, CompValue):
        return {str(v) for v in expr["_vars"]}
    return set()


def _expr_vars(e: Any) -> set[str]:
    """The variables an expression names, wherever in it."""
    if isinstance(e, RdfVariable):
        return {str(e)}
    if isinstance(e, (list, tuple)):
        return {v for x in e for v in _expr_vars(x)}
    if isinstance(e, CompValue):
        return {v for x in e.values() for v in _expr_vars(x)}
    return set()


def _bound_vars(group: "_Group") -> set[str]:
    """The variables ``group`` and the OPTIONALs in it bind."""
    out = {str(t) for triple in group.triples for t in triple if _is_var(t)}
    out |= {var for var, _ in group.binds}
    for o in group.optionals:
        out |= _bound_vars(o)
    return out


def _reads_a_variable_predicate(group: "_Group") -> bool:
    """Whether ``group``, or an OPTIONAL in it, has a variable predicate."""
    return any(_is_var(p) for _, p, _ in group.triples) or any(
        _reads_a_variable_predicate(o) for o in group.optionals
    )


def _opened_vars(group: "_Group") -> set[str]:
    """The predicates and objects of ``group``'s variable predicates: each is of another kind in
    each part."""
    out = {str(t) for _, p, o in group.triples if _is_var(p) for t in (p, o) if _is_var(t)}
    for o in group.optionals:
        out |= _opened_vars(o)
    return out


class _NoSolution(Exception):
    """This part of the query has no solution: it is left out of the union."""


def _holds_somewhere(condition: CypherExpr) -> None:
    """Raises where a group's condition is settled false (or an error) for its part."""
    if isinstance(condition, Literal) and condition.value in (False, None):
        raise _NoSolution


def _bind_iri(var: str, iri: str, bindings: dict[str, tuple[CypherExpr, str | None]]) -> None:
    """``var`` is the IRI ``iri``: bound to it, or already that IRI."""
    bound = bindings.get(var)
    if bound is None:
        bindings[var] = (IriConst(iri), None)
    elif not (isinstance(bound[0], IriConst) and bound[0].iri == iri):
        raise _NoSolution


def _ends_of(reads: list[tuple[int, str, Predicate, str | None]]) -> dict[str, str] | None:
    """The label each read's object takes, or None where the reads disagree on what it is: one
    edge's target and another's, or a node and a value."""
    ends: dict[str, str] = {}
    values: set[str] = set()
    for _, obj, _, target in reads:
        if target is None:
            values.add(obj)
        elif ends.setdefault(obj, target) != target:
            return None
    return None if values & set(ends) else ends


def _exists_body(matches: list[MatchClause], where: CypherExpr | None) -> CypherAST:
    """The statement of an EXISTS over ``matches``, each of them required."""
    return CypherAST(
        pipeline=[
            MatchStep(
                [dataclasses.replace(m, optional=False) for m in matches],
                WhereClause(where) if where is not None else None,
            )
        ],
        return_clause=ReturnClause([ReturnItem(Literal(1, "number"), "one", name="one")]),
        order_by=[],
        skip=None,
        limit=None,
    )


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
    # GROUP BY an expression with no AS: rdflib binds it by an Extend with no variable under the
    # Group and gives its key as None. Each is named and grouped by; nothing can project it, so
    # which None names which expression does not matter.
    unnamed: list[CompValue] = []
    below = group.p
    while below.name == "Extend" and below.var is None:
        unnamed.append(below)
        below = below.p
    unnamed.reverse()
    keys = []
    for key in group.expr or []:
        if key is None:
            extend = unnamed.pop(0)
            extend["var"] = RdfVariable(f"__group{len(keys)}__")
            key = extend.var
        if not isinstance(key, RdfVariable):
            raise SparqlRefused("GROUP BY groups by variables; bind an expression to one first")
        keys.append(str(key))
    grouped = group.p
    aggregates = {str(a.res): a for a in p.A}
    return grouped, _Aggregation(keys, aggregates, list(reversed(rebinds)), having)


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

# What one side of a comparison is, and the kinds a variable predicate's object takes part by part.
_IRI, _NODE, _VALUE, _NUMBER, _TEXT = "iri", "node", "value", "number", "text"
_DATES = frozenset({f"{XSD}date", f"{XSD}dateTime"})
#: The kinds an operator or function takes, of what it is given.
_WANTS = {
    **dict.fromkeys(
        (
            "Builtin_CONTAINS",
            "Builtin_STRSTARTS",
            "Builtin_STRENDS",
            "Builtin_STRLEN",
            "Builtin_LCASE",
            "Builtin_UCASE",
            "Builtin_STRBEFORE",
            "Builtin_STRAFTER",
            "Builtin_SUBSTR",
            "Builtin_REPLACE",
            "Builtin_REGEX",
            "Builtin_CONCAT",
        ),
        frozenset({_TEXT}),
    ),
    **dict.fromkeys(
        (
            "AdditiveExpression",
            "MultiplicativeExpression",
            "UnaryMinus",
            "UnaryPlus",
            "Builtin_ABS",
            "Builtin_CEIL",
            "Builtin_FLOOR",
            "Builtin_ROUND",
        ),
        frozenset({_NUMBER}),
    ),
    **dict.fromkeys(
        (
            "Builtin_YEAR",
            "Builtin_MONTH",
            "Builtin_DAY",
            "Builtin_HOURS",
            "Builtin_MINUTES",
            "Builtin_SECONDS",
        ),
        _DATES,
    ),
}


def _kind_of(datatype: str | None) -> str | None:
    """A column or literal's kind: a number, text, or its datatype; None where it has none."""
    if datatype in _NUMERIC:
        return _NUMBER
    return _TEXT if datatype == f"{XSD}string" else datatype


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


def _keep_negated_inverse_iris() -> None:
    """rdflib's grammar (7.6) parses a negated set's inverse member, the ``^b`` of ``!(a|^b)``, as
    an InversePath that keeps no IRI: its rule names no Param, so the IRI is dropped. Give the rule
    the Param it lacks, so the member reaches the algebra as ``InversePath(part=b)``."""
    one_of = _rdf_parser.PathOneInPropertySet
    assert isinstance(one_of, MatchFirst)  # iri | A | '^' (iri | A)
    inverse = next(e for e in one_of.exprs if isinstance(e, Comp) and e.name == "InversePath")
    inverse.expr = Suppress("^") + Param("part", _rdf_parser.iri | _rdf_parser.A)


_keep_negated_inverse_iris()


def _negated_member(arg: Any) -> tuple[Any, bool]:
    """A negated set's member: its IRI, and whether it is an inverse edge."""
    if isinstance(arg, InvPath):
        return arg.arg, True
    if isinstance(arg, CompValue) and arg.name == "InversePath":
        return arg.part, True
    return arg, False


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
        members = [_negated_member(a) for a in path.args]
        sets = []
        for inverse in (False, True):
            iris = [str(iri) for iri, inv in members if inv == inverse]
            if iris:
                names = tuple(_edge_type(iri, vocab) for iri in iris if iri in vocab.edges)
                sets.append(NegatedEdges(names, inverse=inverse))
        # !(a|^b) is either: an edge forward that is not a, or one backward that is not b.
        return sets[0] if len(sets) == 1 else AltPath(tuple(sets))
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

    def __init__(self, vocab: Vocabulary, lm: Any, limit: int, allowed: int) -> None:
        self.vocab = vocab
        self._lm = lm
        self.limit = limit  # graph.union_limit
        self.allowed = allowed  # the branches this query may have
        self._placed: set[str] = set()
        self._axioms = 0  # REQ-2163: the ontology._axioms nodes placed so far
        self._rows = 0  # the subject-row joins placed in this statement
        self._opened: set[str] = set()  # this part's variable predicates and their objects

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
            if _is_axiom(p, o):
                continue  # read from ontology._axioms: its terms are no nodes of the graph
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
        # A subject nothing constrains names no class, so it is no request for one whose subjects
        # the role cannot name.
        everything = {label for label in self.vocab.classes_of if self._node(label).key_readable}
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
        deferred: "_Deferred | None" = None,
        reads: dict[int, Predicate] | None = None,
        through_rows: bool = False,
    ) -> MatchStep:
        """The MATCH of ``group`` with its nodes labelled ``labels``. ``bindings`` gains each
        variable the group binds to a value: (its expression, its datatype). ``reads`` is the
        predicate each variable predicate stands for in this part, by its triple's place in the
        group (:meth:`predicate_reads`); ``through_rows`` reads them through the subject's rows
        where its table has no key (:meth:`_row_of`)."""
        named: dict[str, str] = {}
        keys: dict[str, tuple] = {}
        conditions: list[CypherExpr] = []
        edges: list[tuple[str, RelPattern, str]] = []
        nodes: list[str] = []
        bound_before = set(bindings)

        def node(term: Any) -> str:
            var = self._subject_node(term, named, keys)
            if var not in nodes:
                nodes.append(var)
            return var

        for i, (s, p, o) in enumerate(group.triples):
            if _is_axiom(p, o):
                closure = _closure_path(p)
                axiom = self._axiom_node(labels, "_closure" if closure else "_axioms")
                nodes.append(axiom)
                conditions.extend(
                    self._closure_conditions(axiom, s, closure, o, bindings)
                    if closure
                    else self._axiom_conditions(axiom, s, p, o, bindings)
                )
                continue
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
                read = (reads or {})[i]
                row = self._row_of(sv, labels, conditions, nodes) if through_rows else sv
                bindings[str(p)] = (IriConst(read.iri), None)
                if read.constant is not None:
                    _bind_iri(str(o), read.constant, bindings)
                elif read.property is not None:
                    value = Property(Variable(row), read.property)
                    conditions.extend(self._read_value(str(o), value, read.datatype, bindings))
                else:
                    if str(o) in bindings:
                        raise _NoSolution
                    rel = RelPattern(None, [read.rel_type], None, None, "right")  # type: ignore[list-item]
                    edges.append((row, rel, node(o)))
                continue
            iri = str(p)
            if iri == RDF_TYPE:
                if _is_var(o):
                    try:
                        if str(o) in labels:
                            raise _NoSolution  # a class is no subject of the data
                        _bind_iri(str(o), self.vocab.classes_of[labels[sv]][0], bindings)
                    except _NoSolution:
                        if not optional:
                            raise
                        conditions.append(Literal(False, "boolean"))  # it did not match
                continue
            if iri in self.vocab.edges:
                rel_type = self.vocab.edges[iri][0].rel_type
                edges.append((sv, RelPattern(None, [rel_type], None, None, "right"), node(o)))
                continue
            cp = self._column(iri, labels[sv])
            value: CypherExpr = Property(Variable(sv), cp.prop)
            if _is_var(o):
                if str(o) in labels:
                    conditions.append(Literal(False, "boolean"))  # a node is no value
                else:
                    try:
                        conditions.extend(self._read_value(str(o), value, cp.datatype, bindings))
                    except _NoSolution:
                        if not optional:
                            raise
                        conditions.append(Literal(False, "boolean"))  # it did not match
            else:
                conditions.append(Binary("=", value, _literal(o)))

        for var, (iri,) in keys.items():
            conditions.extend(self._key_conditions(var, labels[var], iri))
        for var, expr in group.binds:
            if deferred is not None and not _free_vars(expr) <= {*bindings, *labels}:
                deferred.binds.append((var, expr))
            else:
                bindings[var] = self._bound(var, expr, bindings, labels)
        if deferred is not None:
            deferred.filters.extend(group.filters)
        else:
            for f in group.filters:
                conditions.append(self.expr(f, bindings, labels))
                if not optional:
                    _holds_somewhere(conditions[-1])

        placed_before = set(self._placed)
        matches = self._clauses(nodes, edges, labels, optional)
        if optional:
            conditions = self._optional_bindings(
                [v for v in nodes if v not in placed_before],
                matches,
                conditions,
                [v for v in bindings if v not in bound_before],
                bindings,
                labels,
            )
        where = None
        if conditions:
            cond = conditions[0]
            for c in conditions[1:]:
                cond = Binary("AND", cond, c)
            where = WhereClause(cond)
        return MatchStep(matches, where)

    # -- variable predicates --------------------------------------------------------------

    def predicate_reads(
        self,
        group: _Group,
        labels: dict[str, str],
        mods: dict,
        aggregation: "_Aggregation | None",
    ) -> list[tuple[_Group, dict[str, str], dict[int, Predicate], bool]]:
        """The UNION ALL parts of one interpretation of ``group``, each its group, its labels, the
        predicate each variable predicate stands for and whether it reads them through the rows of
        a subject with no key: ``?s ?p ?o`` is the union of ``?s <iri> ?o`` over the subject's
        predicates, so each part reads a table, or an edge along its registered relationship, as
        naming that predicate does. Only a lone triple, whose ?p and ?o nothing else reads, answers
        row by row for a subject with no key."""
        parts = []
        for flat, flat_labels in self._without_optional_predicates(group, labels):
            lone = aggregation is None and flat is group and self._is_lone(flat, flat_labels, mods)
            for part_labels, reads in self._choices(flat, flat_labels):
                parts.append((flat, part_labels, reads, not lone))
        return parts

    def _is_lone(self, group: _Group, labels: dict[str, str], mods: dict) -> bool:
        """Whether nothing but its own triple reads each variable predicate's ?p and ?o."""
        counts = collections.Counter(
            str(t) for triple in group.triples for t in triple if _is_var(t)
        )
        read_elsewhere = {
            *(v for e in group.filters for v in _expr_vars(e)),
            *(v for _, e in group.binds for v in _expr_vars(e)),
            *(v for c in mods.get("order", []) for v in _expr_vars(c.expr)),
            *(v for o in group.optionals for v in _bound_vars(o)),
        }

        def own(var: Any) -> bool:
            return (
                _is_var(var)
                and counts[str(var)] == 1
                and str(var) not in read_elsewhere
                and str(var) not in labels
            )

        return all(own(p) and own(o) for _, p, o in group.triples if _is_var(p))

    def _without_optional_predicates(
        self, group: _Group, labels: dict[str, str]
    ) -> list[tuple[_Group, dict[str, str]]]:
        """``group`` with each OPTIONAL holding a variable predicate gone, as the groups whose
        solutions together are its own: one with the OPTIONAL's pattern required, whose parts are
        its predicates, and one where it did not match. A LEFT JOIN of a union is no union of LEFT
        JOINs: a part per predicate would leave a row of unbound values for each that did not."""
        for i, opt in enumerate(group.optionals):
            if not _reads_a_variable_predicate(opt):
                continue
            opt_labels = self._optional_labels(opt, labels)
            before, after = group.optionals[:i], group.optionals[i + 1 :]
            required = _Group(
                [*group.triples, *opt.triples],
                [*group.filters, *opt.filters],
                [*group.binds, *opt.binds],
                [*before, *opt.optionals, *after],
                list(group.absent),
            )
            unmatched = dataclasses.replace(
                group, optionals=[*before, *after], absent=[*group.absent, opt]
            )
            return [
                *self._without_optional_predicates(required, {**labels, **opt_labels}),
                *self._without_optional_predicates(unmatched, labels),
            ]
        return [(group, labels)]

    def _optional_labels(self, opt: _Group, labels: dict[str, str]) -> dict[str, str]:
        """The one interpretation of the OPTIONAL ``opt`` under ``labels``."""
        found = self.interpretations(opt, labels)
        if len(found) != 1:
            raise SparqlRefused(
                "An OPTIONAL whose nodes could be of several classes is not supported; "
                "give them a class"
            )
        inner = {v for o in opt.optionals for v in _bound_vars(o)}
        if any(v in inner for f in opt.filters for v in _expr_vars(f)):
            raise SparqlRefused(
                "A FILTER in an OPTIONAL naming a variable an OPTIONAL inside it binds is not "
                "supported"
            )
        return found[0]

    def _value_vars(self, group: _Group) -> set[str]:
        """The variables a named column of ``group`` binds: values, so no edge's object."""
        return {
            str(o)
            for _, p, o in group.triples
            if _is_var(o) and not isinstance(p, Path) and str(p) in self.vocab.columns
        }

    def _bound(self, var: str, expr: Any, bindings, labels) -> tuple[CypherExpr, str | None]:
        """What BIND(``expr`` AS ?``var``) binds. A variable is bound as what it is: a subject of
        a variable predicate's edge stays a subject, and a value keeps its datatype."""
        if isinstance(expr, RdfVariable):
            if str(expr) in bindings:
                if str(expr) in self._opened:
                    self._opened.add(var)
                return bindings[str(expr)]
            if str(expr) in labels and str(expr) in self._opened:
                self._opened.add(var)
                return Variable(str(expr)), None
        return self.expr(expr, bindings, labels), None

    def _choices(
        self, group: _Group, labels: dict[str, str], fixed: dict[str, str] | None = None
    ) -> list[tuple[dict[str, str], dict[int, Predicate]]]:
        """Each way of reading ``group``'s variable predicates, as its labels and the predicate of
        each triple's place: one predicate IRI per variable, ``fixed`` holding those already
        chosen, each triple reading that IRI of its subject."""
        named: dict[str, str] = {}
        keys: dict[str, tuple] = {}
        values = self._value_vars(group)
        triples: dict[str, list[tuple[int, str, list[tuple[Predicate, str | None]]]]] = {}
        for i, (s, p, o) in enumerate(group.triples):
            if _is_axiom(p, o):
                continue
            subject = self._subject_node(s, named, keys)
            if isinstance(p, Path) or (not _is_var(p) and str(p) in self.vocab.edges):
                self._subject_node(o, named, keys)
            if not _is_var(p):
                continue
            if str(p) in labels:
                return []  # a predicate is a vocabulary IRI, and no subject of the data
            if not _is_var(o):
                raise SparqlRefused("A variable predicate with a constant object is not supported")
            reads = self._reads_of(labels[subject], labels.get(str(o)), str(o) in values)
            if len(reads) > self.allowed:
                raise SparqlRefused(
                    union_refusal(
                        f"A variable predicate on {self._node(labels[subject]).label} reads "
                        f"{len(reads)} mapped predicates",
                        self.limit,
                        self.allowed,
                    )
                    + "; name the predicate, or raise the limit (graph.union_limit)"
                )
            triples.setdefault(str(p), []).append((i, str(o), reads))
        axes = []
        for var, reading in triples.items():
            iris = list(dict.fromkeys(r.iri for r, _ in reading[0][2]))
            for _, _, reads in reading[1:]:
                iris = [iri for iri in iris if any(r.iri == iri for r, _ in reads)]
            if fixed is not None and var in fixed:
                iris = [iri for iri in iris if iri == fixed[var]]
            axis = []
            for iri in iris:
                each = [
                    [(i, obj, r, t) for r, t in reads if r.iri == iri] for i, obj, reads in reading
                ]
                axis.extend(itertools.product(*each))
            axes.append(axis)
        parts: list[tuple[dict[str, str], dict[int, Predicate]]] = []
        for combo in itertools.product(*axes):
            chosen = [read for axis_read in combo for read in axis_read]
            ends = _ends_of(chosen)
            if ends is None:
                continue
            parts.append(({**labels, **ends}, {i: read for i, _, read, _ in chosen}))
            if len(parts) > self.allowed:
                raise SparqlRefused(
                    union_refusal(
                        "The variable predicates of a pattern read "
                        f"{len(parts)} combinations of predicates",
                        self.limit,
                        self.allowed,
                    )
                    + "; name a predicate, or raise the limit (graph.union_limit)"
                )
        return parts

    def _reads_of(
        self, label: str, object_label: str | None, value_only: bool
    ) -> list[tuple[Predicate, str | None]]:
        """Each predicate of ``label`` a variable predicate stands for, with the label of its
        object where that is a node: an edge's target, whose subjects the role can name. An object
        that is a node of ``object_label`` is only an edge's; one a column reads elsewhere
        (``value_only``) is only a column's."""
        if object_label is not None:
            return [
                (Predicate(iri, rel_type=ep.rel_type), ep.target)
                for iri, ep in self.vocab.edges_of.get(label, [])
                if ep.target == object_label and self._node(ep.target).key_readable
            ]
        out: list[tuple[Predicate, str | None]] = [
            (Predicate(iri, property=cp.prop, datatype=cp.datatype), None)
            for iri, cp in self.vocab.columns_of.get(label, [])
        ]
        if value_only:
            return out
        out[:0] = [(Predicate(RDF_TYPE, constant=c), None) for c in self.vocab.classes_of[label]]
        out += [
            (Predicate(iri, rel_type=ep.rel_type), ep.target)
            for iri, ep in self.vocab.edges_of.get(label, [])
            if self._node(ep.target).key_readable
        ]
        return out

    def _row_of(
        self, subject: str, labels: dict[str, str], conditions: list[CypherExpr], nodes: list[str]
    ) -> str:
        """The node a variable predicate of ``subject`` reads. A subject of a table with a
        registered key is one row, the one matched, as is one whose key the role is not served,
        which no query can name. One of a table with none is every row its
        name names, so the read joins them, a join the guard refuses by the table's name (V002)
        unless the role may join outside the relationships."""
        nm = self._node(labels[subject])
        if nm.pk_columns or not nm.key_readable:
            return subject
        row = f"_r{self._rows}"
        self._rows += 1
        labels[row] = labels[subject]
        nodes.append(row)
        conditions.append(self._same_node(subject, row, labels))
        return row

    def _read_value(
        self,
        var: str,
        value: CypherExpr,
        datatype: str | None,
        bindings: dict[str, tuple[CypherExpr, str | None]],
    ) -> list[CypherExpr]:
        """``var`` is the column ``value``: bound to it, or equal to what it is bound to."""
        bound = bindings.get(var)
        if bound is None:
            bindings[var] = (value, datatype)
            return [IsNull(value, negated=True)]
        if isinstance(bound[0], IriConst):
            raise _NoSolution  # an IRI is no column's value
        if _kind_of(bound[1]) != _kind_of(datatype):
            raise _NoSolution
        return [Binary("=", value, bound[0])]

    def _absent(
        self,
        opt: _Group,
        labels: dict[str, str],
        bindings: dict[str, tuple[CypherExpr, str | None]],
    ) -> list[CypherExpr]:
        """The conditions that no predicate of the OPTIONAL ``opt`` matches the row, each read
        where its own column or edge is. Raises where one always does."""
        opt_labels = {**labels, **self._optional_labels(opt, labels)}
        self._opened |= _opened_vars(opt)
        fixed = {v: b[0].iri for v, b in bindings.items() if isinstance(b[0], IriConst)}
        out: list[CypherExpr] = []
        for part_labels, reads in self._choices(opt, opt_labels, fixed):
            placed = set(self._placed)
            local = dict(bindings)
            try:
                step = self.lower_group(
                    dataclasses.replace(opt, optionals=[]),
                    part_labels,
                    local,
                    optional=False,
                    reads=reads,
                )
            except _NoSolution:
                continue
            finally:
                self._placed = placed
            where = step.where.expression if step.where is not None else None
            if step.matches:
                present: CypherExpr = SubqueryExpr("EXISTS", _exists_body(step.matches, where))
            elif where is not None:
                present = where
            else:
                raise _NoSolution  # holds of every row: the OPTIONAL always matches
            out.append(Case(None, [(present, Literal(False, "boolean"))], Literal(True, "boolean")))
        return out

    # -- the ontology's axioms (REQ-2163) -------------------------------------------------

    def expand_axiom_joins(self, group: _Group) -> list[_Group]:
        """``group``, with each variable it shares between the ontology's axioms and instance data
        -- a class an instance has (``?x a ?c``) or a predicate it has (``?x ?p ?v``) -- fixed to
        each of the role's class terms or property terms in turn: one group each, which the axiom
        patterns (read in SQL, from ontology._axioms and _closure) keep or answer nothing for. An
        instance's classes and predicates are the role's vocabulary, so a table the role cannot
        read gives none."""

        def names(*terms: Any) -> set[str]:
            return {str(t) for t in terms if _is_var(t)}

        axiom_vars: set[str] = set()
        data_vars: set[str] = set()
        for s, p, o in group.triples:
            if _is_axiom(p, o):
                axiom_vars |= names(s, o)
            else:
                data_vars |= names(s, o) | (set() if isinstance(p, Path) else names(p))
        shared = sorted(axiom_vars & data_vars)
        if not shared:
            return [group]
        root = f"{self.vocab.base}/vocab/term/"
        candidates: list[list[str]] = []
        for var in shared:
            as_class = any(
                not _is_axiom(p, o) and str(p) == RDF_TYPE and _is_var(o) and str(o) == var
                for _s, p, o in group.triples
            )
            as_predicate = any(
                not _is_axiom(p, o) and _is_var(p) and str(p) == var for _s, p, o in group.triples
            )
            if as_class == as_predicate:
                raise SparqlRefused(
                    f"?{var} joins the ontology's axioms to instance data, and is not a class an "
                    "instance has (?x a ?c) or a predicate it has (?x ?p ?v)"
                )
            terms = self.vocab.classes if as_class else self.vocab.columns
            candidates.append(sorted(iri for iri in terms if iri.startswith(root)))
        expanded = []
        for combo in itertools.product(*candidates):
            fixed = {var: URIRef(iri) for var, iri in zip(shared, combo)}

            def put(x: Any) -> Any:
                return fixed.get(str(x), x) if _is_var(x) else x

            expanded.append(
                _Group(
                    [(put(s), put(p), put(o)) for s, p, o in group.triples],
                    list(group.filters),
                    [*group.binds, *((var, iri) for var, iri in fixed.items())],
                    list(group.optionals),
                )
            )
        return expanded

    def _axiom_node(self, labels: dict[str, str], table: str) -> str:
        """A fresh node of ``table`` (ontology._axioms, one row of which is one triple, or
        ontology._closure, one row of which is one kind-of pair)."""
        label = self._lm.sql_only_tables.get((ONTOLOGY_DOMAIN_ID, table))
        if label is None:
            raise SparqlRefused("The ontology's axioms are not readable here")
        var = f"_ax{self._axioms}"
        self._axioms += 1
        labels[var] = label
        return var

    def _table_iris(self) -> dict[int, str]:
        """The class IRI of each table the role reads (the vocabulary's own: its table class
        first), by table id -- what an rdfs:domain row of a table is answered as (REQ-2163)."""
        return {
            self._lm.nodes[label].table_id: iris[0]
            for label, iris in self.vocab.classes_of.items()
            if label in self._lm.nodes
        }

    def _term_name(self, iri: str) -> str | None:
        """The name of the term ``iri`` is the vocabulary IRI of; None for any other IRI."""
        root = f"{self.vocab.base}/vocab/term/"
        return unquote(iri[len(root) :]) if iri.startswith(root) else None

    def _axiom_conditions(
        self,
        var: str,
        s: Any,
        p: Any,
        o: Any,
        bindings: dict[str, tuple[CypherExpr, str | None]],
    ) -> list[CypherExpr]:
        """The row ``var`` states the triple ``s p o``: its predicate, and its subject and object
        each the term, IRI or literal written, or bound to (or by) a variable."""
        iri = str(p)
        conditions: list[CypherExpr] = [
            Binary("=", Property(Variable(var), "predicate"), Literal(iri, "string"))
        ]
        if iri == RDF_TYPE:
            conditions.append(
                Binary("=", Property(Variable(var), "object_iri"), Literal(str(o), "string"))
            )
            objects: tuple[CypherExpr | None, str] = (None, _TERM)
        else:
            kind = _AXIOM_OBJECTS[iri]
            value: CypherExpr = (
                TermIri(Property(Variable(var), "object"))
                if kind == _TERM
                else AxiomObject(var, self._table_iris() if iri == f"{RDFS}domain" else {})
                if kind == _EITHER
                else Property(Variable(var), "object_literal")
            )
            objects = (value, kind)
        if iri == f"{RDFS}domain":
            # A table class is the object of a row of object_table_id, for a table the role reads.
            table_id = Property(Variable(var), "object_table_id")
            tables = self._table_iris()
            conditions.append(
                _either(
                    IsNull(table_id, negated=False),
                    _any_of(table_id, list(tables)) if tables else Literal(False, "boolean"),
                )
            )
        conditions += self._axiom_position(
            s, TermIri(Property(Variable(var), "subject")), _TERM, bindings
        )
        if objects[0] is not None:
            conditions += self._axiom_position(o, objects[0], objects[1], bindings)
        return conditions

    def _closure_conditions(
        self,
        var: str,
        s: Any,
        closure: tuple[str, bool],
        o: Any,
        bindings: dict[str, tuple[CypherExpr, str | None]],
    ) -> list[CypherExpr]:
        """The ontology._closure row ``var`` pairs ``s`` with ``o`` by ``closure``'s predicate,
        through one or more kind-of steps when it is ``+``."""
        predicate, one_or_more = closure
        conditions: list[CypherExpr] = [
            Binary("=", Property(Variable(var), "predicate"), Literal(predicate, "string"))
        ]
        if one_or_more:
            conditions.append(Binary(">=", Property(Variable(var), "hops"), Literal(1, "number")))
        for x, column in ((s, "subject"), (o, "object")):
            conditions += self._axiom_position(
                x, TermIri(Property(Variable(var), column)), _TERM, bindings
            )
        return conditions

    def _axiom_position(
        self,
        x: Any,
        value: CypherExpr,
        kind: str,
        bindings: dict[str, tuple[CypherExpr, str | None]],
    ) -> list[CypherExpr]:
        """``x`` -- a variable, a term's IRI, another IRI or a literal -- is the ``value`` an axiom
        row holds at one position."""
        if _is_var(x):
            name = str(x)
            if name in bindings:
                return [Binary("=", value, bindings[name][0])]
            bindings[name] = (value, None)
            return [IsNull(value, negated=True)] if not isinstance(value, AxiomObject) else []
        if isinstance(x, URIRef):
            term = self._term_name(str(x))
            if kind == _LITERAL or (term is None and kind == _TERM):
                return [Literal(False, "boolean")]
            if term is not None:
                return [Binary("=", value, Literal(term, "string"))]
            assert isinstance(value, AxiomObject)
            table_ids = [i for i, c in value.table_iris.items() if c == str(x)]
            match: CypherExpr = Binary(
                "=", Property(Variable(value.node), "object_iri"), Literal(str(x), "string")
            )
            if table_ids:
                match = _either(
                    match,
                    Binary(
                        "=",
                        Property(Variable(value.node), "object_table_id"),
                        Literal(table_ids[0], "number"),
                    ),
                )
            return [match]
        if isinstance(x, RdfLiteral) and kind == _LITERAL:
            return [Binary("=", value, Literal(str(x), "string"))]
        return [Literal(False, "boolean")]

    def _optional_bindings(
        self,
        new_nodes: list[str],
        matches: list[MatchClause],
        conditions: list[CypherExpr],
        bound: list[str],
        bindings: dict[str, tuple[CypherExpr, str | None]],
        labels: dict[str, str],
    ) -> list[CypherExpr]:
        """An OPTIONAL binds its variables only where it matches as a whole: each it binds is
        unbound where it does not. Where it reaches a node of its own, it matched where that node
        joined (its conditions join it); where it reads only subjects already matched, it has no
        join of its own, and matched where its conditions hold, which then filter no row. The
        conditions left for its join."""
        if new_nodes:
            matched: CypherExpr = IsNull(self._key_of(new_nodes[0], labels), negated=True)
        elif not matches:
            if not conditions:
                return conditions
            matched = conditions[0]
            for c in conditions[1:]:
                matched = Binary("AND", matched, c)
            conditions = []
        else:
            # Edges between subjects already matched: no row of their own, it matched where they
            # exist between the two rows (#303).
            matched = SubqueryExpr("EXISTS", _exists_body(matches, None))
            for c in conditions:
                matched = Binary("AND", matched, c)
            matches.clear()
            conditions = []
        for var in bound:
            value, datatype = bindings[var]
            bindings[var] = (Case(None, [(matched, value)], None), datatype)
        return conditions

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

    def _key_conditions(self, var: str, label: str, iri: str) -> list[CypherExpr]:
        """``var`` is the row its IRI names: its key columns equal the key values."""
        ref = parse_subject_iri(self.vocab.base, iri)
        nm = self._node(label)
        assert ref is not None  # the interpretation only labels it with the table it names
        key_columns = subject_key(nm)
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
        if self._is_kind_error(e, bindings, labels):
            return Literal(None, "null")
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
            floating = self._is_floating(e.expr, bindings)
            for op, other in zip(e.op or [], e.other or [], strict=True):
                operand = self.expr(other, bindings, labels)
                if op == "/":
                    out = self._divide(out, operand, floating or self._is_floating(other, bindings))
                else:
                    out = Binary(op, out, operand)
                floating = floating or self._is_floating(other, bindings)
            return out
        return self._builtin(e, bindings, labels)

    def _kind(self, arg: Any, bindings, labels) -> str | None:
        """The kind of a variable of a variable predicate in this part (each part knows it):
        ``_IRI`` for a node or an IRI, or a datatype's; None for anything else."""
        if not isinstance(arg, RdfVariable) or str(arg) not in self._opened:
            return None
        side = self._side(arg, bindings, labels)
        if side is None:
            return None
        return _IRI if side[0] != _VALUE else side[1]

    def _is_kind_error(self, e: Any, bindings, labels) -> bool:
        """Whether ``e`` applies to a variable of a variable predicate an operator or function not
        of its kind in this part: SPARQL's error (17.2), which reads as unbound, where the engine
        would refuse the whole statement for one predicate of many."""
        wants = _WANTS.get(e.name)
        if wants is None:
            return False
        operands = [v for k, v in e.items() if k != "_vars"]
        for x in operands:
            for arg in x if isinstance(x, (list, tuple)) else [x]:
                kind = self._kind(arg, bindings, labels)
                if kind is not None and kind not in wants:
                    return True
        return False

    def _side(self, x: Any, bindings, labels) -> tuple[str, Any] | None:
        """What one side of a comparison is when the query is read: an IRI, a node, or a value of
        a kind (None where the query does not say). None where the variable is unbound."""
        if isinstance(x, URIRef):
            return _IRI, str(x)
        if isinstance(x, RdfLiteral):
            return _VALUE, _kind_of(str(x.datatype) if x.datatype else f"{XSD}string")
        if not isinstance(x, RdfVariable):
            return _VALUE, None
        if str(x) in labels:
            return _NODE, str(x)
        bound = bindings.get(str(x))
        if bound is None:
            return None
        if isinstance(bound[0], IriConst):
            return _IRI, bound[0].iri
        if isinstance(bound[0], Variable):
            return _NODE, bound[0].name
        return _VALUE, _kind_of(bound[1])

    def _compare(self, op: str, a: Any, b: Any, bindings, labels) -> CypherExpr | None:
        """``a op b`` where a variable of a variable predicate is a side and its kind in this part
        settles it: SQL holds no IRI, a node is no value, and two values of two kinds are SPARQL's
        error, however a column is typed. None where it is the values' to say."""
        if not (self._kind(a, bindings, labels) or self._kind(b, bindings, labels)):
            return None
        left, right = self._side(a, bindings, labels), self._side(b, bindings, labels)
        if left is None or right is None:
            return None
        equality = op in ("=", "<>")
        tags = {left[0], right[0]}
        if tags == {_VALUE}:
            kinds = (left[1], right[1])
            return Literal(None, "null") if None not in kinds and kinds[0] != kinds[1] else None
        if _VALUE in tags:
            return Literal(op == "<>", "boolean") if equality else Literal(None, "null")
        if not equality:
            return Literal(None, "null")
        if tags == {_IRI}:
            return Literal((left[1] == right[1]) == (op == "="), "boolean")
        if tags == {_NODE}:
            same = self._same_node(left[1], right[1], labels)
        else:
            node, iri = (left, right) if left[0] == _NODE else (right, left)
            same = self._iri_match(node[1], iri[1], labels)
        return same if op == "=" else Unary("NOT", same)

    def _relational(self, e: Any, bindings, labels) -> CypherExpr:
        op: str = {"!=": "<>"}.get(str(e.op), str(e.op))
        if op in ("IN", "NOT IN") and self._kind(e.expr, bindings, labels) is not None:
            found = [self._equal_to(e.expr, x, bindings, labels) for x in e.other]
            out = found[0] if found else Literal(False, "boolean")
            for f in found[1:]:
                out = Binary("OR", out, f)
            return out if op == "IN" else Unary("NOT", out)
        settled = self._compare(op, e.expr, e.other, bindings, labels)
        if settled is not None:
            return settled
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
            cond = Binary("IN", self.expr(e.expr, bindings, labels), items, cross_kinds="error")
            return cond if op == "IN" else Unary("NOT", cond)
        return Binary(
            op,
            self.expr(e.expr, bindings, labels),
            self.expr(e.other, bindings, labels),
            cross_kinds="error",
        )

    def _equal_to(self, a: Any, b: Any, bindings, labels) -> CypherExpr:
        """``a = b``, as a RelationalExpression reads it."""
        relation = CompValue("RelationalExpression", expr=a, op="=", other=b)
        return self._relational(relation, bindings, labels)

    def _same_node(self, a: str, b: str, labels: dict[str, str]) -> CypherExpr:
        """Whether two subjects are the same: rows of one table with equal keys."""
        if labels[a] != labels[b]:
            return Literal(False, "boolean")
        nm = self._node(labels[a])
        by_column = {col: prop for prop, col in nm.properties.items()}
        out: CypherExpr | None = None
        for col in subject_key(nm):
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
            if var in labels or isinstance(bindings.get(var, (None,))[0], IriConst):
                return Literal(True, "boolean")
            return IsNull(self.expr(e.arg, bindings, labels), negated=True)
        if name == "Builtin_REGEX":
            (text,) = args("text")
            return Binary("=~", text, self._regex_search(e.pattern, e.flags, bindings, labels))
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
            bound = bindings.get(str(e.arg), (None,))[0]
            if isinstance(bound, IriConst):
                return Literal(bound.iri, "string")
            return FunctionCall("toString", args("arg"))
        if name == "Builtin_ABS":
            return FunctionCall("abs", args("arg"))
        if name == "Builtin_COALESCE":
            return FunctionCall("coalesce", [self.expr(a, bindings, labels) for a in e.arg])
        if name == "Builtin_IF":
            cond, then, other = args("arg1", "arg2", "arg3")
            return Case(None, [(cond, then)], other)
        if name == "Builtin_sameTerm":
            settled = self._compare("=", e.arg1, e.arg2, bindings, labels)
            return settled if settled is not None else Binary("=", *args("arg1", "arg2"))
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
        scalar = self._scalar(e, bindings, labels)
        if scalar is not None:
            return scalar
        raise SparqlRefused(f"The SPARQL function {name.removeprefix('Builtin_')} is not supported")

    def _scalar(self, e: Any, bindings, labels) -> CypherExpr | None:
        """A string, numeric, date-part or cast function, or None where ``e`` is none of them."""
        name = e.name
        args = lambda *keys: [self.expr(getattr(e, k), bindings, labels) for k in keys]  # noqa: E731
        if name in _SHARED_SCALARS:
            return FunctionCall(
                _SHARED_SCALARS[name], args(*_SHARED_SCALAR_ARGS.get(name, ("arg",)))
            )
        if name == "Builtin_CONCAT":
            given = [] if e.arg == RDF_NIL else e.arg
            parts = [self.expr(a, bindings, labels) for a in given]
            return FunctionCall("strconcat", parts) if parts else Literal("", "string")
        if name == "Builtin_SUBSTR":
            # SPARQL counts from 1, the IR's substring from 0.
            text, start = args("arg", "start")
            rest = [self.expr(e.length, bindings, labels)] if e.length is not None else []
            return FunctionCall(
                "substring", [text, Binary("-", start, Literal(1, "number")), *rest]
            )
        if name == "Builtin_ROUND":
            # SPARQL rounds a half up, toward positive infinity; SQL's round away from zero.
            (x,) = args("arg")
            return FunctionCall("floor", [Binary("+", x, Literal(0.5, "number"))])
        if name == "Builtin_REPLACE":
            return self._replace(e, bindings, labels)
        if name == "Builtin_isNUMERIC":
            if self._kind(e.arg, bindings, labels) == _IRI:
                return Literal(False, "boolean")
            return Literal(self._datatype_of(e.arg, bindings) in _NUMERIC, "boolean")
        if name == "Function" and str(e.iri) == f"{XSD}integer":
            (given,) = e.expr
            return self._xsd_integer(given, self.expr(given, bindings, labels), bindings)
        if name == "Function" and str(e.iri) in _XSD_CAST_FNS:
            (x,) = [self.expr(a, bindings, labels) for a in e.expr]
            return FunctionCall(_XSD_CAST_FNS[str(e.iri)], [x])
        if name == "Function":
            raise SparqlRefused(f"The function <{e.iri}> is not supported")
        return None

    def _xsd_integer(self, given: Any, value: CypherExpr, bindings) -> CypherExpr:
        """xsd:integer(x) as XPath casts (SPARQL 1.1 17.1): a number toward zero; a string only
        in an integer's own lexical form, any other an error -- null, as a failed cast leaves a
        BIND unbound. Not Cypher's toInteger, which reads '2.9' as 2."""
        if self._is_numeric(given, bindings):
            return FunctionCall("toInteger", [value])
        lexical = Binary("=~", value, Literal(" *[+-]?[0-9]+ *", "string"))
        return Case(
            subject=None,
            whens=[(lexical, FunctionCall("toInteger", [copy.deepcopy(value)]))],
            default=Literal(None, "null"),
        )

    def _is_numeric(self, arg: Any, bindings) -> bool:
        """Whether ``arg`` is a number as far as the query says when it is read: a literal or a
        column of a numeric datatype, a cast to one, arithmetic, or a numeric function."""
        if isinstance(arg, RdfLiteral):
            return str(arg.datatype) in _NUMERIC
        if isinstance(arg, RdfVariable):
            return str(arg) in bindings and bindings[str(arg)][1] in _NUMERIC
        name = getattr(arg, "name", None)
        if name == "Function":
            return str(arg.iri) in _NUMERIC
        return name in (
            "AdditiveExpression",
            "MultiplicativeExpression",
            "UnaryMinus",
            "UnaryPlus",
            "Builtin_ABS",
            "Builtin_CEIL",
            "Builtin_FLOOR",
            "Builtin_ROUND",
            "Builtin_STRLEN",
        )

    @staticmethod
    def _divide(dividend: CypherExpr, divisor: CypherExpr, floating: bool) -> CypherExpr:
        """SPARQL's op:numeric-divide (SPARQL 1.1 17.4.1.4, XPath 3.1): an integer divided by an
        integer is a decimal, where Cypher's is an integer. Over xsd:float or xsd:double a zero
        divisor answers INF or NaN; over integers and decimals it is an error, which leaves a BIND
        unbound and a FILTER false, as null does (the TRY_CAST of a cast that fails is the same)."""
        quotient = Binary("/", FunctionCall("toFloat", [dividend]), divisor)
        if floating:
            return quotient
        return Case(
            subject=None,
            whens=[
                (Binary("=", copy.deepcopy(divisor), Literal(0, "number")), Literal(None, "null"))
            ],
            default=quotient,
        )

    def _is_floating(self, arg: Any, bindings) -> bool:
        """Whether ``arg`` is an xsd:float or xsd:double, as far as the query says when it is read:
        a literal or a column of that datatype, a cast to one, or arithmetic over one."""
        floating = (f"{XSD}double", f"{XSD}float")
        if isinstance(arg, RdfLiteral):
            return str(arg.datatype) in floating
        if isinstance(arg, RdfVariable):
            return str(arg) in bindings and bindings[str(arg)][1] in floating
        name = getattr(arg, "name", None)
        if name == "Function":
            return str(arg.iri) in floating
        if name in ("AdditiveExpression", "MultiplicativeExpression"):
            return any(self._is_floating(a, bindings) for a in [arg.expr, *(arg.other or [])])
        if name in ("UnaryMinus", "UnaryPlus"):
            return self._is_floating(arg.expr, bindings)
        return False

    def _datatype_of(self, arg: Any, bindings) -> str:
        """The datatype ``arg`` is known by when the query is read: a literal's own, or the
        column's of a variable bound to one."""
        if isinstance(arg, RdfLiteral):
            if arg.datatype is not None:
                return str(arg.datatype)
            return f"{XSD}string" if arg.language is None else f"{RDF}langString"
        if isinstance(arg, RdfVariable) and str(arg) in bindings:
            datatype = bindings[str(arg)][1]
            if datatype is not None:
                return datatype
        raise SparqlRefused("isNUMERIC takes a literal or a variable bound to a column")

    def _replace(self, e: Any, bindings, labels) -> CypherExpr:
        """REPLACE as a replacement of every match in the universal regex dialect (REQ-2142),
        written in each engine's flavour; its pattern and replacement are literals."""
        if not (isinstance(e.pattern, RdfLiteral) and isinstance(e.replacement, RdfLiteral)):
            raise SparqlRefused("REPLACE takes its pattern and replacement as literals")
        if e.flags is not None and not (
            isinstance(e.flags, RdfLiteral) and isinstance(e.flags.value, str)
        ):
            raise SparqlRefused("REPLACE flags are given as a string literal")
        given = str(e.flags) if e.flags is not None else ""
        if set(given) - set("qx"):
            raise SparqlRefused(
                f"REPLACE flags {''.join(sorted(set(given) - set('qx')))} are not supported; "
                "q and x are"
            )
        pattern = str(e.pattern)
        if "q" in given:
            pattern = re.escape(pattern)
        elif "x" in given:
            pattern = re.sub(r"(\\.)|\s", lambda m: m.group(1) or "", pattern)
        replacement = str(e.replacement)
        try:
            universal_regex.check(pattern, replacement)
        except universal_regex.NotUniversal as exc:
            raise SparqlRefused(f"REPLACE's {exc.field}: {exc}") from exc  # type: ignore[attr-defined]
        text = self.expr(e.arg, bindings, labels)
        # A text the pattern does not match is itself.
        return FunctionCall(
            UNIVERSAL_REGEX_REPLACE,
            [text, Literal(pattern, "string"), Literal(replacement, "string"), text],
        )

    def _regex_search(self, pattern: Any, flags: Any, bindings, labels) -> CypherExpr:
        """REGEX's pattern as a match of the whole text, which ``=~`` is: found anywhere in it,
        under its flags (XPath's: i, s and m inline; q the pattern taken as its text; x its
        whitespace dropped)."""
        if flags is not None and not (
            isinstance(flags, RdfLiteral) and isinstance(flags.value, str)
        ):
            raise SparqlRefused("REGEX flags are given as a string literal")
        given = str(flags) if flags is not None else ""
        unknown = set(given) - set("ismqx")
        if unknown:
            raise SparqlRefused(
                f"REGEX flags {''.join(sorted(unknown))} are not XPath's i, s, m, q, x"
            )
        inline = "".join(f for f in "ism" if f in given)
        literal = isinstance(pattern, RdfLiteral) and isinstance(pattern.value, str)
        if ("q" in given or "x" in given) and not literal:
            raise SparqlRefused("REGEX flags q and x take a pattern written as a literal")
        if literal:
            text = str(pattern)
            if "q" in given:
                text = re.escape(text)
            elif "x" in given:
                text = re.sub(r"(\\.)|\s", lambda m: m.group(1) or "", text)
            body: CypherExpr = Literal(text, "string")
        else:
            body = self.expr(pattern, bindings, labels)
        opened = f"(?s:.*)(?{inline}:" if inline else "(?s:.*)(?:"
        parts: list[CypherExpr] = [Literal(opened, "string"), body, Literal(")(?s:.*)", "string")]
        if all(isinstance(x, Literal) for x in parts):
            return Literal("".join(str(x.value) for x in parts if isinstance(x, Literal)), "string")
        return FunctionCall("concat", parts)

    # -- the statement --------------------------------------------------------------------

    def _aggregate(
        self, a: Any, bindings, labels, input_: tuple[CypherExpr, str | None] | None = None
    ) -> tuple[CypherExpr, str | None]:
        """One SPARQL aggregate as the IR's call, with the datatype of its result, reading
        ``input_`` (a value and its datatype) when given, else its own operand."""
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
        if input_ is not None:
            arg, datatype = input_
        elif var is not None and var in labels:
            if name not in ("count",):
                raise SparqlRefused(
                    f"?{var} is a subject; COUNT it, or aggregate one of its properties"
                )
            arg = self._key_of(var, labels)
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
        column = subject_key(nm)[0]
        if column not in by_column:
            raise SparqlRefused(f"{nm.table_name}'s key column {column} is not readable")
        return Property(Variable(var), by_column[column])

    def _group_keys(self, keys: list[str], bindings, labels) -> list[CypherExpr]:
        out: list[CypherExpr] = []
        for key in keys:
            if key in labels:
                nm = self._node(labels[key])
                by_column = {col: prop for prop, col in nm.properties.items()}
                for column in subject_key(nm):
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

    def _solution(
        self,
        group: _Group,
        labels: dict[str, str],
        reads: dict[int, Predicate],
        through_rows: bool,
    ) -> tuple[list[MatchStep], dict[str, tuple[CypherExpr, str | None]], dict[str, str]]:
        """The steps of ``group`` under one interpretation, what each variable is bound to, and
        the labels with its OPTIONALs' nodes."""
        self._placed = set()
        self._rows = 0
        self._opened = _opened_vars(group)
        labels = dict(labels)
        bindings: dict[str, tuple[CypherExpr, str | None]] = {}
        # A group's FILTER holds over its OPTIONALs' answers too, and a BIND may read what they
        # bind: both are read once the OPTIONALs are lowered, on the group's own MATCH.
        deferred = _Deferred() if group.optionals else None
        steps = [
            self.lower_group(
                group,
                labels,
                bindings,
                optional=False,
                deferred=deferred,
                reads=reads,
                through_rows=through_rows,
            )
        ]
        labels = self._lower_optionals(group, labels, bindings, steps)
        conditions: list[CypherExpr] = []
        if deferred is not None:
            for var, expr in deferred.binds:
                bindings[var] = self._bound(var, expr, bindings, labels)
            for f in deferred.filters:
                conditions.append(self.expr(f, bindings, labels))
                _holds_somewhere(conditions[-1])
        for opt in group.absent:
            conditions.extend(self._absent(opt, labels, bindings))
        first = steps[0]
        if first.where is not None:
            conditions.insert(0, first.where.expression)
        if conditions:
            cond = conditions[0]
            for c in conditions[1:]:
                cond = Binary("AND", cond, c)
            first.where = WhereClause(cond)
        return steps, bindings, labels

    def _rebound(self, aggregation: "_Aggregation", bindings, labels) -> CypherExpr | None:
        """Bind the projection's rebindings over the aggregates in ``bindings``: its HAVING."""
        having: CypherExpr | None = None
        for var, expr in aggregation.rebinds:
            source = (
                aggregation.aggregates.get(str(expr)) if isinstance(expr, RdfVariable) else None
            )
            if source is not None and source.name == "Aggregate_Sample" and str(source.vars) == var:
                continue
            dt = (
                bindings[str(expr)][1]
                if isinstance(expr, RdfVariable) and str(expr) in bindings
                else None
            )
            bindings[var] = (self.expr(expr, bindings, labels), dt)
        for h in aggregation.having:
            c = self.expr(h, bindings, labels)
            having = c if having is None else Binary("AND", having, c)
        return having

    def _term(self, var: str, bindings, labels) -> CypherExpr:
        """``var`` as an RDF term of the answer."""
        if var in labels:
            return Term(Variable(var))
        bound = bindings.get(var)
        if bound is None:
            return Term(Literal(None, "null"))
        return Term(bound[0], datatype=bound[1])

    def _order_lanes(self, e: Any, bindings, labels) -> list[CypherExpr]:
        """An ORDER BY over a variable predicate's variables as what every part can sort by, each
        part of another kind: its kind (unbound, then IRIs, then numbers and other literals, as
        SPARQL 1.1 15.1 has it), its number and its text."""
        no_number = FunctionCall("toFloat", [Literal(None, "null")])
        no_text = FunctionCall("toString", [Literal(None, "null")])
        if isinstance(e, RdfVariable):
            var = str(e)
            if var in labels:
                key = FunctionCall("toString", [self._key_of(var, labels)])
                return [Literal(1, "number"), no_number, key]
            bound = bindings.get(var)
            if bound is None:
                return [Literal(0, "number"), no_number, no_text]
            if isinstance(bound[0], IriConst):
                return [Literal(1, "number"), no_number, Literal(bound[0].iri, "string")]
        value = self.expr(e, bindings, labels)
        if self._is_numeric(e, bindings):
            return [Literal(2, "number"), FunctionCall("toFloat", [value]), no_text]
        return [Literal(3, "number"), no_number, FunctionCall("toString", [value])]

    def statement(
        self,
        group: _Group,
        labels: dict[str, str],
        mods: dict,
        form: str,
        template,
        aggregation: "_Aggregation | None" = None,
        reads: dict[int, Predicate] | None = None,
        through_rows: bool = False,
    ):
        """One UNION ALL part: ``group`` under one interpretation, its variable predicates standing
        for ``reads``, answered as ``form``."""
        steps, bindings, labels = self._solution(group, labels, reads or {}, through_rows)

        group_keys: list[CypherExpr] | None = None
        having: CypherExpr | None = None
        if aggregation is not None:
            group_keys = self._group_keys(aggregation.keys, bindings, labels)
            for res, a in aggregation.aggregates.items():
                # A SAMPLE of a key is the key itself: the projection's way of naming it.
                if a.name == "Aggregate_Sample" and str(a.vars) in aggregation.keys:
                    continue
                bindings[res] = self._aggregate(a, bindings, labels)
            having = self._rebound(aggregation, bindings, labels)
            # A subject that is not a key is no value of the group.
            labels = {v: lab for v, lab in labels.items() if v in aggregation.keys}

        ast = CypherAST(
            pipeline=list(steps), return_clause=None, order_by=[], skip=None, limit=None
        )
        if form == "CONSTRUCT":
            ast.construct = [
                TripleTemplate(
                    *(  # type: ignore[arg-type]
                        self._template_term(x, lambda v: self._term(v, bindings, labels))
                        for x in triple
                    )
                )
                for triple in template
            ]
            return ast
        if form == "ASK":
            ast.return_clause = ReturnClause([ReturnItem(Literal(1, "number"), "ask", name="ask")])
            return ast
        items = [ReturnItem(self._term(v, bindings, labels), v, name=v) for v in mods["project"]]
        for i, cond in enumerate(mods.get("order", [])):
            if i in mods.get("lanes", ()):
                lanes = self._order_lanes(cond.expr, bindings, labels)
                for suffix, lane in zip(_ORDER_LANES, lanes, strict=True):
                    items.append(ReturnItem(lane, f"{_ORDER_PREFIX}{i}{suffix}", name=""))
                continue
            items.append(
                ReturnItem(self.expr(cond.expr, bindings, labels), f"{_ORDER_PREFIX}{i}", name="")
            )
        ast.return_clause = ReturnClause(
            items, distinct=bool(mods.get("distinct")), group_by=group_keys, having=having
        )
        return ast

    def aggregate_rows(
        self,
        spec: tuple[_Group, dict[str, str], dict[int, Predicate], bool],
        aggregation: "_Aggregation",
        inputs: dict[str, tuple[set[str | None], set[str | None]]],
    ) -> CypherAST:
        """One part's rows of an aggregate over a variable predicate: each key as its term's text,
        which compares across parts as no JSON does on every engine, and each aggregate's input.
        ``inputs`` gains the kinds and datatypes each aggregate reads across the parts."""
        group, part_labels, reads, through_rows = spec
        steps, bindings, labels = self._solution(group, part_labels, reads, through_rows)
        items = [
            ReturnItem(
                FunctionCall("toString", [self._term(key, bindings, labels)]),
                f"{_GROUP_PREFIX}{i}",
                name=f"{_GROUP_PREFIX}{i}",
            )
            for i, key in enumerate(aggregation.keys)
        ]
        for j, (res, a) in enumerate(aggregation.aggregates.items()):
            if str(a.vars) == "*" or (
                a.name == "Aggregate_Sample" and str(a.vars) in aggregation.keys
            ):
                continue
            kinds, datatypes = inputs.setdefault(res, (set(), set()))
            operand = a.vars
            var = str(operand) if isinstance(operand, RdfVariable) else None
            if a.name == "Aggregate_Count":
                term = (
                    self._term(var, bindings, labels)
                    if var is not None
                    else Term(self.expr(operand, bindings, labels))
                )
                value: CypherExpr = FunctionCall("toString", [term])
            else:
                side = self._side(operand, bindings, labels)
                value = Literal(None, "null")
                if side is not None and side[0] == _VALUE:
                    kinds.add(side[1] if var is not None else None)
                    datatypes.add(bindings[var][1] if var is not None else None)
                    value = self.expr(operand, bindings, labels)
                elif side is not None:
                    kinds.add(side[0])
                    if side[0] == _IRI:
                        value = Literal(side[1], "string")
            items.append(ReturnItem(value, f"{_INPUT_PREFIX}{j}", name=f"{_INPUT_PREFIX}{j}"))
        if not items:
            items.append(ReturnItem(Literal(1, "number"), "one", name="one"))
        return CypherAST(
            pipeline=list(steps),
            return_clause=ReturnClause(items),
            order_by=[],
            skip=None,
            limit=None,
        )

    def aggregate_over(
        self,
        rows: CypherAST,
        aggregation: "_Aggregation",
        inputs: dict[str, tuple[set[str | None], set[str | None]]],
        mods: dict,
    ) -> CypherAST:
        """The statement grouping the parts' rows ``rows`` (:meth:`aggregate_rows`). A key is its
        term's text, so only the answer and an ORDER BY of it read one."""
        keys = {key: Variable(f"{_GROUP_PREFIX}{i}") for i, key in enumerate(aggregation.keys)}
        samples = {
            res
            for res, a in aggregation.aggregates.items()
            if a.name == "Aggregate_Sample" and str(a.vars) in keys
        }
        reading = [*aggregation.having, *(e for _, e in aggregation.rebinds)]
        read = {v for e in reading if not isinstance(e, RdfVariable) for v in _expr_vars(e)}
        if read & ({*keys, *samples}):
            raise SparqlRefused(
                "A key grouped across the predicates of a variable predicate is of no one kind: "
                "an expression can't read it; project it, or name the predicate"
            )
        bindings: dict[str, tuple[CypherExpr, str | None]] = {}
        for j, (res, a) in enumerate(aggregation.aggregates.items()):
            if a.name == "Aggregate_Sample" and str(a.vars) in aggregation.keys:
                continue
            if str(a.vars) == "*":
                bindings[res] = self._aggregate(a, {}, {})
                continue
            column = Variable(f"{_INPUT_PREFIX}{j}")
            kinds, datatypes = inputs.get(res, (set(), set()))
            name = a.name.removeprefix("Aggregate_").upper()
            if len(kinds) > 1:
                raise SparqlRefused(
                    f"{name} reads values of several kinds across the predicates of a variable "
                    "predicate; FILTER them to one, or name the predicate"
                )
            if _NODE in kinds:
                raise SparqlRefused(
                    f"{name} reads subjects across the predicates of a variable predicate; "
                    "COUNT them, or aggregate one of their properties"
                )
            datatype = next(iter(datatypes)) if len(datatypes) == 1 else None
            bindings[res] = self._aggregate(a, {}, {}, input_=(column, datatype))
        having = self._rebound(aggregation, bindings, {})
        items = [
            ReturnItem(
                Term(keys[v]) if v in keys else self._term(v, bindings, {}),
                v,
                name=v,
            )
            for v in mods["project"]
        ]
        for i, cond in enumerate(mods.get("order", [])):
            expr = cond.expr
            ordered = (
                keys[str(expr)]
                if isinstance(expr, RdfVariable) and str(expr) in keys
                else self.expr(expr, bindings, {})
            )
            items.append(ReturnItem(ordered, f"{_ORDER_PREFIX}{i}", name=""))
        return CypherAST(
            pipeline=[],
            return_clause=ReturnClause(
                items,
                distinct=bool(mods.get("distinct")),
                group_by=list(keys.values()),
                having=having,
            ),
            order_by=[],
            skip=None,
            limit=None,
            parts=rows,
        )

    def _lower_optionals(
        self,
        group: _Group,
        labels: dict[str, str],
        bindings: dict[str, tuple[CypherExpr, str | None]],
        steps: list[MatchStep],
    ) -> dict[str, str]:
        """``group``'s OPTIONALs, each an OPTIONAL MATCH after the steps it extends, and the
        OPTIONALs nested in each after it: an inner OPTIONAL extends its outer one's solutions
        (REQ-2135). Returns the labels with the OPTIONALs' nodes."""
        for opt in group.optionals:
            if any(
                _is_var(s) and str(s) in bindings and str(s) not in labels
                for s, _, _ in opt.triples
            ):
                continue  # its subject is a value in this part: a literal has no triples
            opt_labels = self._optional_labels(opt, labels)
            steps.append(self.lower_group(opt, {**labels, **opt_labels}, bindings, optional=True))
            labels = self._lower_optionals(opt, {**labels, **opt_labels}, bindings, steps)
        return labels

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


def _describe_construct(algebra: Any) -> str:
    """The CONSTRUCT of a ``DESCRIBE <iri> ...``: each resource with every triple the model says
    of it, read through its class's predicates as ``<iri> ?p ?o`` reads them. A DESCRIBE over a
    pattern's variables is refused by name."""
    resources = [t for t in algebra.PV if isinstance(t, URIRef)]
    if len(resources) != len(algebra.PV):
        raise SparqlRefused("A DESCRIBE of a variable is not supported; DESCRIBE a resource's IRI")
    triples = [f"{r.n3()} ?p{i} ?o{i}" for i, r in enumerate(resources)]
    where = " UNION ".join(f"{{ {t} }}" for t in triples)
    return f"CONSTRUCT {{ {' . '.join(triples)} }} WHERE {{ {where} }}"


def compile_sparql(text: str, vocab: Vocabulary, lm: Any) -> CompiledSparql:
    """``text``, a SPARQL SELECT, ASK, CONSTRUCT or DESCRIBE of a resource, as the graph IR, read
    against ``vocab`` (built from the label map ``lm``)."""
    try:
        query = translateQuery(parseQuery(text))
    except Exception as exc:  # rdflib raises pyparsing's and its own errors alike
        raise SparqlRefused(f"The query is not SPARQL this endpoint reads: {exc}") from exc
    algebra = query.algebra
    if algebra.name == "DescribeQuery":
        return compile_sparql(_describe_construct(algebra), vocab, lm)
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

    limit, allowed = union_allowance(
        mods.get("limit") is not None
        and not mods.get("order")
        and not mods.get("distinct")
        and aggregation is None
    )
    lowering = _Lowering(vocab, lm, limit, allowed)
    specs = []
    combinations = 0
    alternatives = _alternatives(pattern)
    for group in alternatives:
        for expanded in lowering.expand_axiom_joins(group):
            for labels in lowering.interpretations(expanded, {}):
                combinations += 1
                specs.extend(lowering.predicate_reads(expanded, labels, mods, aggregation))
    opened = {v for group in alternatives for v in _opened_vars(group)}
    mods["lanes"] = {i for i, c in enumerate(mods.get("order", [])) if _expr_vars(c.expr) & opened}
    over_parts = aggregation is not None and bool(opened)
    if aggregation is not None and not over_parts and len(specs) > 1:
        raise SparqlRefused(
            "An aggregate over subjects of several classes (or a UNION) is not supported yet; "
            "give the subjects one class"
        )
    parts: list[CypherAST] = []
    inputs: dict[str, tuple[set[str | None], set[str | None]]] = {}
    for spec in specs:
        try:
            if over_parts:
                assert aggregation is not None
                parts.append(lowering.aggregate_rows(spec, aggregation, inputs))
            else:
                flat, part_labels, reads, through_rows = spec
                parts.append(
                    lowering.statement(
                        flat, part_labels, mods, form, template, aggregation, reads, through_rows
                    )
                )
        except _NoSolution:
            continue
    if combinations > allowed:
        raise SparqlRefused(
            union_refusal(
                f"The query matches {combinations} combinations of classes", limit, allowed
            )
            + "; give its subjects classes, or raise the limit (graph.union_limit)"
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
    if over_parts:
        assert aggregation is not None
        head = lowering.aggregate_over(head, aggregation, inputs, mods)
        mods["lanes"] = set()
    head.order_by = [
        OrderItem(Variable(name), "DESC" if c.order == "DESC" else "ASC")
        for i, c in enumerate(mods.get("order", []))
        for name in (
            [f"{_ORDER_PREFIX}{i}{lane}" for lane in _ORDER_LANES]
            if i in mods["lanes"]
            else [f"{_ORDER_PREFIX}{i}"]
        )
    ]
    head.skip = mods.get("offset")
    head.limit = 1 if form == "ASK" else mods.get("limit")
    head.modifiers_of_whole = True
    return CompiledSparql(form, head, variables)
