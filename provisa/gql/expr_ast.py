# Copyright (c) 2026 Kenneth Stott
# Canary: 57c9c2f9-efe1-4ede-8360-205e2b97ad23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher *expression* AST (REQ-913).

The clause parser (``parser.py``) historically captured expression text as a raw string that the
translator then massaged into SQL with a stack of regex passes. This module gives expressions a real
AST so the translation can be node-to-node (Cypher expr AST -> sqlglot expr AST), with no text
rewriting. ``expr_parser.parse_expression`` produces these nodes; a visitor in the translator lowers
them to ``sqlglot.exp``.

The grammar this AST covers (the regression target for the regex pipeline it replaces):
literals (number/string/bool/null/list/map), variables, parameters ``$p``, property access ``n.p``,
label predicate ``n:Label``, unary ``-``/``NOT``, binary arithmetic/comparison, ``AND/OR/XOR``,
``IS [NOT] NULL``, ``IN``, ``STARTS WITH`` / ``ENDS WITH`` / ``CONTAINS`` / ``=~``, index ``l[i]`` and
slice ``l[a..b]``, function calls (incl. ``count(*)`` and ``DISTINCT`` args), ``CASE`` (simple and
searched), map projection ``n{.a, x: e, .*}``, list comprehension ``[x IN l WHERE p | e]``, pattern
comprehension ``[(a)-[r]->(b) WHERE p | e]``, and subquery expressions ``EXISTS/COUNT/COLLECT { … }``.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from provisa.gql.ir import CypherAST, PathPattern


class CypherExpr:
    """Base class for every Cypher expression node."""


# --- leaves --------------------------------------------------------------- #


@dataclass
class Literal(CypherExpr):
    """A scalar literal. ``kind`` is one of number/string/boolean/null."""

    value: object
    kind: str


@dataclass
class Variable(CypherExpr):
    name: str


@dataclass
class Parameter(CypherExpr):
    name: str  # the ``$name`` without the leading ``$``


# --- access / structure --------------------------------------------------- #


@dataclass
class Property(CypherExpr):
    """``obj.name`` — a property access (or, structurally, a qualified column)."""

    obj: CypherExpr
    name: str


@dataclass
class Index(CypherExpr):
    """``obj[index]`` — list/map element access."""

    obj: CypherExpr
    index: CypherExpr


@dataclass
class Slice(CypherExpr):
    """``obj[start..stop]`` — either bound may be None (open)."""

    obj: CypherExpr
    start: CypherExpr | None
    stop: CypherExpr | None


@dataclass
class ListLiteral(CypherExpr):
    items: list[CypherExpr]


@dataclass
class MapLiteral(CypherExpr):
    entries: list[tuple[str, CypherExpr]]


# --- operators ------------------------------------------------------------ #


@dataclass
class Paren(CypherExpr):
    """An explicitly parenthesized subexpression — preserved so operator precedence written by the
    user survives lowering (sqlglot does not reinsert precedence parens on its own)."""

    inner: CypherExpr


@dataclass
class Unary(CypherExpr):
    """``op operand`` — ``op`` is ``-`` or ``NOT``."""

    op: str
    operand: CypherExpr


@dataclass
class Binary(CypherExpr):
    """``left op right`` — arithmetic, comparison, boolean, and string predicates.

    ``op`` is upper-cased for word operators (AND/OR/XOR/IN/CONTAINS/STARTS WITH/ENDS WITH/=~) and
    kept verbatim for symbolic ones (+ - * / % = <> < <= > >=).
    """

    op: str
    left: CypherExpr
    right: CypherExpr


@dataclass
class IsNull(CypherExpr):
    operand: CypherExpr
    negated: bool


@dataclass
class LabelPredicate(CypherExpr):
    """``operand:Label`` — a node/relationship label test used in expression position."""

    operand: CypherExpr
    labels: list[str]


# --- calls / case --------------------------------------------------------- #


@dataclass
class FunctionCall(CypherExpr):
    name: str
    args: list[CypherExpr]
    distinct: bool = False
    star: bool = False  # ``count(*)``


@dataclass
class Case(CypherExpr):
    """CASE. ``subject`` is set for the simple form (``CASE x WHEN v THEN r``)."""

    subject: CypherExpr | None
    whens: list[tuple[CypherExpr, CypherExpr]]
    default: CypherExpr | None


# --- comprehensions / projections / subqueries ---------------------------- #


@dataclass
class ListComprehension(CypherExpr):
    """``[var IN source WHERE predicate | projection]`` — predicate/projection optional."""

    var: str
    source: CypherExpr
    predicate: CypherExpr | None
    projection: CypherExpr | None


@dataclass
class PatternComprehension(CypherExpr):
    """``[path_var = pattern WHERE predicate | projection]`` — ``path_var`` binds the whole matched
    path when the ``p =`` prefix is present."""

    pattern: PathPattern
    predicate: CypherExpr | None
    projection: CypherExpr
    path_var: str | None = None


@dataclass
class MapProjection(CypherExpr):
    """``var{.a, alias: expr, .*}`` — ``all_props`` is the ``.*`` selector."""

    var: str
    properties: list[str] = field(default_factory=list)
    literal_entries: list[tuple[str, CypherExpr]] = field(default_factory=list)
    all_props: bool = False


@dataclass
class Quantifier(CypherExpr):
    """``all/any/none/single(var IN source WHERE predicate)`` — a list quantifier predicate."""

    kind: str  # ALL | ANY | NONE | SINGLE
    var: str
    source: CypherExpr
    predicate: CypherExpr | None


@dataclass
class Reduce(CypherExpr):
    """``reduce(acc = init, var IN source | step)`` — a fold over a list."""

    accumulator: str
    init: CypherExpr
    var: str
    source: CypherExpr
    step: CypherExpr


@dataclass
class SubqueryExpr(CypherExpr):
    """``EXISTS { … }`` / ``COUNT { … }`` / ``COLLECT { … }`` — its body the statement it is."""

    kind: str  # EXISTS | COUNT | COLLECT
    body: CypherAST


# --- RDF values (REQ-2135) ----------------------------------------------- #


@dataclass
class IriConst(CypherExpr):
    """An absolute IRI written in the query (``<http://…>``, ``skos:Concept``). It is a value of
    the answer only; one that names a subject is parsed back to its table and keys by the front
    end (:func:`provisa.gql.iri.parse_subject_iri`) before the back end sees it."""

    iri: str


@dataclass
class TypedLiteral(CypherExpr):
    """A literal with a datatype IRI (``"42"^^xsd:integer``) or a language tag (``"chat"@fr``),
    lowered to the SQL value of its datatype."""

    lexical: str
    datatype: str | None = None
    lang: str | None = None


@dataclass
class Term(CypherExpr):
    """``value`` as an RDF term of the answer: a node variable as its subject (its table and key
    values, the IRI built outside SQL), an IriConst as that IRI, anything else as a literal with
    ``datatype`` or ``lang`` when given. Lowered to one JSON value (:mod:`provisa.gql.terms`)."""

    value: CypherExpr
    datatype: str | None = None
    lang: str | None = None


AnyExpr = Union[
    Literal,
    Variable,
    Parameter,
    Property,
    Index,
    Slice,
    ListLiteral,
    MapLiteral,
    Unary,
    Binary,
    IsNull,
    LabelPredicate,
    FunctionCall,
    Case,
    ListComprehension,
    PatternComprehension,
    MapProjection,
    Quantifier,
    Reduce,
    SubqueryExpr,
    IriConst,
    TypedLiteral,
    Term,
]


def walk(node: CypherExpr) -> "Iterator[CypherExpr]":
    """``node`` and every expression under it, in the order a statement writes them (each node
    before its operands, operands left to right)."""
    yield node
    for f in fields(node):  # type: ignore[arg-type]
        yield from _walk_value(getattr(node, f.name))


def _walk_value(value: object) -> "Iterator[CypherExpr]":
    if isinstance(value, CypherExpr):
        yield from walk(value)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _walk_value(item)
