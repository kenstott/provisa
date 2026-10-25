# Copyright (c) 2026 Kenneth Stott
# Canary: f56b62d4-6545-49da-984c-b3dae35cb00a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lower a ``CypherExpr`` AST to a ``sqlglot`` expression AST (REQ-913).

This is the node-to-node replacement for the translator's regex text pipeline: every Cypher construct
becomes ``sqlglot.exp`` structurally, never by editing SQL text. Constructs whose lowering needs
translator state (how a variable/property resolves to a physical column, how ``$param`` binds, how a
graph pattern subquery is built) are delegated to an ``ExprContext`` the translator supplies; every
context-free construct (operators, literals, CASE, list ops, comprehensions→lambda) is lowered here.

Each string predicate matches as Cypher says, its value taken literally: ``STARTS WITH →
starts_with()``, ``ENDS WITH → (right(x, length(y)) = y)``, ``CONTAINS → (strpos(x, y) > 0)``, and
``=~`` a match of the whole string, ``regexp_like(x, '^(?:y)$')``,
``^ → POWER()``, list comprehensions/quantifiers/reduce → the engine's higher-order list functions.
"""

from __future__ import annotations

import re
from typing import Protocol, cast

import sqlglot.expressions as exp

from provisa.gql.mistakes import invalid_regex, regex_problem, type_mismatch
from provisa.gql.expr_ast import (
    Binary,
    Case,
    CypherExpr,
    FunctionCall,
    Index,
    IriConst,
    IsNull,
    LabelPredicate,
    ListComprehension,
    ListLiteral,
    Literal,
    MapLiteral,
    MapProjection,
    Paren,
    Parameter,
    PatternComprehension,
    Property,
    Quantifier,
    Reduce,
    Slice,
    SubqueryExpr,
    Term,
    TypedLiteral,
    Unary,
    Variable,
)
from provisa.gql.translator_helpers import _and_all
from provisa.gql.translator_types import CypherTranslateError

#: A regex's leading inline flags, which PostgreSQL takes only at the regex's start.
_REGEX_FLAGS = re.compile(r"^\(\?[a-zA-Z]+\)")


def _whole_match(pattern: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """``pattern`` matching the whole string, as Cypher's ``=~`` does, where regexp_like finds
    the pattern anywhere in it: ``^(?:pattern)$``, any leading inline flags kept first."""
    if isinstance(pattern, exp.Literal) and pattern.is_string:
        text = pattern.this
        flags = _REGEX_FLAGS.match(text)
        head = flags.group() if flags else ""
        return exp.Literal.string(f"{head}^(?:{text[len(head) :]})$")
    return exp.Concat(expressions=[exp.Literal.string("^(?:"), pattern, exp.Literal.string(")$")])


_XSD = "http://www.w3.org/2001/XMLSchema#"
# REQ-2135: a typed literal's datatype as the SQL value it lowers to. A number is written as one;
# the others are cast from their lexical form.
_XSD_NUMBERS = {
    f"{_XSD}{t}"
    for t in (
        "integer",
        "int",
        "long",
        "short",
        "byte",
        "nonNegativeInteger",
        "positiveInteger",
        "nonPositiveInteger",
        "negativeInteger",
        "unsignedInt",
        "unsignedLong",
        "unsignedShort",
        "unsignedByte",
        "decimal",
        "double",
        "float",
    )
}
_XSD_CASTS = {f"{_XSD}date": "DATE", f"{_XSD}dateTime": "TIMESTAMP", f"{_XSD}time": "TIME"}


class ExprContext(Protocol):
    """Translator-supplied resolution for the context-dependent leaves of an expression."""

    def resolve_variable(self, name: str) -> exp.Expression:
        """A bare variable ``n`` — the column/expression it denotes in the current query scope."""
        ...

    def resolve_property(self, obj: exp.Expression, name: str) -> exp.Expression:
        """A property access ``obj.name`` lowered against the object's already-lowered expression."""
        ...

    def property_is_table_column(self, obj: exp.Expression, name: str) -> bool:
        """Whether ``obj.name`` is a registered table's own (typed) column, as opposed to a value
        the translator builds itself — the VARCHAR identity of a node drawn from several tables,
        or a scalar extracted from an assembled row."""
        ...

    def property_type(self, obj: exp.Expression, name: str) -> str | None:
        """The registered data type of ``obj.name`` where it is a registered table's column."""
        ...

    def resolve_parameter(self, name: str) -> exp.Expression:
        """A ``$param`` reference — the positional placeholder it binds to."""
        ...

    def resolve_term(self, node: Term) -> exp.Expression:
        """``node.value`` as one RDF term of the answer (REQ-2135)."""
        ...

    def resolve_function(
        self, name: str, args: list[exp.Expression], *, distinct: bool
    ) -> exp.Expression:
        """A function call after its args are lowered (Cypher→engine renames/casts live here)."""
        ...

    def resolve_label_predicate(self, operand: exp.Expression, labels: list[str]) -> exp.Expression:
        """A label test ``n:Label`` against the lowered operand."""
        ...

    def resolve_map_projection(self, node: MapProjection) -> exp.Expression:
        """``n{.a, x: e, .*}`` — needs the variable's known properties."""
        ...

    def resolve_graph_element(self, name: str) -> exp.Expression | None:
        """A bound node or relationship variable as its JSON object (what RETURN n or RETURN r
        answers), None for any other name."""
        ...

    def resolve_subquery(self, node: SubqueryExpr) -> exp.Expression:
        """``EXISTS/COUNT/COLLECT { … }`` — builds the correlated subquery."""
        ...

    def resolve_graph_identity(
        self, name: str
    ) -> tuple[tuple[str, ...], list[exp.Expression]] | None:
        """A bound node or relationship variable's identity (#305): what kind of element it is (a
        node's table, a relationship's type and ends' tables) and its keys, in one order for
        every element of that kind; None for any other name."""
        ...

    def resolve_graph_absent(self, name: str) -> exp.Expression | None:
        """Where a bound node or relationship variable is null (an OPTIONAL MATCH bound none),
        None for any other name (#304)."""
        ...

    def resolve_pattern_comprehension(self, node: PatternComprehension) -> exp.Expression:
        """``[(a)-[r]->(b) WHERE p | e]`` — builds the correlated path subquery."""
        ...


_BINARY_ARITH: dict[str, type[exp.Expression]] = {
    "+": exp.Add,
    "-": exp.Sub,
    "*": exp.Mul,
    "/": exp.Div,
    "%": exp.Mod,
}
_BINARY_CMP: dict[str, type[exp.Expression]] = {
    "=": exp.EQ,
    "<>": exp.NEQ,
    "!=": exp.NEQ,
    "<": exp.LT,
    "<=": exp.LTE,
    ">": exp.GT,
    ">=": exp.GTE,
}


def _lam(var: str, body: exp.Expression) -> exp.Lambda:
    """An engine lambda ``var -> body`` (sqlglot ``Lambda``)."""
    return exp.Lambda(this=body, expressions=[exp.to_identifier(var)])


def _anon(name: str, *args: exp.Expression) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=list(args))


class ExprLowering:
    """Lowers ``CypherExpr`` nodes to ``sqlglot.exp`` using an ``ExprContext`` for scoped leaves."""

    def __init__(self, ctx: ExprContext) -> None:
        self._ctx = ctx

    def lower(self, node: CypherExpr) -> exp.Expression:
        method = getattr(self, f"_lower_{type(node).__name__}", None)
        if method is None:
            raise NotImplementedError(f"no lowering for {type(node).__name__}")
        return method(node)

    # -- leaves ------------------------------------------------------------ #

    def _lower_Literal(self, node: Literal) -> exp.Expression:
        if node.kind == "null":
            return exp.Null()
        if node.kind == "boolean":
            return exp.true() if node.value else exp.false()
        if node.kind == "string":
            return exp.Literal.string(node.value)
        return exp.Literal.number(node.value)

    def _lower_Variable(self, node: Variable) -> exp.Expression:
        return self._ctx.resolve_variable(node.name)

    def _lower_Parameter(self, node: Parameter) -> exp.Expression:
        return self._ctx.resolve_parameter(node.name)

    def _lower_Property(self, node: Property) -> exp.Expression:
        if isinstance(node.obj, MapLiteral):
            # A map literal's key is its entry's own value; a key it has not is null (#287).
            for key, value in node.obj.entries:
                if key == node.name:
                    return self.lower(value)
            return exp.Null()
        return self._ctx.resolve_property(self.lower(node.obj), node.name)

    def _lower_LabelPredicate(self, node: LabelPredicate) -> exp.Expression:
        return self._ctx.resolve_label_predicate(self.lower(node.operand), node.labels)

    # -- operators --------------------------------------------------------- #

    def _lower_Paren(self, node: Paren) -> exp.Expression:
        return exp.Paren(this=self.lower(node.inner))

    def _lower_Unary(self, node: Unary) -> exp.Expression:
        operand = self.lower(node.operand)
        if node.op == "NOT":
            return exp.Not(this=operand)
        return exp.Neg(this=operand)

    def _is_text_identity(self, node: CypherExpr) -> bool:
        """``x.id`` where ``x`` is not bound to one registered table — see ``_lower_Binary``."""
        if not (isinstance(node, Property) and node.name == "id"):
            return False
        return not self._ctx.property_is_table_column(self.lower(node.obj), node.name)

    def _refuse_mismatched_comparison(self, node: Binary) -> None:
        """REQ-778: a property compared with a literal its registered type never equals or orders
        against is the user's mistake, refused here rather than by the engine."""
        for prop, lit, prop_first in (
            (node.left, node.right, True),
            (node.right, node.left, False),
        ):
            if not (isinstance(prop, Property) and isinstance(lit, Literal)):
                continue
            data_type = self._ctx.property_type(self.lower(prop.obj), prop.name)
            if data_type is None:
                continue
            mistake = type_mismatch(node.op, prop, data_type, lit, prop_first)
            if mistake is not None:
                raise mistake

    def _lower_Binary(self, node: Binary) -> exp.Expression:
        op = node.op
        if op in _BINARY_CMP:
            same = self._same_element(node)
            if same is not None:
                return same
            self._refuse_mismatched_comparison(node)
        left = self.lower(node.left)
        right = self.lower(node.right)
        if op in _BINARY_ARITH:
            return _BINARY_ARITH[op](this=left, expression=right)
        if op in ("=", "<>", "!="):
            # The `id` of a node drawn from several tables is the graph's synthetic identity —
            # engine-typed VARCHAR (built via CAST(pk AS VARCHAR) across heterogeneous node types
            # in a UNION). Comparing it to a bound parameter/literal lets the engine infer the
            # parameter's Python type (e.g. INTEGER) and coerce the VARCHAR column instead, which
            # raises a conversion error on any non-numeric id value (blank/UUID ids from other
            # node types in the same UNION). Casting the other side to VARCHAR forces string-space
            # comparison, matching the column's actual engine type.
            # A registered table's own `id` column is NOT that identity: it has the type the
            # source gave it, and what it is compared to keeps its own type (issue #130 — a
            # TEXT-cast literal against an integer column is refused by Postgres).
            if self._is_text_identity(node.left):
                right = exp.Cast(this=right, to=exp.DataType.build("text"))
            elif self._is_text_identity(node.right):
                left = exp.Cast(this=left, to=exp.DataType.build("text"))
            return _BINARY_CMP[op](this=left, expression=right)
        if op in _BINARY_CMP:
            return _BINARY_CMP[op](this=left, expression=right)
        if op == "^":
            return exp.Pow(this=left, expression=right)
        if op == "AND":
            return exp.And(this=left, expression=right)
        if op == "OR":
            return exp.Or(this=left, expression=right)
        if op == "XOR":
            return exp.Xor(this=left, expression=right)
        if op == "IN":
            return self._lower_in(left, node.right, right)
        if op == "STARTS WITH":
            return _anon("starts_with", left, right)
        if op == "ENDS WITH":
            # The value's own characters, never LIKE wildcards: x LIKE '%' || y read a % or _ in y
            # as a wildcard.
            suffix = exp.Right(this=left, expression=exp.Length(this=right.copy()))
            return exp.Paren(this=exp.EQ(this=suffix, expression=right))
        if op == "CONTAINS":
            strpos = _anon("strpos", left, right)
            return exp.Paren(this=exp.GT(this=strpos, expression=exp.Literal.number(0)))
        if op == "=~":
            pattern = node.right
            if isinstance(pattern, Literal) and pattern.kind == "string":
                # REQ-778: a literal pattern no engine takes is the user's to put right, said here.
                why = regex_problem(str(pattern.value))
                if why is not None:
                    raise invalid_regex(node.left, str(pattern.value), why)
            return _anon("regexp_like", left, _whole_match(right))
        raise NotImplementedError(f"binary operator {op!r}")

    def _same_element(self, node: Binary) -> exp.Expression | None:
        """Two node or relationship variables compared (#305): by identity, the keys of one
        table or one relationship type's ends; never equal across tables or types, and null where
        either is. None where either side is no graph element."""
        if not (isinstance(node.left, Variable) and isinstance(node.right, Variable)):
            return None
        left = self._ctx.resolve_graph_identity(node.left.name)
        right = self._ctx.resolve_graph_identity(node.right.name)
        if left is None or right is None:
            return None
        if node.op not in ("=", "<>", "!="):
            raise CypherTranslateError(
                f"{node.left.name} and {node.right.name} are nodes or relationships, which are "
                "compared by = and <> only"
            )
        (left_kind, left_keys), (right_kind, right_keys) = left, right
        if left_kind == right_kind:
            same = _and_all(
                [exp.EQ(this=a, expression=b) for a, b in zip(left_keys, right_keys, strict=True)]
            )
        else:
            # Unlike elements: never the same, and null where either is (as = of a null is).
            left_null, right_null = (
                exp.Is(this=keys[0].copy(), expression=exp.Null())
                for keys in (left_keys, right_keys)
            )
            either_null = exp.Or(this=left_null, expression=right_null)
            same = exp.Case(ifs=[exp.If(this=either_null, true=exp.Null())], default=exp.false())
        return same if node.op == "=" else exp.Not(this=exp.Paren(this=same))

    def _lower_in(
        self, left: exp.Expression, raw_right: CypherExpr, right: exp.Expression
    ) -> exp.Expression:
        # IN over a literal list becomes an IN (...) tuple; otherwise a membership over an array value.
        if isinstance(raw_right, ListLiteral):
            return exp.In(this=left, expressions=[self.lower(i) for i in raw_right.items])
        return exp.In(this=left, field=right)

    def _lower_IsNull(self, node: IsNull) -> exp.Expression:
        if isinstance(node.operand, Variable):
            # A node or relationship is null by its own row, not by a column of its name (#304).
            absent = self._ctx.resolve_graph_absent(node.operand.name)
            if absent is not None:
                return exp.Not(this=exp.Paren(this=absent)) if node.negated else absent
        is_expr = exp.Is(this=self.lower(node.operand), expression=exp.Null())
        return exp.Not(this=is_expr) if node.negated else is_expr

    # -- calls / case ------------------------------------------------------ #

    def _lower_FunctionCall(self, node: FunctionCall) -> exp.Expression:
        if node.star:
            return exp.Count(this=exp.Star())
        args = [self.lower(a) for a in node.args]
        return self._ctx.resolve_function(node.name, args, distinct=node.distinct)

    def _lower_Case(self, node: Case) -> exp.Expression:
        ifs = [exp.If(this=self.lower(cond), true=self.lower(res)) for cond, res in node.whens]
        case = exp.Case(ifs=ifs)
        if node.subject is not None:
            case.set("this", self.lower(node.subject))
        if node.default is not None:
            case.set("default", self.lower(node.default))
        return case

    # -- structures -------------------------------------------------------- #

    def _lower_ListLiteral(self, node: ListLiteral) -> exp.Expression:
        elements = [
            self._ctx.resolve_graph_element(i.name) if isinstance(i, Variable) else None
            for i in node.items
        ]
        if all(e is None for e in elements):
            return exp.Array(expressions=[self.lower(i) for i in node.items])
        # A list of nodes and relationships ([r, m]) holds each as its JSON object, and anything
        # beside them as JSON too: one list of one type.
        return exp.Array(
            expressions=[
                e if e is not None else _anon("to_json", self.lower(i))
                for i, e in zip(node.items, elements, strict=True)
            ]
        )

    def _lower_MapLiteral(self, node: MapLiteral) -> exp.Expression:
        # {k: v, ...} → MAP(ARRAY['k',...], ARRAY[to_json(v), ...]) — encode each value to a JSON value
        # so a heterogeneous map survives as a single array type (matches the text path's bare-map
        # rewrite). to_json, NOT CAST(v AS JSON): in the IR dialect (Postgres) CAST AS JSON *parses*
        # the input as JSON text, so a bare string 'Siamese' fails; to_json *encodes* any scalar.
        keys = [exp.Literal.string(k) for k, _ in node.entries]
        values = [_anon("to_json", self.lower(v)) for _, v in node.entries]
        return _anon("MAP", exp.Array(expressions=keys), exp.Array(expressions=values))

    def _lower_Index(self, node: Index) -> exp.Expression:
        # Cypher lists are 0-indexed; the engine element_at is 1-indexed → shift positive indices.
        base = self.lower(node.obj)
        idx = self.lower(node.index)
        if isinstance(node.index, Literal) and node.index.kind == "number":
            idx = exp.Literal.number(int(cast("int | float", node.index.value)) + 1)
        return _anon("element_at", base, idx)

    def _lower_Slice(self, node: Slice) -> exp.Expression:
        # Cypher's list[a..b] is 0-based with b excluded; the engine's slice(list, start, length)
        # starts at 1: start a + 1, length b - a.
        base = self.lower(node.obj)
        first = self.lower(node.start) if node.start is not None else exp.Literal.number(0)
        if node.start is None:
            start = exp.Literal.number(1)
        elif isinstance(node.start, Literal) and node.start.kind == "number":
            start = exp.Literal.number(int(cast("int | float", node.start.value)) + 1)
        else:
            start = exp.Add(this=first, expression=exp.Literal.number(1))
        if node.stop is not None:
            stop = self.lower(node.stop)
            length = stop if node.start is None else exp.Sub(this=stop, expression=first.copy())
            return _anon("slice", base, start, length)
        return _anon("slice", base, start, _anon("cardinality", base))

    # -- comprehensions / quantifiers / reduce (engine list functions) ----- #

    def _lower_ListComprehension(self, node: ListComprehension) -> exp.Expression:
        source = self.lower(node.source)
        result = source
        if node.predicate is not None:
            result = _anon("filter", result, _lam(node.var, self.lower(node.predicate)))
        if node.projection is not None:
            result = _anon("transform", result, _lam(node.var, self.lower(node.projection)))
        return result

    def _lower_Quantifier(self, node: Quantifier) -> exp.Expression:
        source = self.lower(node.source)
        pred = _lam(node.var, self.lower(node.predicate)) if node.predicate is not None else None
        if node.kind == "ANY":
            return _anon("any_match", source, pred) if pred else _anon("any_match", source)
        if node.kind == "ALL":
            return _anon("all_match", source, pred) if pred else _anon("all_match", source)
        if node.kind == "NONE":
            return _anon("none_match", source, pred) if pred else _anon("none_match", source)
        # SINGLE → cardinality(filter(list, pred)) = 1
        filtered = _anon("filter", source, pred) if pred else source
        return exp.EQ(this=_anon("cardinality", filtered), expression=exp.Literal.number(1))

    def _lower_Reduce(self, node: Reduce) -> exp.Expression:
        # reduce(list, init, (acc, x) -> step, acc -> acc)
        source = self.lower(node.source)
        init = self.lower(node.init)
        step = exp.Lambda(
            this=self.lower(node.step),
            expressions=[exp.to_identifier(node.accumulator), exp.to_identifier(node.var)],
        )
        identity = _lam(node.accumulator, exp.column(node.accumulator))
        return _anon("reduce", source, init, step, identity)

    # -- context-delegated composite forms --------------------------------- #

    def _lower_MapProjection(self, node: MapProjection) -> exp.Expression:
        return self._ctx.resolve_map_projection(node)

    def _lower_SubqueryExpr(self, node: SubqueryExpr) -> exp.Expression:
        return self._ctx.resolve_subquery(node)

    # -- RDF values (REQ-2135) ---------------------------------------------- #

    def _lower_TypedLiteral(self, node: TypedLiteral) -> exp.Expression:
        dt = node.datatype
        if dt is None or node.lang is not None or dt == f"{_XSD}string":
            return exp.Literal.string(node.lexical)
        if dt in _XSD_NUMBERS:
            return exp.Literal.number(node.lexical)
        if dt == f"{_XSD}boolean":
            if node.lexical not in ("true", "false", "1", "0"):
                raise CypherTranslateError(f"{node.lexical!r} is not an xsd:boolean")
            return exp.true() if node.lexical in ("true", "1") else exp.false()
        if dt in _XSD_CASTS:
            return exp.Cast(
                this=exp.Literal.string(node.lexical), to=exp.DataType.build(_XSD_CASTS[dt])
            )
        raise CypherTranslateError(f"A literal of datatype <{dt}> has no SQL value")

    def _lower_IriConst(self, node: IriConst) -> exp.Expression:
        raise CypherTranslateError(
            f"<{node.iri}> is a value of the answer only: a subject is compared through its key"
        )

    def _lower_Term(self, node: Term) -> exp.Expression:
        return self._ctx.resolve_term(node)

    def _lower_PatternComprehension(self, node: PatternComprehension) -> exp.Expression:
        return self._ctx.resolve_pattern_comprehension(node)
