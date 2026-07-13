# Copyright (c) 2026 Kenneth Stott
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

Emission matches the current path so the A/B differential stays clean: ``STARTS WITH → starts_with()``,
``ENDS WITH → (x LIKE CONCAT('%', y))``, ``CONTAINS → (strpos(x, y) > 0)``, ``=~ → regexp_like()``,
``^ → POWER()``, list comprehensions/quantifiers/reduce → the engine's higher-order list functions.
"""

from __future__ import annotations

from typing import Protocol, cast

import sqlglot.expressions as exp

from provisa.cypher.expr_ast import (
    Binary,
    Case,
    CypherExpr,
    FunctionCall,
    Index,
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
    Unary,
    Variable,
)


class ExprContext(Protocol):
    """Translator-supplied resolution for the context-dependent leaves of an expression."""

    def resolve_variable(self, name: str) -> exp.Expression:
        """A bare variable ``n`` — the column/expression it denotes in the current query scope."""
        ...

    def resolve_property(self, obj: exp.Expression, name: str) -> exp.Expression:
        """A property access ``obj.name`` lowered against the object's already-lowered expression."""
        ...

    def resolve_parameter(self, name: str) -> exp.Expression:
        """A ``$param`` reference — the positional placeholder it binds to."""
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

    def resolve_subquery(self, node: SubqueryExpr) -> exp.Expression:
        """``EXISTS/COUNT/COLLECT { … }`` — builds the correlated subquery."""
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

    def _lower_Binary(self, node: Binary) -> exp.Expression:
        op = node.op
        left = self.lower(node.left)
        right = self.lower(node.right)
        if op in _BINARY_ARITH:
            return _BINARY_ARITH[op](this=left, expression=right)
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
            # Typed Concat so the engine dialect renders its own concat (e.g. Trino's null-safe form),
            # matching the text path which emitted CONCAT(...) and let the serializer expand it.
            concat = exp.Concat(expressions=[exp.Literal.string("%"), right])
            return exp.Paren(this=exp.Like(this=left, expression=concat))
        if op == "CONTAINS":
            strpos = _anon("strpos", left, right)
            return exp.Paren(this=exp.GT(this=strpos, expression=exp.Literal.number(0)))
        if op == "=~":
            return _anon("regexp_like", left, right)
        raise NotImplementedError(f"binary operator {op!r}")

    def _lower_in(
        self, left: exp.Expression, raw_right: CypherExpr, right: exp.Expression
    ) -> exp.Expression:
        # IN over a literal list becomes an IN (...) tuple; otherwise a membership over an array value.
        if isinstance(raw_right, ListLiteral):
            return exp.In(this=left, expressions=[self.lower(i) for i in raw_right.items])
        return exp.In(this=left, field=right)

    def _lower_IsNull(self, node: IsNull) -> exp.Expression:
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
        return exp.Array(expressions=[self.lower(i) for i in node.items])

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
        base = self.lower(node.obj)
        start = self.lower(node.start) if node.start is not None else exp.Literal.number(1)
        if node.stop is not None:
            length = exp.Sub(this=self.lower(node.stop), expression=start)
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

    def _lower_PatternComprehension(self, node: PatternComprehension) -> exp.Expression:
        return self._ctx.resolve_pattern_comprehension(node)
