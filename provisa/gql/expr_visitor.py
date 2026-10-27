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
    AxiomObject,
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
    TermIri,
    TypedLiteral,
    Unary,
    Variable,
)
from provisa.gql import spatial, temporal
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

    def temporal_kind(self, expr: CypherExpr) -> str | None:
        """The temporal kind ``expr`` has (REQ-2173: date, time, ..., duration), or None."""
        ...

    def temporal_zone(self, expr: CypherExpr) -> str | None:
        """The zone a time or date time the query builds names (REQ-2173), or None."""
        ...

    def is_text(self, expr: CypherExpr) -> bool:
        """Whether ``expr`` is a string (size() of it counts its characters)."""
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


#: The functions whose value openCypher types a float.
_FLOAT_FUNCTIONS = frozenset({"tofloat", "tofloatornull"})
#: The registered types of a column holding a non-integer number.
_FLOAT_TYPES = re.compile(r"^(real|float|double|decimal|numeric)", re.I)


def _float_division(left: exp.Expression, right: exp.Expression) -> exp.Expression:
    """openCypher's float division: by zero, Infinity of the dividend's sign, or NaN for zero
    (REQ-2167; SQL's division by zero is an error). Written as PostgreSQL SQL, so every engine
    answers it so."""

    def _float(text: str) -> exp.Expression:
        return exp.cast(exp.Literal.string(text), exp.DataType.build("DOUBLE PRECISION"))

    zero = exp.Literal.number(0)
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=right.copy(), expression=zero.copy()),
                true=exp.Case(
                    ifs=[
                        exp.If(
                            this=exp.GT(this=left.copy(), expression=zero.copy()),
                            true=_float("Infinity"),
                        ),
                        exp.If(
                            this=exp.LT(this=left.copy(), expression=zero.copy()),
                            true=_float("-Infinity"),
                        ),
                        exp.If(
                            this=exp.EQ(this=left.copy(), expression=zero.copy()),
                            true=_float("NaN"),
                        ),
                    ]
                ),
            )
        ],
        default=exp.Div(this=left, expression=right),
    )


def _lam(var: str, body: exp.Expression) -> exp.Lambda:
    """An engine lambda ``var -> body`` (sqlglot ``Lambda``)."""
    return exp.Lambda(this=body, expressions=[exp.to_identifier(var)])


def _anon(name: str, *args: exp.Expression) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=list(args))


def _literal_kind(item: CypherExpr) -> str | None:
    """A literal's Cypher type, None for null or an expression whose type is not written."""
    if isinstance(item, Literal):
        if item.value is None:
            return None
        if isinstance(item.value, bool):
            return "boolean"
        if isinstance(item.value, int):
            return "integer"
        if isinstance(item.value, float):
            return "float"
        return "string"
    if isinstance(item, ListLiteral):
        return "list"
    if isinstance(item, MapLiteral):
        return "map"
    return None


def _mixed_literal_kinds(items: list[CypherExpr]) -> bool:
    """Whether a list literal's items are literals of more than one type."""
    if not all(isinstance(i, (Literal, ListLiteral, MapLiteral)) for i in items):
        return False
    return len({k for k in map(_literal_kind, items) if k is not None}) > 1


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
        held = self._ctx.temporal_kind(node.obj)
        if held == spatial.POINT:
            return spatial.point_property(self.lower(node.obj), node.name)
        if held == temporal.DURATION:
            return temporal.duration_property(self.lower(node.obj), node.name)
        if held is not None:  # REQ-2173: a date's, a time's or a date time's own parts
            zone = self._ctx.temporal_zone(node.obj)
            return temporal.value_property(held, self.lower(node.obj), node.name, zone)
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
        """``x.id`` where ``x`` is not bound to one registered table and is read as text — see
        ``_lower_Binary``. A node drawn from several tables whose ``id`` is one number in each
        reads it as that number."""
        if not (isinstance(node, Property) and node.name == "id"):
            return False
        obj = self.lower(node.obj)
        if self._ctx.property_is_table_column(obj, node.name):
            return False
        held = self._ctx.property_type(obj, node.name)
        return held is None or held.upper() in ("VARCHAR", "TEXT")

    def _is_float(self, node: CypherExpr) -> bool:
        """Whether ``node`` is a float as openCypher types it: a float literal, toFloat, a column
        registered as a non-integer number, or arithmetic over one."""
        if isinstance(node, Literal):
            return isinstance(node.value, float)
        if isinstance(node, FunctionCall):
            return node.name.lower() in _FLOAT_FUNCTIONS
        if isinstance(node, Property):
            data_type = self._ctx.property_type(self.lower(node.obj), node.name)
            return data_type is not None and bool(_FLOAT_TYPES.match(data_type))
        if isinstance(node, Paren):
            return self._is_float(node.inner)
        if isinstance(node, Unary) and node.op == "-":
            return self._is_float(node.operand)
        if isinstance(node, Binary) and node.op in _BINARY_ARITH:
            return self._is_float(node.left) or self._is_float(node.right)
        return False

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
        # SQL binds IS NULL looser than a comparison, Cypher tighter: a = b IS NULL is a = (b IS NULL).
        if isinstance(node.left, IsNull) and op not in ("AND", "OR", "XOR"):
            left = exp.Paren(this=left)
        if isinstance(node.right, IsNull) and op not in ("AND", "OR", "XOR"):
            right = exp.Paren(this=right)
        if op in _BINARY_ARITH:
            # REQ-2173: a duration with a temporal value or another duration, as openCypher reads it.
            left_kind = self._ctx.temporal_kind(node.left)
            right_kind = self._ctx.temporal_kind(node.right)
            moved = node.left if left_kind != temporal.DURATION else node.right
            timed = temporal.arithmetic(
                op, left, left_kind, right, right_kind, self._ctx.temporal_zone(moved)
            )
            if timed is not None:
                return timed
            if op == "/" and (self._is_float(node.left) or self._is_float(node.right)):
                return _float_division(left, right)
            if op == "/":
                # An integer divided by an integer is an integer, as in SQL: typed, so sqlglot does
                # not cast the dividend to DOUBLE PRECISION.
                return exp.Div(this=left, expression=right, typed=True)
            return _BINARY_ARITH[op](this=left, expression=right)
        left_kind = self._ctx.temporal_kind(node.left)
        right_kind = self._ctx.temporal_kind(node.right)
        if left_kind and right_kind and left_kind != right_kind and op in _BINARY_CMP:
            # REQ-2173: values of two temporal kinds are never equal, and have no order.
            if op == "=":
                return exp.false()
            if op in ("<>", "!="):
                return exp.true()
            return exp.Null()
        if op in ("=", "<>", "!=") and left_kind == right_kind == temporal.DURATION:
            return temporal.equality("=" if op == "=" else "<>", left, right)
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
            # Two booleans differ: null where either is null, as Cypher's XOR. exp.Xor is written
            # `a XOR b`, which PostgreSQL SQL does not read, and its expansion on other dialects
            # leaves its operands unbracketed.
            return exp.Paren(
                this=exp.NEQ(this=exp.Paren(this=left), expression=exp.Paren(this=right))
            )
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
        # IN over a literal list becomes an IN (...) tuple; otherwise a membership over an array value:
        # `x IN list`, or for a list computed by an operator `x = ANY((a + b))` (SQL reads no
        # `x IN a + b`), bracketed as the comparison it is in SQL.
        if isinstance(raw_right, ListLiteral):
            return exp.In(this=left, expressions=[self.lower(i) for i in raw_right.items])
        if isinstance(right.unnest(), exp.Binary):
            member = exp.EQ(this=left, expression=exp.Any(this=exp.Paren(this=right.unnest())))
            return exp.Paren(this=member)
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

    def _wall(self, node: CypherExpr, zone: str | None) -> exp.Expression:
        """A date time as its wall clock in ``zone``, or in its own zone (REQ-2173)."""
        return temporal.wall_clock(
            self.lower(node), zone if zone is not None else self._ctx.temporal_zone(node)
        )

    def _wall_or_value(self, node: CypherExpr) -> exp.Expression:
        """A date time's wall clock in its own zone; any other value as it is."""
        if self._ctx.temporal_kind(node) == temporal.DATETIME:
            return self._wall(node, None)
        return self.lower(node)

    def _lower_FunctionCall(self, node: FunctionCall) -> exp.Expression:
        if node.star:
            return exp.Count(this=exp.Star())
        if node.name.lower() == "size" and len(node.args) == 1:
            # A list written in the statement is counted where it is written (its engine may not
            # type an empty one), and the size of null is null (openCypher).
            given = node.args[0]
            if isinstance(given, ListLiteral):
                return exp.Literal.number(len(given.items))
            if isinstance(given, Literal) and given.kind == "null":
                return exp.Null()
            if self._ctx.is_text(given) and not isinstance(given, Literal):
                # A string's size is its character count: counted as text on every engine, never
                # as a list (PostgreSQL refuses CARDINALITY of text).
                return exp.Length(this=self.lower(given))
        if node.name.lower() == spatial.POINT:
            return spatial.constructed(node.args, self.lower)
        if node.name.lower() in temporal.DURATION_FUNCTIONS:  # REQ-2173
            if any(isinstance(a, Literal) and a.kind == "null" for a in node.args):
                return exp.Null()
            kinds = [self._ctx.temporal_kind(a) for a in node.args]
            zones = [self._ctx.temporal_zone(a) for a in node.args]
            return temporal.duration_function(node.name, node.args, kinds, self.lower, zones)
        kind, _, clock = node.name.lower().partition(".")
        if kind in temporal.SQL_TYPES and clock in ("transaction", "statement", "realtime"):
            # REQ-2173: the current value, as the engine's clock reads it.
            if any(isinstance(a, Literal) and a.kind == "null" for a in node.args):
                return exp.Null()
            if node.args:
                raise CypherTranslateError(f"{node.name}() with a time zone is not supported")
            return self._ctx.resolve_function(kind, [], distinct=False)
        if kind in temporal.SQL_TYPES and clock == "truncate":  # REQ-2173
            value_kind = self._ctx.temporal_kind(node.args[1]) if len(node.args) > 1 else None
            return temporal.truncated(
                kind, node.args, value_kind, self._wall_or_value, zone=self._ctx.temporal_zone(node)
            )
        if node.name.lower() in temporal.SQL_TYPES:  # REQ-2173: from text or a map
            built = temporal.temporal_value(
                node.name.lower(),
                node.args,
                self.lower,
                self._ctx.temporal_kind,
                zone=self._ctx.temporal_zone(node),
                lower_wall=self._wall,
                zone_of_entry=self._ctx.temporal_zone,
            )
            if built is not None:
                return built
        if (
            node.name.lower() == "tostring"
            and len(node.args) == 1
            and self._ctx.temporal_kind(node.args[0]) == temporal.DURATION
        ):
            return temporal.iso_text(self.lower(node.args[0]))
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
            if _mixed_literal_kinds(node.items):
                # [1, 'a', true]: one engine type only as each value's JSON (GraphVarKind.JSON_LIST).
                return exp.Array(expressions=[_anon("to_json", self.lower(i)) for i in node.items])
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

    def _lower_TermIri(self, node: TermIri) -> exp.Expression:
        # Compared, ordered or filtered on, a term is its name (REQ-2163).
        return self.lower(node.value)

    def _lower_AxiomObject(self, node: AxiomObject) -> exp.Expression:
        # Compared, ordered or filtered on, an axiom's object is whichever value its row holds.
        return exp.Coalesce(
            this=self.lower(Property(Variable(node.node), "object")),
            expressions=[
                self.lower(Property(Variable(node.node), "object_iri")),
                self.lower(Property(Variable(node.node), "object_literal")),
            ],
        )

    def _lower_Term(self, node: Term) -> exp.Expression:
        return self._ctx.resolve_term(node)

    def _lower_PatternComprehension(self, node: PatternComprehension) -> exp.Expression:
        return self._ctx.resolve_pattern_comprehension(node)
