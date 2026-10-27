# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8f2b61-9e3d-4a75-b0c2-6d1e8f3a7b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A subtype's native semantics on the routed dialect (REQ-2171).

The compiler marks each sort, filter, join or operator use of a subtyped column
(``compiler.native_subtype``). Here, where the routed dialect is known, each marked use is
written the first of these ways that applies:

1. the dialect has the native type: the column, and what it is compared with, are given it;
2. the statement runs at a source that has the native type -- the route sent it there, and the
   source's dialect is the routed dialect, so this is (1) at the source;
3. the subtype declares an emulation for the dialect: the native answer, from the base value;
4. otherwise the operation is refused by name, never answered over the text form.

What a subtype's native form and emulations are is declared once, on ``ir_types.SUBTYPES``.
"""

from __future__ import annotations

import re
from typing import TypeGuard

import sqlglot.expressions as exp

from provisa.core.ir_types import SUBTYPES
from provisa.transpiler.semantic_dialect import ContainedByOrEqual, ContainsOrEqual

MARKER = "provisa_subtype"

COMPARISONS = (
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.NullSafeEQ,
    exp.NullSafeNEQ,
)
# The type's own operators: inet's << <<= >> >>= &&, a range's (and a shape's) && @> <@, a
# tsvector's @@.
OPERATORS = (
    exp.BitwiseLeftShift,
    exp.BitwiseRightShift,
    ContainedByOrEqual,
    ContainsOrEqual,
    exp.ArrayOverlaps,
    exp.ArrayContainsAll,
    exp.ArrayContainedBy,
    exp.MatchAgainst,
)
_OPERATOR_NAMES = {
    exp.BitwiseLeftShift: "<<",
    exp.BitwiseRightShift: ">>",
    ContainedByOrEqual: "<<=",
    ContainsOrEqual: ">>=",
    exp.ArrayOverlaps: "&&",
    exp.ArrayContainsAll: "@>",
    exp.ArrayContainedBy: "<@",
    exp.MatchAgainst: "@@",
}


class SubtypeOperationRefused(ValueError):
    """An operation on a subtyped column the routed dialect can neither run natively nor
    emulate: refused by name, never answered over the column's text form."""

    code = "query.subtype_operation_unsupported"

    def __init__(self, subtype: str, operation: str, dialect: str) -> None:
        self.params = {"subtype": subtype, "operation": operation, "dialect": dialect}
        super().__init__(
            f"{operation} on a {subtype} column needs {subtype}'s own semantics, which {dialect} "
            f"neither has nor emulates; it is not answered over the column's text form"
        )


def subtype_marker(column: exp.Expr, subtype: str) -> exp.Anonymous:
    """The marker the compiler emits for a use of a subtyped column."""
    return exp.Anonymous(this=MARKER, expressions=[column, exp.Literal.string(subtype)])


def marked(node: exp.Expr | None) -> tuple[exp.Expr, str] | None:
    """The column and subtype a marker carries; None when ``node`` is not a marker."""
    if not isinstance(node, exp.Anonymous) or str(node.this).lower() != MARKER:
        return None
    column, subtype = node.expressions
    return column, subtype.name


def is_spatial_call(node: exp.Expr | None) -> TypeGuard[exp.Func]:
    """Whether ``node`` is a spatial function's call (``ST_Contains``, ``ST_Intersects`` ...)."""
    return isinstance(node, exp.Func) and _function_name(node).lower().startswith("st_")


def _function_name(node: exp.Func) -> str:
    return str(node.this) if isinstance(node, exp.Anonymous) else node.sql_name()


# --- native forms -------------------------------------------------------------------------------

_CALL = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\((.*)\)$")


def native_form(value: exp.Expr, form: str) -> exp.Expr:
    """``value`` given a native form: a type name casts to it; a call (``f(g())``) applies the
    innermost function first."""
    call = _CALL.match(form)
    if call is None:
        return exp.Cast(this=value, to=exp.DataType.build(form, udt=True))
    inner = call.group(2)
    argument = native_form(value, inner) if inner else value
    return exp.Anonymous(this=call.group(1), expressions=[argument])


def _call_arguments(node: exp.Func) -> list[exp.Expr]:
    args = [v for k, v in node.args.items() if k != "expressions" and isinstance(v, exp.Expr)]
    return [a for a in args if not isinstance(a, exp.Identifier)] + list(node.expressions)


def _unwrapped(node: exp.Expr) -> exp.Expr:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _value(node: exp.Expr) -> exp.Expr:
    """An operand with its marker removed."""
    found = marked(_unwrapped(node))
    return found[0].copy() if found is not None else node.copy()


def _operation(node: exp.Expr) -> str:
    if isinstance(node, OPERATORS):
        return _OPERATOR_NAMES[type(node)]
    if isinstance(node, exp.Ordered):
        return "ORDER BY"
    if isinstance(node, exp.Between):
        return "BETWEEN"
    if isinstance(node, exp.In):
        return "IN"
    if isinstance(node, exp.Func):
        return _function_name(node)
    return node.key.upper() if not isinstance(node, COMPARISONS) else _COMPARISON_NAMES[type(node)]


_COMPARISON_NAMES = {
    exp.EQ: "=",
    exp.NEQ: "<>",
    exp.GT: ">",
    exp.GTE: ">=",
    exp.LT: "<",
    exp.LTE: "<=",
    exp.NullSafeEQ: "IS NOT DISTINCT FROM",
    exp.NullSafeNEQ: "IS DISTINCT FROM",
}


def _operands(node: exp.Expr) -> list[tuple[str, exp.Expr]]:
    """``(arg key, operand)`` for each operand the operation compares or combines."""
    if isinstance(node, exp.Ordered):
        return [("this", node.this)]
    if isinstance(node, exp.Between):
        return [("this", node.this), ("low", node.args["low"]), ("high", node.args["high"])]
    if isinstance(node, exp.In):
        return [("this", node.this)] + [("expressions", e) for e in node.expressions]
    if isinstance(node, exp.MatchAgainst):
        return [("expressions", e) for e in node.expressions]
    return [("this", node.this), ("expression", node.expression)]


def _each_operand(node: exp.Expr, write) -> None:
    """Replace every operand of ``node`` with ``write(operand without its marker)``."""
    for _key, operand in _operands(node):
        operand.replace(write(_value(operand)))


# --- emulations ---------------------------------------------------------------------------------


def _mac_key(value: exp.Expr) -> exp.Expr:
    """A MAC address as fixed-width hex: separators removed, lower case."""
    stripped = value
    for separator in (":", "-", "."):
        stripped = exp.Anonymous(
            this="REPLACE",
            expressions=[stripped, exp.Literal.string(separator), exp.Literal.string("")],
        )
    return exp.Lower(this=stripped)


_RANGE_BOUNDS = {
    "int4range": "INT",
    "int8range": "BIGINT",
    "numrange": "DECIMAL(38, 10)",
    "tsrange": "TIMESTAMP",
    "tstzrange": "TIMESTAMPTZ",
    "daterange": "DATE",
}


def _one_day() -> exp.Expr:
    return exp.Interval(this=exp.Literal.string("1"), unit=exp.Var(this="DAY"))


# The discrete ranges, by the step between their values.
_DISCRETE_STEP = {
    "int4range": lambda: exp.Literal.number(1),
    "int8range": lambda: exp.Literal.number(1),
    "daterange": _one_day,
}


class _Range:
    """A range's parts, read from its PostgreSQL text form (``[lo,hi)``, ``empty``)."""

    def __init__(self, text: exp.Expr, subtype: str) -> None:
        bound = exp.DataType.build(_RANGE_BOUNDS[subtype])
        inner = exp.Substring(
            this=text.copy(),
            start=exp.Literal.number(2),
            length=exp.Sub(this=exp.Length(this=text.copy()), expression=exp.Literal.number(2)),
        )

        def part(n: int) -> exp.Expr:
            raw = exp.Anonymous(
                this="REPLACE",
                expressions=[
                    exp.SplitPart(
                        this=inner.copy(),
                        delimiter=exp.Literal.string(","),
                        part_index=exp.Literal.number(n),
                    ),
                    exp.Literal.string('"'),
                    exp.Literal.string(""),
                ],
            )
            # 'empty' has no bounds to read.
            written = exp.Case(
                ifs=[
                    exp.If(
                        this=exp.EQ(
                            this=exp.Lower(this=text.copy()),
                            expression=exp.Literal.string("empty"),
                        ),
                        true=exp.Null(),
                    )
                ],
                default=exp.Nullif(this=raw, expression=exp.Literal.string("")),
            )
            return exp.Cast(this=written, to=bound)

        def char_at(start: exp.Expr) -> exp.Expr:
            return exp.Substring(this=text.copy(), start=start, length=exp.Literal.number(1))

        lo, hi = part(1), part(2)
        lo_inc = exp.EQ(this=char_at(exp.Literal.number(1)), expression=exp.Literal.string("["))
        hi_inc = exp.EQ(
            this=char_at(exp.Length(this=text.copy())), expression=exp.Literal.string("]")
        )
        written_empty = exp.EQ(
            this=exp.Lower(this=text.copy()), expression=exp.Literal.string("empty")
        )
        step = _DISCRETE_STEP.get(subtype)
        if step is not None:
            # A discrete range is canonical as PostgreSQL makes it: [lo, hi), a bound written
            # the other way moved by one step ('(1,5]' is '[2,6)').
            lo = exp.Case(
                ifs=[exp.If(this=lo_inc, true=lo.copy())],
                default=exp.Add(this=lo, expression=step()),
            )
            hi = exp.Case(
                ifs=[exp.If(this=hi_inc, true=exp.Add(this=hi.copy(), expression=step()))],
                default=hi,
            )
            lo_inc, hi_inc = exp.true(), exp.false()
            reversed_or_point = exp.GTE(this=lo.copy(), expression=hi.copy())
        else:
            reversed_or_point = _or(
                exp.GT(this=lo.copy(), expression=hi.copy()),
                _and(
                    exp.EQ(this=lo.copy(), expression=hi.copy()),
                    exp.Not(this=_and(lo_inc.copy(), hi_inc.copy())),
                ),
            )
        self.lo, self.hi, self.lo_inc, self.hi_inc = lo, hi, lo_inc, hi_inc
        # Empty as PostgreSQL makes a range whose bounds hold no value ('[3,3)').
        self.empty = _or(
            written_empty,
            _and(
                exp.Not(this=exp.Is(this=lo.copy(), expression=exp.Null())),
                exp.Not(this=exp.Is(this=hi.copy(), expression=exp.Null())),
                reversed_or_point,
            ),
        )


def _or(*parts: exp.Expr) -> exp.Expr:
    return exp.Paren(this=exp.or_(*parts))


def _and(*parts: exp.Expr) -> exp.Expr:
    return exp.Paren(this=exp.and_(*parts))


def _before(lo: exp.Expr, lo_inc: exp.Expr, hi: exp.Expr, hi_inc: exp.Expr) -> exp.Expr:
    """Whether a lower bound comes before an upper bound (an unbounded side always does)."""
    return _or(
        exp.Is(this=lo.copy(), expression=exp.Null()),
        exp.Is(this=hi.copy(), expression=exp.Null()),
        exp.LT(this=lo.copy(), expression=hi.copy()),
        _and(exp.EQ(this=lo.copy(), expression=hi.copy()), lo_inc.copy(), hi_inc.copy()),
    )


def _overlaps(a: _Range, b: _Range) -> exp.Expr:
    return _and(
        exp.Not(this=a.empty.copy()),
        exp.Not(this=b.empty.copy()),
        _before(a.lo, a.lo_inc, b.hi, b.hi_inc),
        _before(b.lo, b.lo_inc, a.hi, a.hi_inc),
    )


def _contains_range(a: _Range, b: _Range) -> exp.Expr:
    lower = _or(
        exp.Is(this=a.lo.copy(), expression=exp.Null()),
        _and(
            exp.Not(this=exp.Is(this=b.lo.copy(), expression=exp.Null())),
            _or(
                exp.LT(this=a.lo.copy(), expression=b.lo.copy()),
                _and(
                    exp.EQ(this=a.lo.copy(), expression=b.lo.copy()),
                    _or(a.lo_inc.copy(), exp.Not(this=b.lo_inc.copy())),
                ),
            ),
        ),
    )
    upper = _or(
        exp.Is(this=a.hi.copy(), expression=exp.Null()),
        _and(
            exp.Not(this=exp.Is(this=b.hi.copy(), expression=exp.Null())),
            _or(
                exp.GT(this=a.hi.copy(), expression=b.hi.copy()),
                _and(
                    exp.EQ(this=a.hi.copy(), expression=b.hi.copy()),
                    _or(a.hi_inc.copy(), exp.Not(this=b.hi_inc.copy())),
                ),
            ),
        ),
    )
    return _or(b.empty.copy(), _and(exp.Not(this=a.empty.copy()), lower, upper))


def _contains_element(a: _Range, element: exp.Expr, subtype: str) -> exp.Expr:
    e = exp.Cast(this=element, to=exp.DataType.build(_RANGE_BOUNDS[subtype]))
    return _and(
        exp.Not(this=a.empty.copy()),
        _or(
            exp.Is(this=a.lo.copy(), expression=exp.Null()),
            exp.LT(this=a.lo.copy(), expression=e.copy()),
            _and(exp.EQ(this=a.lo.copy(), expression=e.copy()), a.lo_inc.copy()),
        ),
        _or(
            exp.Is(this=a.hi.copy(), expression=exp.Null()),
            exp.LT(this=e.copy(), expression=a.hi.copy()),
            _and(exp.EQ(this=e.copy(), expression=a.hi.copy()), a.hi_inc.copy()),
        ),
    )


def _is_range_text(node: exp.Expr, marked_side: bool) -> bool:
    """Whether a range operator's other operand is a range (not an element of one)."""
    if marked_side:
        return True
    node = _unwrapped(node)
    return (
        isinstance(node, exp.Literal)
        and node.is_string
        and (node.name[:1] in "[(" or node.name.lower() == "empty")
    )


def _emulated_range(node: exp.Expr, subtype: str) -> exp.Expr | None:
    """A range operator from the bounds and their inclusivity; None for anything else."""
    if not isinstance(node, (exp.ArrayOverlaps, exp.ArrayContainsAll, exp.ArrayContainedBy)):
        return None
    left, right = node.this, node.expression
    if isinstance(node, exp.ArrayContainedBy):  # a <@ b is b @> a
        left, right = right, left
    left_marked = marked(_unwrapped(left)) is not None
    right_marked = marked(_unwrapped(right)) is not None
    if not _is_range_text(left, left_marked):
        return None  # an element @> a range: no such operator
    container = _Range(_value(left), subtype)
    if isinstance(node, exp.ArrayOverlaps):
        return _overlaps(container, _Range(_value(right), subtype))
    if _is_range_text(right, right_marked):
        return _contains_range(container, _Range(_value(right), subtype))
    return _contains_element(container, _value(right), subtype)


# --- inet on Trino, from its own address functions -----------------------------------------------


def _call(name: str, *args: exp.Expr) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=list(args))


def _family(text: exp.Expr) -> exp.Expr:
    """'6' for an IPv6 address, '4' for an IPv4 one."""
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.GT(
                    this=_call("STRPOS", text.copy(), exp.Literal.string(":")),
                    expression=exp.Literal.number(0),
                ),
                true=exp.Literal.string("6"),
            )
        ],
        default=exp.Literal.string("4"),
    )


def _host(text: exp.Expr) -> exp.Expr:
    return exp.Cast(
        this=_call("SPLIT_PART", text.copy(), exp.Literal.string("/"), exp.Literal.number(1)),
        to=exp.DataType.build("IPADDRESS", udt=True),
    )


def _masklen(text: exp.Expr) -> exp.Expr:
    """The mask length an inet is written with, else its family's full length."""
    full = exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=_family(text), expression=exp.Literal.string("6")),
                true=exp.Literal.number(128),
            )
        ],
        default=exp.Literal.number(32),
    )
    written = exp.Cast(
        this=exp.Nullif(
            this=_call("SPLIT_PART", text.copy(), exp.Literal.string("/"), exp.Literal.number(2)),
            expression=exp.Literal.string(""),
        ),
        to=exp.DataType.build("INTEGER"),
    )
    return exp.Coalesce(this=written, expressions=[full])


def _hex(address: exp.Expr) -> exp.Expr:
    return _call("TO_HEX", exp.Cast(this=address, to=exp.DataType.build("VARBINARY")))


def _network(text: exp.Expr) -> exp.Expr:
    """The inet's network: its address with the bits past its mask cleared."""
    return _call("IP_SUBNET_MIN", _call("IP_PREFIX", _host(text), _masklen(text)))


def _inet_key(text: exp.Expr) -> exp.Expr:
    """An inet's ordering key, compared as text: family, the network (fixed-width hex), the mask
    length, then the whole address. PostgreSQL orders two inets by family, then their networks'
    bits up to the shorter mask, then mask length, then all bits; a shorter mask's network has
    only zeros past it, so comparing the networks whole then the mask length gives that order."""
    mask = _call(
        "LPAD",
        exp.Cast(this=_masklen(text), to=exp.DataType.build("VARCHAR")),
        exp.Literal.number(3),
        exp.Literal.string("0"),
    )
    return exp.DPipe(
        this=exp.DPipe(
            this=exp.DPipe(this=_family(text), expression=_hex(_network(text))),
            expression=mask,
        ),
        expression=_hex(_host(text)),
    )


def _within(inner: exp.Expr, outer: exp.Expr, strict: bool) -> exp.Expr:
    """``inner <<= outer`` (``<<`` when ``strict``): the outer network holds the inner one."""
    longer = (exp.GT if strict else exp.GTE)(this=_masklen(inner), expression=_masklen(outer))
    held = _call("CONTAINS", _call("IP_PREFIX", _host(outer), _masklen(outer)), _host(inner))
    return _and(exp.EQ(this=_family(inner), expression=_family(outer)), longer, held)


def _emulated_trino_address(node: exp.Expr) -> exp.Expr | None:
    if isinstance(node, (exp.Ordered, exp.Between, exp.In, *COMPARISONS)):
        _each_operand(node, _inet_key)
        return node
    if not isinstance(
        node,
        (
            exp.BitwiseLeftShift,
            exp.BitwiseRightShift,
            ContainedByOrEqual,
            ContainsOrEqual,
            exp.ArrayOverlaps,
        ),
    ):
        return None
    a, b = _value(node.this), _value(node.expression)
    if isinstance(node, exp.BitwiseLeftShift):
        return _within(a, b, strict=True)
    if isinstance(node, ContainedByOrEqual):
        return _within(a, b, strict=False)
    if isinstance(node, exp.BitwiseRightShift):
        return _within(b, a, strict=True)
    if isinstance(node, ContainsOrEqual):
        return _within(b, a, strict=False)
    return _or(_within(a, b, strict=False), _within(b, a, strict=False))


# --- address operators on DuckDB, which has <<= and >>= of its own ------------------------------


def _duckdb_inet_key(address: exp.Expr) -> exp.Expr:
    """DuckDB's inet orders by address before mask; PostgreSQL by network, then mask, then
    address (10.0.0.1/24 before 10.0.0.0/32). Its network, then itself, is PostgreSQL's order."""
    return exp.Tuple(
        expressions=[exp.Anonymous(this="network", expressions=[address.copy()]), address]
    )


def _duckdb_address_operator(node: exp.Expr, a: exp.Expr, b: exp.Expr) -> exp.Expr:
    within = ContainedByOrEqual(this=a.copy(), expression=b.copy())
    around = ContainsOrEqual(this=a.copy(), expression=b.copy())
    if isinstance(node, exp.BitwiseLeftShift):  # a << b: strictly within
        return _and(within, exp.Not(this=ContainedByOrEqual(this=b.copy(), expression=a.copy())))
    if isinstance(node, exp.BitwiseRightShift):  # a >> b: strictly around
        return _and(around, exp.Not(this=ContainedByOrEqual(this=a.copy(), expression=b.copy())))
    if isinstance(node, exp.ArrayOverlaps):
        return _or(within, around)
    return within if isinstance(node, ContainedByOrEqual) else around


# --- resolution ---------------------------------------------------------------------------------


def _native(node: exp.Expr, subtype: str, dialect: str, form: str) -> exp.Expr | None:
    """``node`` with the native type given where it decides the answer; None when the dialect's
    native type has no form of this operation."""
    semantics = SUBTYPES[subtype].semantics
    if isinstance(node, (exp.Ordered, exp.Between, exp.In, *COMPARISONS)):
        if semantics == "address" and dialect == "duckdb":
            _each_operand(node, lambda v: _duckdb_inet_key(native_form(v, form)))
        else:
            _each_operand(node, lambda v: native_form(v, form))
        return node
    if is_spatial_call(node):
        # The marked column, and a shape written as WKT text beside it, get the native type.
        for arg in _call_arguments(node):
            inner = _unwrapped(arg)
            found = marked(inner)
            if found is not None:
                arg.replace(native_form(found[0].copy(), form))
            elif isinstance(inner, exp.Literal) and inner.is_string:
                arg.replace(native_form(inner.copy(), form))
        return node
    if dialect == "postgres":
        # Every operator is PostgreSQL's own; the marked side gets the type, the other side's
        # type (a range or an element, a tsquery) PostgreSQL infers.
        for _key, operand in _operands(node):
            found = marked(_unwrapped(operand))
            if found is not None:
                operand.replace(native_form(found[0].copy(), form))
        return node
    if semantics == "address" and dialect == "duckdb" and not isinstance(node, exp.MatchAgainst):
        a = native_form(_value(node.this), form)
        b = native_form(_value(node.expression), form)
        return _duckdb_address_operator(node, a, b)
    return None


def _emulated(node: exp.Expr, subtype: str, dialect: str) -> exp.Expr | None:
    semantics = SUBTYPES[subtype].semantics
    if semantics == "address" and dialect == "trino":
        return _emulated_trino_address(node)
    if semantics == "mac" and isinstance(node, (exp.Ordered, exp.Between, exp.In, *COMPARISONS)):
        _each_operand(node, _mac_key)
        return node
    if semantics == "range":
        return _emulated_range(node, subtype)
    return None


def _resolve_one(node: exp.Expr, subtype: str, dialect: str) -> exp.Expr:
    sub = SUBTYPES[subtype]
    form = sub.native.get(dialect)
    if form is not None:
        written = _native(node, subtype, dialect, form)
        if written is not None:
            return written
    elif "*" in sub.emulated or dialect in sub.emulated:
        written = _emulated(node, subtype, dialect)
        if written is not None:
            return written
    raise SubtypeOperationRefused(subtype, _operation(node), dialect)


def resolve_subtypes(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree`` (parsed from PostgreSQL) with every marked operation written for ``dialect``.
    Raises :class:`SubtypeOperationRefused` for one the dialect cannot answer natively."""
    for marker in list(tree.find_all(exp.Anonymous)):
        found = marked(marker)
        if found is None or marker.parent is None:
            continue  # not a marker, or one an earlier operation already wrote
        node: exp.Expr = marker
        while isinstance(node.parent, exp.Paren):
            node = node.parent
        operation = node.parent
        assert operation is not None
        written = _resolve_one(operation, found[1], dialect)
        if written is not operation:
            operation.replace(written)
    return tree


__all__ = [
    "MARKER",
    "SubtypeOperationRefused",
    "is_spatial_call",
    "marked",
    "native_form",
    "resolve_subtypes",
    "subtype_marker",
]
