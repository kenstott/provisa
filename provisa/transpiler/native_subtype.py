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

What a subtype's native form and emulations are is declared once, on ``ir_types.SUBTYPES``. A
range's or multirange's emulation is provisa.transpiler.pg_ranges, which writes them before this
runs on any dialect but PostgreSQL: what it leaves refused it has refused itself, by name.
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
# The type's own operators: inet's << <<= >> >>= &&, a range's (and a shape's) && @> <@ << >> &<
# &> -|-, a tsvector's @@.
OPERATORS = (
    exp.BitwiseLeftShift,
    exp.BitwiseRightShift,
    ContainedByOrEqual,
    ContainsOrEqual,
    exp.ArrayOverlaps,
    exp.ArrayContainsAll,
    exp.ArrayContainedBy,
    exp.MatchAgainst,
    exp.ExtendsLeft,
    exp.ExtendsRight,
    exp.Adjacent,
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
    exp.ExtendsLeft: "&<",
    exp.ExtendsRight: "&>",
    exp.Adjacent: "-|-",
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


def _or(*parts: exp.Expr) -> exp.Expr:
    return exp.Paren(this=exp.or_(*parts))


def _and(*parts: exp.Expr) -> exp.Expr:
    return exp.Paren(this=exp.and_(*parts))


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
    if dialect == "postgres" and isinstance(node, exp.Func):
        # A function of the type (lower(range)): the marked argument gets the type.
        for arg in _call_arguments(node):
            found = marked(_unwrapped(arg))
            if found is not None:
                arg.replace(native_form(found[0].copy(), form))
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


def _emulated(node: exp.Expr, subtype: str) -> exp.Expr | None:
    semantics = SUBTYPES[subtype].semantics
    if semantics == "mac" and isinstance(node, (exp.Ordered, exp.Between, exp.In, *COMPARISONS)):
        _each_operand(node, _mac_key)
        return node
    return None


def _resolve_one(node: exp.Expr, subtype: str, dialect: str) -> exp.Expr:
    sub = SUBTYPES[subtype]
    form = sub.native.get(dialect)
    if form is not None:
        written = _native(node, subtype, dialect, form)
        if written is not None:
            return written
    elif "*" in sub.emulated or dialect in sub.emulated:
        written = _emulated(node, subtype)
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
