# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The graph IR printed as Cypher text (REQ-2136, REQ-2135): what Open as query shows for a read
the IR holds, and what the Cypher front end reads back to the same statement.

It prints the constructs a GraphQL read lowers to (provisa.graphql_front): MATCH and OPTIONAL
MATCH of node and one-hop relationship patterns with their WHERE, RETURN items, maps, CASE,
function calls (aggregates among them), COLLECT subqueries with their own ORDER BY, SKIP and
LIMIT, and the statement's. Anything else is refused by name (CypherPrintRefused), never printed
approximately.
"""

# Requirements: REQ-2136, REQ-2135

from __future__ import annotations

import re

from provisa.gql.expr_ast import (
    Binary,
    Case,
    CypherExpr,
    FunctionCall,
    IsNull,
    ListLiteral,
    Literal,
    MapLiteral,
    Property,
    SubqueryExpr,
    Unary,
    Variable,
)
from provisa.gql.ir import CypherAST, MatchStep, NodePattern, PathPattern, RelPattern

_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_OPERATORS = {"=", "<>", "<", "<=", ">", ">=", "AND", "OR", "IN"}


class CypherPrintRefused(ValueError):
    """The statement holds a construct the printer has no Cypher text for; the message names it."""


def _name(name: str) -> str:
    return name if _NAME.match(name) else "`" + name.replace("`", "``") + "`"


def _string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


#: How tightly each operator binds: a child binding less tightly than its parent is parenthesised.
_PRECEDENCE = {"OR": 1, "AND": 2, "NOT": 3}
_ATOM = 9


def _precedence(e: CypherExpr) -> int:
    if isinstance(e, Binary):
        return _PRECEDENCE.get(e.op.upper(), 4)
    if isinstance(e, Unary) and e.op.upper() == "NOT":
        return _PRECEDENCE["NOT"]
    if isinstance(e, IsNull):
        return 4
    return _ATOM


def _operand(e: CypherExpr, parent: int, *, strict: bool = False) -> str:
    """``e`` as an operand of an operator of precedence ``parent``: parenthesised when it binds
    less tightly (or as tightly, for an operand where that would regroup it)."""
    text = expression(e)
    p = _precedence(e)
    return f"({text})" if p < parent or (strict and p == parent) else text


def expression(e: CypherExpr) -> str:
    if isinstance(e, Variable):
        return _name(e.name)
    if isinstance(e, Property) and isinstance(e.obj, Variable):
        return f"{_name(e.obj.name)}.{_name(e.name)}"
    if isinstance(e, Literal):
        if e.kind == "string":
            return _string(str(e.value))
        if e.kind == "boolean":
            return "true" if e.value else "false"
        if e.kind == "null":
            return "NULL"
        if (
            e.kind == "number"
            and isinstance(e.value, (int, float))
            and not isinstance(e.value, bool)
        ):
            return repr(e.value)
        raise CypherPrintRefused(f"the literal {e.value!r}")
    if isinstance(e, ListLiteral):
        return "[" + ", ".join(expression(i) for i in e.items) + "]"
    if isinstance(e, MapLiteral):
        return "{" + ", ".join(f"{_name(k)}: {expression(v)}" for k, v in e.entries) + "}"
    if isinstance(e, Binary) and e.op.upper() in _OPERATORS:
        op = e.op.upper()
        p = _precedence(e)
        # AND and OR associate; a comparison of a comparison is grouped as written.
        strict = op not in ("AND", "OR")
        return f"{_operand(e.left, p, strict=strict)} {op} {_operand(e.right, p, strict=True)}"
    if isinstance(e, Unary) and e.op.upper() == "NOT":
        return f"NOT {_operand(e.operand, _PRECEDENCE['NOT'])}"
    if isinstance(e, IsNull):
        return f"{_operand(e.operand, 5)} IS {'NOT ' if e.negated else ''}NULL"
    if isinstance(e, Case) and e.subject is None:
        whens = " ".join(f"WHEN {expression(c)} THEN {expression(r)}" for c, r in e.whens)
        default = "" if e.default is None else f" ELSE {expression(e.default)}"
        return f"CASE {whens}{default} END"
    if isinstance(e, FunctionCall) and _NAME.match(e.name):
        if e.star:
            return f"{e.name}(*)"
        args = ", ".join(expression(a) for a in e.args)
        return f"{e.name}({'DISTINCT ' if e.distinct else ''}{args})"
    if isinstance(e, SubqueryExpr) and e.kind == "COLLECT":
        return "COLLECT { " + " ".join(statement_lines(e.body)) + " }"
    raise CypherPrintRefused(f"{type(e).__name__} has no Cypher text here")


def _node(n: NodePattern) -> str:
    if n.properties or n.property_text:
        raise CypherPrintRefused("an inline property map on a node")
    labels = "".join(f":{_name(label)}" for label in n.labels)
    return f"({_name(n.variable or '')}{labels})"


def _rel(r: RelPattern) -> str:
    if r.path is not None or r.variable_length or r.properties or r.property_text:
        raise CypherPrintRefused("a path or a relationship with properties")
    body = f"[{_name(r.variable) if r.variable else ''}{''.join(':' + _name(t) for t in r.types[:1])}{''.join('|' + _name(t) for t in r.types[1:])}]"
    if r.direction == "right":
        return f"-{body}->"
    if r.direction == "left":
        return f"<-{body}-"
    return f"-{body}-"


def _pattern(p: object) -> str:
    if not isinstance(p, PathPattern):
        raise CypherPrintRefused(f"the pattern {type(p).__name__}")
    out = _node(p.nodes[0])
    for rel, node in zip(p.rels, p.nodes[1:], strict=True):
        out += _rel(rel) + _node(node)
    return out


def statement_lines(ast: CypherAST) -> list[str]:
    """``ast`` as Cypher text, a clause a line."""
    if ast.union_parts or ast.call_subqueries or ast.construct is not None:
        raise CypherPrintRefused("UNION, CALL or CONSTRUCT")
    lines: list[str] = []
    for step in ast.pipeline:
        if not isinstance(step, MatchStep):
            raise CypherPrintRefused(type(step).__name__)
        for m in step.matches:
            if m.variable is not None:
                raise CypherPrintRefused("a path variable")
            lines.append(f"{'OPTIONAL MATCH' if m.optional else 'MATCH'} {_pattern(m.pattern)}")
        if step.where is not None:
            lines.append(f"WHERE {expression(step.where.expression)}")
    ret = ast.return_clause
    if ret is None or ret.group_by is not None or ret.having is not None:
        raise CypherPrintRefused("a statement with no RETURN, or one grouped by keys")
    items = ", ".join(
        expression(i.expression) + (f" AS {_name(i.alias)}" if i.alias else "") for i in ret.items
    )
    lines.append(f"RETURN {'DISTINCT ' if ret.distinct else ''}{items}")
    if ast.order_by:
        lines.append(
            "ORDER BY "
            + ", ".join(f"{expression(o.expression)} {o.direction}" for o in ast.order_by)
        )
    if ast.skip is not None:
        lines.append(f"SKIP {int(ast.skip)}")
    if ast.limit is not None:
        lines.append(f"LIMIT {int(ast.limit)}")
    return lines


def print_cypher(ast: CypherAST) -> str:
    """``ast`` as Cypher text, or :class:`CypherPrintRefused`."""
    return "\n".join(statement_lines(ast))
