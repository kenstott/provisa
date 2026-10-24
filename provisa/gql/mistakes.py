# Copyright (c) 2026 Kenneth Stott
# Canary: b66fc41f-a618-42b7-ac6a-421207995b06
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A mistake in the user's statement, refused at translation in the user's own terms (REQ-778):
it names the clause or expression, what is wrong, and the fix, before any engine runs it. Each is
a stable code with params, so the UI words it in the reader's language (serverErrors catalog)."""

from __future__ import annotations

import dataclasses
import difflib
import re
from typing import TYPE_CHECKING

from provisa.gql.expr_ast import (
    Binary,
    CypherExpr,
    FunctionCall,
    Literal,
    Parameter,
    Property,
    Variable,
    walk,
)
from provisa.gql.group_by import AGGREGATES
from provisa.gql.translator_types import CypherMistake

from provisa.gql.ir import CypherAST, MatchStep, UnwindClause, WithClause

if TYPE_CHECKING:
    from provisa.gql.label_map import CypherLabelMap


def closest_names(name: str, known: set[str]) -> list[str]:
    """The registered names nearest ``name``: up to three, nearest first."""
    return difflib.get_close_matches(name, sorted(known), n=3, cutoff=0.0)


def cypher_text(expr: CypherExpr) -> str | None:
    """expr as the user wrote it, for the simple expressions a message names (a variable, a
    property, a parameter, a literal); None for any other, which a message then leaves out."""
    if isinstance(expr, Variable):
        return expr.name
    if isinstance(expr, Parameter):
        return f"${expr.name}"
    if isinstance(expr, Property):
        owner = cypher_text(expr.obj)
        return None if owner is None else f"{owner}.{expr.name}"
    if isinstance(expr, Literal):
        if expr.kind == "string":
            return f'"{expr.value}"'
        if expr.kind == "null":
            return "null"
        if expr.kind == "boolean":
            return "true" if expr.value else "false"
        return str(expr.value)
    if isinstance(expr, FunctionCall):
        if expr.star:
            return f"{expr.name}(*)"
        args = [cypher_text(a) for a in expr.args]
        if any(a is None for a in args):
            return None
        return f"{expr.name}({'DISTINCT ' if expr.distinct else ''}{', '.join(args)})"  # type: ignore[arg-type]
    if isinstance(expr, Binary):
        left, right = cypher_text(expr.left), cypher_text(expr.right)
        return None if left is None or right is None else f"{left} {expr.op} {right}"
    return None


#: Python's wording of a regex error, as the reader is told it (the character at the error's
#: position fills {ch}).
_REGEX_REASONS = {
    "nothing to repeat": '"{ch}" must follow something to repeat',
    "multiple repeat": '"{ch}" repeats something that already repeats',
    "missing ), unterminated subpattern": 'a "(" is never closed',
    "unbalanced parenthesis": 'a ")" has no "(" to close',
    "unterminated character set": 'a "[" is never closed',
    "min repeat greater than max repeat": "a {{m,n}} repeat has its minimum above its maximum",
}

#: What one of the engines does not take, though Python's re does: RE2 (DuckDB) has no
#: lookaround, backreference, atomic group or possessive repeat; Java (Trino) no (?P<name>).
_ENGINE_UNSUPPORTED = (
    (re.compile(r"\(\?<?[=!]"), "a lookahead or lookbehind is not supported"),
    (re.compile(r"\\[1-9]"), "a backreference is not supported"),
    (re.compile(r"\(\?>"), "an atomic group is not supported"),
    (re.compile(r"(?<!\\)[*+?}]\+"), "a possessive repeat is not supported"),
    (
        re.compile(r"\(\?P[<=>]"),
        'a Python-style "(?P...)" group is not supported; name a group (?<name>...)',
    ),
)


def regex_problem(pattern: str) -> str | None:
    """Why pattern is no regular expression every engine takes, or None when it is one."""
    try:
        re.compile(pattern)
    except re.error as exc:
        pos = exc.pos if exc.pos is not None else 0
        ch = pattern[pos] if pos < len(pattern) else ""
        reason = _REGEX_REASONS.get(exc.msg)
        return reason.format(ch=ch) if reason is not None else exc.msg
    for construct, reason in _ENGINE_UNSUPPORTED:
        if construct.search(pattern):
            return reason
    return None


def invalid_regex(left: CypherExpr, pattern: str, why: str) -> CypherMistake:
    """The refusal of left =~ pattern whose literal pattern is no regular expression."""
    word = "".join(re.findall(r"\w+", pattern)) or "text"
    owner = cypher_text(left)
    expression = f'{owner} =~ "{pattern}"' if owner is not None else f'=~ "{pattern}"'
    return CypherMistake(
        "cypher.invalid_regex",
        f'The pattern "{pattern}" in {expression} is not a valid regular expression: {why}. '
        f'=~ takes a regular expression, e.g. ".*{word}.*", or use CONTAINS "{word}".',
        pattern=pattern,
        expression=expression,
        reason=why,
        word=word,
    )


def unknown_label(label: str, label_map: CypherLabelMap) -> CypherMistake:
    """The refusal of a label no table this role reads has, offering three registered labels,
    the nearest to it first."""
    registered = (
        {nm.label for nm in label_map.nodes.values()}
        | set(label_map.nodes_by_table)
        | set(label_map.domains)
    )
    registered = {name for name in registered if not name.startswith("__")}
    if not registered:
        return CypherMistake(
            "cypher.no_labels",
            f"There is no label :{label}: this role can read no table.",
            label=label,
        )
    near = ", ".join(f":{n}" for n in closest_names(label, registered))
    return CypherMistake(
        "cypher.unknown_label",
        f"There is no label :{label}. Registered labels: {near}.",
        label=label,
        suggestions=near,
    )


_NUMBER_TYPES = re.compile(
    r"^(tinyint|smallint|int|integer|bigint|hugeint|decimal|numeric|real|float|double)", re.I
)
_TEXT_TYPES = re.compile(r"^(varchar|text|char|character|string|nvarchar)", re.I)
_BOOLEAN_TYPES = re.compile(r"^(bool|boolean)$", re.I)


def _kind_of_type(data_type: str) -> str | None:
    """number / text / boolean for a registered type a literal can be told apart from, or None
    for one an engine compares a literal's text to by its own rules (a date, a timestamp)."""
    if _NUMBER_TYPES.match(data_type):
        return "number"
    if _TEXT_TYPES.match(data_type):
        return "text"
    if _BOOLEAN_TYPES.match(data_type):
        return "boolean"
    return None


def _kind_of_literal(lit: Literal) -> str | None:
    """number / text / boolean for a literal; a string that reads as a number is either."""
    if lit.kind == "number":
        return "number"
    if lit.kind == "boolean":
        return "boolean"
    if lit.kind == "string":
        return "text"
    return None


def _reads_as_number(text: str) -> bool:
    try:
        float(text)
    except ValueError:
        return False
    return True


def type_mismatch(
    op: str, prop: Property, data_type: str, lit: Literal, prop_first: bool
) -> CypherMistake | None:
    """The refusal of ``prop op lit`` (or ``lit op prop``) whose literal is of a kind the
    property's registered type is never equal or ordered to; None when they compare."""
    column = _kind_of_type(data_type)
    value = _kind_of_literal(lit)
    if column is None or value is None or column == value:
        return None
    if column == "number" and value == "text" and _reads_as_number(str(lit.value)):
        return None
    name = cypher_text(prop)
    shown = cypher_text(lit)
    if name is None or shown is None:
        return None
    expression = f"{name} {op} {shown}" if prop_first else f"{shown} {op} {name}"
    if column == "number":
        fix = f"Compare {name} to a number, e.g. {name} {op} 10."
    elif column == "text":
        fix = f'Quote the value: {name} {op} "{lit.value}".'
    else:
        fix = f"Compare {name} to true or false."
    return CypherMistake(
        "cypher.type_mismatch",
        f"In {expression}, {shown} cannot be compared with {name}, which is of type "
        f"{data_type}. {fix}",
        expression=expression,
        property=name,
        property_type=data_type,
        value=shown,
        fix=fix,
    )


#: The fields a statement binds a name with: a pattern's node, relationship and path variables,
#: an UNWIND's, a projection's alias, a CALL's imported variables, and an expression's own
#: (a list comprehension's, a quantifier's, a reduce's).
_BINDERS = {
    ("NodePattern", "variable"),
    ("RelPattern", "variable"),
    ("MatchClause", "variable"),
    ("UnwindClause", "variable"),
    ("ReturnItem", "alias"),
    ("ListComprehension", "var"),
    ("Quantifier", "var"),
    ("Reduce", "accumulator"),
    ("Reduce", "var"),
    ("PatternComprehension", "path_var"),
}


def _names(node: object, bound: set[str], used: list[str]) -> None:
    """Every name ``node`` binds (into ``bound``) and every variable it reads (into ``used``, in
    the order the statement writes them), through every clause and expression under it."""
    if isinstance(node, (list, tuple)):
        for item in node:
            _names(item, bound, used)
        return
    if not dataclasses.is_dataclass(node) or isinstance(node, type):
        return
    kind = type(node).__name__
    if isinstance(node, Variable):
        used.append(node.name)
    if kind == "MapProjection":
        used.append(node.var)  # type: ignore[attr-defined]
    if kind == "CallSubquery":
        bound.update(node.imported_vars)  # type: ignore[attr-defined]
    for f in dataclasses.fields(node):
        value = getattr(node, f.name)
        if (kind, f.name) in _BINDERS and isinstance(value, str):
            bound.add(value)
        else:
            _names(value, bound, used)


def _clause_of(name: str, ast: CypherAST) -> str:
    """The clause of ``ast`` that first reads ``name``: WITH, WHERE, UNWIND, RETURN or ORDER BY."""
    parts: list[tuple[str, object]] = []
    for step in ast.pipeline:
        if isinstance(step, WithClause):
            parts.append(("WITH", step.items))
            parts.append(("WHERE", step.where))
        elif isinstance(step, MatchStep):
            parts.append(("WHERE", step.where))
        elif isinstance(step, UnwindClause):
            parts.append(("UNWIND", step.expression))
    parts.append(("RETURN", ast.return_clause))
    parts.append(("ORDER BY", ast.order_by))
    for clause, part in parts:
        used: list[str] = []
        _names(part, set(), used)
        if name in used:
            return clause
    return "MATCH"


def undefined_variable(ast: CypherAST) -> CypherMistake | None:
    """The refusal of a statement that reads a variable nothing in it binds -- no MATCH pattern,
    WITH, UNWIND, CALL or comprehension -- naming it, its clause, and the variables it binds."""
    bound: set[str] = set()
    used: list[str] = []
    _names(ast, bound, used)
    for name in used:
        if name in bound:
            continue
        clause = _clause_of(name, ast)
        # The RETURN's own aliases bind only for its ORDER BY: they are no variables to offer.
        returned = {i.alias for i in ast.return_clause.items} if ast.return_clause else set()
        named = ", ".join(sorted(bound - returned))
        if not named:
            return CypherMistake(
                "cypher.no_variables",
                f"{clause} reads {name}, but the statement binds no variable: a variable is bound "
                "by a MATCH pattern, WITH, UNWIND or CALL.",
                variable=name,
                clause=clause,
            )
        return CypherMistake(
            "cypher.undefined_variable",
            f"{clause} reads {name}, which nothing in the statement binds: a variable is bound by "
            f"a MATCH pattern, WITH, UNWIND or CALL. Bound here: {named}.",
            variable=name,
            clause=clause,
            bound=named,
        )
    return None


def _aggregates(expr: object) -> list[FunctionCall]:
    """The aggregate calls in ``expr`` (not inside a statement of its own, EXISTS { } and the like)."""
    if not isinstance(expr, CypherExpr):
        return []
    return [n for n in walk(expr) if isinstance(n, FunctionCall) and n.name.lower() in AGGREGATES]


def _call_text(call: FunctionCall) -> str:
    """The aggregate as written, or its name with its arguments elided where one is no simple
    expression (a message names the call either way)."""
    written = cypher_text(call)
    return written if written is not None else f"{call.name}(...)"


def aggregate_misuse(ast: CypherAST) -> CypherMistake | None:
    """The refusal of an aggregate where Cypher takes none: in a WHERE (a row filter, before any
    grouping) or inside another aggregate."""
    for step in ast.pipeline:
        where = step.where if isinstance(step, (MatchStep, WithClause)) else None
        if where is None:
            continue
        found = _aggregates(where.expression)
        if found:
            call = _call_text(found[0])
            return CypherMistake(
                "cypher.aggregate_in_where",
                f"WHERE cannot use the aggregate {call}: WHERE filters rows before they are "
                "grouped. Aggregate in a WITH and filter after it.",
                aggregate=call,
            )
    items = [i for w in ast.with_clauses for i in w.items]
    items += ast.return_clause.items if ast.return_clause is not None else []
    for item in items:
        for call in _aggregates(item.expression):
            for arg in call.args:
                nested = _aggregates(arg)
                if nested:
                    outer = _call_text(call)
                    inner = _call_text(nested[0])
                    return CypherMistake(
                        "cypher.nested_aggregate",
                        f"{outer} aggregates the aggregate {inner}: an aggregate cannot be taken "
                        "of another. Aggregate in a WITH first.",
                        outer=outer,
                        inner=inner,
                    )
    return None
