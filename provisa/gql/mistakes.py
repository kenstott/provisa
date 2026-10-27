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
    ListComprehension,
    ListLiteral,
    Literal,
    MapLiteral,
    Parameter,
    Property,
    Unary,
    Variable,
    walk,
)
from provisa.gql.group_by import AGGREGATES
from provisa.gql.translator_types import CypherMistake

from provisa.gql.ir import (
    CypherAST,
    MatchStep,
    NodePattern,
    PathFunction,
    PathPattern,
    UnwindClause,
    WithClause,
)

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
    if kind == "ProcedureCall":  # REQ-2168: YIELD binds each variable it names
        bound.update(var for _, var in node.yields)  # type: ignore[attr-defined]
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
        elif type(step).__name__ == "ProcedureCall":
            parts.append(("CALL", step.args))  # type: ignore[attr-defined]
            parts.append(("WHERE", step.where))  # type: ignore[attr-defined]
    parts.append(("RETURN", ast.return_clause))
    parts.append(("ORDER BY", ast.order_by))
    for clause, part in parts:
        used: list[str] = []
        _names(part, set(), used)
        if name in used:
            return clause
    return "MATCH"


def _property_owners(ast: CypherAST, name: str, label_map: CypherLabelMap) -> list[tuple[str, str]]:
    """Each (variable, label) the statement binds whose label's table has a property ``name``,
    in the order the statement binds them."""
    owners: list[tuple[str, str]] = []

    def visit(node: object) -> None:
        if isinstance(node, (list, tuple)):
            for item in node:
                visit(item)
            return
        if not dataclasses.is_dataclass(node) or isinstance(node, type):
            return
        if isinstance(node, NodePattern) and node.variable and node.labels:
            labels = [label_map.canonical_label(lbl) for lbl in node.labels]
            label = ":".join(labels)
            for nm in label_map.nodes.values():
                named = nm.label == label or (len(labels) == 1 and nm.table_label == labels[0])
                if named and name in nm.properties and (node.variable, label) not in owners:
                    owners.append((node.variable, label))
        for f in dataclasses.fields(node):
            visit(getattr(node, f.name))

    visit(ast)
    return owners


def undefined_variable(
    ast: CypherAST, label_map: CypherLabelMap | None = None
) -> CypherMistake | None:
    """The refusal of a statement that reads a variable nothing in it binds -- no MATCH pattern,
    WITH, UNWIND, CALL or comprehension -- naming it, its clause, and the variables it binds. With
    the role's ``label_map``, a name that is a property of a bound variable's label is offered as
    that variable's property (``COUNT(id)`` meant ``a.id``)."""
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
        base = (
            f"{clause} reads {name}, which nothing in the statement binds: a variable is bound by "
            f"a MATCH pattern, WITH, UNWIND or CALL. Bound here: {named}."
        )
        owners = _property_owners(ast, name, label_map) if label_map is not None else []
        if len(owners) == 1:
            var, label = owners[0]
            return CypherMistake(
                "cypher.undefined_variable_property",
                f"{base} Did you mean {var}.{name}? {var} is bound to :{label}, which has a "
                f"property {name}.",
                variable=name,
                clause=clause,
                bound=named,
                suggestion=f"{var}.{name}",
                owner=var,
                label=label,
            )
        if owners:
            suggestions = " or ".join(f"{var}.{name}" for var, _ in owners)
            return CypherMistake(
                "cypher.undefined_variable_properties",
                f"{base} Did you mean {suggestions}? Each is bound to a label with a property "
                f"{name}.",
                variable=name,
                clause=clause,
                bound=named,
                suggestions=suggestions,
            )
        return CypherMistake(
            "cypher.undefined_variable", base, variable=name, clause=clause, bound=named
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
        for comprehension in walk(item.expression):
            if not isinstance(comprehension, ListComprehension):
                continue
            inside = [comprehension.predicate, comprehension.projection]
            found = [c for part in inside for c in _aggregates(part)]
            if found:
                call = _call_text(found[0])
                return CypherMistake(
                    "cypher.aggregate_in_list",
                    f"[{comprehension.var} IN ... | {call}] aggregates inside a list comprehension, "
                    "which reads one list at a time. Aggregate in a WITH first and use its result "
                    "in the list.",
                    variable=comprehension.var,
                    aggregate=call,
                )
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


def _kind_conflict(variable: str, kinds: tuple[str, str]) -> CypherMistake:
    if "value" in kinds:
        return CypherMistake(
            "cypher.value_as_node",
            f"MATCH reads {variable} as a node or relationship, and the statement binds it to a "
            "value that is neither. Give the node its own variable.",
            variable=variable,
        )
    if "path" in kinds:
        return CypherMistake(
            "cypher.path_variable_reused",
            f"{variable} names the path and a node or relationship in it: a variable names one "
            "thing. Give each its own variable.",
            variable=variable,
        )
    return CypherMistake(
        "cypher.node_and_relationship",
        f"{variable} names both a node and a relationship: a variable names one thing. Give each "
        "its own variable.",
        variable=variable,
    )


def _length_of_entity(expressions: list[CypherExpr], kinds: dict[str, str]) -> CypherMistake | None:
    for expr in expressions:
        for call in walk(expr):
            if not (isinstance(call, FunctionCall) and call.name.lower() == "length"):
                continue
            arg = call.args[0] if len(call.args) == 1 else None
            if isinstance(arg, Variable) and kinds.get(arg.name) in ("node", "relationship"):
                return CypherMistake(
                    "cypher.length_of_entity",
                    f"length({arg.name}) takes a path, and {arg.name} is a node or relationship. "
                    "Bind a path, MATCH p = ..., and take length(p).",
                    variable=arg.name,
                )
    return None


def variable_misuse(ast: CypherAST) -> CypherMistake | None:
    """The refusal of a variable used as two things -- a node and a relationship, or a path and a
    part of it -- or of length() taken of a node or relationship. What a variable names is read from
    the MATCH patterns since the last WITH, which projects a fresh set."""
    kinds: dict[str, str] = {}
    for step in ast.pipeline:
        if isinstance(step, WithClause):
            found = _length_of_entity([i.expression for i in step.items], kinds)
            if found is not None:
                return found
            # A variable bound to null is no value a pattern refuses: an OPTIONAL MATCH from it
            # matches nothing (REQ-778, amended 2026-10-27; translator._null_bound_matches).
            kinds = {
                item.alias: "value"
                for item in step.items
                if item.alias
                and isinstance(item.expression, (Literal, ListLiteral, MapLiteral))
                and not (isinstance(item.expression, Literal) and item.expression.kind == "null")
            }
            continue
        if not isinstance(step, MatchStep):
            continue
        for clause in step.matches:
            pattern = clause.pattern
            if isinstance(pattern, PathFunction):
                pattern = pattern.pattern
            if not isinstance(pattern, PathPattern):
                continue
            named = [(n.variable, "node") for n in pattern.nodes]
            named += [(r.variable, "relationship") for r in pattern.rels]
            if clause.variable is not None:
                named.append((clause.variable, "path"))
            for variable, kind in named:
                if variable is None:
                    continue
                bound = kinds.setdefault(variable, kind)
                if bound != kind:
                    return _kind_conflict(variable, (bound, kind))
                if kind == "path":
                    continue
                if variable == clause.variable:
                    return _kind_conflict(variable, (kind, "path"))
    if ast.return_clause is None:
        return None
    return _length_of_entity([i.expression for i in ast.return_clause.items], kinds)


def _columns(ast: CypherAST) -> list[str]:
    items = ast.return_clause.items if ast.return_clause is not None else []
    return [item.alias or item.name for item in items]


def _unions(ast: CypherAST) -> tuple[list[CypherAST], list[bool]]:
    """The statements a UNION chain joins, in order, and whether each join is UNION ALL."""
    parts, alls = [ast], []
    while parts[-1].union_parts:
        following, is_all = parts[-1].union_parts[0]
        parts.append(following)
        alls.append(is_all)
    return parts, alls


def projection_misuse(ast: CypherAST) -> CypherMistake | None:
    """The refusal of two columns of one name in a RETURN or WITH, of UNION joining statements
    whose columns differ, or of UNION and UNION ALL mixed in one statement."""
    parts, alls = _unions(ast)
    for part in parts:
        clauses = [("WITH", [i.alias or i.name for i in w.items]) for w in part.with_clauses]
        clauses.append(("RETURN", _columns(part)))
        for clause, columns in clauses:
            repeated = next((c for n, c in enumerate(columns) if c in columns[:n]), None)
            if repeated is not None:
                return CypherMistake(
                    "cypher.column_name_conflict",
                    f"{clause} names two columns {repeated}: each column has its own name. Rename "
                    "one with AS.",
                    clause=clause,
                    column=repeated,
                )
    if len(set(alls)) > 1:
        return CypherMistake(
            "cypher.union_mixed",
            "UNION and UNION ALL are mixed in one statement: use one of them throughout.",
        )
    first = _columns(parts[0])
    for part in parts[1:]:
        other = _columns(part)
        if other != first:
            return CypherMistake(
                "cypher.union_columns_differ",
                f"The statements UNION joins return different columns, {', '.join(first)} and "
                f"{', '.join(other)}: each returns the same column names.",
                first=", ".join(first),
                second=", ".join(other),
            )
    return None


def _statement_expressions(ast: CypherAST) -> list[CypherExpr]:
    found: list[CypherExpr] = []
    for step in ast.pipeline:
        if isinstance(step, WithClause):
            found += [i.expression for i in step.items]
        where = step.where if isinstance(step, (MatchStep, WithClause)) else None
        if where is not None:
            found.append(where.expression)
    if ast.return_clause is not None:
        found += [i.expression for i in ast.return_clause.items]
    return found


def _not_boolean(expr: CypherExpr) -> bool:
    """A value written in the statement that is no boolean and no null."""
    if isinstance(expr, Literal):
        return expr.kind not in ("boolean", "null")
    return isinstance(expr, (ListLiteral, MapLiteral))


def expression_misuse(ast: CypherAST) -> CypherMistake | None:
    """The refusal of AND, OR, XOR or NOT given a value written in the statement that is no
    boolean, or of an aggregate of rand(), whose value changes on every call."""
    for expr in _statement_expressions(ast):
        for node in walk(expr):
            if isinstance(node, Binary) and node.op in ("AND", "OR", "XOR"):
                operands = [node.left, node.right]
                operator = node.op
            elif isinstance(node, Unary) and node.op.upper() == "NOT":
                operands = [node.operand]
                operator = "NOT"
            else:
                operands = []
                operator = ""
            if any(_not_boolean(o) for o in operands):
                return CypherMistake(
                    "cypher.boolean_operand",
                    f"{operator} takes true, false or null, and is given a value of another type "
                    "written in the statement.",
                    operator=operator,
                )
            if isinstance(node, FunctionCall) and node.name.lower() in AGGREGATES:
                random = [
                    n
                    for a in node.args
                    for n in walk(a)
                    if isinstance(n, FunctionCall) and n.name.lower() == "rand"
                ]
                if random:
                    call = _call_text(node)
                    return CypherMistake(
                        "cypher.nondeterministic_aggregate",
                        f"{call} aggregates rand(), which changes on every call. Compute it in a "
                        "WITH first and aggregate the result.",
                        aggregate=call,
                    )
    return None
