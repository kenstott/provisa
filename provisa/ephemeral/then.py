# Copyright (c) 2026 Kenneth Stott
# Canary: 289fd9ea-dab1-4918-a99b-d3ede704cd2f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A write followed by reads in the same statement (REQ-2176): the write, then the rest as a
read over the session, which sees what it wrote.

``<head> <rest>``, where the head is the statement's leading clauses and its first write (a
CREATE -- several one after another are one --, a MERGE, a label SET or REMOVE, or a property
SET or REMOVE of session objects) and the rest is every clause after it (RETURN, WITH, MATCH,
another write). The head is done by its handler, which gives back, for each row it wrote, what
each variable the rest names is bound to: a node by its graph identity, a session relationship
by its id, any other value as it is. The rest then runs as one statement over those rows::

    UNWIND $__bound AS __b
    MATCH (n:L) WHERE id(n) = __b.n            -- a node, by its identity
    MATCH ()-[r:T]->() WHERE r.__rel_id = __b.r  -- a session relationship, by its id
    [WITH n, r, __b.x AS x]                    -- a value, as it was
    <rest>

through the seam again, so a later write in it is done the same way and its own rest read after
it. The rows keep their number and order, so an aggregate, SKIP and LIMIT in the rest count what
the write gave. A statement whose head is the governed path's (a model CREATE, a governed MERGE)
is left to it whole.
"""

# Requirements: REQ-2176

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, fields
from typing import Any

from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import store
from provisa.gql.label_map import SESSION_REL_ID

#: A variable's binding after a write: ("n", graph identity, labels) for a node, ("r", rel id,
#: type) for a session relationship, ("m", None, what) for a model relationship (which the rest
#: cannot find again), ("v", value) for any other value; None for null.
Bound = tuple | None
#: The variables the rest names, each row's bindings of them.
BoundRows = list[dict[str, Bound]]

#: The parameter the rest's rows are given in, and the variable each is unwound to.
PARAM = "__bound"
ROW = "__b"
#: While the rest runs, a session relationship's id reads as this property (read.overlay), which
#: the rest finds it again by; it is taken out of the answer's values.
REL_ID = SESSION_REL_ID
_FLAG = "cypher.bound"
#: While the rest runs, the label the session's nodes with no label are read by (read.overlay):
#: a node the write gave no label is found again among them alone, never in a union of every
#: label the role reads.
UNLABELED = "__session_unlabeled"

_CLAUSES = frozenset(
    {
        "MATCH",
        "OPTIONAL",
        "UNWIND",
        "WITH",
        "RETURN",
        "CREATE",
        "MERGE",
        "SET",
        "REMOVE",
        "DELETE",
        "DETACH",
        "CALL",
        "FOREACH",
        "UNION",
    }
)


@dataclass(frozen=True)
class Answered:
    """A statement's answer when its write is followed by reads: the rest's columns and rows,
    and what every write in it did."""

    columns: list[str]
    rows: list[dict[str, Any]]
    counters: dict[str, int]


def counters(made: Any) -> dict[str, int]:
    """What a write's answer counts, by name."""
    if isinstance(made, Answered):
        return dict(made.counters)
    return {f.name: getattr(made, f.name) for f in fields(made)}


def _summed(*parts: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = {}
    for part in parts:
        for name, n in part.items():
            out[name] = out.get(name, 0) + n
    return out


@dataclass(frozen=True)
class Split:
    """A statement as its first write and the rest after it."""

    head: str
    rest: str
    #: The head's write: "create", "merge", "delete", "labels" or "props".
    kind: str
    #: The variables in scope after the head, in order.
    scope: tuple[str, ...]


def clause_starts(statement: str) -> list[int]:
    """Where each clause of the statement begins: a clause keyword outside a string, a bracket
    or a map, that is not part of the clause before it (OPTIONAL MATCH, DETACH DELETE, a MERGE's
    ON CREATE SET and ON MATCH SET) or a name (``n.set``, ``:Create``)."""
    from provisa.cypher.parser import _tokenize

    tokens, _ = _tokenize(statement)
    out: list[int] = []
    depth = 0
    for i, t in enumerate(tokens):
        if t.type not in ("STRING", "IDENT", "NUMBER") and t.value in ("(", "[", "{"):
            depth += 1
            continue
        if t.type not in ("STRING", "IDENT", "NUMBER") and t.value in (")", "]", "}"):
            depth -= 1
            continue
        quoted = t.end - t.pos != len(t.value)  # `CREATE` is a name
        if depth or t.type != "IDENT" or quoted or t.value.upper() not in _CLAUSES:
            continue
        before = [tokens[j].value.upper() for j in range(max(0, i - 2), i)]
        word = t.value.upper()
        if before and before[-1] in (".", ":", "AS"):
            continue
        if word == "MATCH" and before[-1:] == ["OPTIONAL"]:
            continue
        if word == "DELETE" and before[-1:] == ["DETACH"]:
            continue
        if word in ("CREATE", "MATCH") and before[-1:] == ["ON"]:
            continue
        if word == "SET" and len(before) == 2 and before[0] == "ON":
            continue
        out.append(t.pos)
    return out


def scope_of(steps: list[Any]) -> list[str]:
    """The variables in scope after ``steps`` (a statement's leading clauses), in order."""
    from provisa.gql.ir import MatchStep, ProcedureCall, UnwindClause, WithClause

    out: list[str] = []

    def add(name: str | None) -> None:
        if name and name not in out:
            out.append(name)

    for step in steps:
        if isinstance(step, MatchStep):
            for m in step.matches:
                for n in getattr(m.pattern, "nodes", []):
                    add(n.variable)
                for r in getattr(m.pattern, "rels", []):
                    add(r.variable)
                add(m.variable)
        elif isinstance(step, UnwindClause):
            add(step.variable)
        elif isinstance(step, WithClause):
            names = [item.alias or item.name for item in step.items]
            if "*" not in names:
                out = []
            for name in names:
                if name != "*":
                    add(name)
        elif isinstance(step, ProcedureCall):
            for _, var in step.yields:
                add(var)
    return out


def _paths(steps: list[Any]) -> set[str]:
    """The path variables ``steps`` bind (``p = (a)-->(b)``), which a rest cannot find again."""
    from provisa.gql.ir import MatchStep

    return {
        m.variable
        for step in steps
        if isinstance(step, MatchStep)
        for m in step.matches
        if m.variable
    }


def split_of(statement: str) -> Split | None:
    """The statement as its first write and the rest after it; None when it writes nothing or
    its first write ends it."""
    from provisa.cypher.parser import parse_cypher_statement, writes_in
    from provisa.ephemeral.props import prop_items
    from provisa.gql.ir import CreateClause, DeleteClause, MergeClause, RemoveClause, SetClause

    if not writes_in(statement):
        return None
    ast = parse_cypher_statement(statement)
    pipeline: list[Any] = list(ast.pipeline)  # its write clauses among its steps
    writes = [i for i, step in enumerate(pipeline) if any(step is w for w in ast.writes)]
    if not writes or ast.union_parts or ast.call_subqueries:
        return None
    at = writes[0]
    first: Any = pipeline[at]
    end = at
    if isinstance(first, CreateClause):
        while end + 1 < len(pipeline) and isinstance(pipeline[end + 1], CreateClause):
            end += 1
        kind = "create"
    elif isinstance(first, MergeClause):
        kind = "merge"
    elif isinstance(first, DeleteClause):
        kind = "delete"
    elif isinstance(first, (SetClause, RemoveClause)):
        items = first.items
        if all(i.property is None and not getattr(i, "value", None) for i in items):
            kind = "labels"
        elif prop_items(items) is not None:
            kind = "props"
        else:
            return None
    else:
        return None
    if end == len(pipeline) - 1 and ast.return_clause is None:
        return None
    last = pipeline[end].start
    later = [pos for pos in clause_starts(statement) if pos > last]
    if not later:
        return None
    rest_at = later[0]
    scope = scope_of(pipeline[:at])
    if kind in ("create", "merge"):
        patterns = (
            [path for c in pipeline[at : end + 1] for path in c.patterns]
            if kind == "create"
            else [pipeline[at].pattern]
        )
        for path in patterns:
            for n in path.nodes:
                if n.variable and n.variable not in scope:
                    scope.append(n.variable)
            for r in path.rels:
                if r.variable and r.variable not in scope:
                    scope.append(r.variable)
    paths = _paths(pipeline[:at]) & names_in(statement[rest_at:])
    if paths:
        raise CypherParseError(
            f"A read after a write finds its rows again by their nodes and relationships: the "
            f"path {', '.join(sorted(paths))} is neither; return it before the write"
        )
    return Split(statement[:rest_at].rstrip(), statement[rest_at:], kind, tuple(scope))


_CARRY = re.compile(
    r"\s*WITH\s+((?:[A-Za-z_][A-Za-z0-9_]*\s*,\s*)*[A-Za-z_][A-Za-z0-9_]*)\s+"
    r"(?=(?:MATCH|OPTIONAL|UNWIND|CREATE|MERGE|SET|REMOVE|DELETE|DETACH|RETURN|WITH|CALL)\b)",
    re.I,
)
_CARRY_WHERE = re.compile(
    r"\s*WITH\s+((?:[A-Za-z_][A-Za-z0-9_]*\s*,\s*)*[A-Za-z_][A-Za-z0-9_]*)\s+WHERE\b", re.I
)
_MODIFIERS = re.compile(r"\b(?:ORDER\s+BY|SKIP|LIMIT)\b", re.I)
_NAME = re.compile(r"`(?:[^`]|``)*`|(?<![\w.$])[A-Za-z_][A-Za-z0-9_]*")
_STRING = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")


def names_in(text: str) -> set[str]:
    """The names a clause's text holds outside its strings (a property key after ``.`` and a
    parameter after ``$`` are not names)."""
    return {
        m.group()[1:-1].replace("``", "`") if m.group().startswith("`") else m.group()
        for m in _NAME.finditer(_STRING.sub("''", text))
    }


def bound_value(value: Any, label_map: Any, graph: store.Graph) -> Bound:
    """What a value a read returned is bound to, for a rest to find it again."""
    from provisa.ephemeral.write import _identity
    from provisa.gql.assembler import Edge, Node

    if value is None:
        return None
    if isinstance(value, Edge):
        rel_id, rel_type = str(value.id), value.type
    elif isinstance(value, dict) and "type" in value and ("startNode" in value or "start" in value):
        rel_id, rel_type = str(value.get("identity", value.get("id"))), str(value["type"])
    elif isinstance(value, Node) or (
        isinstance(value, dict) and "label" in value and "properties" in value
    ):
        node_id, keys = _identity(value, label_map, graph)
        return ("n", node_id, keys)
    else:
        return ("v", value)
    if rel_id in graph.rel_ends:
        return ("r", rel_id, rel_type)
    return ("m", None, rel_type)


def _quoted(name: str) -> str:
    return (
        name
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
        else ("`" + name.replace("`", "``") + "`")
    )


def rest_read(
    split: Split, rows: BoundRows, graph: store.Graph
) -> tuple[str, list[dict[str, Any]]]:
    """The rest as one statement over the head's rows, and the rows it is given. A WITH that
    only carries variables on (``WITH a, b``) is what finding them again already is, and is
    dropped: a variable found again with no label is carried by no WITH (the compiler's)."""
    rest = split.rest
    scope = split.scope
    condition = None
    carried_on = _CARRY.match(rest) or _CARRY_WHERE.match(rest)
    if carried_on is not None:
        names = {n.strip().strip("`") for n in carried_on.group(1).split(",")}
        after = rest[carried_on.end() :]
        if carried_on.re is _CARRY_WHERE:
            # WITH a WHERE <condition>: the condition is the rebinding's own, up to the next
            # clause, when the WITH orders and limits nothing.
            ends = [pos for pos in clause_starts(after) if pos > 0]
            text = after[: ends[0]] if ends else after
            if not ends or _MODIFIERS.search(_STRING.sub("''", text)):
                names = set()
            else:
                condition, after = text.strip(), after[ends[0] :]
        if names and names <= set(scope):
            rest = after
            scope = tuple(v for v in scope if v in names | names_in(condition or ""))
        else:
            condition = None
    used = [v for v in scope if v in names_in(rest) | names_in(condition or "")]
    matches: list[str] = []
    carried: list[str] = []  # WITH items
    found: list[str] = []  # the variables a MATCH finds again
    fields_of: dict[str, list[str]] = {}
    for var in used:
        held = [row.get(var) for row in rows]
        kinds = {b[0] for b in held if b is not None}
        name = _quoted(var)
        field = f"{ROW}.{_quoted(var)}"
        if not kinds:
            carried.append(f"null AS {name}")
            fields_of[var] = []
            continue
        if None in held:
            raise CypherParseError(
                f"A read after a write finds {var} again in every row the write gave: it is "
                f"null in some and not in others"
            )
        if len(kinds) > 1:
            raise CypherParseError(
                f"{var} is bound to values of more than one kind before the read"
            )
        [kind] = kinds
        if kind == "v":
            carried.append(f"{field} AS {name}")
            fields_of[var] = [var]
        elif kind == "m":
            raise CypherParseError(
                f"{var} is a model relationship: a read after a write finds a session's "
                f"relationship again, not a model one; return it before the write"
            )
        elif kind == "r":
            types = {b[2] for b in held if b is not None}
            if len(types) > 1:
                raise CypherParseError(
                    f"{var} is bound to relationships of several types before the read"
                )
            [rel_type] = types
            matches.append(
                f"MATCH ()-[{name}:{_quoted(rel_type)}]->() WHERE {name}.{REL_ID} = {field}"
            )
            found.append(name)
            fields_of[var] = [var]
        else:
            ids = [b[1] for b in held if b is not None]
            ephemeral = [i for i in ids if i in graph.node_keys]
            firsts = {graph.node_keys[i][0] for i in ephemeral}
            if len(ephemeral) == len(ids) and len(firsts) == 1:
                [label] = firsts
                matches.append(
                    f"MATCH ({name}:{_quoted(label or UNLABELED)}) WHERE id({name}) = {field}"
                )
                fields_of[var] = [var]
            else:
                # Read with no label, a node's id is ``Label|key`` -- a session node's label
                # whichever of its own its row is read by.
                tail = f"{ROW}.{_quoted(var + '__e')}"
                if not ephemeral:
                    where = f"id({name}) = {field}"
                elif len(ephemeral) == len(ids):
                    where = f"id({name}) ENDS WITH {tail}"
                else:
                    where = f"(id({name}) = {field} OR id({name}) ENDS WITH {tail})"
                matches.append(f"MATCH ({name}) WHERE {where}")
                fields_of[var] = [var, var + "__e"]
            found.append(name)
    params: list[dict[str, Any]] = []
    for row in rows:
        out: dict[str, Any] = {"__live": True}
        for var, names in fields_of.items():
            if not names:
                continue  # null in every row: carried as null, never found again
            b = row[var]
            assert b is not None
            if b[0] == "v":
                out[var] = b[1]
            elif b[0] == "r":
                out[var] = b[1]
            elif len(names) == 1:
                out[var] = b[1]
            else:
                ephemeral = b[1] in graph.node_keys
                out[var] = "" if ephemeral else b[1]
                out[var + "__e"] = f"|{b[1]}" if ephemeral else None
        params.append(out)
    lead = [f"UNWIND ${PARAM} AS {ROW}"]
    if not params:
        # No row: one that is no row, which no node or relationship is found again by.
        params.append(
            {
                "__live": False,
                **{n: None if n.endswith("__e") else "" for ns in fields_of.values() for n in ns},
            }
        )
        if not matches:
            lead.append(f"WITH {ROW} WHERE {ROW}.__live")
    if condition is not None and not matches and not carried:
        lead.append(f"WITH {ROW} WHERE {condition}")
    elif condition is not None and not carried:
        matches[-1] += f" AND ({condition})"
    lead += matches
    if carried:
        lead.append("WITH " + ", ".join([*found, *carried]))
        if condition is not None:
            lead[-1] += f" WHERE {condition}"
    return " ".join([*lead, rest.strip()]), params


def _columns(statement: str) -> list[str]:
    """The columns a statement answers with, as its RETURN names them."""
    from provisa.cypher.parser import parse_cypher_statement

    ast = parse_cypher_statement(statement)
    if ast.return_clause is None:
        return []
    return [item.alias or item.name for item in ast.return_clause.items]


def _without_rel_ids(value: Any) -> Any:
    """``value`` with a session relationship's id property (REL_ID) taken out."""
    from provisa.gql.assembler import Edge, Path

    if isinstance(value, Edge):
        value.properties.pop(REL_ID, None)
    elif isinstance(value, Path):
        for edge in value.edges:
            edge.properties.pop(REL_ID, None)
    elif isinstance(value, dict):
        props = value.get("properties")
        if isinstance(props, dict):
            props.pop(REL_ID, None)
        for v in value.values():
            if isinstance(v, (dict, list)):
                _without_rel_ids(v)
    elif isinstance(value, list):
        for v in value:
            _without_rel_ids(v)
    return value


#: Does a statement's head and gives back what it wrote and each row's bindings of ``keep``.
Head = Callable[[str, tuple[str, ...]], Awaitable[tuple[Any, BoundRows] | None]]
Again = Callable[[str, dict[str, Any]], Awaitable[Any]]


def rel_ids_read(session: Any) -> bool:
    """Whether a session relationship's id is read as a property (REL_ID) now: only while a
    rest finds its relationships again."""
    return bool(session.extensions.get(_FLAG))


async def write_then_read(
    statement: str,
    params: dict[str, Any],
    session: Any,
    head: Callable[[Split], Awaitable[tuple[Any, BoundRows] | None]],
    again: Again,
    run_read: Callable[[str, dict[str, Any]], Awaitable[list[dict[str, Any]]]],
    run_write: Callable[[str, dict[str, Any]], Awaitable[None]],
) -> Answered | None:
    """Do ``statement`` when a write in it is followed by more: the head by ``head``, the rest
    over its rows through ``again`` (the seam: a later write), else ``run_read``. None when no
    write is followed by more, or the head is the governed path's."""
    from provisa.cypher.parser import writes_in

    split = split_of(statement)
    if split is None:
        return None
    done = await head(split)
    if done is None:
        return None
    made, rows = done
    tail, bound = rest_read(split, rows, store.graph(session))
    values = {**params, PARAM: bound}
    held = session.extensions.get(_FLAG)
    session.extensions[_FLAG] = True
    try:
        later = await again(tail, values)
        if later is None and writes_in(tail):
            await run_write(tail, values)
            answer = Answered([], [], {})
        elif later is None:
            answer_rows = await run_read(tail, values)
            columns = list(answer_rows[0]) if answer_rows else _columns(tail)
            answer = Answered(columns, [_without_rel_ids(r) for r in answer_rows], {})
        elif isinstance(later, Answered):
            answer = Answered(
                later.columns, [_without_rel_ids(r) for r in later.rows], later.counters
            )
        else:
            answer = Answered([], [], counters(later))
    finally:
        if held is None:
            session.extensions.pop(_FLAG, None)
    return Answered(answer.columns, answer.rows, _summed(counters(made), answer.counters))
