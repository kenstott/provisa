# Copyright (c) 2026 Kenneth Stott
# Canary: b3f8b1a6-c0db-4298-b4b6-13ffd20e0f9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SET n.p = v and REMOVE n.p of the session's own nodes and relationships (REQ-2176).

``[MATCH ... | UNWIND ... | WITH ...] SET a.p = <expr>[, b.q = <expr> ...]`` (or ``REMOVE a.p,
...``) reads the objects it names and each value through the surface's own governed read, as
the session's role, then writes each value into the object's row of its anchor (or its type's
property table). REMOVE n.p clears the property, exactly as SET n.p = null does, as Neo4j
defines it; a null object (an OPTIONAL MATCH's) is passed over.

A model node's property is a governed column, written only by SET of a declared column: a SET
whose objects are all model nodes is the governed path's, unchanged, and a REMOVE of one is
refused by name, as it is there.
"""

# Requirements: REQ-2176

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from provisa.compiler.temp_tables import TempSession
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import read as session_read
from provisa.ephemeral import store
from provisa.ephemeral.write import Read


@dataclass(frozen=True)
class PropClause:
    """A statement's leading clauses, its verb (SET or REMOVE), and each (variable, property,
    the value's expression as written; None for REMOVE)."""

    prefix: str
    verb: str
    items: tuple[tuple[str, str, str | None], ...]


@dataclass(frozen=True)
class Propertied:
    """What a property SET or REMOVE did, for its answer's counters."""

    properties_set: int


_NAME = r"(?:[A-Za-z_][A-Za-z0-9_]*|`(?:[^`]|``)*`)"
_ITEM = re.compile(rf"\s*({_NAME})\s*\.\s*({_NAME})\s*(?:=\s*(.*?))?\s*\Z", re.S)


def _unquoted(name: str) -> str:
    return name[1:-1].replace("``", "`") if name.startswith("`") else name


def _items_text(text: str) -> list[str]:
    """``text`` (a SET's or REMOVE's items) split at its commas outside brackets and strings."""
    from provisa.cypher.parser import _tokenize

    tokens, _ = _tokenize(text)
    cuts: list[int] = []
    depth = 0
    for t in tokens:
        if t.type in ("STRING", "IDENT", "NUMBER"):
            continue
        if t.value in ("(", "[", "{"):
            depth += 1
        elif t.value in (")", "]", "}"):
            depth -= 1
        elif t.value == "," and not depth:
            cuts.append(t.pos)
    out, at = [], 0
    for cut in cuts:
        out.append(text[at:cut])
        at = cut + 1
    out.append(text[at:])
    return out


def prop_clause(statement: str) -> PropClause | None:
    """The statement's property SET or REMOVE, or None when it is anything else: only one SET
    or REMOVE of property items alone, ending the statement, is one."""
    from provisa.cypher.parser import parse_cypher_statement, writes_in
    from provisa.gql.ir import RemoveClause, SetClause

    if not writes_in(statement):
        return None
    ast = parse_cypher_statement(statement)
    writes = ast.writes
    if len(writes) != 1 or ast.return_clause is not None or ast.pipeline[-1] is not writes[0]:
        return None
    [clause] = writes
    if not isinstance(clause, (SetClause, RemoveClause)):
        return None
    if any(item.property is None for item in clause.items):
        return None
    verb = "SET" if isinstance(clause, SetClause) else "REMOVE"
    body = statement[clause.start :].lstrip()[len(verb) :]
    items: list[tuple[str, str, str | None]] = []
    for text in _items_text(body):
        m = _ITEM.match(text)
        if m is None or (verb == "SET") != (m.group(3) is not None):
            return None
        items.append((_unquoted(m.group(1)), _unquoted(m.group(2)), m.group(3)))
    return PropClause(statement[: clause.start].strip(), verb, tuple(items))


async def write_props(
    statement: str,
    params: dict[str, Any],
    label_map: Any,
    session: TempSession,
    state: Any,
    read: Read,
    keep: tuple[str, ...] | None = None,
) -> tuple[Propertied, list[dict[str, Any]]] | None:
    """Run ``statement`` when it is a property SET or REMOVE of the session's objects, and with
    ``keep`` (the variables a read after it names) give each row's bindings of them
    (then.Bound); None when it is anything else, or a SET of model nodes alone (the governed
    path's)."""
    from provisa.ephemeral.then import bound_value

    clause = prop_clause(statement)
    if clause is None:
        return None
    if clause.verb == "SET" and not session_read.involved(session):
        return None  # no session object to write: the governed path's
    graph = store.graph(session)
    keep = keep or ()
    named = sorted({var for var, _, _ in clause.items})
    variables = sorted(set(named) | set(keep))
    returned = [f"{v} AS _v_{v}" for v in variables]
    returned += [
        f"{value} AS _s{i}" for i, (_, _, value) in enumerate(clause.items) if value is not None
    ]
    answer = await read(f"{clause.prefix} RETURN {', '.join(returned)}".strip(), params)
    nodes: dict[str, dict[str, Any]] = {}
    rels: dict[str, dict[str, Any]] = {}
    model: set[str] = set()
    written = 0
    for row in answer:
        objects = {var: bound_value(row[f"_v_{var}"], label_map, graph) for var in named}
        for i, (var, prop, value) in enumerate(clause.items):
            held = objects[var]
            if held is None:
                continue  # a null object takes no property
            if held[0] == "n" and held[1] in graph.node_keys:
                nodes.setdefault(held[1], {})[prop] = None if value is None else row[f"_s{i}"]
            elif held[0] == "r":
                rels.setdefault(held[1], {})[prop] = None if value is None else row[f"_s{i}"]
            elif held[0] == "n":
                model.add(var)
                continue
            else:
                raise CypherParseError(f"{var} is not a node or a relationship: no property is set")
            written += 1
    if model and clause.verb == "REMOVE":
        raise CypherParseError(
            f"A REMOVE of a property is not written on a model node ({', '.join(sorted(model))}): "
            f"a model node's property is a governed column, written only by SET of a declared column"
        )
    if model:
        if nodes or rels:
            raise CypherParseError(
                f"A SET writes {', '.join(sorted(model))} -- model nodes -- and the session's own "
                f"in one statement: write each in a statement of its own"
            )
        return None
    if nodes or rels:
        await store.set_properties(state, session, nodes, rels)
    bound_rows = [
        {var: bound_value(row[f"_v_{var}"], label_map, graph) for var in keep} for row in answer
    ]
    return Propertied(written), bound_rows
