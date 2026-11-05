# Copyright (c) 2026 Kenneth Stott
# Canary: b3f8b1a6-c0db-4298-b4b6-13ffd20e0f9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SET and REMOVE of a property of the session's own nodes and relationships (REQ-2176).

``[MATCH ... | UNWIND ... | WITH ...] SET a.p = <expr>[, b = <map>, c += <map> ...]`` (or ``REMOVE
a.p, ...``) reads the objects it names and each value through the surface's own governed read, as
the session's role, then writes each value into the object's row of its anchor (or its type's
property table). ``SET v.p = e`` writes one property; ``SET v = e`` makes the object's properties
exactly the map ``e`` (a map, or a node's or relationship's properties), clearing every other;
``SET v += e`` writes each of the map's, keeping the rest. A null value clears the property;
REMOVE n.p clears it, exactly as SET n.p = null does, as Neo4j defines it; a null object (an
OPTIONAL MATCH's) is passed over. A map, or a list holding one, is no property value and is
refused by name, as Cypher refuses it.

A model node's property is a governed column, written only by SET of a declared column: a SET
whose objects are all model nodes is the governed path's, unchanged, and a REMOVE of one is
refused by name, as it is there.
"""

# Requirements: REQ-2176

from __future__ import annotations


from dataclasses import dataclass
from typing import Any

from provisa.compiler.temp_tables import TempSession
from provisa.cypher import counters
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import read as session_read
from provisa.ephemeral import store
from provisa.ephemeral.write import Read


@dataclass(frozen=True)
class PropItem:
    """One item of a property SET or REMOVE: its variable, the property it writes (None for the
    whole map, ``v = e`` or ``v += e``), its value's expression as written (None for REMOVE), and
    whether a map is added to the properties held (``+=``) rather than replacing them."""

    variable: str
    property: str | None
    value: str | None
    merge: bool = False


@dataclass(frozen=True)
class PropClause:
    """A statement's leading clauses, its verb (SET or REMOVE), and its items."""

    prefix: str
    verb: str
    items: tuple[PropItem, ...]


@dataclass(frozen=True)
class Propertied:
    """What a property SET or REMOVE did, for its answer's counters."""

    properties_set: int


def prop_items(items: list[Any]) -> tuple[PropItem, ...] | None:
    """The IR's SET or REMOVE items as property items, or None when one of them is a label's."""
    out: list[PropItem] = []
    for item in items:
        if getattr(item, "labels", None):
            return None
        value = getattr(item, "value_text", None)
        if item.property is None and value is None:
            return None
        out.append(PropItem(item.variable, item.property, value, getattr(item, "merge", False)))
    return tuple(out)


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
    items = prop_items(clause.items)
    if items is None:
        return None
    verb = "SET" if isinstance(clause, SetClause) else "REMOVE"
    return PropClause(statement[: clause.start].strip(), verb, items)


def _names_unknown_label(statement: str, label_map: Any) -> bool:
    """Whether the statement's leading clauses match a label neither the model nor the session
    has: its read refuses it by name (REQ-778), as a read of it and a label SET of it are, rather
    than the governed path taking a form it does not write."""
    from provisa.cypher.parser import parse_cypher_statement
    from provisa.ephemeral.classify import resolve
    from provisa.gql.ir import MatchStep

    for step in parse_cypher_statement(statement).pipeline:
        if not isinstance(step, MatchStep):
            continue
        for m in step.matches:
            for node in getattr(m.pattern, "nodes", []):
                if node.labels and resolve(list(node.labels), label_map).free:
                    return True
    return False


def _held_properties(value: Any) -> dict[str, Any]:
    """The properties a node or relationship a read returned holds now (its built-in ``_id`` and
    a relationship's read id aside)."""
    from provisa.ephemeral.then import REL_ID
    from provisa.gql.assembler import Edge, Node
    from provisa.gql.select_builder import NODE_KEY_PROPERTY

    if isinstance(value, (Node, Edge)):
        props = value.properties
    elif isinstance(value, dict) and isinstance(value.get("properties"), dict):
        props = value["properties"]
    else:
        return {}
    return {
        k: v for k, v in props.items() if k not in (NODE_KEY_PROPERTY, REL_ID) and v is not None
    }


def _as_map(value: Any, var: str) -> dict[str, Any]:
    """The map a ``SET v = e`` or ``SET v += e`` writes: a map itself, or a node's or
    relationship's properties."""
    from provisa.gql.assembler import Edge, Node

    if isinstance(value, (Node, Edge)) or (
        isinstance(value, dict) and "properties" in value and ("label" in value or "type" in value)
    ):
        return _held_properties(value)
    if isinstance(value, dict):
        return dict(value)
    raise CypherParseError(
        f"SET {var} = ... takes a map, a node or a relationship: "
        f"{type(value).__name__} is none of them"
    )


def refuse_unstorable(prop: str, value: Any) -> None:
    """Refuse a property value Cypher does not store: a map, or a list holding a map or a list."""
    if isinstance(value, dict) or (
        isinstance(value, list) and any(isinstance(v, (dict, list)) for v in value)
    ):
        raise CypherParseError(
            f"{prop} cannot be set to {value!r}: a property's value is a number, a string, a "
            f"boolean, a temporal value, a point, or a list of one of them -- never a map, nor a "
            f"list holding a map or a list"
        )


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
    if (
        clause.verb == "SET"
        and not session_read.involved(session)
        and not _names_unknown_label(statement, label_map)
    ):
        return None  # no session object to write: the governed path's
    graph = store.graph(session)
    keep = keep or ()
    named = sorted({item.variable for item in clause.items})
    variables = sorted(set(named) | set(keep))
    returned = [f"{v} AS _v_{v}" for v in variables]
    returned += [
        f"{item.value} AS _s{i}" for i, item in enumerate(clause.items) if item.value is not None
    ]
    answer = await read(f"{clause.prefix} RETURN {', '.join(returned)}".strip(), params)
    nodes: dict[str, dict[str, Any]] = {}
    rels: dict[str, dict[str, Any]] = {}
    model: set[str] = set()
    written = 0
    for row in answer:
        objects = {var: bound_value(row[f"_v_{var}"], label_map, graph) for var in named}
        for i, item in enumerate(clause.items):
            var = item.variable
            held = objects[var]
            if held is None:
                continue  # a null object takes no property
            if held[0] == "n" and held[1] in graph.node_keys:
                target = nodes.setdefault(held[1], {})
            elif held[0] == "r":
                target = rels.setdefault(held[1], {})
            elif held[0] == "n":
                model.add(var)
                continue
            else:
                raise CypherParseError(f"{var} is not a node or a relationship: no property is set")
            value = None if item.value is None else row[f"_s{i}"]
            if item.property is not None:
                updates = {item.property: value}
            else:
                given = {} if value is None else _as_map(value, var)
                updates = dict(given)
                if not item.merge:
                    # SET v = e: every property e does not hold is cleared.
                    held_now = {**_held_properties(row[f"_v_{var}"]), **target}
                    updates = {k: None for k in held_now if k not in given} | updates
            for prop, v in updates.items():
                refuse_unstorable(prop, v)
            target.update(updates)
            written += len(updates)
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
    # Each property a SET writes or a REMOVE clears, as Neo4j's properties-set counts it.
    counters.count(properties_set=written)
    bound_rows = [
        {var: bound_value(row[f"_v_{var}"], label_map, graph) for var in keep} for row in answer
    ]
    return Propertied(written), bound_rows
