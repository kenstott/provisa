# Copyright (c) 2026 Kenneth Stott
# Canary: 25e89173-576a-4ab5-ab34-7c924f358e6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DELETE and DETACH DELETE where the session holds ephemeral objects (REQ-2176).

``[MATCH ... | UNWIND ... | WITH ...] [DETACH] DELETE a, r, ...`` reads what it names through the
surface's own governed read over the session's overlay, then:

- a session node is removed from the session, DETACH removing every session relationship it
  has; DELETE of one that still has relationships is refused, as Neo4j refuses it;
- a session relationship is removed: its identity is its own row (rel_id), so a parallel one is
  never touched;
- a model node is the governed path's: a plain DELETE is the governed delete of its row, its
  session tags going with it, and one with session relationships is refused; a DETACH DELETE of
  one is the governed DETACH DELETE of its row (its junction rows detached, a foreign-key
  relationship refused by name), its session tags and relationships going with it;
- a DELETE of a model relationship is the governed path's whole statement.

Every refusal is decided before any write, so a refused statement writes nothing; the governed
deletes then run, and the session's parts are removed last. The two paths are not one
transaction (REQ-883). With no ephemeral objects in the session, a DELETE is the governed
path's, unchanged.
"""

# Requirements: REQ-2176, REQ-883

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from provisa.compiler.temp_tables import TempSession
from provisa.cypher import counters
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import store
from provisa.ephemeral.write import Read, Write, _identity, _model_identity


def _never_deleted() -> tuple[type, ...]:
    from provisa.gql import expr_ast as e

    return (
        e.Literal,
        e.TypedLiteral,
        e.Binary,
        e.Unary,
        e.IsNull,
        e.LabelPredicate,
        e.Quantifier,
        e.MapProjection,
    )


#: The expressions whose value is never a node, a relationship or a path (InvalidDelete,
#: InvalidArgumentType): refused before anything is read.
_NEVER_DELETED = _never_deleted()


@dataclass(frozen=True)
class DeleteOf:
    """A statement's DELETE, read from the graph IR: its leading clauses, each expression it
    deletes as written, the variables among them (deleted, so null to a read after it), and
    whether it detaches."""

    prefix: str
    targets: tuple[str, ...]
    variables: tuple[str, ...]
    detach: bool


def delete_of(statement: str) -> DeleteOf | None:
    """The statement's DELETE, or None when it deletes nothing. A DELETE followed by more
    clauses is done as its statement's head (then.py), which this reads up to the DELETE."""
    from provisa.cypher.parser import parse_cypher_statement, writes_in
    from provisa.gql.expr_ast import Literal, Variable
    from provisa.gql.ir import DeleteClause

    if not writes_in(statement):
        return None
    ast = parse_cypher_statement(statement)
    deletes = [step for step in ast.pipeline if isinstance(step, DeleteClause)]
    if not deletes:
        return None
    clause = deletes[0]
    if len(ast.writes) > 1 or ast.pipeline[-1] is not clause or ast.return_clause is not None:
        return None  # followed by more: the head of a write then read (then.py)
    for expression, text in zip(clause.expressions, clause.texts):
        if isinstance(expression, _NEVER_DELETED) and not (
            isinstance(expression, Literal) and expression.kind == "null"
        ):
            raise CypherParseError(
                f"DELETE {text}: a DELETE removes a node, a relationship or a path (or the ones "
                f"a list or map holds); {text} is none of them. To remove a label, use REMOVE"
            )
    variables = [e.name for e in clause.expressions if isinstance(e, Variable)]
    return DeleteOf(
        statement[: clause.start].strip(),
        tuple(dict.fromkeys(clause.texts)),
        tuple(dict.fromkeys(variables)),
        clause.detach,
    )


@dataclass(frozen=True)
class Deleted:
    """What a DELETE removed, for its answer's counters."""

    nodes: int
    relationships: int


def _is_edge(value: Any) -> bool:
    from provisa.gql.assembler import Edge

    return isinstance(value, Edge) or (
        isinstance(value, dict) and "type" in value and ("identity" in value or "start" in value)
    )


def _edge_id(value: Any) -> str:
    from provisa.gql.assembler import Edge

    return str(value.id if isinstance(value, Edge) else value.get("identity", value.get("id")))


def _node_properties(value: Any) -> dict[str, Any]:
    from provisa.gql.assembler import Node

    return value.properties if isinstance(value, Node) else dict(value["properties"])


async def delete(
    statement: str,
    params: dict[str, Any],
    label_map: Any,
    session: TempSession,
    state: Any,
    read: Read,
    governed_write: Write,
) -> Deleted | None:
    """Run ``statement`` when it deletes while the session holds ephemeral objects; None when it
    deletes nothing, the session holds none, or the statement is wholly the governed path's."""
    done = await delete_bound(statement, params, label_map, session, state, read, governed_write)
    return None if done is None else done[0]


async def delete_bound(
    statement: str,
    params: dict[str, Any],
    label_map: Any,
    session: TempSession,
    state: Any,
    read: Read,
    governed_write: Write,
    keep: tuple[str, ...] | None = None,
    whole: str | None = None,
) -> tuple[Deleted, list[dict[str, Any]]] | None:
    """:func:`delete`, and with ``keep`` (the variables a read after it names) each row's
    bindings of them (then.Bound): a variable it deleted is null after it. ``whole``: the
    statement ``statement`` heads (then.py), whose form decides whether the governed path takes
    it while the session holds nothing; ``statement`` itself when None."""
    from provisa.ephemeral.then import bound_value

    d = delete_of(statement)
    if d is None:
        return None
    graph = store.graph(session)
    if graph.empty and _governed_form(whole or statement):
        return None  # no ephemeral involvement: the governed path's, unchanged
    keep = keep or ()
    targets = [f"{t} AS _d{i}" for i, t in enumerate(d.targets)]
    held = list(dict.fromkeys([*keep, *d.variables]))
    rows = await read(
        f"{d.prefix} RETURN {', '.join([*targets, *(f'{v} AS _v_{v}' for v in held)])}".strip(),
        params,
    )
    session_nodes: set[str] = set()
    session_rels: set[str] = set()
    model_nodes: dict[str, tuple[Any, dict[str, Any]]] = {}  # identity -> (mapping, properties)
    for row in rows:
        for i, text in enumerate(d.targets):
            for kind, held in _deleted(row[f"_d{i}"], text, label_map, graph):
                if kind == "rel":
                    session_rels.add(held)
                elif kind == "node":
                    session_nodes.add(held)
                elif kind == "model":
                    identity, mapping, props = held
                    model_nodes[identity] = (mapping, props)
                else:
                    return None  # a model relationship: the governed path's whole statement
    if graph.empty and model_nodes:
        return None  # no ephemeral involvement: the governed path's, unchanged
    if whole is not None and whole.startswith(statement):
        _refuse_deleted_access(
            whole[len(statement) :],
            [v for v in d.variables if any(row[f"_v_{v}"] is not None for row in rows)],
        )
    bound_rows = [
        {
            var: None if var in d.variables else bound_value(row[f"_v_{var}"], label_map, graph)
            for var in keep
        }
        for row in rows
    ]
    touching = await _relationships_of(state, session, session_nodes | set(model_nodes))
    if not d.detach:
        held = sorted(n for n, rels in touching.items() if rels - session_rels)
        if held:
            raise CypherParseError(
                f"Cannot delete node {held[0]}, because it still has relationships. To delete "
                f"this node, you must first delete its relationships, or DETACH DELETE it."
            )
    tags = {
        (node_id, label)
        for node_id, label in await store.held_labels(state, session)
        if node_id in model_nodes
    }
    if d.detach:
        from provisa.cypher.write_translator import CypherWriteParseError, WriteTranslator

        translator = WriteTranslator(label_map)
        for nm, _props in model_nodes.values():
            try:
                translator.detached(nm)
            except CypherWriteParseError as exc:
                raise CypherParseError(str(exc)) from exc
    # Every refusal is decided: the governed deletes first, then the session's parts.
    for identity, (nm, props) in sorted(model_nodes.items()):
        key = nm.property_of(nm.id_column)
        if _model_identity(nm, props) != identity:
            raise CypherParseError(f"A deleted {nm.label} is read with its key, {key}")
        await governed_write(
            f"MATCH (n:{nm.label} {{{key}: $key}}) {'DETACH ' if d.detach else ''}DELETE n",
            {"key": props[key]},
        )
    detached = {rel for rels in touching.values() for rel in rels}
    rels = sorted(session_rels | detached)
    await store.delete_rels(state, session, rels)
    if tags:
        await store.remove_labels(state, session, tags)
    if session_nodes:
        await store.delete_nodes(state, session, sorted(session_nodes))
    # The session's parts; each model row was counted as the governed path deleted it.
    counters.count(nodes_deleted=len(session_nodes), relationships_deleted=len(rels))
    return Deleted(len(session_nodes) + len(model_nodes), len(rels)), bound_rows


def _refuse_deleted_access(rest: str, deleted: list[str]) -> None:
    """Refuse a read after the DELETE of a property, the labels or the keys of a node or
    relationship it deleted (openCypher's DeletedEntityAccess): they are no longer there to
    read. Its id, its type and the variable itself still are."""
    from provisa.ephemeral.then import _STRING

    text = _STRING.sub("''", rest)
    for var in deleted:
        name = re.escape(var)
        if re.search(
            rf"(?<![\w.$]){name}\s*(?:\.\s*[A-Za-z_`]|\[)"
            rf"|\b(?:labels|keys|properties)\s*\(\s*{name}\s*\)",
            text,
            re.I,
        ):
            raise CypherParseError(
                f"{var} was deleted by this statement: its properties and labels are no longer "
                f"there to read (DeletedEntityAccess). Read them before the DELETE"
            )


def _governed_form(statement: str) -> bool:
    """Whether the governed write path takes ``statement`` as written (a DELETE of a model's
    rows). One it does not (an OPTIONAL MATCH, a path, a list's element) is read here: in a
    session holding nothing, what it reaches is null or a model's row."""
    from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write

    try:
        parse_cypher_write(statement)
    except CypherWriteParseError:
        return False
    return True


def _deleted(value: Any, text: str, label_map: Any, graph: store.Graph) -> list[tuple[str, Any]]:
    """What deleting ``value`` (the value of ``text``, a DELETE's expression) removes: each
    ("node", id) of the session's, ("model", (identity, mapping, properties)) of the model's,
    ("rel", rel_id) of the session's, or ("model_rel", id) of the model's. A path is its nodes
    and relationships; a list holds what each element is; null deletes nothing. Anything else is
    refused by name, as Neo4j refuses it."""
    from provisa.gql.assembler import Path

    if value is None:
        return []
    if isinstance(value, list):
        return [held for v in value for held in _deleted(v, text, label_map, graph)]
    if isinstance(value, Path):
        return [
            held
            for v in [*value.nodes, *value.edges]
            for held in _deleted(v, text, label_map, graph)
        ]
    if isinstance(value, dict) and "nodes" in value and "edges" in value:
        return [
            held
            for v in [*value["nodes"], *value["edges"]]
            for held in _deleted(v, text, label_map, graph)
        ]
    if isinstance(value, str) and value[:1] in ("{", "["):
        # A graph element held in a list or map is read as its JSON text.
        import json

        try:
            decoded = json.loads(value)
        except ValueError:
            decoded = None
        if isinstance(decoded, (dict, list)):
            return _deleted(decoded, text, label_map, graph)
    if _is_edge(value):
        edge_id = _edge_id(value)
        return [("rel", edge_id)] if edge_id in graph.rel_ends else [("model_rel", edge_id)]
    session_id = _session_node(value, graph)
    if session_id is not None:
        return [("node", session_id)]
    if isinstance(value, str) and value in graph.rel_ends:
        return [("rel", value)]
    if _is_node(value):
        node_id, read_by = _identity(value, label_map, graph)
        if node_id in graph.node_keys:
            return [("node", node_id)]
        return [("model", (node_id, label_map.nodes[read_by[0]], _node_properties(value)))]
    raise CypherParseError(
        f"DELETE {text}: {value!r} is not a node, a relationship or a path, which a DELETE removes"
    )


def _is_node(value: Any) -> bool:
    from provisa.gql.assembler import Node

    return isinstance(value, Node) or (
        isinstance(value, dict) and "label" in value and "properties" in value
    )


def _session_node(value: Any, graph: store.Graph) -> str | None:
    """The session node ``value`` is, read as its id (``e:1``, or ``Label|e:1`` read by a
    label), as a map of its built-in ids, or as a node whose id names it; None otherwise."""
    from provisa.gql.assembler import Node

    if isinstance(value, Node):
        value = {"id": value.id, "properties": value.properties}
    if isinstance(value, dict):
        for key in ("__graph_id", "__node_id", "graphId"):
            if value.get(key) in graph.node_keys:
                return str(value[key])
        if "properties" not in value:
            return None
        held = (value.get("properties") or {}).get("_id")
        if held in graph.node_keys:
            return str(held)
        value = value.get("id")
    if not isinstance(value, str):
        return None
    if value in graph.node_keys:
        return value
    after = value.split("|", 1)[1] if "|" in value else None
    return after if after in graph.node_keys else None


async def _relationships_of(
    state: Any, session: TempSession, node_ids: set[str]
) -> dict[str, set[str]]:
    """Each node's session relationships (their rel_ids), either end."""
    from provisa.pgwire import temp_exec

    out: dict[str, set[str]] = {n: set() for n in node_ids}
    if not node_ids or store.RELS not in session.tables:
        return out
    listed = sorted(node_ids)
    for end in ("from_id", "to_id"):
        for rel_id, node_id in await temp_exec.keyed_rows(
            state, session, store.RELS, end, listed, (store.REL_ID, end)
        ):
            out[str(node_id)].add(str(rel_id))
    return out
