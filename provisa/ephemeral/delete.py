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

from dataclasses import dataclass
from typing import Any

from provisa.compiler.temp_tables import TempSession
from provisa.cypher import counters
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import store
from provisa.ephemeral.write import Read, Write, _identity, _model_identity


@dataclass(frozen=True)
class DeleteOf:
    """A statement's DELETE, read from the graph IR: its leading clauses, the variables it
    deletes, and whether it detaches."""

    prefix: str
    variables: tuple[str, ...]
    detach: bool


def delete_of(statement: str) -> DeleteOf | None:
    """The statement's DELETE, or None when it deletes nothing."""
    from provisa.cypher.parser import parse_cypher_statement, writes_in
    from provisa.gql.expr_ast import Variable
    from provisa.gql.ir import DeleteClause

    if not writes_in(statement):
        return None
    ast = parse_cypher_statement(statement)
    deletes = [step for step in ast.pipeline if isinstance(step, DeleteClause)]
    if not deletes:
        return None
    clause = deletes[0]
    if len(ast.writes) > 1 or ast.pipeline[-1] is not clause or ast.return_clause is not None:
        raise CypherParseError(
            "A DELETE of ephemeral objects is its statement's one write and ends it"
        )
    variables: list[str] = []
    for expression in clause.expressions:
        if not isinstance(expression, Variable):
            raise CypherParseError("A DELETE of ephemeral objects names each node or relationship")
        variables.append(expression.name)
    return DeleteOf(
        statement[: clause.start].strip(), tuple(dict.fromkeys(variables)), clause.detach
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
    d = delete_of(statement)
    graph = store.graph(session)
    if d is None or graph.empty:
        return None
    rows = await read(
        f"{d.prefix} RETURN {', '.join(f'{v} AS _v_{v}' for v in d.variables)}".strip(), params
    )
    session_nodes: set[str] = set()
    session_rels: set[str] = set()
    model_nodes: dict[str, tuple[Any, dict[str, Any]]] = {}  # identity -> (mapping, properties)
    for row in rows:
        for var in d.variables:
            value = row[f"_v_{var}"]
            if value is None:
                continue  # an OPTIONAL MATCH that matched nothing deletes nothing
            if _is_edge(value):
                edge_id = _edge_id(value)
                if not edge_id.startswith("r:"):
                    return None  # a model relationship: the governed path's whole statement
                session_rels.add(edge_id)
                continue
            node_id, read_by = _identity(value, label_map, graph)
            if node_id in graph.node_keys:
                session_nodes.add(node_id)
            else:
                model_nodes[node_id] = (label_map.nodes[read_by[0]], _node_properties(value))
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
    return Deleted(len(session_nodes) + len(model_nodes), len(rels))


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
