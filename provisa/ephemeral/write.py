# Copyright (c) 2026 Kenneth Stott
# Canary: ebe48b4d-969f-46b8-ab2a-56a42ad5bbfc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ephemeral-write handler (REQ-2176): a CREATE the governing rule does not make a governed
write makes its nodes and relationships in the session.

``[MATCH ... | UNWIND ... | WITH ...] CREATE <patterns>`` is read first as a governed read -- the
statement's own leading clauses, returning every value the patterns write and every node they
reach back to -- through the surface's own read, as the session's role, so a value taken from
model data passes that data's rules. Each row it gives makes the patterns' new nodes and
relationships once, landed in the session's tables (:mod:`.store`).
"""

# Requirements: REQ-2176

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from provisa.compiler.temp_tables import TempSession
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import pattern, store
from provisa.ephemeral.classify import CreateKind, classify_create
from provisa.ephemeral.pattern import CreatePattern
from provisa.ephemeral.store import NewNode, NewRel, Writes

#: The surface's own governed read: a Cypher read and its values to its rows (column -> value,
#: a node as its answer writes it).
Read = Callable[[str, dict[str, Any]], Awaitable[list[dict[str, Any]]]]


@dataclass(frozen=True)
class Created:
    """What a CREATE made, for its answer's counters."""

    nodes: int
    relationships: int
    labels: int


def _items(p: CreatePattern, kind: CreateKind, bound: set[str]) -> list[str]:
    """The read's RETURN items: each new node's and relationship's property values, and each
    node the patterns reach back to."""
    items = [
        f"{value} AS _n{c.index}_{k}"
        for c in kind.created
        for k, value in p.nodes[c.index].properties
    ]
    items += [
        f"{value} AS _r{j}_{k}" for j, rel in enumerate(p.rels) for k, value in rel.properties
    ]
    reached = sorted(
        {n.variable for n in p.nodes if n.variable is not None and n.variable in bound}
    )
    items += [f"{var} AS _v_{var}" for var in reached]
    return items or ["1 AS _one"]


def _identity(node: Any, label_map: Any) -> str:
    """A node the read returned, as its graph identity: an ephemeral node's id, or a model
    node's ``Label|key``."""
    from provisa.gql.assembler import Node

    if isinstance(node, Node):
        label, props = node.label, node.properties
    elif isinstance(node, dict) and "properties" in node and "label" in node:
        label, props = node["label"], node["properties"]
    else:
        raise CypherParseError("A created relationship's end is a node")
    key = props.get("_id")
    if isinstance(key, str) and key.startswith("e:"):
        return key
    owners = [nm for nm in label_map.nodes.values() if nm.label == label]
    if len(owners) != 1:
        raise CypherParseError(f"A created relationship's end, {label}, is no one node")
    nm = owners[0]
    # A compound key's _id is its identity already (select_builder.node_key).
    return str(key) if len(nm.key_columns) > 1 else f"{nm.label}|{key}"


async def create(
    statement: str,
    params: dict[str, Any],
    label_map: Any,
    session: TempSession,
    state: Any,
    read: Read,
) -> Created | None:
    """Run ``statement`` when it is a CREATE the governing rule makes ephemeral; None when it
    creates nothing or is the governed write (the governed path's, unchanged)."""
    split = pattern.create_clause(statement)
    if split is None:
        return None
    prefix, patterns = split
    p = pattern.parse_patterns(patterns)
    bound = pattern.bound(prefix)
    graph = store.graph(session)
    kind = classify_create(p, bound, label_map, graph)
    if kind.governed:
        return None
    answer = await read(f"{prefix} RETURN {', '.join(_items(p, kind, bound))}".strip(), params)
    writes = Writes()
    for row in answer:
        ids: dict[int, str] = {}
        for c in kind.created:
            node_id = graph.new_id()
            ids[c.index] = node_id
            anchor = store.anchor_table(c.anchor) if c.anchor is not None else store.DEFAULT_ANCHOR
            props = {k: row[f"_n{c.index}_{k}"] for k, _ in p.nodes[c.index].properties}
            writes.nodes.append(NewNode(node_id, anchor, c.labels.written, props))
        made = {p.nodes[c.index].variable: c.index for c in kind.created}
        for i, node in enumerate(p.nodes):
            if i in ids:
                continue
            var = node.variable
            assert var is not None
            ids[i] = ids[made[var]] if var in made else _identity(row[f"_v_{var}"], label_map)
        for j, rel in enumerate(p.rels):
            props = {k: row[f"_r{j}_{k}"] for k, _ in rel.properties}
            writes.rels.append(
                NewRel(graph.new_id("r"), rel.rel_type, ids[rel.source], ids[rel.target], props)
            )
    for c in kind.created:
        if c.anchor is not None:
            graph.anchors.setdefault(c.anchor, store.anchor_table(c.anchor))
    await store.write(state, session, writes)
    return Created(
        len(writes.nodes),
        len(writes.rels),
        sum(len(n.labels) for n in writes.nodes),
    )
