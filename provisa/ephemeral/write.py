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
#: The surface's own governed write: a Cypher write and its values, done as the session's role.
Write = Callable[[str, dict[str, Any]], Awaitable[None]]


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


def _identity(node: Any, label_map: Any, graph: store.Graph) -> tuple[str, tuple[str, ...]]:
    """A node the read returned: its graph identity (an ephemeral node's id, or a model node's
    ``Label|key``) and the labels it is read by."""
    from provisa.gql.assembler import Node

    if isinstance(node, Node):
        label, props = node.label, node.properties
    elif isinstance(node, dict) and "properties" in node and "label" in node:
        label, props = node["label"], node["properties"]
    else:
        raise CypherParseError("A created relationship's end is a node")
    key = props.get("_id")
    if isinstance(key, str) and key.startswith("e:"):
        return key, graph.node_keys[key]
    by_label = {nm.label: nm for nm in label_map.nodes.values()}
    owners = [nm for nm in label_map.nodes.values() if nm.label == label]
    owner = owners[0] if len(owners) == 1 else None
    # A model node read under a label not its own (a session tag) is its graph identity
    # (read.py), ``Label|key``, its own label first.
    if isinstance(key, str) and "|" in key:
        named = by_label.get(key.split("|", 1)[0])
        if named is not None and named is not owner:
            return key, (named.type_name,)
    if owner is None:
        raise CypherParseError(f"A created relationship's end, {label}, is no one node")
    nm = owner
    # A compound key's _id is its identity already (select_builder.node_key).
    identity = str(key) if len(nm.key_columns) > 1 else f"{nm.label}|{key}"
    return identity, (nm.type_name,)


def _model_identity(nm: Any, props: dict[str, Any]) -> str:
    """The graph identity (``Label|key``, select_builder.node_identity) of the model row a
    governed write makes of ``props``."""
    columns = nm.key_columns if len(nm.key_columns) > 1 else (nm.id_column,)
    parts: list[str] = []
    for column in columns:
        prop = nm.property_of(column)
        if prop not in props:
            raise CypherParseError(
                f"A relationship ends on the new {nm.label}: the CREATE names its {prop}"
            )
        parts.append(str(props[prop]))
    if len(parts) == 1:
        return f"{nm.label}|{parts[0]}"
    escaped = (part.replace("\\", "\\\\").replace("|", "\\|") for part in parts)
    return f"{nm.label}|" + "|".join(escaped)


def _governed_row(node: pattern.NodeSpec, labels: tuple[str, ...]) -> str:
    """The governed write of one model row, its values bound by property name."""
    values = ", ".join(f"{k}: ${k}" for k, _ in node.properties)
    return f"CREATE (n:{':'.join(labels)}{' {' + values + '}' if values else ''})"


async def create(
    statement: str,
    params: dict[str, Any],
    label_map: Any,
    session: TempSession,
    state: Any,
    read: Read,
    governed_write: Write,
) -> Created | None:
    """Run ``statement`` when the governing rule does not make it wholly the governed write;
    None when it creates nothing or is the governed write (the governed path's, unchanged).

    A CREATE mixing governed and session parts splits: each governed relationship goes to the
    governed path first, as its own statement over the leading clauses; then, for each row the
    leading clauses give, each governed row is written by the governed path and the session's
    parts land in the session. The two paths are not one transaction: the outcome is the parts
    written as statements of their own."""
    split = pattern.create_clause(statement)
    if split is None:
        return None
    prefix, patterns, trailing = split
    p = pattern.parse_patterns(patterns)
    bound = pattern.bound(prefix)
    graph = store.graph(session)
    kind = classify_create(p, bound, label_map, graph)
    if kind.governed:
        return None
    if trailing is not None:
        raise CypherParseError(
            f"A CREATE of ephemeral objects ends its statement; it is followed by {trailing}"
        )
    for rel, governed in zip(p.rels, kind.rels_governed, strict=True):
        if governed:
            source, target = p.nodes[rel.source].variable, p.nodes[rel.target].variable
            await governed_write(
                f"{prefix} CREATE ({source or ''})-[:{rel.rel_type}]->({target or ''})".strip(),
                params,
            )
    answer = await read(f"{prefix} RETURN {', '.join(_items(p, kind, bound))}".strip(), params)
    made = {p.nodes[c.index].variable: c.index for c in kind.created}
    created_at = {c.index for c in kind.created}
    # The governed rows a session relationship ends on: only theirs need a graph identity.
    ended_on = {
        end if end in created_at else made.get(p.nodes[end].variable, -1)
        for rel, governed in zip(p.rels, kind.rels_governed, strict=True)
        if not governed
        for end in (rel.source, rel.target)
    }
    writes = Writes()
    rows_written = 0
    for row in answer:
        ids: dict[int, tuple[str, tuple[str, ...]]] = {}
        for c in kind.created:
            props = {k: row[f"_n{c.index}_{k}"] for k, _ in p.nodes[c.index].properties}
            if c.governed:
                nm = label_map.nodes[c.labels.models[0]]
                await governed_write(_governed_row(p.nodes[c.index], c.labels.written), props)
                rows_written += 1
                if c.index in ended_on:
                    ids[c.index] = (_model_identity(nm, props), (nm.type_name,))
                continue
            node_id = graph.new_id()
            keys = c.labels.keys
            ids[c.index] = (node_id, keys or ("",))
            anchor = store.anchor_table(c.anchor) if c.anchor is not None else store.DEFAULT_ANCHOR
            writes.nodes.append(NewNode(node_id, anchor, keys, props))
        for i, node in enumerate(p.nodes):
            if i in ids or i in created_at:
                continue
            var = node.variable
            assert var is not None
            if var in made:
                if made[var] in ids:
                    ids[i] = ids[made[var]]
                continue
            ids[i] = _identity(row[f"_v_{var}"], label_map, graph)
        for j, rel in enumerate(p.rels):
            if kind.rels_governed[j]:
                continue
            props = {k: row[f"_r{j}_{k}"] for k, _ in rel.properties}
            (source, from_labels), (target, to_labels) = ids[rel.source], ids[rel.target]
            writes.rels.append(
                NewRel(
                    graph.new_id("r"), rel.rel_type, source, target, props, from_labels, to_labels
                )
            )
    for c in kind.created:
        if c.anchor is not None:
            graph.anchors.setdefault(c.anchor, store.anchor_table(c.anchor))
    await store.write(state, session, writes)
    return Created(
        len(writes.nodes) + rows_written,
        len(writes.rels),
        sum(len(n.labels) for n in writes.nodes) + rows_written,
    )
