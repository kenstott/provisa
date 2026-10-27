# Copyright (c) 2026 Kenneth Stott
# Canary: 0357c07d-dc9c-4fae-ab60-477afec15998
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's ephemeral graph in its temporary tables (REQ-2176).

- ``nodes(node_id, table)``: each ephemeral node and the anchor table holding its properties;
- ``node_labels(node_id, label)``: every label an ephemeral node carries, and every session tag
  on a model node (its node_id the model node's graph identity, ``Label|key``);
- ``rels(rel_id, type, from_id, to_id, table)``: each ephemeral relationship, either end
  ephemeral or model, and the table holding its properties;
- anchor tables: one per free label that anchors nodes (``node_id`` and a typed column per
  property), the default one for a node with no free label, and one per relationship type for
  its properties (``rel_id`` and its columns).

Each is a temporary table of the session (:mod:`provisa.pgwire.temp_exec`): its own schema of
the store, the session's caps, dropped when the session ends. A write lands each table it
touches again whole, its columns grown and widened (:mod:`.types`).
"""

# Requirements: REQ-2176

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from provisa.compiler.temp_tables import TempSession
from provisa.ephemeral import types

NODES = "cypher__nodes"
NODE_LABELS = "cypher__node_labels"
RELS = "cypher__rels"
NODE_ID = "node_id"
REL_ID = "rel_id"

_TEXT = "text"
_FIXED: dict[str, list[tuple[str, str]]] = {
    NODES: [(NODE_ID, _TEXT), ("table", _TEXT)],
    NODE_LABELS: [(NODE_ID, _TEXT), ("label", _TEXT)],
    RELS: [
        (REL_ID, _TEXT),
        ("type", _TEXT),
        ("from_id", _TEXT),
        ("to_id", _TEXT),
        ("table", _TEXT),
    ],
}
#: The anchor table of a node that has no free label.
DEFAULT_ANCHOR = "cypher__anchor"


def anchor_table(label: str) -> str:
    """The anchor table a free label creates for the nodes it anchors."""
    return f"cypher__anchor__{label}"


def rel_table(rel_type: str) -> str:
    """The table a relationship type's properties are held in."""
    return f"cypher__rel__{rel_type}"


@dataclass
class Graph:
    """What the session's graph holds, kept beside its tables so a statement is planned without
    reading them: the anchor tables by the label that created them, every label carried and
    every relationship type written."""

    anchors: dict[str, str] = field(default_factory=dict)
    labels: set[str] = field(default_factory=set)
    types: set[str] = field(default_factory=set)
    # A label -> the anchor tables holding ephemeral nodes that carry it.
    label_anchors: dict[str, set[str]] = field(default_factory=dict)
    # A label -> the model nodes (type names) a session tag gives it to.
    tag_types: dict[str, set[str]] = field(default_factory=dict)
    # Each (type, source label, target label) a session relationship was written between: an
    # ephemeral end by each label it carries ("" for none), a model end by its type name.
    rel_pairs: set[tuple[str, str, str]] = field(default_factory=set)
    # An ephemeral node's id -> the labels it carries (as node_labels holds them).
    node_keys: dict[str, tuple[str, ...]] = field(default_factory=dict)
    last_id: int = 0

    def new_id(self, prefix: str = "e") -> str:
        """A new node's or relationship's id, unique in the session."""
        self.last_id += 1
        return f"{prefix}:{self.last_id}"

    @property
    def empty(self) -> bool:
        return not (self.labels or self.types or self.anchors)


_KEY = "cypher"


def graph(session: TempSession) -> Graph:
    """The session's ephemeral graph, empty until a statement makes one."""
    held = session.extensions.get(_KEY)
    if held is None:
        held = session.extensions[_KEY] = Graph()
    return held


@dataclass
class Tag:
    """A session label on a model node: its graph identity and its model type name."""

    node_id: str
    type_name: str
    label: str


@dataclass
class NewNode:
    node_id: str
    anchor: str  # its anchor table
    labels: tuple[str, ...]
    properties: dict[str, Any]


@dataclass
class NewRel:
    rel_id: str
    rel_type: str
    from_id: str
    to_id: str
    properties: dict[str, Any]
    # The labels its ends are read by: an ephemeral end's labels ("" for none), a model end's
    # type name.
    from_labels: tuple[str, ...] = ()
    to_labels: tuple[str, ...] = ()


@dataclass
class Writes:
    """What one statement adds to the session's graph."""

    nodes: list[NewNode] = field(default_factory=list)
    tags: list[Tag] = field(default_factory=list)
    rels: list[NewRel] = field(default_factory=list)


def _engine(state: Any) -> str:
    return state.federation_engine.engine.name


async def _add(
    state: Any,
    session: TempSession,
    table: str,
    keys: tuple[tuple[str, str], ...],
    rows: list[dict[str, Any]],
) -> None:
    """Add ``rows`` to the session's ``table``, its columns grown and widened."""
    from provisa.pgwire import temp_exec

    held = session.tables[table].columns if table in session.tables else []
    columns, all_rows = types.relanded(
        held, keys, await temp_exec.held_rows(state, session, table), rows, _engine(state)
    )
    await temp_exec.reland(state, session, table, columns, all_rows)


async def write(state: Any, session: TempSession, writes: Writes) -> None:
    """Land ``writes``: each table they touch once, nodes before the labels and relationships
    that name them."""
    g = graph(session)
    by_anchor: dict[str, list[dict[str, Any]]] = {}
    for node in writes.nodes:
        by_anchor.setdefault(node.anchor, []).append({NODE_ID: node.node_id, **node.properties})
    for table, rows in by_anchor.items():
        await _add(state, session, table, ((NODE_ID, _TEXT),), rows)
    if writes.nodes:
        await _add(
            state,
            session,
            NODES,
            tuple(_FIXED[NODES]),
            [{NODE_ID: n.node_id, "table": n.anchor} for n in writes.nodes],
        )
    for node in writes.nodes:
        for label in node.labels or ("",):
            g.label_anchors.setdefault(label, set()).add(node.anchor)
        g.node_keys[node.node_id] = node.labels or ("",)
    for tag in writes.tags:
        g.tag_types.setdefault(tag.label, set()).add(tag.type_name)
    labels = [(n.node_id, label) for n in writes.nodes for label in n.labels]
    labels += [(t.node_id, t.label) for t in writes.tags]
    if labels:
        await _add(
            state,
            session,
            NODE_LABELS,
            tuple(_FIXED[NODE_LABELS]),
            [{NODE_ID: node_id, "label": label} for node_id, label in labels],
        )
        g.labels.update(label for _, label in labels)
    by_type: dict[str, list[dict[str, Any]]] = {}
    for rel in writes.rels:
        by_type.setdefault(rel.rel_type, []).append({REL_ID: rel.rel_id, **rel.properties})
    for rel_type, rows in by_type.items():
        await _add(state, session, rel_table(rel_type), ((REL_ID, _TEXT),), rows)
    if writes.rels:
        await _add(
            state,
            session,
            RELS,
            tuple(_FIXED[RELS]),
            [
                {
                    REL_ID: r.rel_id,
                    "type": r.rel_type,
                    "from_id": r.from_id,
                    "to_id": r.to_id,
                    "table": rel_table(r.rel_type),
                }
                for r in writes.rels
            ],
        )
        g.types.update(r.rel_type for r in writes.rels)
        g.rel_pairs.update(
            (r.rel_type, s, t) for r in writes.rels for s in r.from_labels for t in r.to_labels
        )
