# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9f54a6-4aaa-4e86-86d1-2fb3efae227d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which write is governed and which ephemeral (REQ-2176).

The governing rule: a governed model write needs an exact, unambiguous target -- one model table
(and for a relationship one registered junction) -- and nothing else. A CREATE of a node naming
exactly one model table and no free label is the governed write it is today; any other CREATE
makes an ephemeral node carrying every label given. A label resolves as the model's (a
``Domain:Table`` pair, or a table label) before the session's; a domain label otherwise is
refused, since domains are sealed.

An ephemeral node's anchor -- the table holding its properties -- is the first of its labels
naming an existing ephemeral anchor, else the first free label (which creates it), else the
session's default anchor. Only ephemeral tables anchor.
"""

# Requirements: REQ-2176, REQ-661, REQ-572

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from provisa.cypher.parser import CypherParseError
from provisa.ephemeral.pattern import CreatePattern
from provisa.ephemeral.store import Graph


@dataclass(frozen=True)
class Labels:
    """A node pattern's labels, resolved: the model node it names (its type name), and the
    labels that are not the model's, in the order written."""

    model: str | None
    free: tuple[str, ...]
    written: tuple[str, ...]

    @property
    def keys(self) -> tuple[str, ...]:
        """The labels a node of these carries, as the session holds them: a model label as its
        node's type name, a free one as written."""
        return ((self.model,) if self.model else ()) + self.free


def _domain(label_map: Any, label: str) -> str | None:
    canonical = label_map.canonical_label(label)
    return canonical if canonical in label_map.domains else None


def _model_node(label_map: Any, label: str) -> str | None:
    """The model node a single label names: a node's own key or its unique table label."""
    canonical = label_map.canonical_label(label)
    if canonical in label_map.nodes:
        return canonical
    named = label_map.nodes_by_table.get(canonical, [])
    return named[0] if len(named) == 1 else None


def resolve(labels: list[str], label_map: Any) -> Labels:
    """Resolve a written node pattern's labels against the model (``label_map``, the role's)."""
    written = tuple(labels)
    if labels and _domain(label_map, labels[0]) is not None:
        domain = _domain(label_map, labels[0])
        assert domain is not None
        table = labels[1] if len(labels) > 1 else None
        in_domain = [
            t
            for t in label_map.domains[domain]
            if table is not None
            and label_map.nodes[t].table_label.lower() == label_map.canonical_label(table).lower()
        ]
        if len(in_domain) != 1:
            raise CypherParseError(
                f"{labels[0]} is a domain: a node of it is written as {labels[0]}:<Table>, one of "
                f"its tables (domains are sealed)"
            )
        return Labels(in_domain[0], tuple(labels[2:]), written)
    model: list[str] = []
    free: list[str] = []
    for label in labels:
        node = _model_node(label_map, label)
        (model if node is not None else free).append(node or label)
    if len(model) > 1:
        raise CypherParseError(
            f"A node is one model table at most: {', '.join(model)} are each the model's"
        )
    return Labels(model[0] if model else None, tuple(free), written)


@dataclass(frozen=True)
class Created:
    """A node a CREATE makes: its pattern's index, its labels, and its anchor table's label
    (None for the session's default anchor)."""

    index: int
    labels: Labels
    anchor: str | None


@dataclass(frozen=True)
class CreateKind:
    """Whether a CREATE is the governed model write or ephemeral, and each node it makes."""

    governed: bool
    created: tuple[Created, ...]


def classify_create(
    pattern: CreatePattern, bound: set[str], label_map: Any, graph: Graph
) -> CreateKind:
    """The governing rule for a CREATE's ``pattern`` (``bound``: the variables its leading
    clauses bind, which it reaches back to rather than creates)."""
    created: list[Created] = []
    named: set[str] = set(bound)
    for i, node in enumerate(pattern.nodes):
        if node.variable is not None and node.variable in named:
            if node.labels or node.properties:
                raise CypherParseError(
                    f"Variable `{node.variable}` already declared: a CREATE cannot give it "
                    f"labels or properties"
                )
            continue
        if node.variable is not None:
            named.add(node.variable)
        labels = resolve(node.labels, label_map)
        created.append(Created(i, labels, _anchor(labels, graph)))
    exact = [c.labels.model is not None and not c.labels.free for c in created]
    model_types = [_model_type(label_map, r.rel_type) for r in pattern.rels]
    if not created:
        # Relationships between nodes the statement matched: each a registered junction's row
        # (REQ-665), or each the session's.
        if all(model_types):
            return CreateKind(True, ())
        if any(model_types):
            raise CypherParseError(
                "A CREATE writes model relationships or session relationships, not both: "
                + ", ".join(r.rel_type for r, m in zip(pattern.rels, model_types) if m)
                + " are the model's; write them in a statement of their own"
            )
        return CreateKind(False, ())
    if any(exact) and not all(exact):
        raise CypherParseError(
            "A CREATE makes model rows or session nodes, not both: "
            + ", ".join(
                ":" + ":".join(c.labels.written) for c, e in zip(created, exact, strict=True) if e
            )
            + " name one model table each; write them in a statement of their own"
        )
    return CreateKind(all(exact), tuple(created))


def _model_type(label_map: Any, rel_type: str) -> bool:
    return rel_type.upper() in label_map.aliases or rel_type.upper() in label_map.relationships


def _anchor(labels: Labels, graph: Graph) -> str | None:
    for label in labels.free:
        if label in graph.anchors:
            return label
    return labels.free[0] if labels.free else None
