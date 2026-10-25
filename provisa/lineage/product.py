# Copyright (c) 2026 Kenneth Stott
# Canary: e93b0c47-2a18-4d65-8f7e-61c5d3a9b802
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product's lineage and its nearest physical inputs (REQ-1640, REQ-2126).

The product's lineage is its member columns, everything they derive from and what derives from
them directly, a checker table's columns left out: a checker's rows are scan outcomes about a
member, not part of its data flow. Its inputs are found on that graph, after the caller's read
rights have cut it (REQ-1161): upstream of each member, through plain views and a view's internal
steps, to the first physical dataset, a registered source table or a materialized view; a member
read straight from a source has that source as its input. A member is never an input of its own
product.
"""

# Requirements: REQ-1640, REQ-2126

from __future__ import annotations

import copy

from provisa.lineage.graph import LineageGraph, Node
from provisa.lineage.merge import ancestor_closure

NATIVE = "native"


def product_graph(
    graph: LineageGraph, member_columns: set[str], *, checker_relations: set[str]
) -> LineageGraph:
    """The product's lineage over ``graph``: ``member_columns`` (node ids), their ancestry and one
    hop downstream. A member column nothing mentions joins as an isolated source node; every
    member column is an output. ``graph`` is not changed."""
    graph = copy.copy(graph)
    graph.nodes = dict(graph.nodes)
    for nid in member_columns - set(graph.nodes):
        relation, _, column = nid.rpartition(".")
        graph.nodes[nid] = Node(id=nid, column=column, relation=relation, kind="source")
    out = ancestor_closure(graph, member_columns)
    for e in graph.edges:
        if e.source not in member_columns:
            continue
        child = graph.nodes[e.target]
        if child.relation in checker_relations:
            continue
        out.nodes.setdefault(e.target, child)
        out.add_edge(e)
    out.outputs = sorted(member_columns)
    return out


def nearest_inputs(graph: LineageGraph, members: set[str], *, base: set[str]) -> list[str]:
    """The relations of the product's nearest physical inputs over ``graph``: ``members`` are its
    member relations, ``base`` the registered source tables. A native relation stands for the
    source a base member is read from."""
    up: dict[str, list[str]] = {}
    for e in graph.edges:
        up.setdefault(e.target, []).append(e.source)
    found: set[str] = set()
    for nid, node in graph.nodes.items():
        if node.relation not in members:
            continue
        if node.relation in base:
            found.update(
                graph.nodes[p].relation or ""
                for p in up.get(nid, [])
                if graph.nodes[p].kind == NATIVE
            )
            continue
        seen = {nid}
        stack = [nid]
        while stack:
            for p in up.get(stack.pop(), []):
                if p in seen:
                    continue
                seen.add(p)
                parent = graph.nodes[p]
                if parent.relation in members:
                    continue
                if parent.relation in base or parent.materialized or parent.kind == NATIVE:
                    found.add(parent.relation or "")
                else:
                    stack.append(p)
    found.discard("")
    return sorted(found)
