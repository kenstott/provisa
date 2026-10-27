# Copyright (c) 2026 Kenneth Stott
# Canary: b47e1d0c-3f92-4a68-8e25-d9c6a1f07b53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A lineage graph cut to what its caller can read (REQ-1161, REQ-2116).

Every lineage read answers with the fields its caller is served and nothing else. A registered
column shows when the caller is served it; a native field shows when the registered column it feeds
shows; a physical column nobody registered never shows, since nobody reads it through the model. A
view's internal step (a CTE or command-call column, not a field) shows only when every field it
feeds shows, so a hidden view's logic goes with it. A hidden node's neighbours join directly
(A→B→C reads A→C), and the joined edge carries no transform; an edge into a node that also had a
hidden parent loses its transform too, since the expression names that parent. Nothing marks what
was cut.
"""

# Requirements: REQ-1161, REQ-2116

from __future__ import annotations

from provisa.lineage.graph import Edge, LineageGraph

NATIVE = "native"


def readable_graph(
    graph: LineageGraph, *, registered: set[str], readable: set[str]
) -> LineageGraph:
    """``graph`` with only what a caller reads. ``registered``: the registered relations;
    ``readable``: the node ids of the registered columns the caller is served. ``graph`` is not
    changed."""
    up: dict[str, list[Edge]] = {}
    down: dict[str, list[str]] = {}
    for e in graph.edges:
        up.setdefault(e.target, []).append(e)
        down.setdefault(e.source, []).append(e.target)

    def is_field(nid: str) -> bool:
        node = graph.nodes[nid]
        return node.kind != NATIVE and node.relation in registered

    def fields_fed(nid: str) -> set[str]:
        """The fields an internal step feeds, through other internal steps."""
        found: set[str] = set()
        seen = {nid}
        stack = [nid]
        while stack:
            for child in down.get(stack.pop(), []):
                if child in seen or child not in graph.nodes:
                    continue
                seen.add(child)
                if is_field(child):
                    found.add(child)
                elif graph.nodes[child].kind != "source":
                    stack.append(child)
        return found

    def shown(nid: str) -> bool:
        node = graph.nodes[nid]
        if node.kind == NATIVE:
            return any(c in readable and is_field(c) for c in down.get(nid, []))
        if is_field(nid):
            return nid in readable
        if node.kind == "source":
            return False
        return all(f in readable for f in fields_fed(nid))

    keep = {nid for nid in graph.nodes if shown(nid)}

    def shown_ancestors(nid: str) -> list[str]:
        """The nearest shown nodes above ``nid``, walking up through hidden ones."""
        out: list[str] = []
        seen = {nid}
        stack = [nid]
        while stack:
            for e in up.get(stack.pop(), []):
                if e.source in seen or e.source not in graph.nodes:
                    continue
                seen.add(e.source)
                if e.source in keep:
                    out.append(e.source)
                else:
                    stack.append(e.source)
        return out

    result = LineageGraph()
    for nid in graph.nodes:
        if nid in keep:
            result.nodes[nid] = graph.nodes[nid]
    for target in result.nodes:
        parents = up.get(target, [])
        hidden_parent = any(e.source not in keep for e in parents)
        joined: set[str] = set()
        for e in parents:
            if e.source in keep:
                joined.add(e.source)
                result.add_edge(e if not hidden_parent else Edge(e.source, target, "", ()))
        for e in parents:
            if e.source in keep:
                continue
            for ancestor in shown_ancestors(e.source):
                if ancestor not in joined:
                    joined.add(ancestor)
                    result.add_edge(Edge(ancestor, target, "", ()))
    result.outputs = [o for o in graph.outputs if o in keep]
    return result
