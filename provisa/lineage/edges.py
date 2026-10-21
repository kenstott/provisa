# Copyright (c) 2026 Kenneth Stott
# Canary: 3006a5bb-3781-4c61-8688-e36a3f888aac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The column lineage edges a model build writes (REQ-2129).

Each edge joins a column a view (or a statement command) reads to a column it produces, and names
that view (or command). They are folded from the same per-view sub-DAGs the federation graph is the
union of (provisa.lineage.merge.federation_subdags): a view's internal steps -- a CTE, a subquery --
are walked through, so the edge joins the view's input column to its output column and a chain of
views is never cut at a step that is not a registered relation.
"""

from __future__ import annotations

from dataclasses import dataclass

from provisa.lineage.graph import LineageGraph

VIEW = "view"
COMMAND = "command"


@dataclass(frozen=True)
class FoldedEdge:
    source_relation: str
    source_column: str
    target_relation: str
    target_column: str
    via: str  # the deriving view's relation, or the writing command's name
    via_kind: str  # VIEW or COMMAND


def fold(graph: LineageGraph, via: str, via_kind: str) -> set[FoldedEdge]:
    """Every (input column, output column) pair of one view's or command's graph: each output
    joined to each column it is computed from that nothing in the graph computes."""
    upstream: dict[str, list[str]] = {}
    for edge in graph.edges:
        upstream.setdefault(edge.target, []).append(edge.source)
    out: set[FoldedEdge] = set()
    for output in graph.outputs:
        target = graph.nodes[output]
        # Outputs are keyed <relation>.<column> before a graph is folded (qualify_outputs).
        assert target.relation is not None, output
        seen: set[str] = set()
        stack = list(upstream.get(output, []))
        while stack:
            node_id = stack.pop()
            if node_id in seen:
                continue
            seen.add(node_id)
            above = upstream.get(node_id)
            if above:
                stack.extend(above)
                continue
            node = graph.nodes[node_id]
            # A literal or a function call reads no column: there is no input to name.
            if node.relation is None or node.kind == "command":
                continue
            out.add(
                FoldedEdge(
                    node.relation, node.column, target.relation, target.column, via, via_kind
                )
            )
    return out


def folded_edges(
    view_graphs: list[tuple[str, LineageGraph]],
    command_graphs: list[tuple[str, LineageGraph]],
) -> set[FoldedEdge]:
    """The whole model's edges: every view's, then every statement command's write."""
    edges: set[FoldedEdge] = set()
    for relation, graph in view_graphs:
        edges |= fold(graph, relation, VIEW)
    for name, graph in command_graphs:
        edges |= fold(graph, name, COMMAND)
    return edges
