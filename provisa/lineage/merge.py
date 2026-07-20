# Copyright (c) 2026 Kenneth Stott
# Canary: e3b7d1a6-9c48-4a02-8a15-6d0e2f7c4b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Federation-wide lineage merge + cycle characterization (REQ-1161).

Every per-statement column-level graph (REQ-1160) unions into ONE federation-wide provenance graph by
node identity (``relation.column``) — a view's output columns are another view's input columns, so the
per-view sub-graphs stitch into a single DAG from base source columns to every derived column. Node
identity is the correctness backbone: two references to the same dataset MUST resolve to one node id
or the merged graph fragments silently.

Cycles are DETECTED and CHARACTERIZED, never auto-rejected: a cycle that crosses a MATERIALIZED node
(an MV / CTAS snapshot) is a legal time-lagged feedback loop — the snapshot is the version boundary
that makes it well-defined; a cycle with NO materialization boundary is the likely design error (no
evaluation order, no stable value). The platform describes the distinction for operator judgment.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlglot.errors import SqlglotError

from provisa.lineage.graph import (
    LineageGraph,
    Node,
    build_column_graph,
    qualify_outputs,
    requalify_relations,
)


@dataclass
class Cycle:  # REQ-1161
    """One directed cycle in the merged graph, with its characterization."""

    nodes: tuple[str, ...]  # member node ids, in cycle order
    has_materialization_boundary: bool

    @property
    def classification(self) -> str:
        """``feedback`` (legal, time-lagged — a materialized boundary is on the loop) or ``error``
        (likely design defect — a circular definition with no version boundary)."""
        return "feedback" if self.has_materialization_boundary else "error"

    def to_dict(self) -> dict:
        return {
            "nodes": list(self.nodes),
            "has_materialization_boundary": self.has_materialization_boundary,
            "classification": self.classification,
        }


@dataclass
class MergedGraph:  # REQ-1161
    graph: LineageGraph
    cycles: list[Cycle] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = self.graph.to_dict()
        d["cycles"] = [c.to_dict() for c in self.cycles]
        return d


def merge_graphs(graphs: list[LineageGraph]) -> MergedGraph:
    """Union per-statement graphs into one federation graph by node id, then detect + characterize
    cycles (REQ-1161). A node seen in several graphs is deduped; ``materialized`` and a real relation
    are sticky (once known, kept), so a view's output node and another view's input reference collapse
    to one node carrying the strongest attributes."""
    merged = LineageGraph()
    seen_edges: set[tuple[str, str, str]] = set()
    for g in graphs:
        for node in g.nodes.values():
            _merge_node(merged, node)
        for edge in g.edges:
            key = (edge.source, edge.target, edge.transform)
            if key not in seen_edges:
                seen_edges.add(key)
                merged.add_edge(edge)
        for out in g.outputs:
            if out not in merged.outputs:
                merged.outputs.append(out)
    cycles = _find_cycles(merged)
    return MergedGraph(graph=merged, cycles=cycles)


def _merge_node(merged: LineageGraph, node: Node) -> None:
    """Union a node into the merged graph, keeping the strongest attributes (sticky materialized /
    real relation / non-source kind)."""
    existing = merged.nodes.get(node.id)
    if existing is None:
        merged.nodes[node.id] = Node(
            id=node.id,
            column=node.column,
            relation=node.relation,
            kind=node.kind,
            materialized=node.materialized,
        )
        return
    existing.materialized = existing.materialized or node.materialized
    if existing.relation is None and node.relation is not None:
        existing.relation = node.relation
    # 'command' is the most specific boundary kind; prefer it, else prefer a produced ('derived') over
    # a bare 'source' leaf when the same id is both referenced and produced across statements.
    if node.kind == "command" or (existing.kind == "source" and node.kind == "derived"):
        existing.kind = node.kind


def _find_cycles(graph: LineageGraph) -> list[Cycle]:
    """Every simple directed cycle in the merged graph (edges point source→target, i.e. upstream→
    downstream). Each is characterized by whether any node on it is materialized (REQ-1161)."""
    adj: dict[str, list[str]] = {}
    for e in graph.edges:
        adj.setdefault(e.source, []).append(e.target)

    cycles: list[Cycle] = []
    seen_signatures: set[frozenset[str]] = set()
    WHITE, GREY, BLACK = 0, 1, 2
    color: dict[str, int] = {}
    stack: list[str] = []

    def visit(node: str) -> None:
        color[node] = GREY
        stack.append(node)
        for nxt in adj.get(node, ()):
            if color.get(nxt, WHITE) == GREY:  # back-edge → cycle from nxt to top of stack
                i = stack.index(nxt)
                members = stack[i:]
                sig = frozenset(members)
                if sig not in seen_signatures:
                    seen_signatures.add(sig)
                    cycles.append(
                        Cycle(
                            nodes=tuple(members),
                            has_materialization_boundary=any(
                                graph.nodes[m].materialized for m in members if m in graph.nodes
                            ),
                        )
                    )
            elif color.get(nxt, WHITE) == WHITE:
                visit(nxt)
        stack.pop()
        color[node] = BLACK

    for n in list(graph.nodes):
        if color.get(n, WHITE) == WHITE:
            visit(n)
    return cycles


def mark_materialized(graph: LineageGraph, materialized_relations: set[str]) -> None:
    """Flag every node whose relation is in ``materialized_relations`` (REQ-1161). The caller supplies
    the set of MV/CTAS relation names from the registry — the version boundaries in the graph."""
    for node in graph.nodes.values():
        if node.relation is not None and node.relation in materialized_relations:
            node.materialized = True


def slice_graph(
    graph: LineageGraph, node_id: str, *, direction: str = "both", depth: int | None = None
) -> LineageGraph:
    """A focused sub-graph around ``node_id`` (REQ-1161 progressive rendering / focus tools).

    ``direction``: ``upstream`` (what it derives from), ``downstream`` (what derives from it), or
    ``both`` (ego-graph). ``depth`` bounds the hop distance (None = unbounded). The full graph is
    computed once; this scopes the VIEW so a federation-scale graph renders legibly."""
    if node_id not in graph.nodes:
        raise ValueError(f"node {node_id!r} not in graph")
    up: dict[str, list[str]] = {}
    down: dict[str, list[str]] = {}
    for e in graph.edges:
        down.setdefault(e.source, []).append(e.target)
        up.setdefault(e.target, []).append(e.source)

    keep: set[str] = {node_id}
    if direction in ("upstream", "both"):
        _reach(node_id, up, depth, keep)
    if direction in ("downstream", "both"):
        _reach(node_id, down, depth, keep)

    out = LineageGraph()
    for nid in keep:
        out.nodes[nid] = graph.nodes[nid]
    for e in graph.edges:
        if e.source in keep and e.target in keep:
            out.add_edge(e)
    out.outputs = [o for o in graph.outputs if o in keep]
    return out


def _reach(start: str, adj: dict[str, list[str]], depth: int | None, keep: set[str]) -> None:
    """BFS from ``start`` over ``adj``, adding reached nodes to ``keep`` up to ``depth`` hops."""
    frontier = [(start, 0)]
    while frontier:
        node, d = frontier.pop()
        if depth is not None and d >= depth:
            continue
        for nxt in adj.get(node, ()):
            if nxt not in keep:
                keep.add(nxt)
                frontier.append((nxt, d + 1))


def build_federation_graph(
    views: list[tuple[str, str]],
    *,
    commands: dict[str, dict] | None = None,
    materialized_relations: set[str] | None = None,
    dialect: str = "postgres",
) -> MergedGraph:
    """Merge every view/MV definition into one federation-wide provenance graph (REQ-1161).

    ``views`` is a list of (relation_name, sql). Each is resolved to a single-statement graph, its
    outputs qualified as ``relation_name.column`` so a downstream view's source reference stitches to
    the upstream view's output node. ``materialized_relations`` are flagged BEFORE the merge so cycle
    characterization sees the version boundaries. A view whose SQL will not parse is skipped (it cannot
    contribute lineage), not fatal to the whole federation graph."""
    mats = materialized_relations or set()
    # A view that reads another view references it by bare table name (sqlglot drops the schema), so
    # requalify those refs to the full relation before stitching — else the same view appears twice
    # (once qualified as an output, once as a disconnected bare-name source).
    bare_to_full = {relation.split(".")[-1]: relation for relation, _ in views}
    graphs: list[LineageGraph] = []
    for relation, sql in views:
        try:
            g = build_column_graph(sql, dialect=dialect, commands=commands or {})
        except SqlglotError:
            continue
        requalify_relations(g, bare_to_full)
        qualify_outputs(g, relation)
        mark_materialized(g, mats)
        graphs.append(g)
    return merge_graphs(graphs)
