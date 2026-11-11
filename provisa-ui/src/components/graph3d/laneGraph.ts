// Copyright (c) 2026 Kenneth Stott
// Canary: b3e7c1a9-0d46-4f82-9c5e-6a2f8d1b7e04
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174, REQ-2239: the lane diagram (laneDiagram.ts) as the one graph draws it: each node with
// its type and lane, the lines between them, and the term links laid over the placed term lane.

import type { GraphNode } from "./graphModel";
import type { Lane, LaneDiagram } from "./laneDiagram";
import type { GraphEdgeSpec, GraphNodeSpec, GraphSpec } from "../provisa-graph/model";
import type { NodeType } from "../provisa-graph/tokens";

/** What a graph node is, as the one graph names its types: a term by its role. */
const typeOf = (n: GraphNode): NodeType => (n.type === "term" && n.role ? n.role : n.type);

/** A term's status, as the graph draws a term held back; an approved term is drawn plain. */
const statusOf = (n: GraphNode): GraphNodeSpec["status"] =>
  n.status === "draft" || n.status === "deprecated" || n.status === "retired"
    ? n.status
    : undefined;

/** The lane diagram as the one graph draws it. A line between lanes runs left to right, from the
 * product side, and has no direction to show; a link inside the term lane runs from the broader term
 * down, its arrow at the narrower end's other side (it points from the narrower term to the
 * broader); a fold node's line is indirect. */
export function laneGraph(d: LaneDiagram, moreLabel: (count: number) => string): GraphSpec {
  const g = d.graph;
  const nodes: GraphNodeSpec[] = [
    ...g.nodes.map((n) => {
      const status = statusOf(n);
      return {
        id: n.id,
        type: typeOf(n),
        name: n.label,
        lane: d.lane.get(n.id) as Lane,
        ...(status ? { status } : {}),
      };
    }),
    ...d.more.map((m) => ({
      id: m.id,
      type: "more" as const,
      kind: "more" as const,
      name: moreLabel(m.count),
      lane: "column",
      data: { table: m.table },
    })),
  ];
  const more = new Set(d.more.map((m) => m.id));
  const edges: GraphEdgeSpec[] = [
    ...d.links.map((k) => {
      const inTerms = d.lane.get(k.from) === "term" && d.lane.get(k.to) === "term";
      return inTerms
        ? { id: k.id, source: k.from, target: k.to, label: k.label ?? "", arrow: "source" as const }
        : {
            id: k.id,
            source: k.to,
            target: k.from,
            ...(more.has(k.from) || more.has(k.to) ? { dashed: true } : {}),
          };
    }),
    ...d.arcs.map((k) => ({
      id: k.id,
      source: k.from,
      target: k.to,
      label: k.label ?? "",
      arrow: "target" as const,
      overlay: true,
    })),
  ];
  return { nodes, edges };
}
