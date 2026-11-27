// Copyright (c) 2026 Kenneth Stott
// Canary: 4c8e2a71-9d36-4f05-b1e7-3a6d0c5f9b82
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27: lineage works like the 2D ontology graph), REQ-1665: the flow
// preset, lineage's. ELK lays the graph out in layers left to right, the read order of lineage,
// each group laid out with its nodes as one column of them (a relation's fields stacked in the
// order the view gives). What no line reaches -- a group none of whose nodes has a line, a node with
// none -- is packed under the laid-out part to the pane's shape, as the layered preset packs its
// orphans: ELK's component packing does not apply inside a hierarchy, so it would stack them all
// in one tall column.

import { packBoxes } from "./arrange";
import type { Core } from "./core";
import { PROVISA_ELK } from "./elkLayout";

/** The space between two things packed under the laid-out part, and above them. */
const PACK_GAP = 48;

/** A group's ELK options: its nodes one column, in the view's order, with room for its title. */
const GROUP_OPTIONS = {
  "elk.padding": "[top=40,left=28,bottom=28,right=28]",
  "elk.separateConnectedComponents": "false",
  "elk.spacing.nodeNode": "10",
  "elk.layered.spacing.nodeNodeBetweenLayers": "40",
  "elk.layered.nodePlacement.strategy": "SIMPLE",
};

export function flowLayout(eles: unknown) {
  return {
    name: PROVISA_ELK,
    animate: false,
    fit: false,
    eles,
    nodeLayoutOptions: (n: { isParent(): boolean }) => (n.isParent() ? GROUP_OPTIONS : {}),
    elk: {
      algorithm: "layered",
      "elk.direction": "RIGHT",
      "elk.hierarchyHandling": "INCLUDE_CHILDREN",
      // Room between layers for a line's label: lineage names each transform on its line.
      "elk.layered.spacing.nodeNodeBetweenLayers": "140",
      "elk.spacing.nodeNode": "44",
      "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
      "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
      "elk.layered.crossingMinimization.hierarchicalSweepiness": "1",
      "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES",
    },
  };
}

/** What of the core the flow preset reads and moves. */
interface FlowNode {
  id(): string;
  parent(): FlowEles & { id(): string; empty(): boolean; children(): FlowEles };
  children(): FlowEles;
  position(): { x: number; y: number };
  height(): number;
  isOrphan(): boolean;
  isParent(): boolean;
  ancestors(): ArrayLike<FlowNode>;
  descendants(): FlowEles;
  union(other: FlowEles | FlowNode): FlowEles;
  shift(by: { x: number; y: number }): void;
}
interface Box {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  w: number;
  h: number;
}
interface FlowEles {
  forEach(fn: (n: FlowNode) => void): void;
  boundingBox(o: object): Box;
  shift(by: { x: number; y: number }): void;
}
interface FlowCore {
  nodes(selector?: string): FlowEles;
  edges(): { forEach(fn: (e: { source(): FlowNode; target(): FlowNode }) => void): void };
  getElementById(id: string): FlowNode & FlowEles;
  batch(fn: () => void): void;
}

/** The group ``n`` is drawn in at the top, or ``n`` itself when it is in none: its ancestors run
 * outward, the outermost last. */
function topId(n: FlowNode): string {
  const outward = n.ancestors();
  return outward.length > 0 ? outward[outward.length - 1].id() : n.id();
}

/** The top-level things of the graph -- groups, and nodes in no group -- split by whether a line
 * reaches them (or one of a group's nodes). */
export function flowParts(core: Core): { laid: string[]; isolated: string[] } {
  const cy = core as unknown as FlowCore;
  const reached = new Set<string>();
  cy.edges().forEach((e) => {
    for (const end of [e.source(), e.target()]) reached.add(topId(end));
  });
  const laid: string[] = [];
  const isolated: string[] = [];
  cy.nodes().forEach((n) => {
    if (n.isOrphan()) (reached.has(n.id()) ? laid : isolated).push(n.id());
  });
  return { laid, isolated };
}

/** Each of ``ids`` (laid out already, each a group with its nodes or a lone node) moved, whole,
 * under what is laid out, packed to a pane of ``aspectRatio``. */
export function packUnder(core: Core, ids: string[], aspectRatio: number): void {
  if (ids.length === 0) return;
  const cy = core as unknown as FlowCore;
  const outside = new Set(ids);
  const boxOf = (id: string) => {
    const n = cy.getElementById(id);
    return n.union(n.descendants()).boundingBox({});
  };
  // The extent of what stays where ELK put it.
  let above: { x1: number; y2: number } | null = null;
  cy.nodes().forEach((n) => {
    if (outside.has(topId(n))) return;
    const bb = n.union(n.descendants()).boundingBox({});
    above = above
      ? { x1: Math.min(above.x1, bb.x1), y2: Math.max(above.y2, bb.y2) }
      : { x1: bb.x1, y2: bb.y2 };
  });
  const boxes = ids.map((id) => {
    const bb = boxOf(id);
    return { id, w: bb.w, h: bb.h, x1: bb.x1, y1: bb.y1 };
  });
  const at = packBoxes(boxes, aspectRatio, PACK_GAP);
  const top = above as { x1: number; y2: number } | null;
  const x0 = top ? top.x1 : 0;
  const y0 = top ? top.y2 + PACK_GAP : 0;
  cy.batch(() => {
    for (const b of boxes) {
      // packBoxes places every box it is given.
      const to = at.get(b.id);
      if (!to) throw new Error(`packUnder: ${b.id} was not packed`);
      const n = cy.getElementById(b.id);
      // A group's place is its nodes': it moves with them.
      const movable = n.isParent() ? n.descendants() : n;
      movable.shift({ x: x0 + to.x - b.x1, y: y0 + to.y - b.y1 });
    }
  });
}

/** The gap between two nodes of a group, as ELK spaces them (GROUP_OPTIONS). */
const GROUP_NODE_GAP = 10;

/** Each group's fold node (``kind: "more"``: the nodes it stands for are not drawn) moved under the
 * group's other nodes: it has no line, so ELK may put it anywhere in the group's column. */
export function foldsLast(core: Core): void {
  const cy = core as unknown as FlowCore;
  cy.batch(() =>
    cy.nodes('[kind = "more"]').forEach((fold) => {
      const group = fold.parent();
      if (group.empty()) return;
      let bottom = -Infinity;
      group.children().forEach((n) => {
        if (n.id() !== fold.id()) bottom = Math.max(bottom, n.position().y + n.height() / 2);
      });
      if (bottom === -Infinity) return;
      const at = fold.position();
      fold.shift({ x: 0, y: bottom + GROUP_NODE_GAP + fold.height() / 2 - at.y });
    }),
  );
}
