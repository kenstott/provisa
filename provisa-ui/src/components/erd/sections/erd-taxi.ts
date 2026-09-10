// Copyright (c) 2026 Kenneth Stott
// Canary: 19a4b659-e4f1-438d-9901-d7faacab0cb7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CyInstance, CyElement } from "../../graph/cytoscape-types";

type Pt = { x: number; y: number };

// Gap between the parallel runs of edges that share a node pair, in model pixels.
const LANE_GAP = 12;
// How far each lane's turn moves along the corridor, as a fraction of its length.
const TURN_STEP = 0.08;

// The default endpoint: the edge meets the node boundary on the line to its centre.
const DEFAULT_ENDPOINT = "outside-to-node";

// A point expressed the way `curve-style: segments` wants it: `weight` along the line from the
// source endpoint to the target endpoint, `distance` across it (positive to the line's left,
// as cytoscape's (-dy, dx) normal has it).
function segmentCoords(p: Pt, s: Pt, t: Pt): { weight: number; distance: number } {
  const vx = t.x - s.x;
  const vy = t.y - s.y;
  const len2 = vx * vx + vy * vy;
  const len = Math.sqrt(len2);
  const px = p.x - s.x;
  const py = p.y - s.y;
  return { weight: (px * vx + py * vy) / len2, distance: (px * -vy + py * vx) / len };
}

type Side = "top" | "bottom" | "left" | "right";

// Which side of each node an edge leaves from and arrives at: the facing sides along the
// dominant axis, so the route between them is a Z of at most two turns.
function sides(edge: CyElement): { vertical: boolean; sgn: number; source: Side; target: Side } {
  const sPos = edge.source().position();
  const tPos = edge.target().position();
  const vertical = Math.abs(tPos.y - sPos.y) >= Math.abs(tPos.x - sPos.x);
  const sgn = (vertical ? Math.sign(tPos.y - sPos.y) : Math.sign(tPos.x - sPos.x)) || 1;
  if (vertical) {
    return {
      vertical,
      sgn,
      source: sgn > 0 ? "bottom" : "top",
      target: sgn > 0 ? "top" : "bottom",
    };
  }
  return { vertical, sgn, source: sgn > 0 ? "right" : "left", target: sgn > 0 ? "left" : "right" };
}

// Where an edge meets a node: the offset along the side, from its centre.
function boundaryOffset(node: CyElement, side: Side, offset: number): Pt {
  switch (side) {
    case "top":
      return { x: offset, y: -node.height() / 2 };
    case "bottom":
      return { x: offset, y: node.height() / 2 };
    case "left":
      return { x: -node.width() / 2, y: offset };
    case "right":
      return { x: node.width() / 2, y: offset };
  }
}

// The right-angle path of one edge: a Z from its slot on the source side to its slot on the
// target side, with the cross-run at `turn` of the way along the corridor.
export function laneRoute(
  edge: CyElement,
  sourceOffset: number,
  targetOffset: number,
  turn: number,
): {
  sourceEndpoint: string;
  targetEndpoint: string;
  weights: string;
  distances: string;
} | null {
  const src = edge.source();
  const tgt = edge.target();
  const sPos = src.position();
  const tPos = tgt.position();
  const { vertical, sgn, source, target } = sides(edge);
  const sOff = boundaryOffset(src, source, sourceOffset);
  const tOff = boundaryOffset(tgt, target, targetOffset);
  const sEnd: Pt = { x: sPos.x + sOff.x, y: sPos.y + sOff.y };
  const tEnd: Pt = { x: tPos.x + tOff.x, y: tPos.y + tOff.y };
  const along = vertical ? tEnd.y - sEnd.y : tEnd.x - sEnd.x;
  // Nodes whose bodies overlap along the dominant axis leave no corridor to lay a route in.
  if (Math.abs(along) < 1 || Math.sign(along) !== sgn) return null;
  const turnAt = (vertical ? sEnd.y : sEnd.x) + turn * along;
  const p1: Pt = vertical ? { x: sEnd.x, y: turnAt } : { x: turnAt, y: sEnd.y };
  const p2: Pt = vertical ? { x: tEnd.x, y: turnAt } : { x: turnAt, y: tEnd.y };
  const c1 = segmentCoords(p1, sEnd, tEnd);
  const c2 = segmentCoords(p2, sEnd, tEnd);
  return {
    sourceEndpoint: `${sOff.x}px ${sOff.y}px`,
    targetEndpoint: `${tOff.x}px ${tOff.y}px`,
    weights: `${c1.weight} ${c2.weight}`,
    distances: `${c1.distance} ${c2.distance}`,
  };
}

type Attachment = { edge: CyElement; end: "source" | "target"; across: number };
type Slot = { offset: number; index: number; count: number };

// Every edge gets its own join point on each node it touches. The edges on one side of a node
// are spread along that side, ordered by where their other end lies so the runs do not cross.
function assignSlots(edges: CyElement[]): Map<CyElement, { source: Slot; target: Slot }> {
  const bySide = new Map<string, Attachment[]>();
  const add = (key: string, a: Attachment) => {
    const list = bySide.get(key);
    if (list) list.push(a);
    else bySide.set(key, [a]);
  };
  for (const edge of edges) {
    const { vertical, source, target } = sides(edge);
    const sPos = edge.source().position();
    const tPos = edge.target().position();
    const acrossOf = (p: Pt) => (vertical ? p.x : p.y);
    add(`${edge.source().id()}:${source}`, { edge, end: "source", across: acrossOf(tPos) });
    add(`${edge.target().id()}:${target}`, { edge, end: "target", across: acrossOf(sPos) });
  }
  const slots = new Map<CyElement, { source: Slot; target: Slot }>();
  const slotFor = (edge: CyElement) => {
    let s = slots.get(edge);
    if (!s) {
      s = { source: { offset: 0, index: 0, count: 1 }, target: { offset: 0, index: 0, count: 1 } };
      slots.set(edge, s);
    }
    return s;
  };
  for (const [key, list] of bySide) {
    list.sort((a, b) => a.across - b.across);
    const node = list[0].edge[list[0].end]();
    const side = key.slice(key.lastIndexOf(":") + 1) as Side;
    const extent = side === "top" || side === "bottom" ? node.width() : node.height();
    // Lanes keep their gap until the side runs out of room, then close up to fit.
    const gap = Math.min(LANE_GAP, extent / (list.length + 1));
    list.forEach((a, i) => {
      slotFor(a.edge)[a.end] = {
        offset: (i - (list.length - 1) / 2) * gap,
        index: i,
        count: list.length,
      };
    });
  }
  return slots;
}

// Right-angle edges into one node all met it at the same point, so edges that ran the same
// corridor were drawn exactly on top of each other. Cytoscape's taxi routing measures from the
// node centres and ignores manual endpoints, so the edges are drawn as explicit segments
// instead: each edge has its own join point on both nodes, and its cross-run is staggered along
// the corridor by its slot. An edge whose nodes overlap along the corridor falls back to taxi
// routing, whose fallbacks handle close nodes.
export function applyTaxiLanes(cy: CyInstance): void {
  const edges: CyElement[] = [];
  cy.edges(".erd-rel").forEach((edge) => {
    edges.push(edge);
  });
  const slots = assignSlots(edges);
  for (const edge of edges) {
    const { source, target } = slots.get(edge)!;
    const turn = 0.5 + (source.index - (source.count - 1) / 2) * TURN_STEP;
    const route = laneRoute(edge, source.offset, target.offset, turn);
    if (!route) {
      edge.style({
        "curve-style": "taxi",
        "source-endpoint": DEFAULT_ENDPOINT,
        "target-endpoint": DEFAULT_ENDPOINT,
      });
      continue;
    }
    edge.style({
      "curve-style": "segments",
      "edge-distances": "endpoints",
      "source-endpoint": route.sourceEndpoint,
      "target-endpoint": route.targetEndpoint,
      "segment-weights": route.weights,
      "segment-distances": route.distances,
    });
  }
}

// Bezier routing separates parallel edges by curvature, so the endpoints return to the default.
export function clearTaxiLanes(cy: CyInstance): void {
  cy.edges(".erd-rel").style({
    "source-endpoint": DEFAULT_ENDPOINT,
    "target-endpoint": DEFAULT_ENDPOINT,
    "edge-distances": "intersection",
  });
}
