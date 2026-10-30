// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e1b87-f2d3-4a5c-8e6f-0d7b3c9a1e52
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { GNode } from "../graph-model";
import type { CyInstance, CyElement } from "../cytoscape-types";

export function resolveNodeLabel(n: GNode): string {
  if ("name" in n.properties) return String(n.properties["name"]);
  const nameKey = Object.keys(n.properties).find((k) => k.toLowerCase().includes("name"));
  if (nameKey) return String(n.properties[nameKey]);
  if ("title" in n.properties) return String(n.properties["title"]);
  return String(n.id);
}

export function computeLabelSizeRanges(
  cy: CyInstance,
  sizeByProp: Record<string, string>,
): Map<string, { min: number; max: number }> {
  const ranges = new Map<string, { min: number; max: number }>();
  cy.nodes().forEach((nd) => {
    if (nd.data("_cluster") || nd.data("_port")) return;
    const lbl = nd.data("label") as string;
    const sby = sizeByProp[lbl];
    if (!sby) return;
    const gn = nd.data("_node") as GNode | undefined;
    if (!gn) return;
    const v = Number(gn.properties[sby]);
    if (isNaN(v)) return;
    const cur = ranges.get(sby);
    if (!cur) {
      ranges.set(sby, { min: v, max: v });
    } else {
      ranges.set(sby, { min: Math.min(cur.min, v), max: Math.max(cur.max, v) });
    }
  });
  return ranges;
}

export function applyNodeSize(
  node: CyElement,
  lbl: string,
  gn: GNode | undefined,
  sizeByProp: Record<string, string>,
  sizeOverrides: Record<string, number>,
  sizeMultiplier: Record<string, number>,
  ranges: Map<string, { min: number; max: number }>,
): void {
  const base = sizeOverrides[lbl] ?? 44;
  const sby = sizeByProp[lbl];
  const multiplier = sizeMultiplier[lbl] ?? 3;
  const inCluster = node.data("_inCluster") as boolean;
  let sz: number;
  if (sby && gn) {
    const range = ranges.get(sby);
    const v = Number(gn.properties[sby]);
    if (range && !isNaN(v) && range.max > range.min) {
      const t = (v - range.min) / (range.max - range.min);
      sz = base * (1 + t * (multiplier - 1)) * (inCluster ? 0.5 : 1);
    } else {
      sz = inCluster ? base / 2 : base;
    }
  } else {
    sz = inCluster ? base / 2 : base;
  }
  node.style({ width: sz, height: sz });
}

// The nodes the user placed, where they are, as fcose's fixedNodeConstraint: fcose moves a locked
// node during its simulation and only the final write is refused, so the rest were laid out around
// where the simulation had carried it, not where the user put it.
/** A drawing's place on the canvas: where each node stands, by its own id, which outlives a change
 * of its label, and the view of them. */
export interface HeldDrawing {
  positions: Map<number, { x: number; y: number }>;
  zoom: number;
  pan: { x: number; y: number };
}

export function holdDrawing(cy: CyInstance): HeldDrawing {
  const positions = new Map<number, { x: number; y: number }>();
  cy.nodes().forEach((n) => {
    const gn = n.data("_node") as GNode | undefined;
    if (gn) positions.set(gn.id, { ...n.position() });
  });
  return { positions, zoom: cy.zoom(), pan: { ...cy.pan() } };
}

/** Puts each node of a new drawing where ``held`` has it, under the same view, and each new node
 * beside a neighbour that was held. The ids of the nodes that are new, for the layout to place. */
export function restoreDrawing(cy: CyInstance, drawing: HeldDrawing, spread: number): Set<string> {
  const held = drawing.positions;
  cy.zoom(drawing.zoom);
  cy.pan(drawing.pan);
  const fresh: CyElement[] = [];
  cy.nodes().forEach((n) => {
    const gn = n.data("_node") as GNode | undefined;
    const position = gn && held.get(gn.id);
    if (position) n.position(position);
    else fresh.push(n);
  });
  for (const n of fresh) {
    const beside = n.neighborhood("node").filter((nb: CyElement) => {
      const gn = nb.data("_node") as GNode | undefined;
      return gn !== undefined && held.has(gn.id);
    });
    if (beside.length === 0) continue;
    const from = beside[0].position();
    const angle = Math.random() * 2 * Math.PI;
    n.position({ x: from.x + spread * Math.cos(angle), y: from.y + spread * Math.sin(angle) });
  }
  return new Set(fresh.map((n) => n.id() as string));
}

export function anchoredPositions(
  cy: CyInstance,
  anchored: Set<string>,
): Array<{ nodeId: string; position: { x: number; y: number } }> {
  const fixed: Array<{ nodeId: string; position: { x: number; y: number } }> = [];
  anchored.forEach((id) => {
    const n = cy.getElementById(id);
    if (n.length === 0) return; // a node of a previous result, gone from this one
    const { x, y } = n.position();
    fixed.push({ nodeId: id, position: { x, y } });
  });
  return fixed;
}
