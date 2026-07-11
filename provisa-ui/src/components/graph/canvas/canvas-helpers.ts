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
  node.style({ width: sz, height: sz, "text-max-width": `${sz - 4}px` });
}
