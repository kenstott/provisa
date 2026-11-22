// Copyright (c) 2026 Kenneth Stott
// Canary: 271bc8d5-309e-4266-a03d-ea7393636407
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// How long the force layout wants an edge. A cluster-to-cluster edge carries a label that is never
// rotated, so it is as wide as its text: two such edges meeting at a supernode put their labels
// at their middles, half the far side of their triangle apart. An edge twice its label's width
// (end to end, between the supernodes' borders, as fcose measures) keeps the two labels clear of
// each other and of both supernodes. Every other edge takes the edge-distance setting.

import { textWidth } from "../../provisa-graph/model";

/** The cluster-edge label's font size and box padding (graph-stylesheet.ts, edge[?_metaEdge]). */
export const META_LABEL_SIZE = 9;
export const META_LABEL_PAD = 2;

interface LayoutEdge {
  data(key: string): unknown;
}

/** A cluster-edge label's drawn width: its text and its box's padding on both sides. */
export function metaLabelWidth(label: string): number {
  return textWidth(label, META_LABEL_SIZE) + 2 * META_LABEL_PAD;
}

/** fcose's idealEdgeLength: ``distance`` for an edge, at least twice its label for a cluster edge. */
export function idealEdgeLength(distance: number): (edge: LayoutEdge) => number {
  return (edge) =>
    edge.data("_metaEdge")
      ? Math.max(distance, 2 * metaLabelWidth(edge.data("label") as string))
      : distance;
}
