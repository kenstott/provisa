// Copyright (c) 2026 Kenneth Stott
// Canary: 3a8f6c2e-71b4-4d95-b2a0-8e5d1c7f4b63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2189, REQ-2239: a saved layout's places, read off the graph and stood on it again. Places
// are by the view's node and group ids; what they mean (a table, a domain) is the view's.

import type { CyInstance } from "../graph/cytoscape-types";

export type Pt = { x: number; y: number };

export interface GraphPlaces {
  /** Each drawn node's position, by id. */
  nodes: Map<string, Pt>;
  /** Each drawn group's box: its centre and its inner size, by id. */
  groups: Map<string, Pt & { w: number; h: number }>;
}

/** Places to stand: a node at its point, a group at least its size (a collapsed group, one with
 * no members drawn, at its point). A null coordinate keeps the automatic layout's. */
export interface StandPlaces {
  nodes: { id: string; x: number | null; y: number | null }[];
  groups: { id: string; x: number | null; y: number | null; w: number | null; h: number | null }[];
}

/** Read the graph's places off the canvas. */
export function readPlaces(cy: CyInstance): GraphPlaces {
  const nodes = new Map<string, Pt>();
  cy.nodes(".pg-node").forEach((n) => {
    nodes.set(n.id(), n.position());
  });
  const groups = new Map<string, Pt & { w: number; h: number }>();
  cy.nodes(".pg-group").forEach((n) => {
    groups.set(n.id(), { ...n.position(), w: n.width(), h: n.height() });
  });
  return { nodes, groups };
}

/** Stand the graph as ``places`` hold it, with what was moved since (``moved``, by node id) over
 * it. A group keeps at least its saved size; a collapsed one stands where it was saved. */
export function standPlaces(cy: CyInstance, places: StandPlaces, moved: Map<string, Pt>): void {
  cy.batch(() => {
    for (const place of places.nodes) {
      const node = cy.getElementById(place.id);
      if (node.empty() || place.x === null || place.y === null) continue;
      node.position({ x: place.x, y: place.y });
    }
    for (const place of places.groups) {
      const node = cy.getElementById(place.id);
      if (node.empty()) continue;
      if (node.children().empty()) {
        if (place.x !== null && place.y !== null) node.position({ x: place.x, y: place.y });
      } else if (place.w !== null && place.h !== null) {
        node.style({ "min-width": place.w, "min-height": place.h });
      }
    }
    moved.forEach((at, id) => {
      const node = cy.getElementById(id);
      if (!node.empty()) node.position(at);
    });
  });
}
