// Copyright (c) 2026 Kenneth Stott
// Canary: 29c7a81f-d814-4e52-96df-0c3b880c695f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2143: what a focus draws, and how much a graph draws at all, stated once. The Ontology View's
// focus (its page's search, or a node picked) is the matches and what lies within `depth` hops of
// them; reach() draws that, and the query the view reads and exports (glossaryGraphData) asks the
// server for exactly that. A graph that would draw more than NODE_BUDGET nodes is not drawn: its
// count is said instead, and with no search the domains are drawn with their counts.

import type { EdgeKind, NodeType } from "./graphModel";

export interface Neighbourhood {
  /** Hops from a match, each edge one hop; Infinity follows a connected part to its end. */
  depth: number;
  /** Nodes drawn where reached but never passed through. */
  stopAt: readonly NodeType[];
  /** A table is passed through only when entered by this edge (a product's member). */
  tableEnteredBy: EdgeKind;
}

/** Every graph's, the Ontology View's and a product's alike: two hops -- a match, what it touches,
 * and what those touch. */
export const NEIGHBOURHOOD: Neighbourhood = {
  depth: 2,
  stopAt: ["product", "domain"],
  tableEnteredBy: "member",
};

/** The most nodes a graph draws; past it the count is said instead. */
export const NODE_BUDGET = 500;
