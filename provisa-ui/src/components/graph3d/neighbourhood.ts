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
// them; reach() draws that, and the query the view reads and exports (ontologyData) asks the
// server for exactly that. A graph that would draw more than NODE_BUDGET nodes is not drawn: its
// count is said instead, and with no search the domains are drawn with their counts.
// REQ-2174 (amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS): a hop is a step along a term link;
// each term drawn brings its grounding (its columns, their tables, the tables it grounds and the
// products holding them) at no hop, and a focus's own (a product's tables, columns and terms) come
// with it at none.

import type { EdgeKind, NodeType } from "./graphModel";

export interface Neighbourhood {
  /** Steps along the term links from the terms a match starts from (reach); Infinity follows a
   * connected part to its end. */
  depth: number;
  /** Nodes drawn where reached but never passed through. */
  stopAt: readonly NodeType[];
  /** A table is passed through only when entered by this edge (a product's member). */
  tableEnteredBy: EdgeKind;
}

/** Every graph's, the Ontology View's and a product's alike: two steps across the term links from
 * a match. */
export const NEIGHBOURHOOD: Neighbourhood = {
  depth: 2,
  stopAt: ["product", "domain"],
  tableEnteredBy: "member",
};

/** The most nodes a graph draws; past it the count is said instead. */
export const NODE_BUDGET = 500;
