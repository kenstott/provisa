// Copyright (c) 2026 Kenneth Stott
// Canary: a946b39c-1383-4bd5-84cf-474ac922cc9f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// An OPTIONAL MATCH that matched nothing answers null for its node and relationship (as Neo4j
// does); a frame draws nothing for a null -- no ghost node, no edge to it.

import { describe, it, expect } from "vitest";
import { drawnElements } from "../graph-drawing";

const domain = {
  id: 1,
  label: "Meta:Domains",
  tableLabel: "Domains",
  properties: { id: "pet-store" },
};

describe("a frame's drawing of a row with nulls", () => {
  it("draws the matched node and nothing for the null node or relationship", () => {
    const { nodes, edges } = drawnElements(
      {
        rows: [{ n: domain, r: null, m: null }],
        graph_columns: { n: "node", r: "relationship", m: "node" },
      },
      300,
    );
    expect([...nodes.values()].map((n) => n.label)).toEqual(["Meta:Domains"]);
    expect(edges.size).toBe(0);
  });
});
