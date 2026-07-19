// Copyright (c) 2026 Kenneth Stott
// Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import { buildCypherScript } from "../components/graph/graph-export";
import type { GNode, GEdge } from "../components/graph/graph-model";

const nodeA: GNode = { id: 1, label: "Person", tableLabel: "persons", properties: { name: "Alice" } };
const nodeB: GNode = { id: 2, label: "Person", tableLabel: "persons", properties: { name: "Bob" } };

const edge: GEdge = {
  identity: "e1",
  start: 1,
  end: 2,
  type: "KNOWS",
  properties: {},
  startNode: nodeA,
  endNode: nodeB,
};

describe("buildCypherScript", () => {
  it("includes MERGE statements for nodes", () => {
    const script = buildCypherScript([nodeA, nodeB], []);
    expect(script).toContain("MERGE (n:`persons` {_provisa_id: 1})");
    expect(script).toContain("MERGE (n:`persons` {_provisa_id: 2})");
  });

  it("includes MATCH/MERGE statements for edges", () => {
    const script = buildCypherScript([nodeA, nodeB], [edge]);
    expect(script).toContain("MERGE (a)-[:`KNOWS`]->(b)");
    expect(script).toContain("_provisa_id: 1");
    expect(script).toContain("_provisa_id: 2");
  });

  it("exports zero relationships when edge list is empty", () => {
    const script = buildCypherScript([nodeA], []);
    const lines = script.split("\n").filter((l) => l.includes("MERGE (a)-["));
    expect(lines).toHaveLength(0);
  });

  it("exports all relationships when multiple edges provided", () => {
    const edgeBack: GEdge = {
      identity: "e2",
      start: 2,
      end: 1,
      type: "KNOWS",
      properties: {},
      startNode: nodeB,
      endNode: nodeA,
    };
    const script = buildCypherScript([nodeA, nodeB], [edge, edgeBack]);
    const relLines = script.split("\n").filter((l) => l.includes("MERGE (a)-["));
    expect(relLines).toHaveLength(2);
  });

  it("filters internal provisa node properties from export", () => {
    const nodeWithInternals: GNode = {
      id: 3,
      label: "Person",
      tableLabel: "persons",
      properties: { name: "Carol", degIn: 5, degOut: 3, degTotal: 8, scl1: 0.5, l1Cluster: "A" },
    };
    const script = buildCypherScript([nodeWithInternals], []);
    expect(script).toContain("name:");
    expect(script).not.toContain("degIn");
    expect(script).not.toContain("degOut");
    expect(script).not.toContain("l1Cluster");
  });
});
