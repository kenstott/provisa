// Copyright (c) 2026 Kenneth Stott
// Canary: 9a8b6d70-7e10-446f-bcef-c45147483d66
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): each node role has its own colour across the kinds of graph. Two
// types share a colour only when they are one concept under two names (SAME_CONCEPT); the 3D
// ontology graph draws a role in the colour the 2D graphs draw it.

import { describe, expect, it } from "vitest";
import { NODE_TYPES, SAME_CONCEPT, type NodeType } from "../tokens";
import { lookColour, nodeLook } from "../../graph3d/nodeLook";
import type { GraphNode, NodeType as Graph3dType } from "../../graph3d/graphModel";
import { ROLE_TYPE } from "../../lineage/lineageSpec";

const TYPES = Object.keys(NODE_TYPES) as NodeType[];
const concept = (t: NodeType): readonly NodeType[] =>
  SAME_CONCEPT.find((g) => g.includes(t)) ?? [t];

describe("each node role has its own colour", () => {
  it("gives no two roles one colour, save two names of one concept", () => {
    const clashes: string[] = [];
    for (const [i, a] of TYPES.entries())
      for (const b of TYPES.slice(i + 1))
        if (
          NODE_TYPES[a].colour.toLowerCase() === NODE_TYPES[b].colour.toLowerCase() &&
          !concept(a).includes(b)
        )
          clashes.push(`${a}=${b} ${NODE_TYPES[a].colour}`);
    expect(clashes).toEqual([]);
  });

  it("names of one concept do share their colour", () => {
    for (const group of SAME_CONCEPT)
      expect(new Set(group.map((t) => NODE_TYPES[t].colour)).size).toBe(1);
  });

  it("gives lineage's roles colours and icons of their own, none an ontology role's", () => {
    const lineage = new Set<NodeType>(Object.values(ROLE_TYPE));
    // The lineage roles drawn only by lineage (a dataset is a table, a command a command).
    const own: NodeType[] = ["intermediate", "output", "native", "job", "external"];
    for (const t of own) expect(lineage.has(t)).toBe(true);
    const ontology: NodeType[] = [
      "product",
      "table",
      "column",
      "term",
      "concept",
      "class",
      "property",
      "relation",
    ];
    for (const t of own)
      for (const o of ontology) {
        if (concept(t).includes(o)) continue;
        expect(NODE_TYPES[t].colour, `${t} vs ${o}`).not.toBe(NODE_TYPES[o].colour);
      }
    // The two the audit found sharing an icon draw their own.
    expect(NODE_TYPES.intermediate.icon).not.toBe(NODE_TYPES.relation.icon);
    expect(NODE_TYPES.external.icon).not.toBe(NODE_TYPES.column.icon);
    // A native field is not drawn in a term's colour.
    expect(NODE_TYPES.native.colour).not.toBe(NODE_TYPES.term.colour);
  });
});

describe("the 3D ontology graph draws a role in its 2D colour", () => {
  const n = (over: Partial<GraphNode> & Pick<GraphNode, "type">): GraphNode => ({
    id: "x",
    label: "x",
    ...over,
  });
  const kinds: Graph3dType[] = [
    "product",
    "domain",
    "table",
    "relation",
    "command",
    "checker",
    "profile",
    "tag",
  ];

  it("draws each kind in the 2D colour of its type", () => {
    for (const type of kinds)
      expect(lookColour(nodeLook(n({ type })).colour), type).toBe(NODE_TYPES[type].colour);
  });

  it("draws a term in the term's colour everywhere, a role in its role's", () => {
    expect(lookColour(nodeLook(n({ type: "term" })).colour)).toBe(NODE_TYPES.term.colour);
    for (const role of ["concept", "class", "property"] as const)
      expect(lookColour(nodeLook(n({ type: "term", role })).colour)).toBe(NODE_TYPES[role].colour);
  });

  it("draws a column in the column's colour, or a product's when a data product holds it", () => {
    expect(lookColour(nodeLook(n({ type: "column" })).colour)).toBe(NODE_TYPES.column.colour);
    expect(lookColour(nodeLook(n({ type: "column", inProduct: true })).colour)).toBe(
      NODE_TYPES.product.colour,
    );
  });

  it("gives no two 3D kinds one colour", () => {
    const colours = kinds.map((type) => lookColour(nodeLook(n({ type })).colour));
    colours.push(
      lookColour(nodeLook(n({ type: "term" })).colour),
      lookColour(nodeLook(n({ type: "column" })).colour),
    );
    expect(new Set(colours).size).toBe(colours.length);
  });
});
