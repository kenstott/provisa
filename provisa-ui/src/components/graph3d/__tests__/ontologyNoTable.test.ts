// Copyright (c) 2026 Kenneth Stott
// Canary: 8b7e48bb-5db9-4b9a-85de-a6149e92c4ee
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// J1218 (REQ-2130): a term that grounds no table in the domain read is answered no tables -- the
// server's answer under a domain filter (tests/unit/test_graphql_front_projections.py). It draws
// no table and no IS edge, and is no class: a property stays a property. A class term's table is
// drawn once, named by its domain and table.

import { describe, expect, it } from "vitest";
import { glossaryGraph, type TermRow } from "../ontologyData";

const term = (id: number, name: string, refCount: number, tables: TermRow["tables"]): TermRow => ({
  id,
  name,
  definition: `${name}.`,
  refCount,
  deprecated: false,
  retired: false,
  status: "approved",
  grounding: "grounded",
  tables,
});

// As the server answers the Taxonomy preset's read under the "shelter" domain filter: the class
// grounds its table, the property and the concept ground none there.
const TERMS: TermRow[] = [
  term(1, "animal", 0, [
    { id: 100, tableName: "animals", domainId: "shelter", dataProductTables: [] },
  ]),
  term(2, "breed name", 1, []),
  term(3, "start date", 1, []),
  term(4, "adoption", 0, []),
];
const G = glossaryGraph(TERMS, []);
const node = (id: string) => G.nodes.find((n) => n.id === id);

describe("a term with no table in the domain read", () => {
  it("draws no table node and no IS edge", () => {
    expect(G.nodes.filter((n) => n.type === "table").map((n) => n.id)).toEqual(["t:100"]);
    expect(G.edges.filter((e) => e.label === "IS").map((e) => [e.a, e.b])).toEqual([
      ["term:1", "t:100"],
    ]);
  });

  it("is not drawn as a class", () => {
    expect(node("term:1")?.role).toBe("class");
    expect(node("term:2")?.role).toBe("property");
    expect(node("term:3")?.role).toBe("property");
    expect(node("term:4")?.role).toBe("concept");
  });

  it("leaves no node labelled with a null", () => {
    expect(node("t:100")?.label).toBe("shelter.animals");
    expect(G.nodes.filter((n) => n.label.includes("null"))).toEqual([]);
  });
});
