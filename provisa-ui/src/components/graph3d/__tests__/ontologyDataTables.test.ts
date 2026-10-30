// Copyright (c) 2026 Kenneth Stott
// Canary: 74c980d6-1d77-4974-b5e4-c91e73e9f9a5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Ontology View's tables and Data layer (REQ-2130, amended 2026-10-29). Columns: each column a
// term IS, joined to its table; Data: the data product of each table drawn, joined to it. Nothing a
// term does not reach (a product's other datasets and columns), and no relationship between tables.

import { describe, expect, it } from "vitest";
import {
  GLOSSARY_SCOPE,
  visibleGraph,
  type GraphData,
  type GraphFilters,
  type LayerId,
} from "../graphModel";
import { nodeLook } from "../nodeLook";
import { glossaryGraph, type TermEdgeRow, type TermRow } from "../glossaryGraphData";

const ref = (table: number, column: string, domain: string, product: string | null) => ({
  column_key: `${table}:${column}`,
  column_name: column,
  table_id: table,
  table_name: `t${table}`,
  domain_id: domain,
  table: { id: table, product: product ? { id: product, name: product, domain_id: domain } : null },
});

const term = (
  id: number,
  name: string,
  refs: ReturnType<typeof ref>[],
  domain = "sales",
): TermRow => ({
  id,
  name,
  definition: `${name}.`,
  ref_count: 0,
  deprecated: false,
  retired: false,
  status: "approved",
  grounding: "grounded",
  domains: [{ domain_id: domain }],
  refs,
});

const TERMS: TermRow[] = [
  term(1, "Customer", [ref(10, "email", "sales", "c360")]),
  term(2, "Order", [ref(11, "id", "sales", null)]),
  term(3, "Supplier", [ref(20, "name", "procurement", null)], "procurement"),
];
const EDGES: TermEdgeRow[] = [];

const G: GraphData = glossaryGraph(TERMS, EDGES);
const all: GraphFilters = {
  domains: [],
  tags: [],
  sensitivities: [],
  focus: [],
  edgeTypes: [],
  hiddenStatuses: [],
};
const view = (layers: LayerId[], filters: GraphFilters = all) =>
  visibleGraph(G, new Set(layers), filters, new Set(), GLOSSARY_SCOPE);
const ids = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const has = (g: GraphData, kind: string, a: string, b: string) =>
  g.edges.some((e) => e.kind === kind && e.a === a && e.b === b);

describe("the Ontology View's Data layer", () => {
  it("adds the product of each table drawn, joined to it, and nothing else", () => {
    const g = view(["ontology", "columns", "data"]);
    expect(ids(g)).toEqual(expect.arrayContaining(["p:c360", "t:10", "c:10:email"]));
    expect(has(g, "member", "p:c360", "t:10")).toBe(true);
    expect(nodeLook(g.nodes.find((n) => n.id === "c:10:email")!).variant).toBe("in_product");
    expect(g.nodes.filter((n) => n.type === "product").map((n) => n.id)).toEqual(["p:c360"]);
    // Off, no product is drawn.
    expect(view(["ontology", "columns"]).nodes.some((n) => n.type === "product")).toBe(false);
  });

  it("is scoped by the domain filter", () => {
    const g = view(["ontology", "data", "columns"], { ...all, domains: ["procurement"] });
    expect(ids(g)).not.toContain("p:c360");
    expect(ids(g)).not.toContain("t:10");
    expect(ids(g)).toContain("c:20:name");
  });
});

describe("the Ontology View's tables", () => {
  it("draws with Columns each column's table, the column joined to it", () => {
    const g = view(["ontology", "columns"]);
    expect(g.nodes.filter((n) => n.type === "table").map((n) => n.id).sort()).toEqual([
      "t:10",
      "t:11",
      "t:20",
    ]);
    expect(has(g, "has_column", "c:11:id", "t:11")).toBe(true);
    expect(has(g, "has_column", "c:20:name", "t:20")).toBe(true);
    // A table is a cylinder, the fourth shape.
    expect(nodeLook(g.nodes.find((n) => n.id === "t:11")!).shape).toBe("cylinder");
    // Without Columns, none.
    expect(view(["ontology"]).nodes.some((n) => n.type === "table")).toBe(false);
  });

  it("offers no table layer", () => {
    expect(Object.keys(GLOSSARY_SCOPE.layers).sort()).toEqual([
      "columns",
      "data",
      "domains",
      "ontology",
    ]);
  });
});

describe("a search focus with the tables", () => {
  // The search's terms: Order. Its column and the column's table are drawn, and nothing past them.
  const focus = { ...all, focus: ["term:2"] };

  it("is the term, its column and the column's table", () => {
    const g = view(["ontology", "columns"], focus);
    expect(ids(g)).toEqual(["c:11:id", "t:11", "term:2"]);
    expect(has(g, "has_column", "c:11:id", "t:11")).toBe(true);
  });

  it("draws a term's grounded table, not that table's other columns", () => {
    // Account IS customers (10): a class term, grounded in the table.
    const account: TermRow = {
      ...term(4, "Account", []),
      tables: [{ id: 10, domain_id: "sales", table_name: "t10" }],
    };
    const g = visibleGraph(
      glossaryGraph([...TERMS, account], EDGES),
      new Set<LayerId>(["ontology", "columns"]),
      { ...all, focus: ["term:4"] },
      new Set(),
      GLOSSARY_SCOPE,
    );
    expect(ids(g)).toEqual(["t:10", "term:4"]);
    expect(has(g, "term_binding", "term:4", "t:10")).toBe(true);
  });
});
