// Copyright (c) 2026 Kenneth Stott
// Canary: 74c980d6-1d77-4974-b5e4-c91e73e9f9a5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Ontology View's Data layer and Tables toggle. Data: each data product's datasets with their
// columns -- every column, in the in-a-data-product colour -- joined to the dataset, and the IS edge
// from each term to a column it grounds. Tables (with Columns): one table per table owning a column
// drawn, each column joined to it, and the relationships declared between the tables drawn, under
// their names and in the edge-type filter. The domain filter and Orphans apply to both.

import { describe, expect, it } from "vitest";
import {
  GLOSSARY_SCOPE,
  visibleGraph,
  type GraphData,
  type GraphFilters,
  type LayerId,
} from "../graphModel";
import { nodeLook } from "../nodeLook";
import {
  glossaryGraph,
  type ProductRow,
  type TermEdgeRow,
  type TermRow,
} from "../glossaryGraphData";

const rel = (id: string, target: number, cypherType: string) => ({
  id,
  cypher_type: cypherType,
  target_table_id: target,
});

const ref = (table: number, column: string, domain: string, product: string | null) => ({
  column_key: `${table}:${column}`,
  column_name: column,
  table_id: table,
  table_name: `t${table}`,
  domain_id: domain,
  table: {
    id: table,
    product: product ? { id: product, name: product, domain_id: domain } : null,
    // orders (11) declares PLACED_BY to customers (10).
    source_relationships: table === 11 ? [rel("orders_customer", 10, "PLACED_BY")] : [],
  },
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
  live: true,
  domains: [{ domain_id: domain }],
  refs,
});

const TERMS: TermRow[] = [
  term(1, "Customer", [ref(10, "email", "sales", "c360")]),
  term(2, "Order", [ref(11, "id", "sales", null)]),
  term(3, "Supplier", [ref(20, "name", "procurement", null)], "procurement"),
];
const EDGES: TermEdgeRow[] = [];
const PRODUCTS: ProductRow[] = [
  {
    id: "c360",
    name: "Customer 360",
    domain_id: "sales",
    member_tables: [
      {
        id: 10,
        domain_id: "sales",
        table_name: "t10",
        table_columns: [
          { column_name: "email", column_key: "10:email", glossary_ref: [{ term_id: 1 }] },
          { column_name: "tier", column_key: "10:tier", glossary_ref: null },
        ],
        source_relationships: [],
      },
    ],
  },
];
// A table no column drawn belongs to: never drawn, though a relationship reaches it.
const UNOWNED: ProductRow = {
  id: "ops",
  name: "Ops",
  domain_id: "sales",
  // returns (13): a dataset with no column drawn, declaring a relationship to orders.
  member_tables: [
    {
      id: 13,
      domain_id: "sales",
      table_name: "t13",
      table_columns: [],
      source_relationships: [rel("returns_order", 11, "BELONGS_TO_ORDER")],
    },
  ],
};

const G: GraphData = glossaryGraph(TERMS, EDGES, [...PRODUCTS, UNOWNED]);
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
  it("shows a product's datasets with every one of their columns, in the in-a-product colour", () => {
    const g = view(["ontology", "data"]);
    expect(ids(g)).toEqual(expect.arrayContaining(["p:c360", "t:10", "c:10:email", "c:10:tier"]));
    expect(has(g, "member", "p:c360", "t:10")).toBe(true);
    expect(has(g, "has_column", "t:10", "c:10:email")).toBe(true);
    expect(has(g, "has_column", "t:10", "c:10:tier")).toBe(true);
    for (const id of ["c:10:email", "c:10:tier"])
      expect(nodeLook(g.nodes.find((n) => n.id === id)!).variant).toBe("in_product");
    // The IS edge from the term to the column it grounds; none to the column no term is.
    expect(has(g, "term_binding", "term:1", "c:10:email")).toBe(true);
    expect(g.edges.some((e) => e.b === "c:10:tier" && e.kind === "term_binding")).toBe(false);
    // Columns outside the products are Columns', not Data's.
    expect(ids(g)).not.toContain("c:11:id");
    expect(ids(g)).not.toContain("t:11");
  });

  it("is scoped by the domain filter", () => {
    const g = view(["ontology", "data", "columns"], { ...all, domains: ["procurement"] });
    expect(ids(g)).not.toContain("c:10:tier");
    expect(ids(g)).not.toContain("t:10");
    expect(ids(g)).toContain("c:20:name");
  });
});

describe("the Ontology View's Tables", () => {
  it("adds one table per table owning a column drawn, each column joined to it", () => {
    const off = view(["ontology", "columns"]);
    expect(off.nodes.filter((n) => n.type === "table")).toEqual([]);
    const on = view(["ontology", "columns", "ownerTables"]);
    expect(ids(on)).toEqual(expect.arrayContaining(["t:10", "t:11", "t:20"]));
    expect(has(on, "has_column", "t:11", "c:11:id")).toBe(true);
    expect(has(on, "has_column", "t:20", "c:20:name")).toBe(true);
    // Only the owning tables: returns (13) owns no column drawn, so is not drawn, and nor is the
    // relationship it declares to orders, which is.
    expect(G.nodes.some((n) => n.id === "t:13")).toBe(true);
    expect(ids(on)).not.toContain("t:13");
    expect(has(on, "relationship", "t:13", "t:11")).toBe(false);
    // A table is a cylinder, the fourth shape.
    expect(nodeLook(on.nodes.find((n) => n.id === "t:11")!).shape).toBe("cylinder");
  });

  it("joins the tables drawn by the relationships declared between them, filtered by name", () => {
    const g = view(["ontology", "columns", "ownerTables"]);
    expect(g.edges.find((e) => e.kind === "relationship")).toMatchObject({
      a: "t:11",
      b: "t:10",
      label: "PLACED_BY",
    });
    const filtered = view(["ontology", "columns", "ownerTables"], {
      ...all,
      edgeTypes: ["KIND_OF"],
    });
    expect(filtered.edges.some((e) => e.kind === "relationship")).toBe(false);
    // Off with Tables.
    expect(view(["ontology", "columns"]).edges.some((e) => e.kind === "relationship")).toBe(false);
  });
});

describe("a search focus with Tables", () => {
  // The search's terms: Order. Its column's table is drawn, and goes no further: not orders'
  // relationship to customers, customers' columns, nor the terms those are.
  const focus = { ...all, focus: ["term:2"] };

  it("is the focus without Tables plus only the tables owning its columns", () => {
    const off = view(["ontology", "columns"], focus);
    const on = view(["ontology", "columns", "ownerTables"], focus);
    expect(ids(off)).toEqual(["c:11:id", "term:2"]);
    expect(ids(on)).toEqual([...ids(off), "t:11"].sort());
    expect(has(on, "has_column", "t:11", "c:11:id")).toBe(true);
  });

  it("draws a term's grounded table, not that table's other columns", () => {
    // Account IS customers (10): a class term, grounded in the table.
    const account: TermRow = {
      ...term(4, "Account", []),
      tables: [{ id: 10, domain_id: "sales", table_name: "t10" }],
    };
    const g = visibleGraph(
      glossaryGraph([...TERMS, account], EDGES, PRODUCTS),
      new Set<LayerId>(["ontology", "columns", "ownerTables"]),
      { ...all, focus: ["term:4"] },
      new Set(),
      GLOSSARY_SCOPE,
    );
    expect(ids(g)).toEqual(["t:10", "term:4"]);
    expect(has(g, "term_binding", "term:4", "t:10")).toBe(true);
  });
});
