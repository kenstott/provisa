// Copyright (c) 2026 Kenneth Stott
// Canary: 6f3a9d24-1c8e-4b57-a0d3-e5b72c9f1846
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28; 2026-11-07, WHAT THE GRAPH DRAWS): the 2D view's lane diagram --
// products, their tables, those tables' columns and the terms tied to them, left to right; rank
// edges set the term lane's levels and every other term link is an arc over it; a table's columns
// tied to no term one node until expanded; a product's hops counted along the lanes; Mermaid text.

import { describe, expect, it } from "vitest";
import mermaid from "mermaid";
import { NO_FILTERS, visibleGraph, type GraphData, type LayerId } from "../graphModel";
import { LANES, laneDiagram, laneMermaid, moreId, type Lane } from "../laneDiagram";
import { DATA, ENGINE_PRESETS as PRESETS, ENGINE_SCOPE } from "./fixture";

const shownBy = (data: GraphData, preset: "all" | "ontology", expanded = new Set<string>()) =>
  visibleGraph(data, new Set<LayerId>(PRESETS[preset].layers), NO_FILTERS, expanded, ENGINE_SCOPE);

const lanesOf = (d: ReturnType<typeof laneDiagram>) => {
  const out: Record<Lane, string[]> = { term: [], column: [], table: [], product: [] };
  for (const n of d.graph.nodes) out[d.lane.get(n.id) as Lane].push(n.id);
  for (const l of Object.keys(out) as Lane[]) out[l].sort();
  return out;
};

const linked = (d: ReturnType<typeof laneDiagram>) =>
  d.links.map((k) => `${k.from}>${k.to}`).sort();

describe("the lane diagram's lanes", () => {
  it("puts terms, their columns, their tables and their products in lanes on the All preset", () => {
    const d = laneDiagram(shownBy(DATA, "all"), DATA, new Set());
    expect(lanesOf(d)).toEqual({
      term: ["term:customer", "term:party"],
      column: ["c:customers.email"],
      table: ["t:customers", "t:orders"],
      product: ["p:c360"],
    });
  });

  it("draws nothing outside the four lanes: no domain, command, checker, profile, tag or relation", () => {
    const d = laneDiagram(shownBy(DATA, "all"), DATA, new Set());
    const types = new Set(d.graph.nodes.map((n) => n.type));
    expect([...types].sort()).toEqual(["column", "product", "table", "term"]);
  });

  it("reads the tables from the columns on the Ontology preset, with every product they belong to", () => {
    const shown = shownBy(DATA, "ontology");
    // The preset draws no tables of its own.
    expect(shown.nodes.some((n) => n.type === "table")).toBe(false);
    const d = laneDiagram(shown, DATA, new Set());
    expect(lanesOf(d).table).toEqual(["t:customers"]);
    // billing owns none of the tables drawn.
    expect(lanesOf(d).product).toEqual(["p:c360"]);
    expect(linked(d)).toContain("t:customers>p:c360");
  });

  it("lays out a product's snippet -- its hops counted along the lanes -- in the same lanes", () => {
    const d = laneDiagram(shownBy(DATA, "all"), DATA, new Set(), { focus: "p:c360", depth: 3 });
    expect(lanesOf(d).product).toEqual(["p:c360"]);
    expect(lanesOf(d).column).toEqual(["c:customers.email"]);
    expect(lanesOf(d).table).toEqual(["t:customers", "t:orders"]);
    expect(lanesOf(d).term).toEqual(["term:customer"]);
  });

  it("are drawn products to terms, left to right", () => {
    expect(LANES).toEqual(["product", "table", "column", "term"]);
  });
});

describe("the lane diagram's edges", () => {
  const withEdges: GraphData = {
    nodes: [
      ...DATA.nodes,
      { id: "term:client", type: "term", label: "Client" },
      { id: "term:buyer", type: "term", label: "Buyer" },
    ],
    // One edge per pair of terms: the graph keeps one edge of a kind between two nodes.
    edges: [
      ...DATA.edges,
      { id: "20", a: "term:buyer", b: "term:customer", kind: "term_edge", label: "SYNONYM_OF" },
      { id: "21", a: "term:buyer", b: "term:party", kind: "term_edge", label: "RELATED_TO" },
      { id: "22", a: "term:client", b: "term:customer", kind: "term_edge", label: "DERIVED_FROM" },
      { id: "23", a: "term:client", b: "term:party", kind: "term_edge", label: "PART_OF" },
    ],
  };

  it("lays rank edges out in the term lane, the broader term above, every other link an arc", () => {
    const d = laneDiagram(shownBy(withEdges, "all"), withEdges, new Set());
    const termLinks = d.links.filter(
      (k) => d.lane.get(k.from) === "term" && d.lane.get(k.to) === "term",
    );
    expect(termLinks.map((k) => `${k.from}>${k.to}:${k.label}`).sort()).toEqual([
      "term:party>term:client:PART_OF",
      "term:party>term:customer:KIND_OF",
    ]);
    // Lateral and flow links are drawn over the lane, each named by its type; they place nothing.
    expect(d.arcs.map((k) => `${k.from}>${k.to}:${k.label}`).sort()).toEqual([
      "term:buyer>term:customer:SYNONYM_OF",
      "term:buyer>term:party:RELATED_TO",
      "term:client>term:customer:DERIVED_FROM",
    ]);
    expect(
      d.graph.edges
        .filter((e) => e.kind === "term_edge")
        .map((e) => e.label)
        .sort(),
    ).toEqual(["DERIVED_FROM", "KIND_OF", "PART_OF", "RELATED_TO", "SYNONYM_OF"]);
  });

  it("draws the term links the Term relationships filter keeps, and no other", () => {
    const shown = visibleGraph(
      withEdges,
      new Set<LayerId>(PRESETS.all.layers),
      { ...NO_FILTERS, edgeTypes: ["KIND_OF", "SYNONYM_OF"] },
      new Set(),
      ENGINE_SCOPE,
    );
    const d = laneDiagram(shown, withEdges, new Set());
    expect(
      d.graph.edges
        .filter((e) => e.kind === "term_edge")
        .map((e) => e.label)
        .sort(),
    ).toEqual(["KIND_OF", "SYNONYM_OF"]);
  });

  it("ends a chain at its columns, links columns to their table and tables to their products", () => {
    const d = laneDiagram(shownBy(DATA, "all"), DATA, new Set());
    expect(linked(d)).toEqual([
      "c:customers.email>t:customers",
      `${moreId("t:orders")}>t:orders`,
      "t:customers>p:c360",
      "t:orders>p:c360",
      "term:customer>c:customers.email",
      "term:party>term:customer",
    ]);
  });

  it("draws no lineage or relationship edge", () => {
    const d = laneDiagram(shownBy(withEdges, "all"), withEdges, new Set());
    const kinds = new Set(d.graph.edges.map((e) => e.kind));
    expect([...kinds].sort()).toEqual(["has_column", "member", "term_binding", "term_edge"]);
  });
});

describe("a table's unbound columns", () => {
  it("are one +N columns node until the table is expanded", () => {
    const d = laneDiagram(shownBy(DATA, "all"), DATA, new Set());
    expect(
      d.more.map(({ columns, ...m }) => ({ ...m, columns: columns.map((c) => c.id) })),
    ).toEqual([
      { id: moreId("t:orders"), table: "t:orders", count: 1, columns: ["c:orders.total"] },
    ]);
    expect(d.graph.nodes.some((n) => n.id === "c:orders.total")).toBe(false);
  });

  it("expand in place, and fold back into the node", () => {
    const open = new Set(["t:orders"]);
    const d = laneDiagram(shownBy(DATA, "all", open), DATA, open);
    expect(d.more).toEqual([]);
    expect(d.lane.get("c:orders.total")).toBe("column");
    expect(linked(d)).toContain("c:orders.total>t:orders");
    const folded = laneDiagram(shownBy(DATA, "all"), DATA, new Set());
    expect(folded.more.map((m) => m.table)).toEqual(["t:orders"]);
  });

  it("count every unbound column of the table", () => {
    const data: GraphData = {
      nodes: [
        ...DATA.nodes,
        { id: "c:orders.placed", type: "column", label: "placed", parent: "t:orders" },
        { id: "c:orders.status", type: "column", label: "status", parent: "t:orders" },
      ],
      edges: DATA.edges,
    };
    const d = laneDiagram(shownBy(data, "all"), data, new Set());
    expect(
      d.more.map(({ columns, ...m }) => ({ ...m, columns: columns.map((c) => c.id) })),
    ).toEqual([
      {
        id: moreId("t:orders"),
        table: "t:orders",
        count: 3,
        columns: ["c:orders.total", "c:orders.placed", "c:orders.status"],
      },
    ]);
  });
});

describe("the optional product lane", () => {
  const loose: GraphData = {
    nodes: [
      { id: "term:sku", type: "term", label: "SKU" },
      { id: "t:items", type: "table", label: "sales.items" },
      { id: "c:items.sku", type: "column", label: "sku", parent: "t:items" },
    ],
    edges: [
      { id: "1", a: "t:items", b: "c:items.sku", kind: "has_column" },
      { id: "2", a: "term:sku", b: "c:items.sku", kind: "term_binding" },
    ],
  };

  it("ends a table in no product at the tables lane", () => {
    const d = laneDiagram(shownBy(loose, "all"), loose, new Set());
    expect(lanesOf(d)).toEqual({
      term: ["term:sku"],
      column: ["c:items.sku"],
      table: ["t:items"],
      product: [],
    });
    expect(linked(d)).toEqual(["c:items.sku>t:items", "term:sku>c:items.sku"]);
  });

  it("draws a graph of products alone in the product lane", () => {
    const shown = visibleGraph(
      DATA,
      new Set<LayerId>(PRESETS.products.layers),
      NO_FILTERS,
      new Set(),
      ENGINE_SCOPE,
    );
    const d = laneDiagram(shown, DATA, new Set());
    expect(lanesOf(d).product).toEqual(["p:billing", "p:c360"]);
  });
});

describe("the diagram as Mermaid", () => {
  const title = (l: Lane) =>
    ({ term: "Terms", column: "Columns", table: "Tables", product: "Products" })[l];
  const more = (n: number) => `+${n} columns`;

  it("is a flowchart LR with one subgraph per lane drawn", () => {
    const text = laneMermaid(laneDiagram(shownBy(DATA, "all"), DATA, new Set()), title, more);
    const lines = text.split("\n");
    expect(lines[0]).toBe("flowchart LR");
    expect(lines.filter((l) => l.trim().startsWith("subgraph"))).toEqual([
      '  subgraph lane_product["Products"]',
      '  subgraph lane_table["Tables"]',
      '  subgraph lane_column["Columns"]',
      '  subgraph lane_term["Terms"]',
    ]);
    expect(text).toContain('["+1 columns"]');
    expect(text).toContain('["customer_360"]');
  });

  it("leaves out a lane with nothing in it", () => {
    const shown = visibleGraph(
      DATA,
      new Set<LayerId>(PRESETS.products.layers),
      NO_FILTERS,
      new Set(),
      ENGINE_SCOPE,
    );
    const text = laneMermaid(laneDiagram(shown, DATA, new Set()), title, more);
    expect(text.match(/subgraph/g)).toHaveLength(1);
  });

  it("is valid Mermaid, a quote in a name included", async () => {
    const data: GraphData = {
      nodes: DATA.nodes.map((n) => (n.id === "term:party" ? { ...n, label: 'Party "legal"' } : n)),
      edges: DATA.edges,
    };
    const text = laneMermaid(laneDiagram(shownBy(data, "all"), data, new Set()), title, more);
    await expect(mermaid.parse(text)).resolves.toBeTruthy();
  });
});

describe("two term edges of different types between the same two terms", () => {
  // Customer is a kind of Party and related to it: the rank edge survives the lateral one.
  const both: GraphData = {
    nodes: DATA.nodes,
    edges: [
      ...DATA.edges.filter((e) => e.id !== "14"),
      { id: "30", a: "term:customer", b: "term:party", kind: "term_edge", label: "RELATED_TO" },
      { id: "31", a: "term:customer", b: "term:party", kind: "term_edge", label: "KIND_OF" },
    ],
  };

  it("stay two edges in the graph drawn, each with its own type", () => {
    const shown = shownBy(both, "all");
    expect(
      shown.edges
        .filter((e) => e.kind === "term_edge")
        .map((e) => e.label)
        .sort(),
    ).toEqual(["KIND_OF", "RELATED_TO"]);
  });

  it("keep the rank edge laid out and the lateral one an arc, whichever edge comes first", () => {
    for (const edges of [both.edges, [...both.edges].reverse()]) {
      const g = { nodes: both.nodes, edges };
      const d = laneDiagram(shownBy(g, "all"), g, new Set());
      expect(
        d.graph.edges
          .filter((e) => e.kind === "term_edge")
          .map((e) => e.label)
          .sort(),
      ).toEqual(["KIND_OF", "RELATED_TO"]);
      expect(linked(d)).toContain("term:party>term:customer");
      expect(d.arcs.map((k) => k.label)).toEqual(["RELATED_TO"]);
    }
  });
});

describe("a product's hops in 2D, counted along the lanes", () => {
  // Customer is tied to customers.email; Party is a step from it, Agent a step from Party.
  const chain: GraphData = {
    nodes: [...DATA.nodes, { id: "term:agent", type: "term", label: "Agent" }],
    edges: [
      ...DATA.edges,
      { id: "40", a: "term:party", b: "term:agent", kind: "term_edge", label: "PART_OF" },
      { id: "41", a: "term:party", b: "term:customer", kind: "term_edge", label: "RELATED_TO" },
    ],
  };
  const at = (depth: number) =>
    laneDiagram(shownBy(chain, "all"), chain, new Set(), { focus: "p:c360", depth });

  it("draws the product's tables at 1 hop, and no column", () => {
    const d = at(1);
    expect(lanesOf(d)).toEqual({
      product: ["p:c360"],
      table: ["t:customers", "t:orders"],
      column: [],
      term: [],
    });
    expect(d.more).toEqual([]);
  });

  it("draws their columns at 2, each table's folded into one node with no term in view", () => {
    const d = at(2);
    expect(lanesOf(d).term).toEqual([]);
    expect(lanesOf(d).column).toEqual([]);
    expect(d.more.map((m) => [m.table, m.count])).toEqual([
      ["t:customers", 1],
      ["t:orders", 1],
    ]);
  });

  it("draws the terms tied to the columns at 3, the tied columns as themselves", () => {
    const d = at(3);
    expect(lanesOf(d).term).toEqual(["term:customer"]);
    expect(lanesOf(d).column).toEqual(["c:customers.email"]);
    expect(d.more.map((m) => m.table)).toEqual(["t:orders"]);
  });

  it("steps once across the term links for each hop past 3", () => {
    expect(lanesOf(at(4)).term).toEqual(["term:customer", "term:party"]);
    expect(lanesOf(at(5)).term).toEqual(["term:agent", "term:customer", "term:party"]);
  });

  it("draws the focused product alone in the product lane, and no other product's tables", () => {
    const d = at(5);
    expect(lanesOf(d).product).toEqual(["p:c360"]);
    expect(d.graph.nodes.some((n) => n.type === "relation" || n.type === "command")).toBe(false);
  });
});

describe("the product lane, read from the graph", () => {
  it("names a table's product though no layer drawn draws products", () => {
    const shown = visibleGraph(
      DATA,
      new Set<LayerId>(["ontology"]),
      NO_FILTERS,
      new Set(),
      ENGINE_SCOPE,
    );
    expect(shown.nodes.some((n) => n.type === "product")).toBe(false);
    const d = laneDiagram(shown, DATA, new Set());
    expect(lanesOf(d).product).toEqual(["p:c360"]);
    expect(linked(d)).toContain("t:customers>p:c360");
  });
});
