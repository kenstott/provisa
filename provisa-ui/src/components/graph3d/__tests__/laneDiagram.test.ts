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
// products, their tables, those tables' columns and the terms tied to them, left to right; a rank
// edge joins a term to its broader term inside the term lane and every other term link is an arc
// over it; the term lane placed by the lanes preset, its terms level with their columns (REQ-2239,
// amended 2026-11-11); a table's columns tied to no term one node until expanded; a product's hops
// counted as everywhere, a step between terms (amended 2026-11-12), before the lanes are laid out;
// Mermaid text.

import { describe, expect, it } from "vitest";
import mermaid from "mermaid";
import { NO_FILTERS, visibleGraph, type GraphData, type LayerId } from "../graphModel";
import {
  LANES,
  connectedTo,
  laneDiagram,
  laneMermaid,
  moreId,
  type At,
  type Lane,
} from "../laneDiagram";
import { tipPlacement } from "../../provisa-graph/tipPlacement";
import { NODE_GAP } from "../../provisa-graph/lanes";
import { SIZE } from "../../provisa-graph/tokens";
import { placedDiagram } from "./crossingFixtures";
import { pickLights } from "../graphScene";
import {
  ASSOCIATION_CHAIN,
  BREED_NAME,
  KIN,
  KIN2,
  TEST,
  TEST2,
  TEST3,
} from "./associationChain";
import { DATA, ENGINE_PRESETS as PRESETS, ENGINE_SCOPE } from "./fixture";

const shownBy = (data: GraphData, preset: "all" | "ontology", expanded = new Set<string>()) =>
  visibleGraph(data, new Set<LayerId>(PRESETS[preset].layers), NO_FILTERS, expanded, ENGINE_SCOPE);

/** What a product's focus draws at ``depth`` hops (graphModel's reach), as the 2D view lays it out. */
const focusedBy = (data: GraphData, focus: string, depth: number) =>
  visibleGraph(
    data,
    new Set<LayerId>(PRESETS.all.layers),
    { ...NO_FILTERS, focus: [focus] },
    new Set(),
    { ...ENGINE_SCOPE, neighbourhood: { ...ENGINE_SCOPE.neighbourhood, depth } },
  );

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

  it("lays out a product's snippet -- what its hops reach -- in the same lanes", () => {
    const d = laneDiagram(focusedBy(DATA, "p:c360", 1), DATA, new Set());
    expect(lanesOf(d).product).toEqual(["p:c360"]);
    expect(lanesOf(d).column).toEqual(["c:customers.email"]);
    expect(lanesOf(d).table).toEqual(["t:customers", "t:orders"]);
    expect(lanesOf(d).term).toEqual(["term:customer", "term:party"]);
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

describe("a product's hops in 2D, counted between terms", () => {
  // Customer is tied to customers.email; Party is a step from it, Agent a step from Party.
  const chain: GraphData = {
    nodes: [...DATA.nodes, { id: "term:agent", type: "term", label: "Agent" }],
    edges: [
      ...DATA.edges,
      { id: "40", a: "term:party", b: "term:agent", kind: "term_edge", label: "PART_OF" },
      { id: "41", a: "term:party", b: "term:customer", kind: "term_edge", label: "RELATED_TO" },
    ],
  };
  const at = (depth: number) => laneDiagram(focusedBy(chain, "p:c360", depth), chain, new Set());

  it("draws the product's tables, their tied columns and terms at 1 hop, and a term a step on", () => {
    const d = at(1);
    expect(lanesOf(d)).toEqual({
      product: ["p:c360"],
      table: ["t:customers", "t:orders"],
      column: ["c:customers.email"],
      term: ["term:customer", "term:party"],
    });
    // orders' total is tied to no term drawn: one node.
    expect(d.more.map((m) => [m.table, m.count])).toEqual([["t:orders", 1]]);
  });

  it("steps once across the term links for each hop past 1, and to the end at All", () => {
    expect(lanesOf(at(2)).term).toEqual(["term:agent", "term:customer", "term:party"]);
    expect(lanesOf(at(Infinity)).term).toEqual(["term:agent", "term:customer", "term:party"]);
  });

  it("draws the focused product alone in the product lane, and no other product's tables", () => {
    const d = at(Infinity);
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

describe("the term lane's places (REQ-2239, amended 2026-11-11, THE SCORE IS THE ANGLE OF THE LINES)", () => {
  // Crop year binds a column of both tables; loss ratio and premium one each. Loss ratio and premium
  // sit level with their columns; crop year sits level with one of its two (either leaves its lines
  // as flat in total), on the row of fewer crossings, the higher on a tie.
  const data: GraphData = {
    nodes: [
      { id: "term:loss", type: "term", label: "loss ratio" },
      { id: "term:premium", type: "term", label: "premium" },
      { id: "term:year", type: "term", label: "crop year" },
      { id: "t:claims", type: "table", label: "claims" },
      { id: "t:policies", type: "table", label: "policies" },
      { id: "c:claims.year", type: "column", label: "crop_year", parent: "t:claims" },
      { id: "c:claims.loss", type: "column", label: "loss_ratio", parent: "t:claims" },
      { id: "c:policies.year", type: "column", label: "crop_year", parent: "t:policies" },
      { id: "c:policies.premium", type: "column", label: "premium", parent: "t:policies" },
    ],
    edges: [
      { id: "b1", a: "term:year", b: "c:claims.year", kind: "term_binding" },
      { id: "b2", a: "term:year", b: "c:policies.year", kind: "term_binding" },
      { id: "b3", a: "term:premium", b: "c:policies.premium", kind: "term_binding" },
      { id: "b4", a: "term:loss", b: "c:claims.loss", kind: "term_binding" },
    ],
  };
  const d = laneDiagram(data, data, new Set());

  it("draws the crop-year graph with loss ratio and premium level, crop year level with its first column", () => {
    const at = placedDiagram(d);
    expect(at("term:loss").y).toBe(at("c:claims.loss").y);
    expect(at("term:premium").y).toBe(at("c:policies.premium").y);
    // Either column of crop year crosses one line (the columns' order is the tables'): the higher.
    expect(at("term:year").y).toBe(at("c:claims.year").y);
  });

  it("keeps a parent that binds no column on the row above its child", () => {
    const tree = laneDiagram(FOCUS_DATA, FOCUS_DATA, new Set());
    const at = placedDiagram(tree);
    expect(at("term:customer").y).toBe(at("c:customers.email").y);
    expect(at("term:customer").y - at("term:party").y).toBe(SIZE.nodeHeight + NODE_GAP);
    expect(at("term:customer").x).toBe(at("term:party").x);
  });

  it("gives every term a row of its own, a node gap apart at the height ProvisaGraph draws a term", () => {
    const tree = laneDiagram(FOCUS_DATA, FOCUS_DATA, new Set());
    const at = placedDiagram(tree);
    const ys = ["term:party", "term:customer", "term:order", "term:invoice"]
      .map((id) => at(id).y)
      .sort((a, b) => a - b);
    for (let i = 1; i < ys.length; i++)
      expect(ys[i] - ys[i - 1]).toBeGreaterThanOrEqual(SIZE.nodeHeight + NODE_GAP);
  });

  it("puts terms with no line out of the lane below the rest, each on its own row", () => {
    const lone = Array.from({ length: 12 }, (_, i) => `term:lone${i}`);
    const only: GraphData = {
      nodes: lone.map((id) => ({ id, type: "term" as const, label: id })),
      edges: [],
    };
    const at = placedDiagram(laneDiagram(only, only, new Set()));
    const ys = lone.map((id) => at(id).y);
    expect(new Set(ys).size).toBe(lone.length);
    for (let i = 1; i < ys.length; i++) expect(ys[i] - ys[i - 1]).toBe(SIZE.nodeHeight + NODE_GAP);
  });
});

const FOCUS_DATA: GraphData = {
  nodes: [
    { id: "term:party", type: "term", label: "Party" },
    { id: "term:customer", type: "term", label: "Customer" },
    { id: "term:order", type: "term", label: "Order" },
    { id: "term:invoice", type: "term", label: "Invoice" },
    { id: "t:customers", type: "table", label: "customers" },
    { id: "t:orders", type: "table", label: "orders" },
    { id: "c:customers.email", type: "column", label: "email", parent: "t:customers" },
    { id: "c:orders.id", type: "column", label: "id", parent: "t:orders" },
    { id: "p:c360", type: "product", label: "c360" },
  ],
  edges: [
    { id: "k", a: "term:customer", b: "term:party", kind: "term_edge", label: "KIND_OF" },
    { id: "r", a: "term:customer", b: "term:invoice", kind: "term_edge", label: "RELATED_TO" },
    { id: "b1", a: "term:customer", b: "c:customers.email", kind: "term_binding" },
    { id: "b2", a: "term:order", b: "c:orders.id", kind: "term_binding" },
    { id: "m1", a: "p:c360", b: "t:customers", kind: "member" },
    { id: "m2", a: "p:c360", b: "t:orders", kind: "member" },
  ],
};

describe("what a focused node brings forward", () => {
  const d = laneDiagram(FOCUS_DATA, FOCUS_DATA, new Set());

  it("brings a term's broader term, its columns, their tables and products, and no other path", () => {
    const { nodes } = connectedTo(d, "term:customer");
    expect([...nodes].sort()).toEqual(
      [
        "term:customer",
        "term:party",
        "term:invoice",
        "c:customers.email",
        "t:customers",
        "p:c360",
      ].sort(),
    );
  });

  it("does not double back: the product a term reaches does not bring the product's other tables", () => {
    const { nodes } = connectedTo(d, "term:order");
    expect(nodes.has("t:orders")).toBe(true);
    expect(nodes.has("p:c360")).toBe(true);
    expect(nodes.has("t:customers")).toBe(false);
  });

  it("brings a column's table and product one way and its terms and their chain the other", () => {
    const { nodes } = connectedTo(d, "c:customers.email");
    expect([...nodes].sort()).toEqual(
      ["c:customers.email", "t:customers", "p:c360", "term:customer", "term:party"].sort(),
    );
  });

  it("lights the lines it followed, each one a line drawn", () => {
    const { links } = connectedTo(d, "term:customer");
    const drawnIds = new Set([...d.links, ...d.arcs].map((k) => k.id));
    expect(links.size).toBeGreaterThan(0);
    for (const id of links) expect(drawnIds.has(id)).toBe(true);
  });
});

// REQ-2174 (amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS) and REQ-2239: a term selected in
// 2D lights everything it reaches by any drawn link within the hops shown -- its ranking paths and
// its associations, each term so reached with its columns, their tables and the products holding
// them -- as the 3D view lights every edge of the picked node whatever its type (J1230).
describe("what a selected term lights, associations included", () => {
  /** test3's graph at ``depth`` hops, as the 2D view lays it out. */
  const at = (depth: number) =>
    laneDiagram(focusedBy(ASSOCIATION_CHAIN, TEST3, depth), ASSOCIATION_CHAIN, new Set());
  const sorted = (s: Iterable<string>) => [...s].sort();
  const drawnLines = (d: ReturnType<typeof laneDiagram>) => [...d.links, ...d.arcs];
  /** The drawn line between ``a`` and ``b``, either way round. */
  const lineOf = (d: ReturnType<typeof laneDiagram>, a: string, b: string) => {
    const k = drawnLines(d).find(
      (x) => (x.from === a && x.to === b) || (x.from === b && x.to === a),
    );
    if (!k) throw new Error(`no line drawn between ${a} and ${b}`);
    return k.id;
  };

  it("lights a term reached only by RELATED_TO, its link, its column, table and product", () => {
    const d = at(1);
    const { nodes, links } = connectedTo(d, TEST3);
    for (const id of [KIN, "c:203:kin", "t:203", "p:kin"]) expect(nodes.has(id)).toBe(true);
    expect(links.has(lineOf(d, TEST3, KIN))).toBe(true);
    expect(links.has(lineOf(d, KIN, "c:203:kin"))).toBe(true);
    expect(links.has(lineOf(d, "c:203:kin", "t:203"))).toBe(true);
    expect(links.has(lineOf(d, "t:203", "p:kin"))).toBe(true);
  });

  it("does not draw a term two association steps away at 1 hop, and lights nothing of it", () => {
    const d = at(1);
    expect(d.lane.has(KIN2)).toBe(false);
    const { nodes } = connectedTo(d, TEST3);
    for (const id of [KIN2, "c:204:kin2", "t:204", "p:kin2"]) expect(nodes.has(id)).toBe(false);
  });

  it("lights a term two association steps away, with its column, table and product, at 2 hops", () => {
    const d = at(2);
    expect(d.lane.get(KIN2)).toBe("term");
    const { nodes, links } = connectedTo(d, TEST3);
    for (const id of [KIN2, "c:204:kin2", "t:204", "p:kin2"]) expect(nodes.has(id)).toBe(true);
    expect(links.has(lineOf(d, KIN, KIN2))).toBe(true);
  });

  it("lights animal breed name, reached through test's association, with its grounding at 3 hops", () => {
    const d = at(3);
    const { nodes, links } = connectedTo(d, TEST3);
    expect(sorted(nodes)).toEqual(
      sorted([
        TEST3,
        TEST2,
        TEST,
        BREED_NAME,
        KIN,
        KIN2,
        "c:202:label",
        "t:202",
        "p:kn",
        "c:201:breed_name",
        "t:201",
        "p:ph",
        "c:203:kin",
        "t:203",
        "p:kin",
        "c:204:kin2",
        "t:204",
        "p:kin2",
      ]),
    );
    expect(links.has(lineOf(d, TEST, BREED_NAME))).toBe(true);
    const drawnIds = new Set(drawnLines(d).map((k) => k.id));
    for (const id of links) expect(drawnIds.has(id)).toBe(true);
  });

  it("lights for a column, as before: its table and product, its term and that term's ranking path", () => {
    const { nodes } = connectedTo(at(3), "c:202:label");
    expect(sorted(nodes)).toEqual(sorted(["c:202:label", "t:202", "p:kn", TEST3, TEST2, TEST]));
  });

  it("lights for a table, as before: its product, its columns drawn, their terms and ranking paths", () => {
    const { nodes } = connectedTo(at(3), "t:202");
    expect(sorted(nodes)).toEqual(sorted(["t:202", "p:kn", "c:202:label", TEST3, TEST2, TEST]));
  });

  it("lights for a product, as before: its tables, their columns drawn, terms and ranking paths", () => {
    const { nodes } = connectedTo(at(3), "p:kn");
    expect(sorted(nodes)).toEqual(sorted(["p:kn", "t:202", "c:202:label", TEST3, TEST2, TEST]));
  });

  it("agrees with 3D: what the 3D pick lights is lit in 2D, and 2D lights only what 3D draws joined to the pick", () => {
    for (const depth of [1, 2, 3]) {
      const shown = focusedBy(ASSOCIATION_CHAIN, TEST3, depth);
      const d = laneDiagram(shown, ASSOCIATION_CHAIN, new Set());
      const lit2d = connectedTo(d, TEST3);
      // 3D draws ``shown`` (a table's columns folded into it); its pick of test3 lights these.
      const lit3d = shown.edges.filter((e) => pickLights(e, TEST3));
      expect(lit3d.map((e) => e.id).sort()).toEqual([
        `term_binding:${TEST3}>t:202`,
        `term_edge:${TEST3}>${TEST2}:PART_OF`,
        `term_edge:${TEST3}>${KIN}:RELATED_TO`,
      ]);
      for (const e of lit3d) {
        expect(lit2d.nodes.has(e.a)).toBe(true);
        expect(lit2d.nodes.has(e.b)).toBe(true);
      }
      // The term links as one line each; the binding folded to its table in 3D runs through the
      // column in 2D, each line of it lit.
      expect(lit2d.links.has(lineOf(d, TEST3, TEST2))).toBe(true);
      expect(lit2d.links.has(lineOf(d, TEST3, KIN))).toBe(true);
      expect(lit2d.links.has(lineOf(d, TEST3, "c:202:label"))).toBe(true);
      expect(lit2d.links.has(lineOf(d, "c:202:label", "t:202"))).toBe(true);
      // Each node 2D lights is drawn in 3D (a column as its table), joined to the pick there.
      const in3d = (id: string) =>
        ASSOCIATION_CHAIN.nodes.find((n) => n.id === id && n.type === "column")?.parent ?? id;
      const joined = new Set([TEST3]);
      for (let grew = true; grew; ) {
        grew = false;
        for (const e of shown.edges)
          for (const [x, y] of [
            [e.a, e.b],
            [e.b, e.a],
          ])
            if (joined.has(x) && !joined.has(y)) {
              joined.add(y);
              grew = true;
            }
      }
      for (const id of lit2d.nodes) expect(joined.has(in3d(id)), id).toBe(true);
    }
  });
});

describe("where a hover card goes", () => {
  const win = { width: 1000, height: 800 };
  const size = { width: 300, height: 120 };
  const box = (left: number, top: number, right: number, bottom: number) => ({
    left,
    top,
    right,
    bottom,
  });
  const point = (x: number, y: number) => box(x, y, x, y);

  it("goes to the side with nothing drawn", () => {
    const toRight = { boxes: [box(520, 300, 900, 500)], lines: [] };
    expect(tipPlacement(point(500, 400), size, toRight, win).startsWith("left")).toBe(true);
    const toLeft = { boxes: [box(100, 300, 480, 500)], lines: [] };
    expect(tipPlacement(point(500, 400), size, toLeft, win).startsWith("right")).toBe(true);
  });

  it("stays off the lines as well as the nodes", () => {
    // A line running across the right of the pointer; nodes nowhere near.
    const lines: [At, At][] = [[{ x: 520, y: 300 }, { x: 900, y: 520 }]];
    expect(tipPlacement(point(500, 400), size, { boxes: [], lines }, win).startsWith("right")).toBe(false);
  });

  it("clears the node hovered, not just the pointer inside it", () => {
    const node = box(520, 390, 640, 416);
    const place = tipPlacement(node, size, { boxes: [node], lines: [] }, win);
    expect(place).toBe("right-start");
  });

  it("never goes past the window, however empty that side is", () => {
    const left = { boxes: [box(100, 300, 880, 500)], lines: [] };
    expect(tipPlacement(point(950, 400), size, left, win).startsWith("right")).toBe(false);
  });
});
