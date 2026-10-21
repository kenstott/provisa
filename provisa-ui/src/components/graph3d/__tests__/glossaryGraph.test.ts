// Copyright (c) 2026 Kenneth Stott
// Canary: 5d2b8e47-9a16-4c03-b7f5-3e1c6a9d8f20
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: the glossary's Ontology View, as REQ-2130's scenario sets it: a concrete term bound to
// a column, an abstract term linked to it by KIND_OF, an undefined term bound to a column, an
// abstract term with no edges, a deprecated term and a retired term.

import { describe, expect, it } from "vitest";
import {
  GLOSSARY_SCOPE,
  layoutGraph,
  visibleGraph,
  type GraphData,
  type GraphFilters,
  type LayerId,
} from "../graphModel";
import { glossaryGraph, type TermEdgeRow, type TermRow } from "../glossaryGraphData";

const ref = (table: number, column: string, product?: string) => ({
  column_key: `${table}:${column}`,
  column_name: column,
  table_id: table,
  table_name: `t${table}`,
  domain_id: "sales",
  table: {
    id: table,
    product: product ? { id: product, name: product, domain_id: "sales" } : null,
  },
});

const term = (id: number, name: string, over: Partial<TermRow>): TermRow => ({
  id,
  name,
  definition: `${name}, defined.`,
  is_abstract: false,
  deprecated: false,
  retired: false,
  live: true,
  grounded: true,
  domains: [{ domain_id: "sales" }],
  refs: [],
  ...over,
});

const TERMS: TermRow[] = [
  term(1, "Customer", { refs: [ref(10, "email", "c360")] }),
  term(2, "Party", { is_abstract: true, domains: [{ domain_id: "*" }] }),
  term(3, "Loyalty tier", { definition: null, live: false, refs: [ref(10, "tier")] }),
  term(4, "Concept", { is_abstract: true, live: false, grounded: false }),
  term(5, "Old customer", { deprecated: true, live: false, domains: [{ domain_id: "finance" }] }),
  term(6, "Ancient", { retired: true, live: false }),
];
const EDGES: TermEdgeRow[] = [
  { id: 1, from_term_id: 1, to_term_id: 2, rel_type: "KIND_OF" },
  { id: 2, from_term_id: 5, to_term_id: 1, rel_type: "REPLACES" },
  { id: 3, from_term_id: 3, to_term_id: 1, rel_type: "PART_OF" },
];

const say = (r: string) => r;
const G: GraphData = glossaryGraph(TERMS, EDGES, say);
const node = (g: GraphData, id: string) => g.nodes.find((n) => n.id === id);
const ids = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const defaults: GraphFilters = {
  domains: [],
  tags: [],
  sensitivities: [],
  focus: null,
  edgeTypes: [],
  hiddenStatuses: GLOSSARY_SCOPE.hiddenStatuses,
};
const preset = (p: "ontology" | "taxonomy") => {
  const found = GLOSSARY_SCOPE.presets[p];
  if (!found) throw new Error(p);
  return found;
};
const view = (layers: LayerId[], filters: GraphFilters = defaults) =>
  visibleGraph(G, new Set(layers), filters, new Set(), GLOSSARY_SCOPE);

describe("the Ontology View's terms", () => {
  it("says why a proposal is one: undefined, or ungrounded", () => {
    expect(node(G, "term:3")).toMatchObject({ status: "proposed", note: "undefined" });
    expect(node(G, "term:4")).toMatchObject({ status: "proposed", note: "ungrounded" });
    expect(node(G, "term:1")?.status).toBe("live");
  });

  it("says ungrounded of a defined proposal even when the grounded column was not read", () => {
    const g = glossaryGraph([term(9, "X", { live: false, grounded: undefined })], [], say);
    expect(node(g, "term:9")?.note).toBe("ungrounded");
    const u = glossaryGraph(
      [term(9, "X", { live: false, grounded: undefined, definition: "" })],
      [],
      say,
    );
    expect(node(u, "term:9")?.note).toBe("undefined");
  });

  it("draws abstract terms hollow and concrete ones solid", () => {
    expect(node(G, "term:2")?.hollow).toBe(true);
    expect(node(G, "term:1")?.hollow).toBe(false);
  });

  it("shows proposed and deprecated terms by default and hides retired ones", () => {
    const g = view(preset("ontology").layers);
    expect(ids(g)).toEqual(expect.arrayContaining(["term:3", "term:4", "term:5"]));
    expect(ids(g)).not.toContain("term:6");
    expect(node(g, "term:5")?.status).toBe("deprecated");
    const all = view(preset("ontology").layers, { ...defaults, hiddenStatuses: [] });
    expect(ids(all)).toContain("term:6");
    const noProposals = view(preset("ontology").layers, {
      ...defaults,
      hiddenStatuses: ["proposed", "retired"],
    });
    expect(ids(noProposals)).not.toContain("term:3");
  });
});

describe("the Ontology View's domains", () => {
  it("links a term to each domain it is in, an enterprise-wide one to every domain", () => {
    const into = (id: string) =>
      G.edges
        .filter((e) => e.kind === "in_domain" && e.a === id)
        .map((e) => e.b)
        .sort();
    expect(into("term:1")).toEqual(["d:sales"]);
    expect(into("term:2")).toEqual(["d:finance", "d:sales"]);
    // Not coloured by domain: a term carries no colour of its own, only links.
    expect(node(G, "term:2")?.domains).toBeUndefined();
  });

  it("a domain filter keeps an enterprise-wide term", () => {
    const g = view(preset("ontology").layers, { ...defaults, domains: ["finance"] });
    expect(ids(g)).toContain("term:2");
    expect(ids(g)).not.toContain("term:1");
  });
});

describe("the Ontology View's presets", () => {
  it("Ontology draws every edge type", () => {
    const g = view(preset("ontology").layers);
    expect(
      g.edges
        .filter((e) => e.kind === "term_edge")
        .map((e) => e.label)
        .sort(),
    ).toEqual(["KIND_OF", "PART_OF", "REPLACES"]);
  });

  it("Taxonomy draws only KIND_OF and PART_OF, as a tree from the top down", () => {
    const tax = preset("taxonomy");
    const g = view(tax.layers, { ...defaults, edgeTypes: tax.edgeTypes ?? [] });
    expect(g.edges.map((e) => e.label).sort()).toEqual(["KIND_OF", "PART_OF"]);
    const at = layoutGraph(g, tax.layout, []);
    const y = (id: string) => (at.get(id) as number[])[1];
    // Party above Customer, Customer above its part.
    expect(y("term:2")).toBeGreaterThan(y("term:1"));
    expect(y("term:1")).toBeGreaterThan(y("term:3"));
  });

  it("the data layer, off by default, adds the bound columns' tables and their products", () => {
    expect(preset("ontology").layers).not.toContain("data");
    const g = view([...preset("ontology").layers, "data"]);
    expect(ids(g)).toEqual(expect.arrayContaining(["t:10", "p:c360"]));
    // The columns are folded into their table.
    expect(ids(g)).not.toContain("c:10:email");
    expect(g.edges.find((e) => e.kind === "term_binding" && e.a === "term:1")?.b).toBe("t:10");
  });
});
