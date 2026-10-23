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
// a column, a concept linked to it by KIND_OF, an undefined term bound to a column, a concept with
// no edges, a deprecated term and a retired term.

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
import { NO_FILTERS } from "../graphModel";

const NO_FILTERS_ONTOLOGY: GraphFilters = { ...NO_FILTERS };

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
  definition: `${name}, as the business uses it.`,
  ref_count: 0,
  deprecated: false,
  retired: false,
  live: true,
  domains: [{ domain_id: "sales" }],
  refs: [],
  ...over,
});

const TERMS: TermRow[] = [
  term(1, "Customer", { ref_count: 1, refs: [ref(10, "email", "c360")] }),
  term(2, "Party", { ref_count: 0, domains: [{ domain_id: "*" }] }),
  term(3, "Loyalty tier", {
    definition: null,
    live: false,
    ref_count: 1,
    refs: [ref(10, "tier")],
  }),
  term(4, "Concept", { ref_count: 0, live: false }),
  term(5, "Old customer", { deprecated: true, live: false, domains: [{ domain_id: "finance" }] }),
  term(6, "Ancient", { retired: true, live: false }),
];
const EDGES: TermEdgeRow[] = [
  { id: 1, from_term_id: 1, to_term_id: 2, rel_type: "KIND_OF" },
  { id: 2, from_term_id: 5, to_term_id: 1, rel_type: "REPLACES" },
  { id: 3, from_term_id: 3, to_term_id: 1, rel_type: "PART_OF" },
];

const G: GraphData = glossaryGraph(TERMS, EDGES);
const node = (g: GraphData, id: string) => g.nodes.find((n) => n.id === id);
const ids = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const defaults: GraphFilters = {
  domains: [],
  tags: [],
  sensitivities: [],
  focus: [],
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
  it("marks a class whose tables are not all declared one entity, naming its tables", () => {
    const tables = [
      { table_id: 10, table_name: "customers", domain_id: "sales" },
      { table_id: 11, table_name: "contacts", domain_id: "crm" },
    ];
    const g = glossaryGraph(
      [
        term(1, "Customer", { tables, identity_gap: true }),
        term(2, "Order", { tables: tables.slice(0, 1), identity_gap: false }),
      ],
      [],
    );
    const node = (id: string) => g.nodes.find((n) => n.id === id);
    expect(node("term:1")?.identityGap).toEqual(["sales.customers", "crm.contacts"]);
    expect(node("term:2")?.identityGap).toBeUndefined();
  });

  it("names a proposed term by its title, its standing marked by style alone", () => {
    expect(node(G, "term:3")).toMatchObject({ status: "proposed", label: "Loyalty tier" });
    expect(node(G, "term:4")).toMatchObject({ status: "proposed", label: "Concept" });
    expect(node(G, "term:1")).toMatchObject({ status: "live", label: "Customer" });
    expect(G.nodes.some((n) => /undefined|·/.test(n.label))).toBe(false);
    // The definition rides along for the side panel, never in the label; none, nothing.
    expect(node(G, "term:1")?.definition).toBe("Customer, as the business uses it.");
    expect(node(G, "term:3")?.definition).toBeUndefined();
  });

  it("draws a term the server answered no domains and no references for (null lists)", () => {
    const g = glossaryGraph([term(7, "Lone", { domains: null, refs: null })], []);
    expect(node(g, "term:7")?.label).toBe("Lone");
  });

  it("gives each term its role -- class, property or concept -- and marks columns a product holds", () => {
    expect(node(G, "term:2")?.role).toBe("concept");
    expect(node(G, "term:1")?.role).toBe("property");
    const grounded = glossaryGraph(
      [term(9, "Order", { tables: [{ table_id: 20, table_name: "orders", domain_id: "sales" }] })],
      [],
    );
    expect(node(grounded, "term:9")?.role).toBe("class");
    // customers.email's table is c360's; customers.tier's too, so both are in a product.
    expect(node(G, "c:10:email")?.inProduct).toBe(true);
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

  it("the data layer, off by default, adds the products' datasets, unfolded into their columns", () => {
    expect(preset("ontology").layers).not.toContain("data");
    const g = view([...preset("ontology").layers, "data"]);
    expect(ids(g)).toEqual(expect.arrayContaining(["t:10", "p:c360", "c:10:email"]));
    // A column is drawn as itself, joined to its dataset, and the term IS it.
    expect(g.edges.find((e) => e.kind === "term_binding" && e.a === "term:1")?.b).toBe(
      "c:10:email",
    );
    expect(g.edges).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ kind: "has_column", a: "t:10", b: "c:10:email" }),
      ]),
    );
  });

  it("draws a class term's tables with the ontology: the term IS the table, a cylinder", () => {
    const grounded = glossaryGraph(
      [term(7, "Order", { tables: [{ table_id: 20, table_name: "orders", domain_id: "sales" }] })],
      [],
    );
    const on = (layers: LayerId[]) =>
      visibleGraph(grounded, new Set(layers), NO_FILTERS_ONTOLOGY, new Set(), GLOSSARY_SCOPE);
    const g = on(["ontology"]);
    expect(g.nodes.find((n) => n.id === "t:20")).toMatchObject({
      type: "table",
      label: "sales.orders",
      stored: true,
    });
    expect(g.edges).toEqual([
      expect.objectContaining({ kind: "term_binding", a: "term:7", b: "t:20", label: "IS" }),
    ]);
    // Not drawn without the ontology.
    expect(on(["domains"]).nodes.map((n) => n.id)).not.toContain("t:20");
  });
});
