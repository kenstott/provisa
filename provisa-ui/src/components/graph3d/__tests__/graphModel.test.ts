// Copyright (c) 2026 Kenneth Stott
// Canary: 9a7e4c21-6b3d-4f58-8e0a-1d5c2b7f3e96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the data product graph's layers, presets, filters and layouts.

import { describe, expect, it } from "vitest";
import {
  NO_FILTERS,
  PRESETS,
  PRODUCT_SCOPE,
  availableLayers,
  layoutGraph,
  panelGraph,
  visibleGraph,
  type GraphData,
  type LayerId,
} from "../graphModel";
import { DATA } from "./fixture";

const ids = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const kinds = (g: GraphData) => [...new Set(g.edges.map((e) => e.kind))].sort();
const preset = (p: keyof typeof PRESETS) => new Set<LayerId>(PRESETS[p].layers);

describe("the data product graph's layers", () => {
  it("offers only the layers the data has something in", () => {
    const noLineage = { ...DATA, edges: DATA.edges.filter((e) => e.kind !== "lineage") };
    expect(availableLayers(DATA, PRODUCT_SCOPE).has("lineage")).toBe(true);
    expect(availableLayers(noLineage, PRODUCT_SCOPE).has("lineage")).toBe(false);
  });

  it("Knowledge graph draws every part of the product but its relationships to outside tables", () => {
    const g = visibleGraph(DATA, preset("knowledge"), NO_FILTERS, new Set(), PRODUCT_SCOPE);
    expect(kinds(g)).not.toContain("relationship_outside");
    expect(ids(g)).not.toContain("t:invoices");
    expect(kinds(g)).toEqual(
      expect.arrayContaining(["member", "command", "checks", "profiled", "tagged", "lineage"]),
    );
  });

  it("folds a table's columns into it until it is expanded", () => {
    const folded = visibleGraph(DATA, preset("knowledge"), NO_FILTERS, new Set(), PRODUCT_SCOPE);
    expect(ids(folded)).not.toContain("c:customers.email");
    // The term's binding lands on the table the column is folded into.
    expect(folded.edges.find((e) => e.kind === "term_binding")).toMatchObject({
      a: "term:customer",
      b: "t:customers",
    });
    const open = visibleGraph(
      DATA,
      preset("knowledge"),
      NO_FILTERS,
      new Set(["t:customers"]),
      PRODUCT_SCOPE,
    );
    expect(ids(open)).toContain("c:customers.email");
    expect(open.edges.find((e) => e.kind === "term_binding")?.b).toBe("c:customers.email");
  });

  it("removing the tags layer removes only the tags and their edges", () => {
    const on = visibleGraph(DATA, preset("knowledge"), NO_FILTERS, new Set(), PRODUCT_SCOPE);
    const layers = preset("knowledge");
    layers.delete("tags");
    const off = visibleGraph(DATA, layers, NO_FILTERS, new Set(), PRODUCT_SCOPE);
    expect(ids(on).filter((id) => !ids(off).includes(id))).toEqual(["g:gold"]);
    expect(on.edges.length - off.edges.length).toBe(2);
    expect(kinds(off)).not.toContain("tagged");
  });

  it("Lineage draws lineage edges alone, in lanes left to right from the members", () => {
    const g = visibleGraph(DATA, preset("lineage"), NO_FILTERS, new Set(), PRODUCT_SCOPE);
    expect(kinds(g)).toEqual(["lineage"]);
    expect(ids(g)).toEqual(["t:orders", "x:raw_orders"]);
    const at = layoutGraph(g, PRESETS.lineage.layout, ["t:customers", "t:orders"]);
    expect((at.get("x:raw_orders") as number[])[0]).toBeLessThan(
      (at.get("t:orders") as number[])[0],
    );
  });

  it("Product map draws products by domain and the links between them", () => {
    const g = visibleGraph(DATA, preset("productMap"), NO_FILTERS, new Set(), PRODUCT_SCOPE);
    expect(ids(g)).toEqual(["d:finance", "d:sales", "p:billing", "p:c360"]);
    expect(g.edges.find((e) => e.kind === "product_link")?.paths).toBe(2);
  });

  it("Ontology draws terms, their edges, the bound columns and the products owning them", () => {
    const g = visibleGraph(DATA, preset("ontology"), NO_FILTERS, new Set(), PRODUCT_SCOPE);
    expect(ids(g)).toEqual(
      expect.arrayContaining(["term:customer", "term:party", "c:customers.email", "p:c360"]),
    );
    expect(ids(g)).not.toContain("t:customers");
    expect(g.edges.find((e) => e.kind === "owns")).toMatchObject({
      a: "p:c360",
      b: "c:customers.email",
    });
  });
});

describe("the data product graph's filters", () => {
  const all = new Set<LayerId>(["products", "tables", "commands", "quality", "tags", "ontology"]);

  it("a domain filter applies to every layer", () => {
    const g = visibleGraph(
      DATA,
      all,
      { ...NO_FILTERS, domains: ["finance"] },
      new Set(),
      PRODUCT_SCOPE,
    );
    expect(ids(g)).toEqual(["d:finance", "p:billing"]);
  });

  it("a sensitivity filter keeps what carries it, and what hangs only off the rest goes", () => {
    const g = visibleGraph(
      DATA,
      all,
      { ...NO_FILTERS, sensitivities: ["pii"] },
      new Set(),
      PRODUCT_SCOPE,
    );
    expect(ids(g)).toContain("t:customers");
    expect(ids(g)).not.toContain("t:orders");
    expect(ids(g)).not.toContain("k:orders_dq");
  });

  it("a tag filter keeps the tag and what carries it", () => {
    const g = visibleGraph(DATA, all, { ...NO_FILTERS, tags: ["gold"] }, new Set(), PRODUCT_SCOPE);
    expect(ids(g)).toEqual(expect.arrayContaining(["g:gold", "p:c360", "t:customers"]));
    expect(ids(g)).not.toContain("t:orders");
  });

  it("a focus product keeps what is reached from it, not through another product", () => {
    const g = visibleGraph(
      DATA,
      preset("knowledge"),
      { ...NO_FILTERS, focus: ["p:billing"] },
      new Set(),
      PRODUCT_SCOPE,
    );
    expect(ids(g)).toEqual(["d:finance", "p:billing", "p:c360"]);
  });
});

describe("the detail panel's graph", () => {
  it("is the product, its member tables, its commands and the terms bound to its columns", () => {
    // A checker is a member of the product as well; the panel leaves it to the quality layer.
    const withChecker: GraphData = {
      nodes: DATA.nodes,
      edges: [...DATA.edges, { id: "m-dq", a: "p:c360", b: "k:orders_dq", kind: "member" }],
    };
    const g = panelGraph(withChecker, "p:c360");
    expect(ids(g)).toEqual(["f:refresh", "p:c360", "t:customers", "t:orders", "term:customer"]);
    expect(kinds(g)).toEqual(["command", "member", "term_binding"]);
  });
});

describe("column lineage (REQ-2129)", () => {
  const withColumns: GraphData = {
    nodes: [...DATA.nodes, { id: "c:9:amount", type: "column", label: "raw.orders.amount" }],
    edges: [
      ...DATA.edges,
      { id: "cl", a: "c:9:amount", b: "c:orders.total", kind: "column_lineage", label: "sales.v" },
    ],
  };

  it("draws a column's upstream columns once its table is expanded, and not before", () => {
    const folded = visibleGraph(
      withColumns,
      preset("knowledge"),
      NO_FILTERS,
      new Set(),
      PRODUCT_SCOPE,
    );
    expect(kinds(folded)).not.toContain("column_lineage");
    expect(ids(folded)).not.toContain("c:9:amount");
    const open = visibleGraph(
      withColumns,
      preset("knowledge"),
      NO_FILTERS,
      new Set(["t:orders"]),
      PRODUCT_SCOPE,
    );
    expect(open.edges.find((e) => e.kind === "column_lineage")).toMatchObject({
      a: "c:9:amount",
      b: "c:orders.total",
    });
  });
});
