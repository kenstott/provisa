// Copyright (c) 2026 Kenneth Stott
// Canary: 8e3c5a17-2d9b-4f60-a4c8-1b7e6d2f9a35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-07, WHAT THE GRAPH DRAWS): the 3D view's hops counted from the focus
// alone (1 its tables and commands, 2 their columns, 3 the terms tied to them, 4 the other columns
// tied to those terms); Related tables drawing the tables related to the tables drawn, named by the
// relationship, with their products, one step and nothing of their own; a table's columns tied to
// no term drawn folded into one node; the product panel's and the full graph's defaults.

import { describe, expect, it } from "vitest";
import {
  NO_FILTERS,
  PRODUCT_MAP_SCOPE,
  foldId,
  foldUnbound,
  visibleGraph,
  type GraphData,
  type LayerId,
} from "../graphModel";
import { productMapGraph, type ProductRow, type TermRow } from "../ontologyData";
import { PRODUCT_MAP, WHOLE, productPreset, snippetLayers } from "../ontologyPresets";
import en from "../../../i18n/locales/en/graph3d.json";

// Pet name is tied to pets.name (a member's column) and to animals.nickname (another table's).
const TERMS: TermRow[] = [
  {
    id: 7,
    name: "Pet name",
    definition: null,
    refCount: 2,
    deprecated: false,
    retired: false,
    status: "approved",
    grounding: "grounded",
    refs: [
      { columnKey: "3:name", columnName: "name", tableId: 3, tableName: "pets", domainId: "sales" },
      {
        columnKey: "6:nickname",
        columnName: "nickname",
        tableId: 6,
        tableName: "animals",
        domainId: "shelter",
      },
    ],
  },
];

// P1 holds pets (three columns, one tied to Pet name) and a command; pets is related to owners,
// which P2 holds.
const PRODUCTS: ProductRow[] = [
  {
    id: "p1",
    name: "P1",
    domainId: "sales",
    commands: [{ name: "refund", domainId: "sales" }],
    memberTables: [
      {
        id: 3,
        domainId: "sales",
        tableName: "pets",
        tableColumns: [
          { columnName: "name", columnKey: "3:name", glossaryRef: [{ termId: 7 }] },
          { columnName: "age", columnKey: "3:age" },
          { columnName: "born", columnKey: "3:born" },
        ],
        sourceRelationships: [
          {
            id: "r1",
            cypherType: "OWNED_BY",
            targetTableId: 5,
            targetTable: {
              id: 5,
              tableName: "owners",
              domainId: "crm",
              product: { id: "p2", name: "Owners", domainId: "crm" },
            },
          },
        ],
      },
    ],
  },
];

const GRAPH = productMapGraph(TERMS, [], PRODUCTS);
const LAYERS: LayerId[] = ["ontology", "columns", "ownerTables", "data", "commands"];

const drawn = (depth: number, layers: LayerId[], g: GraphData = GRAPH) =>
  visibleGraph(g, new Set(layers), { ...NO_FILTERS, focus: ["p:p1"] }, new Set(), {
    ...PRODUCT_MAP_SCOPE,
    neighbourhood: { ...PRODUCT_MAP_SCOPE.neighbourhood, depth },
  });
const ids = (g: GraphData) => g.nodes.map((n) => n.id).sort();

describe("the 3D view's hops, counted from the focus alone", () => {
  it("draws the product's tables and commands at 1 hop, and none of their columns", () => {
    expect(ids(drawn(1, LAYERS))).toEqual(["f:refund", "p:p1", "t:3"]);
  });

  it("draws their columns at 2", () => {
    expect(ids(drawn(2, LAYERS))).toEqual([
      "c:3:age",
      "c:3:born",
      "c:3:name",
      "f:refund",
      "p:p1",
      "t:3",
    ]);
  });

  it("draws the terms tied to those columns at 3", () => {
    const g = drawn(3, LAYERS);
    expect(ids(g)).toContain("term:7");
    expect(ids(g)).not.toContain("c:6:nickname");
  });

  it("draws the other columns tied to those terms at 4", () => {
    expect(ids(drawn(4, LAYERS))).toContain("c:6:nickname");
  });
});

describe("Related tables", () => {
  const ON: LayerId[] = [...LAYERS, "related"];

  it("draws each table related to a table drawn, named by the relationship, with its product", () => {
    const g = drawn(1, ON);
    expect(ids(g)).toEqual(["f:refund", "p:p1", "p:p2", "t:3", "t:5"]);
    expect(g.edges.find((e) => e.kind === "relationship")).toMatchObject({
      a: "t:3",
      b: "t:5",
      label: "OWNED_BY",
    });
    expect(g.edges.find((e) => e.kind === "member" && e.b === "t:5")?.a).toBe("p:p2");
    expect(g.nodes.find((n) => n.id === "t:5")?.label).toBe("crm.owners");
  });

  it("is off: no related table and no product of one", () => {
    expect(ids(drawn(4, LAYERS))).not.toContain("t:5");
    expect(ids(drawn(4, LAYERS))).not.toContain("p:p2");
  });

  it("follows one step only: never a related table's columns, terms or relationships", () => {
    // Were owners read with its own column and relationship, neither is drawn.
    const g: GraphData = {
      nodes: [
        ...GRAPH.nodes,
        { id: "c:5:id", type: "column", label: "owners.id", parent: "t:5", layers: ["data"] },
        { id: "t:8", type: "table", label: "crm.addresses", layers: [] },
        { id: "term:9", type: "term", label: "Owner id" },
      ],
      edges: [
        ...GRAPH.edges,
        { id: "x1", a: "t:5", b: "c:5:id", kind: "has_column", layers: ["data"] },
        {
          id: "x2",
          a: "t:5",
          b: "t:8",
          kind: "relationship",
          label: "LIVES_AT",
          layers: ["related"],
        },
        { id: "x3", a: "term:9", b: "c:5:id", kind: "term_binding", layers: ["columns"] },
      ],
    };
    const at4 = ids(drawn(4, ON, g));
    expect(at4).toContain("t:5");
    for (const id of ["c:5:id", "t:8", "term:9"]) expect(at4).not.toContain(id);
  });

  it("counts no hop: the related table is drawn at 1 hop as at 4", () => {
    for (const depth of [1, 2, 3, 4]) expect(ids(drawn(depth, ON))).toContain("t:5");
  });
});

describe("a table's columns tied to no term drawn", () => {
  const label = (count: number) => `${count} columns with no term in this view`;

  it("fold into one node, named by their count, while no term is drawn", () => {
    const g = foldUnbound(drawn(2, LAYERS), new Set(), label);
    const fold = g.nodes.find((n) => n.id === foldId("t:3"));
    expect(fold).toMatchObject({
      type: "column",
      parent: "t:3",
      label: "3 columns with no term in this view",
      folded: ["c:3:name", "c:3:age", "c:3:born"],
    });
    expect(ids(g).filter((id) => id.startsWith("c:"))).toEqual([]);
    expect(g.edges.find((e) => e.b === foldId("t:3"))).toMatchObject({
      a: "t:3",
      kind: "has_column",
    });
  });

  it("leave a column tied to a term drawn as itself", () => {
    const g = foldUnbound(drawn(3, LAYERS), new Set(), label);
    expect(ids(g)).toContain("c:3:name");
    expect(g.nodes.find((n) => n.id === foldId("t:3"))).toMatchObject({
      label: "2 columns with no term in this view",
      folded: ["c:3:age", "c:3:born"],
    });
  });

  it("are drawn one by one once the table is expanded", () => {
    const g = foldUnbound(drawn(3, LAYERS), new Set(["t:3"]), label);
    expect(g.nodes.some((n) => n.folded)).toBe(false);
    expect(ids(g)).toEqual(expect.arrayContaining(["c:3:age", "c:3:born", "c:3:name"]));
  });

  it("are named so in English", () => {
    expect(en.graph3d.moreColumns_other).toBe("{{count}} columns with no term in this view");
    expect(en.graph3d.moreColumns_one).toBe("{{count}} column with no term in this view");
  });
});

describe("the defaults", () => {
  it("open a product's panel at 1 hop with Related tables on", () => {
    expect(productPreset("p1").depth).toBe(1);
    expect(snippetLayers(productPreset("p1"))).toContain("related");
  });

  it("open the full graph at 2 hops with Related tables off", () => {
    expect(PRODUCT_MAP.depth).toBe(2);
    expect(WHOLE.depth).toBe(2);
  });

  it("name the controls as the amendment does", () => {
    expect(en.graph3d.filterEdgeType).toBe("Term relationships");
    expect(en.graph3d.relatedTables).toBe("Related tables");
    expect(en.graph3d.hopsFrom).toBe("Hops from {{focus}}");
    expect(en.graph3d.layer_lineage).toBe("Inputs");
  });
});
