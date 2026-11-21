// Copyright (c) 2026 Kenneth Stott
// Canary: 8e3c5a17-2d9b-4f60-a4c8-1b7e6d2f9a35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-07, WHAT THE GRAPH DRAWS; 2026-11-12, A HOP IS A STEP BETWEEN TERMS):
// the 3D view's hops counted from the focus alone, each a step between terms -- a product's tables,
// commands, columns and the terms tied to them come with it at no hop, each term with its
// grounding (the other columns tied to it, their tables); Related tables drawing the tables related to the tables drawn, named by the
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
    dataProductCommands: [{ trackedFunction: { name: "refund", domainId: "sales" } }],
    dataProductTables: [
      {
        registeredTable: {
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
                dataProductTables: [{ dataProduct: { id: "p2", name: "Owners", domainId: "crm" } }],
              },
            },
          ],
        },
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

describe("the 3D view's hops, counted between terms from the focus alone", () => {
  // Pet name is a KIND_OF Animal name, a PART_OF Animal; neither is tied to a column.
  const chain: GraphData = productMapGraph(
    [
      ...TERMS,
      { ...TERMS[0], id: 8, name: "Animal name", refCount: 0, refs: [] },
      { ...TERMS[0], id: 9, name: "Animal", refCount: 0, refs: [] },
    ],
    [
      { id: 1, fromTermId: 7, toTermId: 8, relType: "KIND_OF" },
      { id: 2, fromTermId: 8, toTermId: 9, relType: "PART_OF" },
    ],
    PRODUCTS,
  );

  it("draws the product's tables, commands, columns and their terms at 1 hop, each term grounded", () => {
    // Pet name brings the other column it is tied to, animals.nickname, with its table.
    expect(ids(drawn(1, LAYERS))).toEqual([
      "c:3:age",
      "c:3:born",
      "c:3:name",
      "c:6:nickname",
      "f:refund",
      "p:p1",
      "t:3",
      "t:6",
      "term:7",
    ]);
  });

  it("draws no more past 1 hop where no term link leads on", () => {
    for (const depth of [2, 3, 4, Infinity])
      expect(ids(drawn(depth, LAYERS))).toEqual(ids(drawn(1, LAYERS)));
  });

  it("steps once across the term links for each hop", () => {
    const at = (depth: number) => ids(drawn(depth, LAYERS, chain)).filter((id) => id.startsWith("term:"));
    expect(at(1)).toEqual(["term:7", "term:8"]);
    expect(at(2)).toEqual(["term:7", "term:8", "term:9"]);
    expect(at(Infinity)).toEqual(["term:7", "term:8", "term:9"]);
  });
});

describe("Related tables", () => {
  const ON: LayerId[] = [...LAYERS, "related"];

  it("draws each table related to a table drawn, named by the relationship, with its product", () => {
    const g = drawn(1, ON);
    expect(ids(g)).toEqual([
      "c:3:age",
      "c:3:born",
      "c:3:name",
      "c:6:nickname",
      "f:refund",
      "p:p1",
      "p:p2",
      "t:3",
      "t:5",
      "t:6",
      "term:7",
    ]);
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
    for (const depth of [1, 2, 3, 4, Infinity]) expect(ids(drawn(depth, ON))).toContain("t:5");
  });
});

describe("a table's columns tied to no term drawn", () => {
  const label = (count: number) => `${count} columns with no term in this view`;

  it("fold into one node, named by their count, while no term is drawn", () => {
    const g = foldUnbound(drawn(1, LAYERS, productMapGraph([], [], PRODUCTS)), new Set(), label);
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
    const g = foldUnbound(drawn(1, LAYERS), new Set(), label);
    expect(ids(g)).toContain("c:3:name");
    expect(g.nodes.find((n) => n.id === foldId("t:3"))).toMatchObject({
      label: "2 columns with no term in this view",
      folded: ["c:3:age", "c:3:born"],
    });
  });

  it("are drawn one by one once the table is expanded", () => {
    const g = foldUnbound(drawn(1, LAYERS), new Set(["t:3"]), label);
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
