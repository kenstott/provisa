// Copyright (c) 2026 Kenneth Stott
// Canary: 7d2b9e41-3f86-4a1c-b5e0-9c4a6f8d2e17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended, THE ONE GRAPH'S PRESETS AND LAYERS): nothing the product graph showed is
// lost -- the one graph has optional layers for a product's commands, its quality checks, its
// links to other products and its lineage, each read only while it is on.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { RoleSchema, rootKey, type Introspection, type MetaReader } from "../../../api/metaGraph";
import type { LineageNode, ProductInput, ProductLineage } from "../../../api/lineage";
import {
  ONTOLOGY_SCOPE,
  NO_FILTERS,
  availableLayers,
  visibleGraph,
  type LayerId,
  type QueryScope,
} from "../graphModel";
import {
  ontologyGraph,
  ontologyQuery,
  readsProducts,
  withProductLineage,
  type ProductRow,
} from "../ontologyData";
import { SNIPPET_LAYERS } from "../useOntology";

const GOLDEN = resolve(__dirname, "../../../../../tests/fixtures/graphql_golden");

function reader(): MetaReader {
  const doc = JSON.parse(readFileSync(resolve(GOLDEN, "meta_schema.json"), "utf8")) as {
    introspection: Introspection;
    root_fields: { domain_id: string; table_name: string; graphql_field: string }[];
  };
  return {
    schema: new RoleSchema(doc.introspection),
    roots: new Map(
      doc.root_fields.map((r) => [rootKey(r.domain_id, r.table_name), r.graphql_field]),
    ),
  };
}

const scope = (layers: LayerId[], focus: string | null = null): QueryScope => ({
  layers: new Set<LayerId>(["ontology", ...layers]),
  search: [],
  hiddenStatuses: [],
  edgeTypes: [],
  domains: [],
  orphans: true,
  tags: [],
  sensitivities: [],
  focus,
  depth: 2,
});

const PRODUCT_LAYERS: LayerId[] = ["commands", "quality", "productLinks", "lineage"];

describe("the product layers' reads", () => {
  const m = reader();

  it("are off by default in a snippet, and offered in the full view", () => {
    for (const l of PRODUCT_LAYERS) {
      expect(SNIPPET_LAYERS).not.toContain(l);
      expect(ONTOLOGY_SCOPE.layerOrder).toContain(l);
    }
  });

  it("read a product's commands only under Commands", () => {
    const off = ontologyQuery(m, scope(["data"])) as string;
    const on = ontologyQuery(m, scope(["data", "commands"])) as string;
    expect(off).not.toContain("commands");
    expect(on).toMatch(/commands: commands \{[^}]*name[^}]*domain_id: domainId/);
  });

  it("read a checker's checked table and the tables' profiles only under Quality", () => {
    const off = ontologyQuery(m, scope(["data"])) as string;
    const on = ontologyQuery(m, scope(["data", "quality"])) as string;
    // A checker member is told by its contract, whatever the layers.
    expect(off).toContain("dq_contract: dqContract");
    expect(off).not.toContain("profileResults");
    expect(on).toContain("checks: checks");
    expect(on).toContain("profile_results: profileResults");
  });

  it("read a product's links to other products only under Product links", () => {
    const off = ontologyQuery(m, scope(["data"])) as string;
    const on = ontologyQuery(m, scope(["data", "productLinks"])) as string;
    expect(off).not.toContain("targetProduct");
    expect(on).toContain("target_product: targetProduct");
    expect(on).toMatch(/kind: \{ eq: "relationship" \}/);
  });

  it("read the products whenever a product layer is on, Data or not", () => {
    for (const l of PRODUCT_LAYERS) {
      expect(readsProducts(scope([l]))).toBe(true);
      expect(ontologyQuery(m, scope([l]))).toContain("data_products");
    }
    expect(readsProducts(scope(["columns"]))).toBe(false);
    // A term's snippet names its products through its columns' tables.
    expect(readsProducts(scope(["commands"], "term:7"))).toBe(false);
  });
});

const PRODUCTS: ProductRow[] = [
  {
    id: "p1",
    name: "P1",
    domain_id: "sales",
    commands: [{ name: "refund", domain_id: "sales" }],
    edges: [
      {
        kind: "relationship",
        path_count: 2,
        aliases: "BOUGHT, PAID",
        target_product: { id: "p2", name: "P2", domain_id: "finance" },
      },
    ],
    member_tables: [
      {
        id: 3,
        domain_id: "sales",
        table_name: "pets",
        table_columns: [{ column_name: "name", column_key: "3:name" }],
        profile_results: [{ id: 40, table_name: "pets" }],
      },
      {
        id: 4,
        domain_id: "sales",
        table_name: "pets_checks",
        dq_contract: "rules: []",
        checks: { id: 3 },
      },
    ],
  },
];

describe("the product layers' graph", () => {
  const g = ontologyGraph([], [], PRODUCTS);
  const drawn = (layers: LayerId[]) =>
    visibleGraph(g, new Set<LayerId>(["ontology", ...layers]), NO_FILTERS, new Set(), ONTOLOGY_SCOPE);
  const ids = (layers: LayerId[]) => drawn(layers).nodes.map((n) => n.id);

  it("draws a product's commands under Commands", () => {
    expect(g.edges.find((e) => e.kind === "command")).toMatchObject({ a: "p:p1", b: "f:refund" });
    expect(ids(["data"])).not.toContain("f:refund");
    expect(ids(["data", "commands"])).toContain("f:refund");
    // Commands alone draws the product its commands hang off.
    expect(ids(["commands"])).toEqual(expect.arrayContaining(["p:p1", "f:refund"]));
  });

  it("draws a checker member as quality evidence, never as a dataset, with its table's profiles", () => {
    expect(g.nodes.find((n) => n.id === "k:4")).toMatchObject({ type: "checker" });
    expect(g.nodes.some((n) => n.id === "t:4")).toBe(false);
    expect(g.edges.find((e) => e.kind === "checks")).toMatchObject({ a: "k:4", b: "t:3" });
    expect(g.edges.find((e) => e.kind === "profiled")).toMatchObject({ a: "r:40", b: "t:3" });
    expect(ids(["data"])).not.toContain("k:4");
    expect(ids(["data"])).not.toContain("r:40");
    expect(ids(["data", "quality"])).toEqual(expect.arrayContaining(["k:4", "r:40", "t:3"]));
  });

  it("joins a product to another by one edge carrying its path count and aliases", () => {
    expect(g.edges.find((e) => e.kind === "product_link")).toMatchObject({
      a: "p:p1",
      b: "p:p2",
      paths: 2,
      label: "BOUGHT, PAID",
    });
    expect(ids(["data"])).not.toContain("p:p2");
    expect(ids(["data", "productLinks"])).toContain("p:p2");
  });

  it("offers Lineage wherever a product is drawn, without reading it", () => {
    expect(availableLayers(g, ONTOLOGY_SCOPE).has("lineage")).toBe(true);
    expect(availableLayers(ontologyGraph([], []), ONTOLOGY_SCOPE).has("lineage")).toBe(false);
    expect(ONTOLOGY_SCOPE.readWhenOn).toEqual({ lineage: "product" });
  });
});

const input = (over: Partial<ProductInput>): ProductInput => ({
  relation: "raw.d",
  kind: "source_table",
  table_id: 9,
  domain_id: "raw",
  table_name: "d",
  alias: null,
  product_id: null,
  source_id: null,
  source_type: null,
  source_schema: null,
  source_table: null,
  ...over,
});

const lineageOf = (over: Partial<ProductLineage>): ProductLineage => ({
  nodes: [],
  edges: [],
  outputs: [],
  dq: { expectations: [], promises: [], related: [], unattached: [] },
  members: [],
  consumers: [],
  inputs: [],
  ...over,
});

describe("the Lineage layer (REQ-2156, REQ-2129)", () => {
  const base = ontologyGraph([], [], PRODUCTS);

  it("draws the endpoint's inputs feeding the product, each named as its reader knows it", () => {
    const g = withProductLineage(
      base,
      "p1",
      lineageOf({
        inputs: [
          input({ relation: "pet_store.pets", domain_id: "pet-store", table_name: "pets" }),
          input({
            relation: "native:sales.orders",
            kind: "source",
            domain_id: "sales",
            table_name: "orders",
            source_id: "shop",
            source_schema: "public",
            source_table: "Orders",
          }),
          input({ relation: "mart.e", domain_id: "mart", table_name: "e", product_id: "p2" }),
        ],
      }),
      [],
    );
    const label = (id: string) => g.nodes.find((n) => n.id === id)?.label;
    expect(label("l:pet_store.pets")).toBe("pet-store.pets");
    expect(label("l:native:sales.orders")).toBe("shop · public.Orders");
    // Another product's member stands for that product: p1 is built from p2.
    expect(g.nodes.some((n) => n.id === "l:mart.e")).toBe(false);
    expect(g.edges.filter((e) => e.kind === "lineage").map((e) => [e.a, e.b])).toEqual([
      ["l:pet_store.pets", "p:p1"],
      ["l:native:sales.orders", "p:p1"],
      ["p:p2", "p:p1"],
    ]);
    const on = (layers: LayerId[]) =>
      visibleGraph(g, new Set<LayerId>(["ontology", ...layers]), NO_FILTERS, new Set(), ONTOLOGY_SCOPE)
        .nodes.map((n) => n.id);
    expect(on(["data"])).not.toContain("l:pet_store.pets");
    expect(on(["data", "lineage"])).toContain("l:pet_store.pets");
  });

  it("joins the column lineage to the columns it names, by their tables, drawn as itself", () => {
    const node = (id: string, relation: string, column: string, kind: LineageNode["kind"]) => ({
      id,
      relation,
      column,
      kind,
      materialized: false,
    });
    const g = withProductLineage(
      base,
      "p1",
      lineageOf({
        nodes: [
          {
            ...node("native:raw.d.Name", "native:raw.d", "Name", "native"),
            source_id: "crm",
            source_schema: "public",
            source_table: "D",
          },
          node("raw.d.name", "raw.d", "name", "source"),
          node("sales.pets.name", "sales.pets", "name", "derived"),
        ],
        edges: [
          { source: "native:raw.d.Name", target: "raw.d.name", transform: "", ops: [] },
          { source: "raw.d.name", target: "sales.pets.name", transform: "", ops: [] },
        ],
        inputs: [input({})],
      }),
      PRODUCTS[0].member_tables ?? [],
    );
    expect(g.edges.filter((e) => e.kind === "column_lineage").map((e) => [e.a, e.b])).toEqual([
      ["lc:native:raw.d.Name", "c:9:name"],
      ["c:9:name", "c:3:name"],
    ]);
    const label = (id: string) => g.nodes.find((n) => n.id === id)?.label;
    expect(label("c:3:name")).toBe("pets.name");
    expect(label("c:9:name")).toBe("raw.d.name");
    expect(label("lc:native:raw.d.Name")).toBe("crm · public.D.Name");
    // A column lineage alone reaches is Lineage's: Columns does not draw it.
    const on = (layers: LayerId[]) =>
      visibleGraph(g, new Set<LayerId>(["ontology", ...layers]), NO_FILTERS, new Set(), ONTOLOGY_SCOPE)
        .nodes.map((n) => n.id);
    expect(on(["data", "columns"])).not.toContain("c:9:name");
    // The ontology draws columns as themselves: column lineage needs no table expanded.
    expect(on(["data", "lineage"])).toEqual(expect.arrayContaining(["c:9:name", "c:3:name"]));
  });
});
