// Copyright (c) 2026 Kenneth Stott
// Canary: 53bcc83c-8a35-463e-9288-2ddb8a8bc380
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135, REQ-2136: the queries the Ontology View and the product graph build, over a fixed grid
// of their scopes, against the meta model's real schema (tests/fixtures/graphql_golden/
// meta_schema.json, the server's own registration read back). They are the GraphQL corpus's
// Ontology part: tests/fixtures/graphql_golden/ontology_queries.json holds them, and this test
// holds the file to what the builders produce now, so it cannot drift. Run with
// PROVISA_GRAPHQL_GOLDEN_CAPTURE=1 to write it.

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { RoleSchema, rootKey, type Introspection, type MetaReader } from "../../../api/metaGraph";
import {
  ontologyCountQuery,
  ontologyHopQuery,
  ontologyOpenedQuery,
  ontologyQuery,
  termLevels,
} from "../ontologyData";
import type { LayerId, QueryScope } from "../graphModel";
import { searchItems } from "../../../lib/searchItems";

const GOLDEN = resolve(__dirname, "../../../../../tests/fixtures/graphql_golden");

interface MetaSchemaDoc {
  introspection: Introspection;
  root_fields: { domain_id: string; table_name: string; graphql_field: string }[];
}

function reader(): MetaReader {
  const doc = JSON.parse(
    readFileSync(resolve(GOLDEN, "meta_schema.json"), "utf8"),
  ) as MetaSchemaDoc;
  return {
    schema: new RoleSchema(doc.introspection),
    roots: new Map(
      doc.root_fields.map((r) => [rootKey(r.domain_id, r.table_name), r.graphql_field]),
    ),
  };
}

/** Every subset of ``items``, the empty one first. */
function subsets<T>(items: T[]): T[][] {
  return items.reduce<T[][]>((all, x) => [...all, ...all.map((s) => [...s, x])], [[]]);
}

const LAYERS: LayerId[] = ["domains", "data", "columns"];
const SEARCHES = ["", "party"];
const STATUSES: QueryScope["hiddenStatuses"][] = [
  [],
  ["retired"],
  ["draft", "deprecated", "retired"],
];
const EDGE_TYPES = [[], ["KIND_OF", "PART_OF"]];
const DOMAINS = [[], ["sales"]];

/** The grid of the Ontology View's scopes: its layers (the terms' always), a search on or off
 * (a search reads the matches' neighbourhood to its depth), the statuses hidden, the edge types
 * kept and a domain filter. */
function ontologyScopes(): [string, QueryScope][] {
  const out: [string, QueryScope][] = [];
  for (const layers of subsets(LAYERS))
    for (const search of SEARCHES)
      for (const hiddenStatuses of STATUSES)
        for (const edgeTypes of EDGE_TYPES)
          for (const domains of DOMAINS) {
            const key = [
              ["ontology", ...layers].join("+"),
              `search=${search}`,
              `hidden=${hiddenStatuses.join(",")}`,
              `edges=${edgeTypes.join(",")}`,
              `domains=${domains.join(",")}`,
            ].join(" ");
            out.push([
              key,
              {
                layers: new Set<LayerId>(["ontology", ...layers]),
                search: searchItems(search),
                hiddenStatuses,
                edgeTypes,
                domains,
                orphans: true,
                tags: [],
                sensitivities: [],
                focus: null,
                depth: 2,
              },
            ]);
          }
  return out;
}

/** REQ-2174 (ONE ONTOLOGY GRAPH): the snippets' -- a term's page and a product's panel, in 3D
 * and in lanes, to each depth a reader can pick -- and the whole ontology in lanes. A product's
 * panel reads by the Product map's rules (ONE GRAPH, RULES PER PRESET), a term's page by the
 * Ontology preset's. */
function snippetScopes(): [string, QueryScope][] {
  const out: [string, QueryScope][] = [];
  for (const focus of [null, "term:7", "p:1"])
    for (const lanes of [false, true])
      for (const depth of [1, 2, 3, 4]) {
        if (focus === null && depth !== 2) continue;
        out.push([
          `focus=${focus ?? "*"} lanes=${lanes} depth=${depth}`,
          {
            layers: new Set<LayerId>(["ontology", "domains", "columns", "data"]),
            search: [],
            hiddenStatuses: ["retired"],
            edgeTypes: [],
            domains: [],
            orphans: true,
            tags: [],
            sensitivities: [],
            lanes,
            focus,
            depth,
            ...(focus?.startsWith("p:") ? { products: true } : {}),
          },
        ]);
      }
  return out;
}

/** REQ-2174 (THE ONE GRAPH'S PRESETS AND LAYERS): the Product map with each layer of a product's
 * own parts on, alone and with Data, and all of them at once; and its Tables, with Columns. */
function productLayerScopes(): [string, QueryScope][] {
  const parts: LayerId[] = ["commands", "quality", "productLinks", "lineage"];
  const sets: LayerId[][] = [
    ...parts.flatMap((l): LayerId[][] => [[l], ["data", l]]),
    ["data", ...parts],
    ["ownerTables"],
    ["data", "ownerTables"],
    // REQ-2174 (amended 2026-11-07): the tables related to those drawn, with their products.
    ["related"],
    ["data", "related"],
  ];
  return sets.map((layers) => [
    `layers=${layers.join("+")}`,
    {
      layers: new Set<LayerId>(["ontology", "domains", "columns", ...layers]),
      search: [],
      hiddenStatuses: ["retired"],
      edgeTypes: [],
      domains: [],
      orphans: true,
      tags: [],
      sensitivities: [],
      focus: null,
      depth: 2,
      products: true,
    },
  ]);
}

/** Each distinct query the builders produce over the grid, by the first scope that builds it. */
function corpus(): Record<string, string> {
  const m = reader();
  const out: Record<string, string> = {};
  const seen = new Set<string>();
  const add = (key: string, q: string | null) => {
    if (q === null || seen.has(q)) return;
    seen.add(q);
    out[key] = q;
  };
  for (const [key, s] of ontologyScopes()) {
    add(`ontology: ${key}`, ontologyQuery(m, s));
    add(`ontology count: ${key}`, ontologyCountQuery(m, s));
  }
  for (const [key, s] of snippetScopes()) {
    add(`ontology snippet: ${key}`, ontologyQuery(m, s));
    add(`ontology snippet count: ${key}`, ontologyCountQuery(m, s));
    // REQ-2174: each hop past the root, by the ids of the terms it reaches.
    for (let hop = 1; hop <= termLevels(s); hop++)
      add(`ontology snippet hop ${hop}: ${key}`, ontologyHopQuery(m, s, hop, [1, 2]));
    // REQ-2136: what the snippet opens as -- its root and every hop -- in one query.
    const hops = Array.from({ length: termLevels(s) }, (_, i) => [2 * i + 1, 2 * i + 2]);
    add(`ontology snippet opened: ${key}`, ontologyOpenedQuery(m, s, hops));
  }
  for (const [key, s] of productLayerScopes())
    add(`ontology product layers: ${key}`, ontologyQuery(m, s));
  return out;
}

describe("the ontology's queries, whole and as snippets, as the GraphQL corpus holds them", () => {
  it("are the ones the builders produce now", () => {
    const produced = corpus();
    const path = resolve(GOLDEN, "ontology_queries.json");
    if (process.env.PROVISA_GRAPHQL_GOLDEN_CAPTURE)
      writeFileSync(path, `${JSON.stringify(produced, null, 1)}\n`);
    expect(JSON.parse(readFileSync(path, "utf8"))).toEqual(produced);
  });

  it("read a column's table with its product, and no relationship declared from it", () => {
    // REQ-2130 (amended 2026-10-29): Data draws the product of each table a term reaches; no
    // relationship between tables is drawn. A term ref reaches its table through registeredTable;
    // the _meta system field reaches the same type and is no relationship to follow.
    const m = reader();
    const scope = (layers: LayerId[]): QueryScope => ({
      layers: new Set<LayerId>(["ontology", ...layers]),
      search: [],
      hiddenStatuses: [],
      edgeTypes: [],
      domains: [],
      orphans: true,
      tags: [],
      sensitivities: [],
      focus: null,
      depth: 2,
    });
    const data = ontologyQuery(m, scope(["columns", "data"]));
    const columns = ontologyQuery(m, scope(["columns"]));
    expect(data).not.toEqual(columns);
    expect(data).not.toContain("data_products");
    const refs = data!.slice(data!.indexOf("refs {"));
    expect(refs).toContain("registeredTable {");
    expect(refs).toContain("dataProductTables {");
    expect(refs).toContain("dataProduct {");
    expect(refs).not.toContain("source_relationships");
    expect(refs).not.toContain("_meta");
  });

  it("reads a term's snippet from the term, and a product's from the terms its tables are", () => {
    // REQ-2174: the root names the focus; its neighbourhood is read a hop at a time to the depth.
    const m = reader();
    const [, base] = snippetScopes()[0];
    const term = ontologyQuery(m, { ...base, focus: "term:7", depth: 2 }) as string;
    expect(term).toMatch(/on_terms\(where: \{[^\n]*id: \{ eq: 7 \}/);
    expect(term).toContain("edgesIn");
    const product = ontologyQuery(m, {
      ...base,
      focus: "p:1",
      depth: 2,
      products: true,
    }) as string;
    expect(product).toContain('productId: { eq: "1" }');
    // The product itself is read, that product alone.
    expect(product).toMatch(/m__dataProducts\(where: \{ id: \{ eq: "1" \} \}\)/);
    // A deeper read reads one more hop, its root the same: no level is nested under another.
    const deeper = { ...base, focus: "term:7", depth: 3 };
    expect(ontologyQuery(m, deeper)).toBe(term);
    const hop = ontologyHopQuery(m, deeper, 3, [1, 2]) as string;
    expect(hop).toMatch(/on_terms\(where: \{ id: \{ in: \[1,2\] \} \}\)/);
    expect(hop.split("edgesOut")).toHaveLength(2);
  });

  it("read no product apart and no relationship between tables by the Ontology preset's rules", () => {
    // REQ-2174 (ONE GRAPH, RULES PER PRESET): the Ontology page and a term's page draw only what a
    // shown term reaches (REQ-2130); the Product map and a product's panel keep its products.
    const m = reader();
    const ontology = [...ontologyScopes(), ...snippetScopes()].filter(([, s]) => !s.products);
    expect(ontology.length).toBeGreaterThan(10);
    for (const [, s] of ontology) {
      const q = ontologyQuery(m, {
        ...s,
        layers: new Set([...s.layers, "ownerTables", "related"]),
      }) as string;
      expect(q).not.toContain("m__dataProducts");
      expect(q).not.toContain("sourceRelationships");
    }
    // The Product map reads relationships only under Related tables, each with the table it reaches.
    const [, tables] = productLayerScopes().find(([k]) => k === "layers=data+ownerTables")!;
    expect(ontologyQuery(m, tables) as string).not.toContain("sourceRelationships");
    const [, map] = productLayerScopes().find(([k]) => k === "layers=data+related")!;
    const mapped = ontologyQuery(m, map) as string;
    expect(mapped).toContain("m__dataProducts");
    expect(mapped).toMatch(/dataProductTables[^{]*\{\s*registeredTable \{/);
    expect(mapped).toContain("sourceRelationships");
    expect(mapped).toMatch(/sourceRelationships \{[^]*targetTable \{/);
    // The product holding a related table is read through the membership junction.
    expect(mapped).toMatch(/targetTable \{[^]*dataProductTables[^{]*\{\s*dataProduct \{/);
  });

  it("each read the system catalog's real root fields", () => {
    const queries = Object.values(corpus());
    expect(queries.length).toBeGreaterThan(10);
    for (const q of queries) expect(q).toMatch(/on_terms|m__dataProducts/);
  });
});

/** REQ-2143 (PAGE SEARCH USES THE WEB-SEARCH SYNTAX): the Ontology View's root under each search
 * the shared cases hold (tests/fixtures/page_search_cases.json), as test_ontology_search.py runs
 * it on DuckDB: the terms it keeps are the ones the page's own filter keeps. */
describe("the Ontology View's root under each shared search case", () => {
  it("is the query the builder produces now", () => {
    const cases = (
      JSON.parse(readFileSync(resolve(GOLDEN, "../page_search_cases.json"), "utf8")) as {
        cases: { query: string }[];
      }
    ).cases;
    const m = reader();
    const produced: Record<string, string | null> = {};
    for (const { query } of cases)
      produced[query] = ontologyCountQuery(m, {
        layers: new Set<LayerId>(["ontology"]),
        search: searchItems(query),
        hiddenStatuses: [],
        edgeTypes: [],
        domains: [],
        orphans: true,
        tags: [],
        sensitivities: [],
        focus: null,
        depth: 0,
      });
    const path = resolve(GOLDEN, "ontology_search_queries.json");
    if (process.env.PROVISA_GRAPHQL_GOLDEN_CAPTURE)
      writeFileSync(path, `${JSON.stringify(produced, null, 1)}\n`);
    expect(JSON.parse(readFileSync(path, "utf8"))).toEqual(produced);
  });
});
