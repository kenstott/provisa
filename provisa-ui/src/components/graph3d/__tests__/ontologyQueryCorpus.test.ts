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
import { ontologyCountQuery, ontologyQuery } from "../ontologyData";
import type { LayerId, QueryScope } from "../graphModel";

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

const LAYERS: LayerId[] = ["domains", "data", "columns", "ownerTables"];
const SEARCHES = [[], ["party"]];
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
              `search=${search.join(",")}`,
              `hidden=${hiddenStatuses.join(",")}`,
              `edges=${edgeTypes.join(",")}`,
              `domains=${domains.join(",")}`,
            ].join(" ");
            out.push([
              key,
              {
                layers: new Set<LayerId>(["ontology", ...layers]),
                search,
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
 * and in lanes, to each depth a reader can pick -- and the whole ontology in lanes. */
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
          },
        ]);
      }
  return out;
}

/** REQ-2174 (THE ONE GRAPH'S PRESETS AND LAYERS): the full view with each layer of a product's
 * own parts on, alone and with Data, and all of them at once. */
function productLayerScopes(): [string, QueryScope][] {
  const parts: LayerId[] = ["commands", "quality", "productLinks", "lineage"];
  const sets: LayerId[][] = [
    ...parts.flatMap((l): LayerId[][] => [[l], ["data", l]]),
    ["data", ...parts],
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
  }
  for (const [key, s] of productLayerScopes()) add(`ontology product layers: ${key}`, ontologyQuery(m, s));
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

  it("read a column's table, with the relationships declared from it, under the owner tables", () => {
    // REQ-2135: the owner-tables layer draws the relationships between the tables it draws. A
    // term ref reaches its table through registeredTable; the _meta system field reaches the same
    // type and is no relationship to follow.
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
    const owner = ontologyQuery(m, scope(["columns", "ownerTables"]));
    const columns = ontologyQuery(m, scope(["columns"]));
    expect(owner).not.toEqual(columns);
    const refs = owner!.slice(
      owner!.indexOf("refs: refs"),
      owner!.indexOf("data_products") >= 0 ? owner!.indexOf("data_products") : undefined,
    );
    expect(refs).toContain("table: registeredTable");
    expect(refs).toContain("source_relationships: sourceRelationships");
    expect(refs).not.toContain("_meta");
  });

  it("reads a term's snippet from the term, and a product's from the terms its tables are", () => {
    // REQ-2174: the root names the focus; its neighbourhood is read nested to the depth.
    const m = reader();
    const [, base] = snippetScopes()[0];
    const term = ontologyQuery(m, { ...base, focus: "term:7", depth: 2 }) as string;
    expect(term).toMatch(/ont__terms\(where: \{[^\n]*id: \{ eq: 7 \}/);
    expect(term).toContain("edgesIn");
    const product = ontologyQuery(m, { ...base, focus: "p:1", depth: 2 }) as string;
    expect(product).toContain('productId: { eq: "1" }');
    // The product itself is read, that product alone.
    expect(product).toMatch(/m__dataProducts\(where: \{ id: \{ eq: "1" \} \}\)/);
    // A deeper read nests one more level of term edges.
    const deeper = ontologyQuery(m, { ...base, focus: "term:7", depth: 3 }) as string;
    expect(deeper.split("edgesOut").length).toBeGreaterThan(term.split("edgesOut").length);
  });

  it("each read the system catalog's real root fields", () => {
    const queries = Object.values(corpus());
    expect(queries.length).toBeGreaterThan(10);
    for (const q of queries) expect(q).toMatch(/ont__terms|m__dataProducts/);
  });
});
