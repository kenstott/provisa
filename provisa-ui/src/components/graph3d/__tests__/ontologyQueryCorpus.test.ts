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
import { ontologyCountQuery, ontologyQuery } from "../glossaryGraphData";
import type { LayerId, QueryScope } from "../graphModel";
import { productGraphQuery, type ProductFilters } from "../productGraphData";

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
              },
            ]);
          }
  return out;
}

/** The product graph's: one product (a member's start) or all, under its filters. */
function productScopes(): [string, string | null, ProductFilters][] {
  const out: [string, string | null, ProductFilters][] = [];
  for (const product of [null, "1"])
    for (const domains of DOMAINS)
      for (const tags of [[], ["pii"]])
        for (const sensitivities of [[], ["confidential"]])
          out.push([
            `product=${product ?? "*"} domains=${domains.join(",")} tags=${tags.join(",")} sensitive=${sensitivities.join(",")}`,
            product,
            { domains, tags, sensitivities, edgeTypes: [] },
          ]);
  return out;
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
  for (const [key, product, f] of productScopes())
    add(`product graph: ${key}`, productGraphQuery(m, product, f));
  return out;
}

describe("the Ontology View's and the product graph's queries, as the GraphQL corpus holds them", () => {
  it("are the ones the builders produce now", () => {
    const produced = corpus();
    const path = resolve(GOLDEN, "ontology_queries.json");
    if (process.env.PROVISA_GRAPHQL_GOLDEN_CAPTURE)
      writeFileSync(path, `${JSON.stringify(produced, null, 1)}\n`);
    expect(JSON.parse(readFileSync(path, "utf8"))).toEqual(produced);
  });

  it("each read the meta model's real root fields", () => {
    const queries = Object.values(corpus());
    expect(queries.length).toBeGreaterThan(10);
    for (const q of queries) expect(q).toMatch(/m__(glossaryTerms|dataProducts)/);
  });
});
