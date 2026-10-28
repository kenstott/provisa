// Copyright (c) 2026 Kenneth Stott
// Canary: 4c1e8a72-9b3d-4f60-a2e5-7d0b6c93f218
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174, REQ-2143: the one graph counts its root terms before it reads them. When the count
// finds none -- a new product's snippet, whose tables bind no term -- nothing nests under the root,
// so the read is the product alone, not the empty neighbourhood's nested edges.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { RoleSchema, rootKey, type Introspection, type MetaReader } from "../../../api/metaGraph";
import type { LayerId, QueryScope } from "../graphModel";

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

const sent = vi.hoisted(() => ({ queries: [] as string[], terms: [] as unknown[] }));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => reader(),
  dataGraphql: async (query: string) => {
    sent.queries.push(query);
    if (query.startsWith("query OntologyCount")) return { _terms: sent.terms };
    return {
      ...(query.includes("_terms:") ? { _terms: [] } : {}),
      data_products: [{ id: "fresh", name: "Fresh", domain_id: "pet-store", status: "draft" }],
    };
  },
}));

import { ontologyQuery } from "../ontologyData";
import { loadOntology, SNIPPET_LAYERS, type Loaded } from "../useOntology";

// What a read within the budget drew.
const drawn = (loaded: Loaded | null) =>
  loaded !== null && loaded.over === undefined ? loaded.data : null;

const scope = (focus: string | null, layers: LayerId[] = SNIPPET_LAYERS): QueryScope => ({
  layers: new Set<LayerId>(layers),
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

beforeEach(() => {
  sent.queries.length = 0;
  sent.terms = [];
});

describe("a read whose root has no terms", () => {
  it("reads the products alone, or nothing when it reads no product", () => {
    const m = reader();
    const products = ontologyQuery(m, scope("p:fresh"), false) as string;
    expect(products).toMatch(/data_products: \w+\(where: \{ id: \{ eq: "fresh" \} \}\)/);
    expect(products).not.toContain("_terms:");
    expect(ontologyQuery(m, scope("term:7"), false)).toBeNull();
  });

  it("draws a new product's snippet from the product, with no term read", async () => {
    const loaded = await loadOntology("analyst", scope("p:fresh"));
    expect(sent.queries).toHaveLength(2);
    expect(sent.queries[1]).not.toContain("_terms:");
    expect(sent.queries[1]).not.toContain("edgesOut");
    expect(loaded?.over).toBeUndefined();
    expect(drawn(loaded)?.nodes.map((n) => n.id)).toEqual(["p:fresh"]);
    // What the view opens as a query is still the whole read.
    expect(loaded?.query).toContain("_terms:");
  });

  it("reads nothing more for a term's snippet whose term is gone", async () => {
    const loaded = await loadOntology("analyst", scope("term:7"));
    expect(sent.queries).toHaveLength(1);
    expect(drawn(loaded)?.nodes).toEqual([]);
  });

  it("reads the terms nested as before once the count finds one", async () => {
    sent.terms = [{ id: 7, domains: [] }];
    await loadOntology("analyst", scope("p:fresh"));
    expect(sent.queries[1]).toContain("_terms:");
    expect(sent.queries[1]).toContain("edgesOut");
  });
});
