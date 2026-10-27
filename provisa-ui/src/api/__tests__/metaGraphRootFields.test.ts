// Copyright (c) 2026 Kenneth Stott
// Canary: 57b029cc-aecc-48a8-ac61-5ea3df6b5617
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: a meta view's root field is the one /data/graph-schema names, for every table the
// role has one for -- a junction a relationship traverses (_term_edges) is no node label,
// and is still queried by its root field.

import { afterEach, describe, expect, it, vi } from "vitest";
import { rootFields, rootKey } from "../metaGraph";

afterEach(() => vi.unstubAllGlobals());

describe("the role's root fields", () => {
  it("come from root_fields, junctions included, asked as the role", async () => {
    const asked: (string | null)[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (_url: string, init?: RequestInit) => {
        asked.push(new Headers(init?.headers).get("X-Provisa-Role"));
        return new Response(
          JSON.stringify({
            node_labels: [{ label: "Terms", domain_id: "ontology" }],
            root_fields: [
              {
                domain_id: "ontology",
                table_name: "_terms",
                graphql_field: "ont__terms",
              },
              {
                domain_id: "ontology",
                table_name: "_term_edges",
                graphql_field: "ont__termEdges",
              },
            ],
            relationship_types: [],
          }),
          { status: 200 },
        );
      }),
    );
    const roots = await rootFields("steward");
    expect(asked).toEqual(["steward"]);
    expect(roots.get(rootKey("ontology", "_term_edges"))).toBe("ont__termEdges");
    expect(roots.get(rootKey("ontology", "_terms"))).toBe("ont__terms");
  });
});
