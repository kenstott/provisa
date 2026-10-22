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
// role has one for -- a junction a relationship traverses (glossary_term_edges) is no node label,
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
            node_labels: [{ label: "GlossaryTerms", domain_id: "meta" }],
            root_fields: [
              {
                domain_id: "meta",
                table_name: "glossary_terms",
                graphql_field: "m__glossaryTerms",
              },
              {
                domain_id: "meta",
                table_name: "glossary_term_edges",
                graphql_field: "m__glossaryTermEdges",
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
    expect(roots.get(rootKey("meta", "glossary_term_edges"))).toBe("m__glossaryTermEdges");
    expect(roots.get(rootKey("meta", "glossary_terms"))).toBe("m__glossaryTerms");
  });
});
