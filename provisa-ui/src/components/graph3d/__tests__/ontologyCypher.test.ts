// Copyright (c) 2026 Kenneth Stott
// Canary: d2c65bfe-825b-4c8e-b30c-1bfa219f2d4e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-26): All hops opens as one Cypher variable-length query from the terms
// the root read. It walks the term links alone -- typed, its ends terms, as far as the view's walk
// went -- and answers each term reached once with the links between them, never every path
// through the whole graph (an untyped, unbounded [*] timed out over 185k nodes, 2026-11-28).

import { afterEach, describe, expect, it, vi } from "vitest";
import { allHopsCypher, termsCypherGraph } from "../ontologyCypher";

vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => ({ schema: null, roots: new Map() }),
  rootQuery: () => "query OntologyTermsTable { registered_tables { id } }",
  dataGraphql: async () => ({ registered_tables: [{ id: 41 }] }),
}));

const GRAPH = { label: "Ontology:Terms", links: ["KIND_OF", "PART_OF", "RELATED_TO"] };

describe("allHopsCypher", () => {
  it("walks a focus's term links to the walk's depth, answering terms and links once", () => {
    expect(allHopsCypher(GRAPH, [7], 4, [])).toBe(
      "MATCH (t:Ontology:Terms {id: 7})-[:KIND_OF|PART_OF|RELATED_TO*0..4]-(a:Ontology:Terms) " +
        "OPTIONAL MATCH (a)-[r:KIND_OF|PART_OF|RELATED_TO]->(b:Ontology:Terms) RETURN DISTINCT a, r, b",
    );
  });

  it("walks from several roots, in id order", () => {
    expect(allHopsCypher(GRAPH, [16, 15], 2, [])).toBe(
      "MATCH (t:Ontology:Terms)-[:KIND_OF|PART_OF|RELATED_TO*0..2]-(a:Ontology:Terms) " +
        "WHERE t.id IN [15, 16] " +
        "OPTIONAL MATCH (a)-[r:KIND_OF|PART_OF|RELATED_TO]->(b:Ontology:Terms) RETURN DISTINCT a, r, b",
    );
  });

  it("keeps only the link types the view keeps", () => {
    expect(allHopsCypher(GRAPH, [7], 3, ["PART_OF", "KIND_OF", "TRANSLATION_OF"])).toBe(
      "MATCH (t:Ontology:Terms {id: 7})-[:KIND_OF|PART_OF*0..3]-(a:Ontology:Terms) " +
        "OPTIONAL MATCH (a)-[r:KIND_OF|PART_OF]->(b:Ontology:Terms) RETURN DISTINCT a, r, b",
    );
  });

  it("reads the roots and their links among themselves when the walk took no hop", () => {
    expect(allHopsCypher(GRAPH, [1, 2], 0, [])).toBe(
      "MATCH (t:Ontology:Terms)-[:KIND_OF|PART_OF|RELATED_TO*0..0]-(a:Ontology:Terms) " +
        "WHERE t.id IN [1, 2] " +
        "OPTIONAL MATCH (a)-[r:KIND_OF|PART_OF|RELATED_TO]->(b:Ontology:Terms) RETURN DISTINCT a, r, b",
    );
  });

  it("reads the roots alone when no link type the view keeps is served", () => {
    expect(allHopsCypher(GRAPH, [7], 2, ["ANTONYM_OF"])).toBe(
      "MATCH (a:Ontology:Terms {id: 7}) RETURN a",
    );
    expect(allHopsCypher({ label: "Terms", links: [] }, [2, 1], 2, [])).toBe(
      "MATCH (a:Terms) WHERE a.id IN [1, 2] RETURN a",
    );
  });

  it("refuses a walk of no roots or of a depth that is not a whole number of hops", () => {
    expect(() => allHopsCypher(GRAPH, [], 2, [])).toThrow(/none/);
    expect(() => allHopsCypher(GRAPH, [7], Infinity, [])).toThrow(/whole number/);
    expect(() => allHopsCypher(GRAPH, [7], -1, [])).toThrow(/whole number/);
  });
});

describe("termsCypherGraph", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("takes the terms' label and the term edges junction's types from the server", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        node_labels: [
          { label: "Meta:TableColumns", table_id: 9 },
          { label: "Ontology:Terms", table_id: 41 },
        ],
        relationship_types: [
          {
            type: "RELATED_TO",
            source: "Ontology:Terms",
            target: "Ontology:Terms",
            junction_table_name: "_term_edges",
          },
          {
            type: "KIND_OF",
            source: "Ontology:Terms",
            target: "Ontology:Terms",
            junction_table_name: "_term_edges",
          },
          // A term's columns: grounding, not a link the walk takes.
          {
            type: "REFERENCED_BY",
            source: "Ontology:Terms",
            target: "Meta:TableColumns",
            junction_table_name: "_term_refs",
          },
          // Term to term by a key, not a row of the term edges junction.
          {
            type: "REPLACED_BY",
            source: "Ontology:Terms",
            target: "Ontology:Terms",
            junction_table_name: null,
          },
        ],
      }),
    }));
    vi.stubGlobal("fetch", fetchMock);
    expect(await termsCypherGraph("analyst")).toEqual({
      label: "Ontology:Terms",
      links: ["KIND_OF", "RELATED_TO"],
    });
    expect(fetchMock).toHaveBeenCalledWith("/data/graph-schema", {
      headers: { "X-Provisa-Role": "analyst" },
    });
  });
});
