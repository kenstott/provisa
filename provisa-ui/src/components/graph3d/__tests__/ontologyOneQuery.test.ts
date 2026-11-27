// Copyright (c) 2026 Kenneth Stott
// Canary: 5f2d8c47-1a93-4e6b-b0c7-83e4d9a1f256
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-26, A CONNECTED ONTOLOGY GRAPH IS ONE QUERY): hops are nesting, not
// more root fields. Each edge's target or source term carries the full term selection, nested to
// the chosen hop count under one root field, so the graph is read in one round trip and the query
// it opens as is the text it read. All hops, which no fixed nesting can express, opens as a Cypher
// variable-length query.

import { describe, expect, it, vi } from "vitest";
import { ontologyGraph, ontologyQuery } from "../ontologyData";
import { readOntology } from "../ontologyRead";
import type { LayerId, QueryScope } from "../graphModel";
import { parse, reader, sorted, server, world, type World } from "./termServer";

const wire = vi.hoisted(() => ({
  read: null as null | ((q: string) => Promise<unknown>),
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => (await import("./termServer")).reader(),
  dataGraphql: async (q: string) => wire.read!(q),
}));
// The terms' Cypher label is the server's (/data/graph-schema); here, the meta model's.
vi.mock("../ontologyCypher", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ontologyCypher")>()),
  termsCypherLabel: async () => "Ontology:Terms",
}));

import { loadOntology } from "../useOntology";

// 7 the focus; 1 and 2 a step off, and linked to each other, so 1 is reached from 7 directly and
// through 2 (and 2 through 1): reached by two paths, read twice; 3 two steps off; 4 three; 5 four.
const WORLD: World = world({
  products: [1, 2, 3, 4, 5, 7].map((id) => ({ id: `p${id}`, name: `product ${id}` })),
  tables: [1, 2, 3, 4, 5, 7].map((id) => ({
    id: 100 + id,
    name: `table_${id}`,
    product: `p${id}`,
    columns: ["c"],
  })),
  terms: [1, 2, 3, 4, 5, 7].map((id) => ({ id, name: `term ${id}`, columns: [[100 + id, "c"]] })),
  links: [
    [7, 1, "KIND_OF"],
    [2, 7, "PART_OF"],
    [2, 1, "RELATED_TO"],
    [1, 3, "KIND_OF"],
    [3, 4, "KIND_OF"],
    [4, 5, "KIND_OF"],
  ],
});

const snippet = (depth: number, focus = "term:7"): QueryScope => ({
  layers: new Set<LayerId>(["ontology", "domains", "columns", "data"]),
  search: [],
  hiddenStatuses: ["retired"],
  edgeTypes: [],
  domains: [],
  orphans: true,
  tags: [],
  sensitivities: [],
  focus,
  depth,
});

/** How deep the term selection nests under the root along edgesOut: each level a targetTerm. */
function nesting(query: string): number {
  let deepest = 0;
  const walk = (fields: ReturnType<typeof parse>, level: number) => {
    for (const f of fields) {
      const next = f.name === "targetTerm" && f.fields.some((g) => g.name === "name");
      if (next) deepest = Math.max(deepest, level + 1);
      walk(f.fields, next ? level + 1 : level);
    }
  };
  walk(parse(query), 0);
  return deepest;
}

describe("a focus's hops, nested under one root field", () => {
  for (const depth of [1, 2, 3, 4])
    it(`is one root field, nested ${depth} deep, read once, opened as itself, at ${depth} hops`, async () => {
      const s = snippet(depth);
      const query = ontologyQuery(reader(), s) as string;
      expect(parse(query)).toHaveLength(1);
      expect(nesting(query)).toBe(depth);
      // Both ways: an edge reaching a term brings its source term too.
      expect(query).toContain("sourceTerm {");
      const srv = server(WORLD);
      const got = await readOntology(reader(), s, srv.read);
      expect(srv.sent).toHaveLength(1);
      expect(got.queries).toEqual([query]);
      expect(got.opened).toEqual({ language: "graphql", query });
    });

  it("draws what the hop count reaches, and no further", async () => {
    const terms = async (depth: number) => {
      const s = snippet(depth);
      const got = await readOntology(reader(), s, server(WORLD).read);
      return sorted(ontologyGraph(got.rows, s))
        .nodes.filter((n) => n.startsWith("term:"))
        .sort();
    };
    expect(await terms(1)).toEqual(["term:1", "term:2", "term:7"]);
    expect(await terms(2)).toEqual(["term:1", "term:2", "term:3", "term:7"]);
    expect(await terms(4)).toEqual(["term:1", "term:2", "term:3", "term:4", "term:5", "term:7"]);
  });

  it("merges a term reached by two paths into one, by its id", async () => {
    const s = snippet(2);
    const srv = server(WORLD);
    const got = await readOntology(reader(), s, srv.read);
    // The answer holds term 1 more than once: from 7, and through 2.
    const answer = srv.sent[0].answer;
    expect(answer.match(/"name":"term 1"/g)?.length).toBeGreaterThan(1);
    const ids = (got.rows._terms ?? []).map((t) => t.id);
    expect(ids).toEqual([...new Set(ids)]);
    expect([...ids].sort()).toEqual([1, 2, 3, 7]);
    const g = sorted(ontologyGraph(got.rows, s));
    expect(g.nodes.filter((n) => n === "term:1")).toHaveLength(1);
    expect(g.edges.filter((e) => e === "term_edge:term:2>term:1:RELATED_TO")).toHaveLength(1);
  });
});

describe("All hops", () => {
  it("opens as one Cypher variable-length query from the focus", async () => {
    wire.read = server(WORLD).read;
    const loaded = await loadOntology("analyst", snippet(Infinity));
    if (loaded === null || loaded.over !== undefined) throw new Error("not drawn");
    expect(loaded.language).toBe("cypher");
    expect(loaded.query).toBe("MATCH p = (t:Ontology:Terms {id: 7})-[*]-(n) RETURN p");
    // It draws the connected part to its end.
    expect(
      loaded.data.nodes
        .filter((n) => n.type === "term")
        .map((n) => n.id)
        .sort(),
    ).toEqual(["term:1", "term:2", "term:3", "term:4", "term:5", "term:7"]);
  });

  it("opens a fixed hop count as its GraphQL", async () => {
    wire.read = server(WORLD).read;
    const loaded = await loadOntology("analyst", snippet(2));
    if (loaded === null || loaded.over !== undefined) throw new Error("not drawn");
    expect(loaded.language).toBe("graphql");
    expect(loaded.query).toBe(ontologyQuery(reader(), snippet(2)));
  });
});
