// Copyright (c) 2026 Kenneth Stott
// Canary: 5f2d8c47-1a93-4e6b-b0c7-83e4d9a1f256
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-27, THE ONE QUERY IS A REACH FILTER; supersedes 2026-11-26's nesting):
// a connected ontology graph is one root field whose where keeps the terms within the chosen number
// of hops of the focus, each term read once with its full selection and its edges' far ends by id,
// in one round trip; the query it opens as is that text. All hops, which no fixed filter can
// express, opens as a Cypher variable-length query.

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
// The terms' Cypher label and link types are the server's (/data/graph-schema); here, the meta
// model's.
vi.mock("../ontologyCypher", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ontologyCypher")>()),
  termsCypherGraph: async () => ({
    label: "Ontology:Terms",
    links: ["KIND_OF", "PART_OF", "RELATED_TO"],
  }),
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

/** The terms read whole under ``query``'s root: an edge's far end is its id alone. */
function wholeEnds(query: string): number {
  let n = 0;
  const walk = (fields: ReturnType<typeof parse>) => {
    for (const f of fields) {
      if ((f.name === "targetTerm" || f.name === "sourceTerm") && f.fields.length > 1) n++;
      walk(f.fields);
    }
  };
  walk(parse(query));
  return n;
}

/** How many steps the root's where reaches from the focus: its where names the focus once, then
 * twice over for each step, 2^(depth + 1) - 1 times. */
const reach = (query: string) =>
  Math.log2((query.match(/\{ id: \{ eq: 7 \} \}/g)?.length ?? 0) + 1) - 1;

describe("a focus's hops, one root field kept by its reach", () => {
  for (const depth of [1, 2, 3, 4])
    it(`is one root field reaching ${depth} hops, each term read once, opened as itself`, async () => {
      const s = snippet(depth);
      const query = ontologyQuery(reader(), s) as string;
      expect(parse(query)).toHaveLength(1);
      expect(wholeEnds(query)).toBe(0);
      expect(reach(query)).toBe(depth);
      // Both ways: an edge reaching a term is read too, its source by id.
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

  it("reads a term reached by two paths once, and drops an edge to a term not read", async () => {
    const s = snippet(1);
    const srv = server(WORLD);
    const got = await readOntology(reader(), s, srv.read);
    // Term 1 is a step from 7 directly and through 2's link: in the answer once.
    const answer = srv.sent[0].answer;
    expect(answer.match(/"name":"term 1"/g)).toHaveLength(1);
    const ids = (got.rows._terms ?? []).map((t) => t.id);
    expect([...ids].sort()).toEqual([1, 2, 7]);
    const g = sorted(ontologyGraph(got.rows, s));
    expect(g.edges.filter((e) => e === "term_edge:term:2>term:1:RELATED_TO")).toHaveLength(1);
    // 1's edge to 3, two steps off, is read with 1 and not drawn: 3 was not read.
    expect(answer).toContain('"toTermId":3');
    expect(g.nodes).not.toContain("term:3");
    expect(g.edges.some((e) => e.includes("term:3"))).toBe(false);
  });
});

describe("All hops", () => {
  it("opens as one Cypher variable-length query from the focus", async () => {
    wire.read = server(WORLD).read;
    const loaded = await loadOntology("analyst", snippet(Infinity));
    if (loaded === null || loaded.over !== undefined) throw new Error("not drawn");
    expect(loaded.language).toBe("cypher");
    // Typed to the term links, its ends terms, four steps out: as far as the walk went (5 is four
    // steps from 7), each term reached once and the links between them, never every path.
    expect(loaded.query).toBe(
      "MATCH (t:Ontology:Terms {id: 7})-[:KIND_OF|PART_OF|RELATED_TO*0..4]-(a:Ontology:Terms) " +
        "OPTIONAL MATCH (a)-[r:KIND_OF|PART_OF|RELATED_TO]->(b:Ontology:Terms) RETURN DISTINCT a, r, b",
    );
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

describe("every Hops value opens as Cypher too (REQ-2136, REQ-2174)", () => {
  for (const depth of [1, 2, 3])
    it(`walks ${depth} hops from the focus, as the view reads`, async () => {
      wire.read = server(WORLD).read;
      const loaded = await loadOntology("analyst", snippet(depth));
      if (loaded === null || loaded.over !== undefined) throw new Error("not drawn");
      expect(loaded.walk).toEqual({ roots: [7], depth, edgeTypes: [] });
    });

  it("walks All's own depth: as far as its walk went", async () => {
    wire.read = server(WORLD).read;
    const loaded = await loadOntology("analyst", snippet(Infinity));
    if (loaded === null || loaded.over !== undefined) throw new Error("not drawn");
    expect(loaded.walk).toEqual({ roots: [7], depth: 4, edgeTypes: [] });
  });

  it("with no focus and no search, reads every term its filters keep at no hop", async () => {
    wire.read = server(WORLD).read;
    const loaded = await loadOntology("analyst", {
      ...snippet(2),
      focus: null,
      edgeTypes: ["KIND_OF"],
    });
    if (loaded === null || loaded.over !== undefined) throw new Error("not drawn");
    expect(loaded.walk).toEqual({ roots: [1, 2, 3, 4, 5, 7], depth: 0, edgeTypes: ["KIND_OF"] });
  });
});
