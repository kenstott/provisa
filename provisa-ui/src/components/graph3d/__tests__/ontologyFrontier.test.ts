// Copyright (c) 2026 Kenneth Stott
// Canary: 9c41e7b2-5d08-4a3f-b6e1-27f0c8d4a915
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT; amended 2026-11-26, A CONNECTED
// ONTOLOGY GRAPH IS ONE QUERY): a term's snippet is one query -- its hops nested under one root
// field, both ways, each end term read whole to the hop count chosen -- read in one round trip and
// opened as that same text; a term met on two paths is merged by id. All hops is walked a hop at a
// time from its root, each term once, and opens as one Cypher variable-length query. Answered by a
// server holding a small term graph with a cycle, a cross link and a hidden term, against the meta
// model's real schema (tests/fixtures/graphql_golden/meta_schema.json).

import { describe, expect, it, vi } from "vitest";
import { ontologyGraph, readTerms } from "../ontologyData";
import { readOntology } from "../ontologyRead";
import { loadOntology } from "../useOntology";
import { snippetLayers, termGraph, termPreset } from "../ontologyPresets";
import {
  NO_FILTERS,
  ONTOLOGY_SCOPE,
  PRODUCT_MAP_SCOPE,
  visibleGraph,
  type GraphData,
  type LayerId,
  type QueryScope,
} from "../graphModel";
import {
  drawnTerms,
  namedTerms,
  ran,
  reader,
  server,
  sorted,
  world,
  type Row,
  type World,
} from "./termServer";

// loadOntology reads through the fixture's server (wired below), as the meta model's schema.
const wire = vi.hoisted(() => ({
  read: null as null | ((q: string) => Promise<unknown>),
  reader: null as null | (() => unknown),
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => wire.reader!(),
  dataGraphql: async (q: string) => wire.read!(q),
}));

// The term graph: 7 the focus; 1 and 2 a step off (1 also links back to 7, and 2 to 1); 3 and 6
// two steps off; 4 three (with a link back to 6); 5 four, past depth 3; 8 retired (hidden),
// reached from 3; 9 linked to nothing. Each term IS one column of a table of its own, which a
// product of its own holds.
const LINKS: [number, number, string][] = [
  [7, 1, "KIND_OF"],
  [1, 7, "RELATED_TO"],
  [2, 7, "PART_OF"],
  [2, 1, "RELATED_TO"],
  [1, 3, "KIND_OF"],
  [6, 2, "PART_OF"],
  [3, 4, "KIND_OF"],
  [4, 6, "RELATED_TO"],
  [4, 5, "KIND_OF"],
  [3, 8, "PART_OF"],
];
const TERM_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9];
const WORLD: World = world({
  products: TERM_IDS.map((id) => ({ id: `p${id}`, name: `product ${id}` })),
  tables: TERM_IDS.map((id) => ({
    id: 100 + id,
    name: `table_${id}`,
    product: `p${id}`,
    columns: ["c"],
  })),
  terms: TERM_IDS.map((id) => ({
    id,
    name: `term ${id}`,
    columns: [[100 + id, "c"]],
    ...(id === 8 ? { status: "retired" } : {}),
  })),
  links: LINKS,
});

/** A term's snippet, as a term's page reads it (REQ-2174, the Ontology preset's rules). */
const snippet = (depth: number): QueryScope => ({
  layers: new Set<LayerId>(["ontology", "domains", "columns", "data"]),
  search: [],
  hiddenStatuses: ["retired"],
  edgeTypes: [],
  domains: [],
  orphans: true,
  tags: [],
  sensitivities: [],
  lanes: false,
  focus: "term:7",
  depth,
});

async function drawn(depth: number) {
  const s = snippet(depth);
  const srv = server(WORLD);
  const got = await readOntology(reader(), s, srv.read);
  return { graph: sorted(ontologyGraph(got.rows, s)), sent: srv.sent, queries: got.queries };
}

// What term:7's snippet draws at depth 3 (REQ-2174, amended 2026-11-12, A HOP IS A STEP BETWEEN
// TERMS): the terms within three steps across the term links (5 is four off; 8 hidden), each with
// its grounding -- its column, that column's table and the product holding it -- whatever the step
// it was reached at, and every link between two of them.
const GROUNDED = [1, 2, 3, 4, 6, 7];
const AT_DEPTH_3 = {
  nodes: [
    ...GROUNDED.flatMap((id) => [`c:${100 + id}:c`, `p:p${id}`, `t:${100 + id}`, `term:${id}`]),
    "d:sales",
  ].sort(),
  edges: [
    ...GROUNDED.flatMap((id) => [
      `has_column:c:${100 + id}:c>t:${100 + id}`,
      `in_domain:term:${id}>d:sales`,
      `member:p:p${id}>t:${100 + id}`,
      `term_binding:term:${id}>c:${100 + id}:c:IS`,
    ]),
    "term_edge:term:1>term:3:KIND_OF",
    "term_edge:term:1>term:7:RELATED_TO",
    "term_edge:term:2>term:1:RELATED_TO",
    "term_edge:term:2>term:7:PART_OF",
    "term_edge:term:3>term:4:KIND_OF",
    "term_edge:term:4>term:6:RELATED_TO",
    "term_edge:term:6>term:2:PART_OF",
    "term_edge:term:7>term:1:KIND_OF",
  ].sort(),
};
/** The one GraphQL query a read opens as. */
const graphqlOf = (opened: Awaited<ReturnType<typeof readOntology>>["opened"]): string => {
  if (opened?.language !== "graphql") throw new Error("not opened as GraphQL");
  return opened.query;
};

describe("a term's snippet, read in one query", () => {
  it("draws every term within three term steps, each with its grounding", async () => {
    expect((await drawn(3)).graph).toEqual(AT_DEPTH_3);
  });

  it("reads every term within three links, each with its grounding, the last hop's too", async () => {
    const { sent } = await drawn(3);
    const [answer] = sent.map((s) => JSON.parse(s.answer) as { _terms: Row[] });
    const read = readTerms(answer._terms as never);
    // Every term within three links, each once once merged by id.
    expect(read.map((t) => t.id).sort()).toEqual(GROUNDED);
    // A term drawn brings its grounding, whatever the hop it is reached at.
    expect(
      read
        .filter((t) => t.refs)
        .map((t) => t.id)
        .sort(),
    ).toEqual(GROUNDED);
  });

  it("sends one query of one root field, whatever the depth", async () => {
    for (const depth of [1, 2, 3, 4]) {
      const { sent, queries } = await drawn(depth);
      expect(sent).toHaveLength(1);
      expect(queries).toEqual([sent[0].query]);
      expect(sent[0].query.split("\n").filter((l) => /^ {2}\w/.test(l))).toHaveLength(1);
    }
  });

  it("stops when a hop reaches nothing new", async () => {
    const s = { ...snippet(3), focus: "term:9" };
    const srv = server(WORLD);
    const got = await readOntology(reader(), s, srv.read);
    expect(srv.sent).toHaveLength(1);
    expect(sorted(ontologyGraph(got.rows, s)).nodes).toContain("term:9");
  });

  it("follows the connected part to its end at All, and stops there", async () => {
    const all = await drawn(Infinity);
    // 5 is four steps off; 8 is hidden; 9 is linked to nothing.
    expect(drawnTerms(all.graph)).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(all.graph).toEqual((await drawn(4)).graph);
    // A hop read past 5 would reach nothing new: none is sent.
    expect(all.sent).toHaveLength(5);
  });
});

/** A product's snippet, as a product's panel and graph read it (the Product map's rules). */
const productSnippet = (depth: number): QueryScope => ({
  ...snippet(depth),
  layers: new Set<LayerId>(snippetLayers({ products: true })),
  products: true,
  focus: "p:p1",
});

describe("a snippet opened as a query: the one query that drew it", () => {
  for (const depth of [1, 2, 3, 4])
    it(`a term's snippet at depth ${depth}`, async () => {
      const s = snippet(depth);
      const got = await readOntology(reader(), s, server(WORLD).read);
      const graph = sorted(ontologyGraph(got.rows, s));
      const opened = graphqlOf(got.opened);
      expect(opened.startsWith("query Ontology {")).toBe(true);
      // The text it read; run, it draws what the snippet drew.
      expect(got.queries).toEqual([opened]);
      expect(namedTerms(opened)).toEqual([7]);
      expect(await ran(WORLD, opened, s)).toEqual(graph);
    });

  it("a term's snippet at All opens as the Cypher walk from its root's terms", async () => {
    const got = await readOntology(reader(), snippet(Infinity), server(WORLD).read);
    expect(got.opened).toEqual({ language: "cypher", roots: [7] });
  });

  it("draws at depth 3 every term within three steps with its grounding", async () => {
    const s = snippet(3);
    const got = await readOntology(reader(), s, server(WORLD).read);
    expect(await ran(WORLD, graphqlOf(got.opened), s)).toEqual(AT_DEPTH_3);
  });

  it("a term's expanded graph (termGraph) opens as what it drew", async () => {
    const p = termGraph(termPreset(7));
    const s: QueryScope = {
      ...snippet(p.depth),
      layers: new Set<LayerId>(snippetLayers(p)),
      focus: p.focus,
    };
    const got = await readOntology(reader(), s, server(WORLD).read);
    const graph = sorted(ontologyGraph(got.rows, s));
    expect(got.queries).toHaveLength(1);
    expect(drawnTerms(graph)).toEqual([1, 2, 7]);
    expect(await ran(WORLD, graphqlOf(got.opened), s)).toEqual(graph);
  });

  // p1 holds term 1's table: 1 is its own term; 7, 2 and 3 a step off; 6 and 4 two; 5 three.
  const PRODUCT_TERMS: [number, number[]][] = [
    [1, [1, 2, 3, 7]],
    [2, [1, 2, 3, 4, 6, 7]],
    [3, [1, 2, 3, 4, 5, 6, 7]],
    [4, [1, 2, 3, 4, 5, 6, 7]],
  ];
  for (const [depth, terms] of PRODUCT_TERMS)
    it(`a product's snippet at depth ${depth}`, async () => {
      const s = productSnippet(depth);
      const got = await readOntology(reader(), s, server(WORLD).read);
      const graph = sorted(ontologyGraph(got.rows, s));
      expect(graph.nodes).toContain("p:p1");
      // A product's own terms are its root, at no hop; each hop a step across the links, nested.
      expect(got.queries).toHaveLength(1);
      expect(drawnTerms(graph)).toEqual(terms);
      expect(await ran(WORLD, graphqlOf(got.opened), s)).toEqual(graph);
    });

  it("a product's snippet at All walks a hop at a time, and opens as Cypher from its terms", async () => {
    const got = await readOntology(reader(), productSnippet(Infinity), server(WORLD).read);
    // The root, then three hops: 5 is three steps off.
    expect(got.queries).toHaveLength(4);
    expect(got.opened).toEqual({ language: "cypher", roots: [1] });
  });

  it("is what the view hands Open as query", async () => {
    wire.reader = reader;
    const s = snippet(3);
    const srv = server(WORLD);
    wire.read = srv.read;
    const loaded = await loadOntology("analyst", s);
    if (loaded === null || loaded.over !== undefined) throw new Error("not read");
    expect(loaded.language).toBe("graphql");
    expect(await ran(WORLD, loaded.query, s)).toEqual(sorted(loaded.data));
    // The count, then the one read it opens as.
    expect(srv.sent.map((x) => x.query)).toEqual([srv.sent[0].query, loaded.query]);
    expect(drawnTerms(sorted(loaded.data))).toEqual(GROUNDED);
  });
});

// REQ-2174 (amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS): the maintainer's chain. test3 is
// PART_OF test2, PART_OF test, RELATED_TO animal breed name, which IS breeds.breed_name, a column of
// a table Pet Health holds. test3 IS kennels.label (Kennel's). breed code IS breeds.breed_code and
// is linked to no term; lineage is RELATED_TO breed code alone.
const TEST3 = 11;
const TEST2 = 12;
const TEST = 13;
const BREED_NAME = 14;
const BREED_CODE = 15;
const LINEAGE = 16;
const CHAIN: World = world({
  products: [
    { id: "ph", name: "Pet Health" },
    { id: "kn", name: "Kennel" },
  ],
  tables: [
    { id: 201, name: "breeds", product: "ph", columns: ["breed_name", "breed_code"] },
    { id: 202, name: "kennels", product: "kn", columns: ["label"] },
  ],
  terms: [
    { id: TEST3, name: "test3", columns: [[202, "label"]] },
    { id: TEST2, name: "test2", columns: [] },
    { id: TEST, name: "test", columns: [] },
    { id: BREED_NAME, name: "animal breed name", columns: [[201, "breed_name"]] },
    { id: BREED_CODE, name: "breed code", columns: [[201, "breed_code"]] },
    { id: LINEAGE, name: "lineage", columns: [] },
  ],
  links: [
    [TEST3, TEST2, "PART_OF"],
    [TEST2, TEST, "PART_OF"],
    [TEST, BREED_NAME, "RELATED_TO"],
    [LINEAGE, BREED_CODE, "RELATED_TO"],
  ],
});

const DEPTHS = [1, 2, 3, 4, Infinity];
const termScope = (depth: number): QueryScope => ({ ...snippet(depth), focus: `term:${TEST3}` });
const productScope = (depth: number): QueryScope => ({ ...productSnippet(depth), focus: "p:ph" });

async function chainRead(s: QueryScope) {
  const got = await readOntology(reader(), s, server(CHAIN).read);
  return { got, graph: ontologyGraph(got.rows, s) };
}
const nodeIds = (g: GraphData) => g.nodes.map((n) => n.id);

describe("a hop is a step between terms", () => {
  it("draws animal breed name with its column, table and product at 3 hops from test3", async () => {
    const at3 = nodeIds((await chainRead(termScope(3))).graph);
    expect(at3).toEqual(
      expect.arrayContaining([`term:${BREED_NAME}`, "c:201:breed_name", "t:201", "p:ph"]),
    );
  });

  it("draws none of them at 2", async () => {
    const at2 = nodeIds((await chainRead(termScope(2))).graph);
    for (const id of [`term:${BREED_NAME}`, "c:201:breed_name", "t:201", "p:ph"])
      expect(at2).not.toContain(id);
    expect(at2).toContain(`term:${TEST}`);
  });

  it("draws the focus's own grounding at 1 hop", async () => {
    const at1 = nodeIds((await chainRead(termScope(1))).graph);
    expect(at1).toEqual(
      expect.arrayContaining([`term:${TEST3}`, "c:202:label", "t:202", "p:kn", `term:${TEST2}`]),
    );
    expect(at1).not.toContain(`term:${TEST}`);
  });

  it("brings no other term of a table reached through a term, at any depth", async () => {
    for (const depth of DEPTHS) {
      const drawnIds = nodeIds((await chainRead(termScope(depth))).graph);
      expect(drawnIds).not.toContain(`term:${BREED_CODE}`);
      expect(drawnIds).not.toContain("c:201:breed_code");
    }
  });

  it("follows the connected part to its end at All", async () => {
    const all = nodeIds((await chainRead(termScope(Infinity))).graph);
    expect(all.filter((id) => id.startsWith("term:")).sort()).toEqual(
      [TEST3, TEST2, TEST, BREED_NAME].map((id) => `term:${id}`).sort(),
    );
  });

  it("draws a product's own tables, columns and terms at 1 hop, and the terms a step from them", async () => {
    const at1 = nodeIds((await chainRead(productScope(1))).graph);
    expect(at1).toEqual(
      expect.arrayContaining([
        "p:ph",
        "t:201",
        "c:201:breed_name",
        "c:201:breed_code",
        `term:${BREED_NAME}`,
        `term:${BREED_CODE}`,
        `term:${TEST}`,
        `term:${LINEAGE}`,
      ]),
    );
    expect(at1).not.toContain(`term:${TEST2}`);
    expect(nodeIds((await chainRead(productScope(2))).graph)).toContain(`term:${TEST2}`);
  });

  for (const [name, scopeAt, rules] of [
    ["a term's snippet", termScope, ONTOLOGY_SCOPE],
    ["a product's snippet", productScope, PRODUCT_MAP_SCOPE],
  ] as const)
    it(`draws by its hops what ${name}'s read asks for, at every depth`, async () => {
      // The whole ontology, read without a focus, by the same rules and layers.
      const whole = (await chainRead({ ...scopeAt(1), focus: null })).graph;
      for (const depth of DEPTHS) {
        const s = scopeAt(depth);
        const view = { ...rules, neighbourhood: { ...rules.neighbourhood, depth } };
        const focus = { ...NO_FILTERS, focus: [s.focus as string] };
        const read = (await chainRead(s)).graph;
        const asked = sorted(visibleGraph(read, s.layers, NO_FILTERS, new Set(), view));
        // The focus's hops over the whole ontology draw what the focused read asked for...
        expect(sorted(visibleGraph(whole, s.layers, focus, new Set(), view))).toEqual(asked);
        // ...and over the read itself prune nothing: the read asks for exactly what is drawn.
        expect(sorted(visibleGraph(read, s.layers, focus, new Set(), view))).toEqual(asked);
      }
    });

  for (const depth of [1, 2, 3])
    it(`opens as the query that drew it at ${depth}`, async () => {
      for (const s of [termScope(depth), productScope(depth)]) {
        const { got, graph } = await chainRead(s);
        expect(got.queries).toEqual([graphqlOf(got.opened)]);
        expect(await ran(CHAIN, graphqlOf(got.opened), s)).toEqual(sorted(graph));
      }
    });

  it("opens as the Cypher walk from its root's terms at All", async () => {
    expect((await chainRead(termScope(Infinity))).got.opened).toEqual({
      language: "cypher",
      roots: [TEST3],
    });
    // Pet Health's own terms: breed name and breed code.
    expect((await chainRead(productScope(Infinity))).got.opened).toEqual({
      language: "cypher",
      roots: [BREED_NAME, BREED_CODE],
    });
  });
});
