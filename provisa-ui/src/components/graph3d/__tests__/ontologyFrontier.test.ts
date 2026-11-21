// Copyright (c) 2026 Kenneth Stott
// Canary: 9c41e7b2-5d08-4a3f-b6e1-27f0c8d4a915
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT): a term's snippet is read hop
// by hop, each hop once -- the terms its frontier's edges reach that no hop has read, by their
// ids -- so the read grows with what is drawn, not some 2.5 times a hop (R509-C2's residual: nested
// edgesOut and edgesIn at each level, each path's terms read again with their refs). Answered by a
// server holding a small term graph with a cycle, a cross link and a hidden term, against the meta
// model's real schema (tests/fixtures/graphql_golden/meta_schema.json).

import { describe, expect, it, vi } from "vitest";
import { ontologyGraph } from "../ontologyData";
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
// The nested read's size against this graph: its query and its answer.
const NESTED = { query: 19_553, answer: 26_720 };

describe("a term's snippet, read a hop at a time", () => {
  it("draws every term within three term steps, each with its grounding", async () => {
    expect((await drawn(3)).graph).toEqual(AT_DEPTH_3);
  });

  it("reads each term once, each with its grounding, the last hop's too", async () => {
    const { sent } = await drawn(3);
    const answers = sent.map((s) => JSON.parse(s.answer) as { _terms: Row[] });
    const read = answers.flatMap((a) => a._terms.map((t) => t.id as number));
    // Every term within three links, each once: the root, then each hop's frontier.
    expect(read.sort()).toEqual(GROUNDED);
    const refsRead = answers.flatMap((a) => a._terms.filter((t) => t.refs).map((t) => t.id));
    // A term drawn brings its grounding, whatever the hop it is reached at.
    expect(refsRead.sort()).toEqual(GROUNDED);
  });

  it("sends one query a hop, each the same size whatever the depth", async () => {
    const three = await drawn(3);
    const four = await drawn(4);
    expect(three.sent).toHaveLength(4);
    expect(three.queries).toEqual(three.sent.map((s) => s.query));
    // A hop's query names its frontier's ids, so it is no bigger a hop further out.
    const widest = Math.max(...three.sent.map((s) => s.query.length));
    expect(widest).toBeLessThan(3000);
    for (const s of four.sent) expect(s.query.length).toBeLessThan(3000);
    const total = three.sent.reduce((n, s) => n + s.query.length, 0);
    expect(total).toBeLessThan(NESTED.query / 2);
    // Each term's answer once: the root's, then each hop's frontier.
    const answered = three.sent.reduce((n, s) => n + s.answer.length, 0);
    expect(answered).toBeLessThan(NESTED.answer / 2);
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

describe("a snippet opened as a query: every hop the snippet drew, in one query", () => {
  for (const depth of [1, 2, 3, Infinity])
    it(`a term's snippet at depth ${depth}`, async () => {
      const s = snippet(depth);
      const got = await readOntology(reader(), s, server(WORLD).read);
      const graph = sorted(ontologyGraph(got.rows, s));
      const opened = got.opened as string;
      expect(opened.startsWith("query Ontology {")).toBe(true);
      // It names every term drawn and nothing past them; run, it draws what the snippet drew.
      expect(namedTerms(opened)).toEqual(drawnTerms(graph));
      expect(await ran(WORLD, opened, s)).toEqual(graph);
      // Each hop's read once, not a level nested under each path: no bigger than the reads sent.
      const sent = got.queries.reduce((n, q) => n + q.length, 0);
      expect(opened.length).toBeLessThan(sent);
      expect(opened.length).toBeLessThan(NESTED.query / 2);
    });

  it("draws at depth 3 every term within three steps with its grounding", async () => {
    const s = snippet(3);
    const got = await readOntology(reader(), s, server(WORLD).read);
    expect(await ran(WORLD, got.opened as string, s)).toEqual(AT_DEPTH_3);
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
    expect(got.queries.length).toBeGreaterThan(1);
    expect(namedTerms(got.opened as string)).toEqual(drawnTerms(graph));
    expect(await ran(WORLD, got.opened as string, s)).toEqual(graph);
  });

  // p1 holds term 1's table: 1 is its own term; 7, 2 and 3 a step off; 6 and 4 two; 5 three.
  const PRODUCT_HOPS: [number, number][] = [
    [1, 1],
    [2, 2],
    [3, 3],
    [4, 3],
    [Infinity, 3],
  ];
  for (const [depth, hops] of PRODUCT_HOPS)
    it(`a product's snippet at depth ${depth}`, async () => {
      const s = productSnippet(depth);
      const got = await readOntology(reader(), s, server(WORLD).read);
      const graph = sorted(ontologyGraph(got.rows, s));
      expect(graph.nodes).toContain("p:p1");
      // A product's own terms are its root, at no hop; each hop is a step across the links.
      expect(got.queries).toHaveLength(1 + hops);
      expect(await ran(WORLD, got.opened as string, s)).toEqual(graph);
    });

  it("is what the view hands Open as query", async () => {
    wire.reader = reader;
    wire.read = server(WORLD).read;
    const s = snippet(3);
    const loaded = await loadOntology("analyst", s);
    if (loaded === null || loaded.over !== undefined) throw new Error("not read");
    expect(await ran(WORLD, loaded.query, s)).toEqual(sorted(loaded.data));
    expect(namedTerms(loaded.query)).toEqual(GROUNDED);
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

  for (const depth of [1, 2, 3, Infinity])
    it(`opens as a query of what it drew at ${depth}`, async () => {
      for (const s of [termScope(depth), productScope(depth)]) {
        const { got, graph } = await chainRead(s);
        expect(await ran(CHAIN, got.opened as string, s)).toEqual(sorted(graph));
      }
      // A term's names every term it drew; a product's root names the product, not its terms.
      const { got, graph } = await chainRead(termScope(depth));
      expect(namedTerms(got.opened as string)).toEqual(drawnTerms(sorted(graph)));
    });
});
