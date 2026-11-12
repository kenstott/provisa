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

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { RoleSchema, rootKey, type Introspection, type MetaReader } from "../../../api/metaGraph";
import { ontologyGraph, type OntologyRows } from "../ontologyData";
import { readOntology } from "../ontologyRead";
import { loadOntology } from "../useOntology";
import { snippetLayers, termGraph, termPreset } from "../ontologyPresets";
import type { GraphData, LayerId, QueryScope } from "../graphModel";

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

// The term graph: 7 the focus; 1 and 2 a hop off (1 also links back to 7, and 2 to 1); 3 and 6 two
// hops off; 4 three (with a link back to 6); 5 four, past depth 3; 8 retired (hidden), reached
// from 3; 9 linked to nothing. Each term IS one column of a table of its own.
const STATUS: Record<number, string> = { 8: "retired" };
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

type Row = Record<string, unknown>;

const product = (id: number): Row => ({
  id: `p${id}`,
  name: `product ${id}`,
  domainId: "sales",
  status: "active",
  ownerRole: "steward",
});
const table = (id: number): Row => ({
  id: 100 + id,
  tableName: `table_${id}`,
  domainId: "sales",
  sourceId: "pg",
  description: null,
  checkedBy: null,
  dataProductTables: [{ productId: `p${id}`, dataProduct: product(id) }],
});
// Each product holds its term's table.
const products = TERM_IDS.map((id) => ({
  ...product(id),
  dataProductTables: [{ productId: `p${id}`, registeredTable: table(id) }],
}));

const terms = new Map<number, Row>();
const edge = (i: number, [from, to, relType]: [number, number, string]): Row => ({
  id: i + 1,
  fromTermId: from,
  toTermId: to,
  relType,
  // Lazy: the graph has a cycle.
  sourceTerm: () => terms.get(from),
  targetTerm: () => terms.get(to),
});
for (const id of TERM_IDS)
  terms.set(id, {
    id,
    name: `term ${id}`,
    definition: null,
    refCount: 1,
    deprecated: false,
    retired: STATUS[id] === "retired",
    status: STATUS[id] ?? "approved",
    grounding: "grounded",
    identityGap: false,
    termDomains: "|sales|",
    domains: [{ domainId: "sales" }],
    experts: [],
    tables: [],
    refs: [
      {
        columnKey: `${100 + id}:c`,
        columnName: "c",
        tableId: 100 + id,
        tableName: `table_${id}`,
        domainId: "sales",
        column: { dataType: "text", description: null, sensitivity: [] },
        registeredTable: table(id),
      },
    ],
    edgesOut: () =>
      LINKS.map((l, i) => [l, i] as const)
        .filter(([l]) => l[0] === id)
        .map(([l, i]) => edge(i, l)),
    edgesIn: () =>
      LINKS.map((l, i) => [l, i] as const)
        .filter(([l]) => l[1] === id)
        .map(([l, i]) => edge(i, l)),
  });

/** A field of a query: its answer key, its name, its where, its own fields. */
interface Field {
  key: string;
  name: string;
  where?: Row;
  fields: Field[];
}

/** A query the builders wrote (one field to a line, as selectionSet lays it out), as fields. */
function parse(query: string): Field[] {
  const root: Field = { key: "", name: "", fields: [] };
  const stack = [root];
  for (const raw of query.split("\n").slice(1, -1)) {
    const line = raw.trim();
    if (line === "}") {
      stack.pop();
      continue;
    }
    const m = /^(?:(\w+): )?(\w+)(?:\(where: (.*)\))?( \{)?$/.exec(line);
    if (!m) throw new Error(`unparsed: ${line}`);
    const where = m[3]
      ? (JSON.parse(m[3].replace(/([{,]\s*)(\w+):/g, '$1"$2":')) as Row)
      : undefined;
    const f: Field = { key: m[1] ?? m[2], name: m[2], fields: [], ...(where ? { where } : {}) };
    stack[stack.length - 1].fields.push(f);
    if (m[4]) stack.push(f);
  }
  return root.fields;
}

const OPS = new Set(["eq", "in", "like", "ilike", "is_null"]);
const value = (row: Row, name: string) => {
  const v = row[name];
  return typeof v === "function" ? (v as () => unknown)() : v;
};
const likeRe = (p: string) =>
  new RegExp(`^${p.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/%/g, ".*")}$`);

/** Whether ``row`` meets ``where``. */
function meets(row: Row, where: Row): boolean {
  return Object.entries(where).every(([k, cond]) => {
    if (k === "_and") return (cond as Row[]).every((c) => meets(row, c));
    if (k === "_or") return (cond as Row[]).some((c) => meets(row, c));
    const v = value(row, k);
    const c = cond as Row;
    if (Object.keys(c).every((op) => OPS.has(op)))
      return Object.entries(c).every(([op, arg]) => {
        if (op === "eq") return v === arg;
        if (op === "in") return (arg as unknown[]).includes(v);
        if (op === "like") return likeRe(arg as string).test(String(v));
        if (op === "ilike") return likeRe(arg as string).test(String(v).toLowerCase());
        return (v === null || v === undefined) === arg;
      });
    if (Array.isArray(v)) return v.some((r) => meets(r as Row, c));
    return v !== null && v !== undefined && meets(v as Row, c);
  });
}

function project(row: Row, fields: Field[]): Row {
  const out: Row = {};
  for (const f of fields) {
    const v = value(row, f.name);
    if (f.fields.length === 0) out[f.key] = v ?? null;
    else if (Array.isArray(v))
      out[f.key] = (v as Row[])
        .filter((r) => !f.where || meets(r, f.where))
        .map((r) => project(r, f.fields));
    else out[f.key] = v ? project(v as Row, f.fields) : null;
  }
  return out;
}

/** The server: each query answered from the term graph, kept with its answer. */
function server() {
  const sent: { query: string; answer: string }[] = [];
  const read = async (query: string): Promise<OntologyRows> => {
    const out: Row = {};
    for (const f of parse(query)) {
      const rows: Row[] =
        f.name === "on_terms" ? [...terms.values()] : f.name === "m__dataProducts" ? products : [];
      if (rows.length === 0) throw new Error(`unexpected root ${f.name}`);
      out[f.key] = rows
        .filter((r) => !f.where || meets(r, f.where))
        .map((r) => project(r, f.fields));
    }
    sent.push({ query, answer: JSON.stringify(out) });
    return out as OntologyRows;
  };
  return { sent, read };
}

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

const sorted = (g: GraphData) => ({
  nodes: g.nodes.map((n) => n.id).sort(),
  edges: g.edges.map((e) => e.id).sort(),
});

async function drawn(depth: number) {
  const s = snippet(depth);
  const srv = server();
  const got = await readOntology(reader(), s, srv.read);
  return { graph: sorted(ontologyGraph(got.rows, s)), sent: srv.sent, queries: got.queries };
}

// What the nested read (one query, a level to each hop) drew for term:7 at depth 3 against this
// graph, before the hops were read apart: the terms within three links (5 is four off; 8 hidden),
// each one's column, table and product, and every link between two of them.
const AT_DEPTH_3 = {
  nodes: [
    "c:101:c",
    "c:102:c",
    "c:103:c",
    "c:106:c",
    "c:107:c",
    "d:sales",
    "p:p1",
    "p:p2",
    "p:p3",
    "p:p6",
    "p:p7",
    "t:101",
    "t:102",
    "t:103",
    "t:106",
    "t:107",
    "term:1",
    "term:2",
    "term:3",
    "term:4",
    "term:6",
    "term:7",
  ],
  edges: [
    "has_column:c:101:c>t:101",
    "has_column:c:102:c>t:102",
    "has_column:c:103:c>t:103",
    "has_column:c:106:c>t:106",
    "has_column:c:107:c>t:107",
    "in_domain:term:1>d:sales",
    "in_domain:term:2>d:sales",
    "in_domain:term:3>d:sales",
    "in_domain:term:6>d:sales",
    "in_domain:term:7>d:sales",
    "member:p:p1>t:101",
    "member:p:p2>t:102",
    "member:p:p3>t:103",
    "member:p:p6>t:106",
    "member:p:p7>t:107",
    "term_binding:term:1>c:101:c:IS",
    "term_binding:term:2>c:102:c:IS",
    "term_binding:term:3>c:103:c:IS",
    "term_binding:term:6>c:106:c:IS",
    "term_binding:term:7>c:107:c:IS",
    "term_edge:term:1>term:3:KIND_OF",
    "term_edge:term:1>term:7:RELATED_TO",
    "term_edge:term:2>term:1:RELATED_TO",
    "term_edge:term:2>term:7:PART_OF",
    "term_edge:term:3>term:4:KIND_OF",
    "term_edge:term:4>term:6:RELATED_TO",
    "term_edge:term:6>term:2:PART_OF",
    "term_edge:term:7>term:1:KIND_OF",
  ],
};
// The nested read's size against this graph: its query and its answer.
const NESTED = { query: 19_553, answer: 26_720 };

describe("a term's snippet, read a hop at a time", () => {
  it("draws what the nested read drew", async () => {
    expect((await drawn(3)).graph).toEqual(AT_DEPTH_3);
  });

  it("reads each term's refs once, and each term's facts once", async () => {
    const { sent } = await drawn(3);
    const answers = sent.map((s) => JSON.parse(s.answer) as { _terms: Row[] });
    const read = answers.flatMap((a) => a._terms.map((t) => t.id as number));
    // Every term within three links, each once: the root, then each hop's frontier.
    expect(read.sort()).toEqual([1, 2, 3, 4, 6, 7]);
    const refsRead = answers.flatMap((a) => a._terms.filter((t) => t.refs).map((t) => t.id));
    // The last hop's terms are past what is touched: only their links are read.
    expect(refsRead.sort()).toEqual([1, 2, 3, 6, 7]);
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
    const srv = server();
    const got = await readOntology(reader(), s, srv.read);
    expect(srv.sent).toHaveLength(1);
    expect(sorted(ontologyGraph(got.rows, s)).nodes).toContain("term:9");
  });
});

/** The terms a query's roots name by id: the focus's, and each hop's frontier. */
function namedTerms(query: string): number[] {
  const ids: number[] = [];
  const walk = (w: unknown) => {
    if (Array.isArray(w)) return w.forEach(walk);
    if (w === null || typeof w !== "object") return;
    for (const [k, v] of Object.entries(w as Row)) {
      const c = v as Row;
      if (k === "id" && "eq" in c) ids.push(c.eq as number);
      else if (k === "id" && "in" in c) ids.push(...(c.in as number[]));
      else walk(v);
    }
  };
  for (const f of parse(query)) if (f.name === "on_terms") walk(f.where);
  return [...new Set(ids)].sort((a, b) => a - b);
}

/** What running ``query`` once answers, as the view draws it: every root of the terms is terms. */
async function ran(query: string, s: QueryScope) {
  const out = (await server().read(query)) as Record<string, unknown>;
  const termsRead = Object.entries(out)
    .filter(([k]) => k.startsWith("_terms"))
    .flatMap(([, v]) => v as Row[]);
  const rows = { ...out, _terms: termsRead } as unknown as OntologyRows;
  return sorted(ontologyGraph(rows, s));
}

const drawnTerms = (g: { nodes: string[] }) =>
  g.nodes
    .filter((n) => n.startsWith("term:"))
    .map((n) => Number(n.slice("term:".length)))
    .sort((a, b) => a - b);

/** A product's snippet, as a product's panel and graph read it (the Product map's rules). */
const productSnippet = (depth: number): QueryScope => ({
  ...snippet(depth),
  layers: new Set<LayerId>(snippetLayers({ products: true })),
  products: true,
  focus: "p:p1",
});

describe("a snippet opened as a query: every hop the snippet drew, in one query", () => {
  for (const depth of [1, 2, 3])
    it(`a term's snippet at depth ${depth}`, async () => {
      const s = snippet(depth);
      const got = await readOntology(reader(), s, server().read);
      const graph = sorted(ontologyGraph(got.rows, s));
      const opened = got.opened as string;
      expect(opened.startsWith("query Ontology {")).toBe(true);
      // It names every term drawn and nothing past them; run, it draws what the snippet drew.
      expect(namedTerms(opened)).toEqual(drawnTerms(graph));
      expect(await ran(opened, s)).toEqual(graph);
      // Each hop's read once, not a level nested under each path: no bigger than the reads sent.
      const sent = got.queries.reduce((n, q) => n + q.length, 0);
      expect(opened.length).toBeLessThan(sent);
      expect(opened.length).toBeLessThan(NESTED.query / 2);
    });

  it("draws at depth 3 what the nested read drew", async () => {
    const s = snippet(3);
    const got = await readOntology(reader(), s, server().read);
    expect(await ran(got.opened as string, s)).toEqual(AT_DEPTH_3);
  });

  it("a term's expanded graph (termGraph) opens as what it drew", async () => {
    const p = termGraph(termPreset(7));
    const s: QueryScope = {
      ...snippet(p.depth),
      layers: new Set<LayerId>(snippetLayers(p)),
      focus: p.focus,
    };
    const got = await readOntology(reader(), s, server().read);
    const graph = sorted(ontologyGraph(got.rows, s));
    expect(got.queries.length).toBeGreaterThan(1);
    expect(namedTerms(got.opened as string)).toEqual(drawnTerms(graph));
    expect(await ran(got.opened as string, s)).toEqual(graph);
  });

  for (const depth of [1, 2, 3, 4])
    it(`a product's snippet at depth ${depth}`, async () => {
      const s = productSnippet(depth);
      const got = await readOntology(reader(), s, server().read);
      const graph = sorted(ontologyGraph(got.rows, s));
      expect(graph.nodes).toContain("p:p1");
      // A product's own terms are its root: only past PRODUCT_TERM_HOPS does a hop step out.
      expect(got.queries).toHaveLength(depth > 3 ? 2 : 1);
      if (depth <= 3) expect(got.opened).toBe(got.queries[0]);
      expect(await ran(got.opened as string, s)).toEqual(graph);
    });

  it("is what the view hands Open as query", async () => {
    wire.reader = reader;
    wire.read = server().read;
    const s = snippet(3);
    const loaded = await loadOntology("analyst", s);
    if (loaded === null || loaded.over !== undefined) throw new Error("not read");
    expect(await ran(loaded.query, s)).toEqual(sorted(loaded.data));
    expect(namedTerms(loaded.query)).toEqual([1, 2, 3, 4, 6, 7]);
  });
});
