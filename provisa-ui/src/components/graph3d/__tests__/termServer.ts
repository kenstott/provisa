// Copyright (c) 2026 Kenneth Stott
// Canary: ba0878ec-2eee-4fe4-a9af-338181aea870
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: a server answering the ontology's queries from a small term world, against the meta
// model's real schema (tests/fixtures/graphql_golden/meta_schema.json): each query parsed as the
// builders lay it out, each where judged on the world's rows, each selection projected from them.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { RoleSchema, rootKey, type Introspection, type MetaReader } from "../../../api/metaGraph";
import { ontologyGraph, type OntologyRows } from "../ontologyData";
import type { GraphData, QueryScope } from "../graphModel";

const GOLDEN = resolve(__dirname, "../../../../../tests/fixtures/graphql_golden");

export function reader(): MetaReader {
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

export type Row = Record<string, unknown>;

/** The rows a server answers from: the terms by id, the data products. */
export interface World {
  terms: Map<number, Row>;
  products: Row[];
}

/** A field of a query: its answer key, its name, its where, its own fields. */
interface Field {
  key: string;
  name: string;
  where?: Row;
  fields: Field[];
}

/** A query the builders wrote (one field to a line, as selectionSet lays it out), as fields. */
export function parse(query: string): Field[] {
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

/** The server: each query answered from ``world``, kept with its answer. */
export function server(world: World) {
  const sent: { query: string; answer: string }[] = [];
  const read = async (query: string): Promise<OntologyRows> => {
    const out: Row = {};
    for (const f of parse(query)) {
      const rows: Row[] =
        f.name === "on_terms"
          ? [...world.terms.values()]
          : f.name === "m__dataProducts"
            ? world.products
            : [];
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

export const sorted = (g: GraphData) => ({
  nodes: g.nodes.map((n) => n.id).sort(),
  edges: g.edges.map((e) => e.id).sort(),
});

/** The terms a query's roots name by id: the focus's, and each hop's frontier. */
export function namedTerms(query: string): number[] {
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

/** What running ``query`` once against ``world`` answers, as the view draws it: every root of the
 * terms is terms. */
export async function ran(world: World, query: string, s: QueryScope) {
  const out = (await server(world).read(query)) as Record<string, unknown>;
  const termsRead = Object.entries(out)
    .filter(([k]) => k.startsWith("_terms"))
    .flatMap(([, v]) => v as Row[]);
  const rows = { ...out, _terms: termsRead } as unknown as OntologyRows;
  return sorted(ontologyGraph(rows, s));
}

export const drawnTerms = (g: { nodes: string[] }) =>
  g.nodes
    .filter((n) => n.startsWith("term:"))
    .map((n) => Number(n.slice("term:".length)))
    .sort((a, b) => a - b);

/** A term world's parts: each table with its columns and the product holding it, each term with
 * the columns it IS (by table and column name), and the links between the terms. */
export interface WorldSpec {
  products: { id: string; name: string }[];
  tables: { id: number; name: string; product: string; columns: string[] }[];
  terms: { id: number; name: string; columns: [number, string][]; status?: string }[];
  links: [number, number, string][];
}

/** The rows of ``spec``, in the sales domain, as the meta model answers them. */
export function world(spec: WorldSpec): World {
  const product = (id: string): Row => {
    const p = spec.products.find((x) => x.id === id);
    if (!p) throw new Error(`no product ${id}`);
    return { id, name: p.name, domainId: "sales", status: "active", ownerRole: "steward" };
  };
  const bound = (table: number, column: string) =>
    spec.terms.filter((t) => t.columns.some(([tb, c]) => tb === table && c === column));
  const tableRow = (id: number): Row => {
    const t = spec.tables.find((x) => x.id === id);
    if (!t) throw new Error(`no table ${id}`);
    return {
      id,
      tableName: t.name,
      domainId: "sales",
      sourceId: "pg",
      description: null,
      dqContract: null,
      checkedBy: null,
      tags: [],
      dataProductTables: [{ productId: t.product, dataProduct: product(t.product) }],
      tableColumns: t.columns.map((c) => ({
        columnName: c,
        columnKey: `${id}:${c}`,
        dataType: "text",
        description: null,
        masks: null,
        tags: [],
        glossaryRef: bound(id, c).map((term) => ({ termId: term.id })),
      })),
    };
  };
  const terms = new Map<number, Row>();
  const edge = (i: number, [from, to, relType]: [number, number, string]): Row => ({
    id: i + 1,
    fromTermId: from,
    toTermId: to,
    relType,
    // Lazy: the graph may have a cycle.
    sourceTerm: () => terms.get(from),
    targetTerm: () => terms.get(to),
  });
  for (const t of spec.terms)
    terms.set(t.id, {
      id: t.id,
      name: t.name,
      definition: null,
      refCount: t.columns.length,
      deprecated: false,
      retired: t.status === "retired",
      status: t.status ?? "approved",
      grounding: t.columns.length > 0 ? "grounded" : "not_grounded",
      identityGap: false,
      termDomains: "|sales|",
      domains: [{ domainId: "sales" }],
      experts: [],
      tables: [],
      refs: t.columns.map(([tb, c]) => {
        const table = tableRow(tb);
        return {
          columnKey: `${tb}:${c}`,
          columnName: c,
          tableId: tb,
          tableName: table.tableName,
          domainId: "sales",
          column: { dataType: "text", description: null, masks: null, sensitivity: [], tags: [] },
          registeredTable: table,
        };
      }),
      edgesOut: () =>
        spec.links
          .map((l, i) => [l, i] as const)
          .filter(([l]) => l[0] === t.id)
          .map(([l, i]) => edge(i, l)),
      edgesIn: () =>
        spec.links
          .map((l, i) => [l, i] as const)
          .filter(([l]) => l[1] === t.id)
          .map(([l, i]) => edge(i, l)),
    });
  const products = spec.products.map((p) => ({
    ...product(p.id),
    dataProductTables: spec.tables
      .filter((t) => t.product === p.id)
      .map((t) => ({ productId: p.id, registeredTable: tableRow(t.id) })),
  }));
  return { terms, products };
}
