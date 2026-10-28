// Copyright (c) 2026 Kenneth Stott
// Canary: 0c9b6e24-71d8-4a35-b2f9-e6a4d1c8b357
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the data product graph's data, read from the meta domain as the signed-in user —
// data_products_meta and its relationships (REQ-2125), the glossary's term edges and the
// sensitive columns — and turned into the graph's nodes and edges.

import {
  operation,
  rootField,
  rowsOf,
  type MetaReader,
  type Selection,
  type Where,
} from "../../api/metaGraph";
import type { GraphData, GraphEdge, GraphNode, NodeInfo, QueryScope } from "./graphModel";
import type { MaskEntry } from "../../lib/masks";
import type { ProductInput, ProductLineage } from "../../api/lineage";
import { catalogTableName, relationName } from "../../naming";

/** What the product graph's query keeps (REQ-2136): its domain, tag, sensitivity and term edge
 * type filters, in its where. None of one is every one. A row the filters drop takes what hangs off
 * it (a product its members, a table its columns, a column the terms it binds). */
export type ProductFilters = Pick<QueryScope, "domains" | "tags" | "sensitivities" | "edgeTypes">;

export const NO_PRODUCT_FILTERS: ProductFilters = {
  domains: [],
  tags: [],
  sensitivities: [],
  edgeTypes: [],
};

const all = (conds: (Where | undefined)[]): Where | undefined => {
  const kept = conds.filter((c): c is Where => c !== undefined);
  return kept.length === 0 ? undefined : kept.length === 1 ? kept[0] : { _and: kept };
};

/** A row in one of ``f``'s domains. */
const inDomains = (f: ProductFilters): Where | undefined =>
  f.domains.length === 0 ? undefined : { domain_id: { in: f.domains } };

/** A row carrying one of ``f``'s tags (one the tag filter judges: it reads its tags). */
const tagged = (f: ProductFilters): Where | undefined =>
  f.tags.length === 0 ? undefined : { tags: { tag_id: { in: f.tags } } };

/** The tags drawn: only those chosen. */
const tagRel = (f: ProductFilters): Selection["rels"] => ({
  tags: {
    alias: "tags",
    sel: { fields: ["tag_id"] },
    ...(f.tags.length === 0 ? {} : { where: { tag_id: { in: f.tags } } }),
  },
});

/** sensitive_coverage's tags, "a, b": one of ``x`` among them, exactly. */
const sensitiveTo = (xs: string[]): Where => ({
  _or: xs.flatMap((x) => [
    { tags: { eq: x } },
    { tags: { like: `${x}, %` } },
    { tags: { like: `%, ${x}` } },
    { tags: { like: `%, ${x}, %` } },
  ]),
});

/** A column with one of ``f``'s sensitivities; every column when none is chosen. */
const sensitiveColumn = (f: ProductFilters): Where | undefined =>
  f.sensitivities.length === 0 ? undefined : { sensitivity: sensitiveTo(f.sensitivities) };

/** REQ-2174 (EVERY LANE NODE INFORMS AND LINKS): a term as a node's facts need it -- its name,
 * definition, status and stewards (its experts). */
export const TERM_FACTS: Selection = {
  fields: ["id", "name", "definition", "status"],
  rels: { experts: { alias: "experts", sel: { fields: ["user_id", "kind"] } } },
};

/** A column's facts: its type, description and masks (its sensitivity is read beside them). */
export const COLUMN_FACTS = ["data_type", "description", "masks"];

/** A table's facts: its source and description, and its checkers' latest scans (its quality). */
export const TABLE_FACTS: Selection = {
  fields: ["source_id", "description"],
  rels: {
    checked_by: { alias: "checkedBy", sel: { fields: ["latest_outcome", "latest_run_at"] } },
  },
};

/** A product's facts: its status and owner (its members are counted from the graph). */
export const PRODUCT_FACTS = ["status", "owner_role"];

/** REQ-2174: the term edges that rank one term under another, the term hierarchy's. */
export const RANK_TYPES = ["KIND_OF", "PART_OF", "VALID_VALUE_OF"];

function columnSel(f: ProductFilters): Selection {
  return {
    fields: ["column_name", "column_key", ...COLUMN_FACTS],
    rels: {
      glossary_ref: {
        alias: "glossaryRef",
        sel: {
          fields: ["term_id"],
          rels: {
            // The bound term; its hierarchy is the rank edges read beside the products.
            term: { alias: "term", sel: TERM_FACTS },
          },
        },
      },
      ...tagRel(f),
      sensitivity: { alias: "sensitivity", sel: { fields: ["tags"] } },
    },
  };
}

function tableSel(f: ProductFilters): Selection {
  const dom = inDomains(f);
  return {
    fields: ["id", "domain_id", "table_name", "dq_contract", "product_id", ...TABLE_FACTS.fields],
    rels: {
      ...TABLE_FACTS.rels,
      table_columns: {
        to: "table_columns",
        sel: columnSel(f),
        where: all([tagged(f), sensitiveColumn(f)]),
      },
      ...tagRel(f),
      profile_results: { alias: "profileResults", sel: { fields: ["id", "table_name"] } },
      checks: { alias: "checks", sel: { fields: ["id"] } },
      source_relationships: {
        alias: "sourceRelationships",
        sel: {
          fields: ["id"],
          rels: {
            target_table: {
              alias: "targetTable",
              sel: { fields: ["id", "domain_id", "table_name", "product_id"] },
            },
          },
        },
        where: dom && { target_table: dom },
      },
    },
  };
}

function productSel(f: ProductFilters): Selection {
  const dom = inDomains(f);
  return {
    fields: ["id", "name", "domain_id", ...PRODUCT_FACTS],
    rels: {
      member_tables: {
        alias: "memberTables",
        sel: tableSel(f),
        // A judged table: in a domain kept, with a tag kept, with a column of a sensitivity kept.
        where: all([
          dom,
          tagged(f),
          f.sensitivities.length === 0
            ? undefined
            : { table_columns: { sensitivity: sensitiveTo(f.sensitivities) } },
        ]),
      },
      commands: { alias: "commands", sel: { fields: ["name", "domain_id"] }, where: dom },
      ...tagRel(f),
      // REQ-2126: the product's links to the rest of the model. A linked product is a bare node:
      // its own parts are its graph's, never read through this one (so a focus on this product,
      // the view's own, prunes nothing the query reads).
      edges: {
        alias: "edges",
        sel: {
          fields: ["kind", "target_product_id", "target_table_id", "hops", "path_count", "aliases"],
          rels: {
            target_product: {
              alias: "targetProduct",
              sel: { fields: ["id", "name", "domain_id"] },
            },
            target_table: {
              alias: "targetTable",
              sel: { fields: ["domain_id", "table_name"] },
            },
          },
        },
        where: dom && { target_product: dom },
      },
    },
  };
}

interface Tagged {
  tags?: { tag_id: string }[] | null;
}

/** A term as TERM_FACTS reads it; a field the role cannot read is absent. */
export interface TermFactsRow {
  id: number;
  name: string;
  definition?: string | null;
  status?: string | null;
  experts?: { user_id: string; kind: string }[] | null;
}

/** A column's, a table's and a product's facts, as their *_FACTS read them. */
interface ColumnFacts {
  data_type?: string | null;
  description?: string | null;
  // table_columns.masks: the ordered mask list (REQ-2059), as JSON.
  masks?: unknown;
}

interface ColumnRow extends Tagged, ColumnFacts {
  column_name: string;
  column_key: string;
  glossary_ref?:
    | {
        term_id: number;
        term?: TermFactsRow | null;
      }[]
    | null;
  // Its row in sensitive_coverage when it is sensitive; undefined when the role cannot read it.
  sensitivity?: { tags: string }[] | null;
}

interface TableRow extends Tagged {
  id: number;
  domain_id: string;
  table_name: string;
  source_id?: string | null;
  description?: string | null;
  checked_by?: { latest_outcome: "pass" | "fail" | null; latest_run_at: string | null }[] | null;
  dq_contract?: string | null;
  product_id?: string | null;
  table_columns?: ColumnRow[] | null;
  profile_results?: { id: number; table_name: string }[] | null;
  checks?: { id: number } | null;
  source_relationships?:
    | {
        id: string; // the relationships table's TEXT key
        target_table?: {
          id: number;
          domain_id: string;
          table_name: string;
          product_id?: string | null;
        } | null;
      }[]
    | null;
}

export interface EdgeRow {
  // data_product_edges' kinds (REQ-2126); the graph draws the relationship rows. A product's
  // lineage is the product lineage endpoint's, cut to its reader's rights (REQ-2156).
  kind: "relationship" | "related";
  target_product_id: string | null;
  target_table_id: number | null;
  hops: number;
  path_count: number;
  aliases: string | null;
  target_product?: { id: string; name: string; domain_id: string } | null;
  target_table?: { domain_id: string; table_name: string } | null;
}

export interface ProductRow extends Tagged {
  id: string;
  name: string;
  domain_id: string;
  status?: string | null;
  owner_role?: string | null;
  member_tables?: TableRow[] | null;
  commands?: { name: string; domain_id: string }[] | null;
  edges?: EdgeRow[] | null;
}

export interface TermEdgeRow {
  id: number;
  from_term_id: number;
  to_term_id: number;
  rel_type: string;
  /** The terms it joins, with their facts (RANK_EDGES reads them). */
  source_term?: TermFactsRow | null;
  target_term?: TermFactsRow | null;
}

/** REQ-2174: the rank edges among terms (``f``'s edge types among them), with the terms they
 * join; productGraph keeps those on a chain up from a bound term. */
export const RANK_EDGES: Selection = {
  fields: ["id", "from_term_id", "to_term_id", "rel_type"],
  rels: {
    source_term: { alias: "sourceTerm", sel: TERM_FACTS },
    target_term: { alias: "targetTerm", sel: TERM_FACTS },
  },
};

function rankTypes(f: ProductFilters): string[] {
  return f.edgeTypes.length === 0 ? RANK_TYPES : RANK_TYPES.filter((t) => f.edgeTypes.includes(t));
}

/** What the product graph's query answers: a part the role cannot read is absent. */
export interface ProductGraphRows {
  data_products: ProductRow[];
  _term_edges?: TermEdgeRow[] | null;
}

export interface SensitiveRow {
  column_key: string;
  table_id: number;
  tags: string;
}

/** The product graph's one query (REQ-2136): the products (one, or all) with their parts, the
 * terms their columns are with those terms' edges, and each column's sensitivity, kept by ``f``. */
export function productGraphQuery(
  m: MetaReader,
  product: string | null,
  f: ProductFilters,
): string | null {
  const field = rootField(
    m,
    "data_products",
    productSel(f),
    all([
      product === null ? undefined : { id: { eq: product } },
      inDomains(f),
      tagged(f),
      f.sensitivities.length === 0
        ? undefined
        : { member_tables: { table_columns: { sensitivity: sensitiveTo(f.sensitivities) } } },
    ]),
  );
  if (field === null) return null;
  // REQ-2174: the term hierarchy above the bound terms, so the lane diagram draws it top-down.
  const ranks = rankField(m, rankTypes(f));
  return operation("DataProductGraph", ranks === null ? [field] : [field, ranks]);
}

/** The root field reading the rank edges of ``types``; null when there are none to read or the
 * role cannot read the term edges. */
export function rankField(m: MetaReader, types: string[]): string | null {
  return types.length === 0
    ? null
    : rootField(m, "_term_edges", RANK_EDGES, { rel_type: { in: types } });
}

/** The rank edges on a chain up from a term bound to one of ``products``' columns: the bound
 * terms' broader terms, theirs, and so on. */
export function rankChains(products: ProductRow[], edges: TermEdgeRow[]): TermEdgeRow[] {
  const up = new Map<number, TermEdgeRow[]>();
  for (const e of edges) up.set(e.from_term_id, [...(up.get(e.from_term_id) ?? []), e]);
  const seen = new Set<number>();
  const kept: TermEdgeRow[] = [];
  const climb = (id: number) => {
    if (seen.has(id)) return;
    seen.add(id);
    for (const e of up.get(id) ?? []) {
      kept.push(e);
      climb(e.to_term_id);
    }
  };
  boundTermIds(products).forEach(climb);
  return kept;
}

/**
 * The graph of the product graph's one query: its columns' sensitivity read off the columns, null
 * when the role cannot read it (as toGraphData takes it). Of the term edges, those ranking a bound
 * term under another, up the chain (rankChains); none when the role cannot read them.
 */
export function productGraph(products: ProductRow[], ranks: TermEdgeRow[] = []): GraphData {
  const columns = products.flatMap((p) =>
    (p.member_tables ?? []).flatMap((t) =>
      (t.table_columns ?? []).map((c) => ({ table: t.id, c })),
    ),
  );
  const readable = (pick: (c: ColumnRow) => unknown) =>
    columns.length === 0 || columns.every(({ c }) => pick(c) !== undefined);
  const sensitive: SensitiveRow[] | null = readable((c) => c.sensitivity)
    ? columns.flatMap(({ table, c }) =>
        (c.sensitivity ?? []).map((x) => ({
          column_key: c.column_key,
          table_id: table,
          tags: x.tags,
        })),
      )
    : null;
  return toGraphData(products, rankChains(products, ranks), sensitive);
}

export const productNode = (id: string) => `p:${id}`;

/** REQ-2156: an input as its reader knows it -- a registered table or view by its catalog name, a
 *  member read straight from its source by the source and the names it gives the object. */
function inputLabel(i: ProductInput): string {
  if (i.kind !== "source")
    return catalogTableName({ domainId: i.domain_id, tableName: i.table_name });
  return sourceObject(i);
}

/** REQ-2156: ``product``'s graph with its lineage, from the product lineage endpoint (cut to what
 *  the reader can read): each input feeds the product; an input that is another product's member
 *  stands for that product, which the product is built from. Its column lineage (REQ-2129) joins
 *  the columns it names: a member's or an input's column by its table, any other by its relation.
 *  ``tables`` gives the relation each of the product's member tables is served under. */
export function withProductLineage(
  g: GraphData,
  product: string,
  lineage: ProductLineage,
  tables: { id: number; domain_id: string; table_name: string }[],
): GraphData {
  const pid = productNode(product);
  const nodes = new Map(g.nodes.map((n) => [n.id, n]));
  const edges = new Map(g.edges.map((e) => [e.id, e]));
  for (const i of lineage.inputs) {
    const from =
      i.product_id && i.product_id !== product ? productNode(i.product_id) : `l:${i.relation}`;
    if (!nodes.has(from))
      nodes.set(
        from,
        from === `l:${i.relation}`
          ? { id: from, type: "relation", label: inputLabel(i), domains: [i.domain_id] }
          : { id: from, type: "product", label: i.product_id as string, domains: [i.domain_id] },
      );
    const id = `lineage:${from}>${pid}`;
    if (!edges.has(id)) edges.set(id, { id, a: from, b: pid, kind: "lineage" });
  }
  const tableOf = new Map<string, number>([
    ...tables.map(
      (t) =>
        [relationName({ domainId: t.domain_id, tableName: t.table_name }), t.id] as [
          string,
          number,
        ],
    ),
    ...lineage.inputs.map((i) => [i.relation, i.table_id] as [string, number]),
  ]);
  const columnOf = new Map<string, string>();
  for (const n of lineage.nodes) {
    const tid = n.relation === null ? undefined : tableOf.get(n.relation);
    const id = tid === undefined ? `lc:${n.id}` : `c:${tid}:${n.column}`;
    columnOf.set(n.id, id);
    // A column the meta read did not draw stands alone, named by where it is read from.
    if (!nodes.has(id))
      nodes.set(id, {
        id,
        type: "column",
        label:
          n.kind === "native"
            ? `${sourceObject(n)}.${n.column}`
            : `${n.relation === null ? "" : `${n.relation}.`}${n.column}`,
        inProduct: false,
      });
  }
  for (const e of lineage.edges) {
    const a = columnOf.get(e.source);
    const b = columnOf.get(e.target);
    // The endpoint sends every node its edges name.
    if (a === undefined || b === undefined)
      throw new Error(`lineage edge ${e.source} -> ${e.target} names a node the response lacks`);
    const id = `column_lineage:${a}>${b}`;
    if (!edges.has(id)) edges.set(id, { id, a, b, kind: "column_lineage" });
  }
  return { nodes: [...nodes.values()], edges: [...edges.values()] };
}

/** An object read straight from its source, by the names the source gives it: the source, then
 *  its schema and table where it has them. */
function sourceObject(n: {
  source_id?: string | null;
  source_schema?: string | null;
  source_table?: string | null;
}): string {
  const object = [n.source_schema, n.source_table].filter(Boolean).join(".");
  return object ? `${n.source_id} · ${object}` : (n.source_id as string);
}
export const tableNode = (id: number) => `t:${id}`;

/** The table ids of ``products``' member tables. */
export function memberTableIds(products: ProductRow[]): number[] {
  return products.flatMap((p) => (p.member_tables ?? []).map((t) => t.id));
}

/** The term ids bound to ``products``' member columns. */
export function boundTermIds(products: ProductRow[]): number[] {
  const ids = new Set<number>();
  for (const p of products)
    for (const t of p.member_tables ?? [])
      for (const c of t.table_columns ?? [])
        for (const r of c.glossary_ref ?? []) ids.add(r.term_id);
  return [...ids];
}

/**
 * The graph of ``products``. ``termEdges`` and ``sensitive`` are null when the role cannot read
 * them: the ontology layer then has no term-to-term edges, and nothing is judged by sensitivity.
 */
export function toGraphData(
  products: ProductRow[],
  termEdges: TermEdgeRow[] | null,
  sensitive: SensitiveRow[] | null,
): GraphData {
  const nodes = new Map<string, GraphNode>();
  const edges = new Map<string, GraphEdge>();
  const node = (n: GraphNode) => {
    if (!nodes.has(n.id)) nodes.set(n.id, n);
  };
  const edge = (a: string, b: string, kind: GraphEdge["kind"], label?: string) => {
    // Two terms joined by edges of two types keep both (a rank edge beside a lateral one).
    const id = `${kind}:${a}>${b}${kind === "term_edge" && label ? `:${label}` : ""}`;
    if (!edges.has(id)) edges.set(id, { id, a, b, kind, ...(label ? { label } : {}) });
  };
  const tagIds = (row: Tagged) => rowsOf(row.tags)?.map((t) => t.tag_id);
  const tag = (row: Tagged, target: string) => {
    for (const t of row.tags ?? []) {
      node({ id: `g:${t.tag_id}`, type: "tag", label: t.tag_id });
      edge(`g:${t.tag_id}`, target, "tagged");
    }
  };
  const sens = new Map<string, string[]>();
  for (const s of sensitive ?? []) sens.set(s.column_key, s.tags.split(", "));
  const term = (row: TermFactsRow) =>
    node({
      id: `term:${row.id}`,
      type: "term",
      label: row.name,
      ...(row.definition ? { definition: row.definition } : {}),
      ...facts(termFacts(row)),
    });

  for (const p of products) {
    const pid = productNode(p.id);
    // A product another one links to is drawn before its own row is read: its row decides.
    nodes.set(pid, {
      id: pid,
      type: "product",
      label: p.name,
      domains: [p.domain_id],
      tags: tagIds(p),
      ...facts({ status: p.status ?? undefined, owner: p.owner_role ?? undefined }),
    });
    node({ id: `d:${p.domain_id}`, type: "domain", label: p.domain_id });
    edge(pid, `d:${p.domain_id}`, "in_domain");
    tag(p, pid);
    for (const e of p.edges ?? []) {
      if (e.kind === "relationship" && e.target_product) {
        const to = e.target_product;
        node({ id: productNode(to.id), type: "product", label: to.name, domains: [to.domain_id] });
        const id = `product_link:${pid}>${productNode(to.id)}`;
        if (!edges.has(id))
          edges.set(id, {
            id,
            a: pid,
            b: productNode(to.id),
            kind: "product_link",
            paths: e.path_count,
            ...(e.aliases ? { label: e.aliases } : {}),
          });
      }
    }
    for (const c of p.commands ?? []) {
      node({ id: `f:${c.name}`, type: "command", label: c.name, domains: [c.domain_id] });
      edge(pid, `f:${c.name}`, "command");
    }
    for (const t of p.member_tables ?? []) {
      const label = `${t.domain_id}.${t.table_name}`;
      if (t.dq_contract != null) {
        // A checker assigned to the product (REQ-1443): its rows are quality evidence.
        const kid = `k:${t.id}`;
        node({ id: kid, type: "checker", label, domains: [t.domain_id] });
        edge(pid, kid, "member");
        if (t.checks) edge(kid, tableNode(t.checks.id), "checks");
        continue;
      }
      const tid = tableNode(t.id);
      const columnSens = (t.table_columns ?? []).flatMap((c) => sens.get(c.column_key) ?? []);
      node({
        id: tid,
        type: "table",
        label,
        domains: [t.domain_id],
        tags: tagIds(t),
        ...(sensitive === null ? {} : { sensitivities: [...new Set(columnSens)] }),
        ...facts(tableFacts(t)),
      });
      edge(pid, tid, "member");
      tag(t, tid);
      for (const c of t.table_columns ?? []) {
        const cid = `c:${c.column_key}`;
        node({
          id: cid,
          type: "column",
          // Named with its table: drawn without it (the Ontology preset), "email" alone says little.
          label: `${t.table_name}.${c.column_name}`,
          parent: tid,
          domains: [t.domain_id],
          // A member's column: the product holds it.
          inProduct: true,
          tags: tagIds(c),
          ...(sensitive === null ? {} : { sensitivities: sens.get(c.column_key) ?? [] }),
          ...facts(columnFacts(c)),
        });
        edge(tid, cid, "has_column");
        tag(c, cid);
        for (const r of c.glossary_ref ?? []) {
          if (!r.term) continue;
          term(r.term);
          edge(`term:${r.term.id}`, cid, "term_binding");
        }
      }
      for (const r of t.profile_results ?? []) {
        // Its table is its parent: a profile is reached on its table's page.
        node({ id: `r:${r.id}`, type: "profile", label: r.table_name, parent: tid });
        edge(`r:${r.id}`, tid, "profiled");
      }
      for (const rel of t.source_relationships ?? []) {
        const to = rel.target_table;
        if (!to) continue;
        if (to.product_id === p.id) {
          edge(tid, tableNode(to.id), "relationship");
        } else {
          const xid = `x:${to.id}`;
          node({
            id: xid,
            type: "relation",
            label: `${to.domain_id}.${to.table_name}`,
            domains: [to.domain_id],
          });
          edge(tid, xid, "relationship_outside");
        }
      }
    }
  }
  for (const e of termEdges ?? []) {
    // An edge to a term the role cannot read has no end to draw.
    if (!e.source_term || !e.target_term) continue;
    term(e.source_term);
    term(e.target_term);
    edge(`term:${e.from_term_id}`, `term:${e.to_term_id}`, "term_edge", e.rel_type);
  }
  return { nodes: [...nodes.values()], edges: [...edges.values()] };
}

/** ``info`` on a node, its facts not read left out; nothing when none was read. */
export function facts(info: NodeInfo): { info?: NodeInfo } {
  const kept = Object.fromEntries(
    Object.entries(info).filter(([, v]) => v !== undefined && v !== null && v !== ""),
  ) as NodeInfo;
  return Object.keys(kept).length === 0 ? {} : { info: kept };
}

/** A term's facts, as TERM_FACTS reads them: its experts are its stewards. */
export function termFacts(t: TermFactsRow): NodeInfo {
  return {
    status: t.status ?? undefined,
    // One-to-many: null when it has none (rowsOf); absent when the role cannot read them.
    stewards: rowsOf(t.experts)
      ?.filter((x) => x.kind === "expert")
      .map((x) => x.user_id),
  };
}

/** A column's facts, as COLUMN_FACTS reads them. */
export function columnFacts(c: ColumnFacts): NodeInfo {
  // A JSON column comes back as its text or as its value, as the role's schema types it
  // (asProduct reads custom_properties the same way).
  const masks = (typeof c.masks === "string" ? JSON.parse(c.masks) : c.masks) as
    | MaskEntry[]
    | null
    | undefined;
  return {
    dataType: c.data_type ?? undefined,
    description: c.description ?? undefined,
    masked: masks == null ? undefined : masks.some((m) => m.mask !== null),
  };
}

/** A table's facts, as TABLE_FACTS reads them: its quality is its checkers' worst latest scan. */
export function tableFacts(t: Pick<TableRow, "source_id" | "description" | "checked_by">): NodeInfo {
  const runs = t.checked_by ?? [];
  const outcomes = runs.map((r) => r.latest_outcome);
  const at = runs.map((r) => r.latest_run_at).filter((x): x is string => x !== null);
  return {
    source: t.source_id ?? undefined,
    description: t.description ?? undefined,
    quality:
      runs.length === 0
        ? undefined
        : outcomes.includes("fail")
          ? "fail"
          : outcomes.includes("pass")
            ? "pass"
            : "none",
    checkedAt: at.length === 0 ? undefined : at.sort().at(-1),
  };
}
