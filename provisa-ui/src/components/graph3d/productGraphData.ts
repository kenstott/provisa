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
import type { GraphData, GraphEdge, GraphNode, QueryScope } from "./graphModel";

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

const TERM_EDGE_FIELDS = ["id", "from_term_id", "to_term_id", "rel_type"];

function columnSel(f: ProductFilters): Selection {
  const typed = f.edgeTypes.length === 0 ? undefined : { rel_type: { in: f.edgeTypes } };
  return {
    fields: ["column_name", "column_key"],
    rels: {
      glossary_ref: {
        alias: "glossaryRef",
        sel: {
          fields: ["term_id"],
          rels: {
            term: {
              alias: "term",
              sel: {
                fields: ["id", "name"],
                // The term's edges either way, with the term at the other end (REQ-2136: one
                // query, where the graph read them by bound term in two more).
                rels: {
                  edges_out: {
                    alias: "edgesOut",
                    sel: {
                      fields: TERM_EDGE_FIELDS,
                      rels: { target_term: { alias: "targetTerm", sel: { fields: ["id", "name"] } } },
                    },
                    where: typed,
                  },
                  edges_in: {
                    alias: "edgesIn",
                    sel: {
                      fields: TERM_EDGE_FIELDS,
                      rels: { source_term: { alias: "sourceTerm", sel: { fields: ["id", "name"] } } },
                    },
                    where: typed,
                  },
                },
              },
            },
          },
        },
      },
      ...tagRel(f),
      sensitivity: { alias: "sensitivity", sel: { fields: ["tags"] } },
      // REQ-2129: the column-level edges deriving this column (view_governance only).
      upstream: {
        alias: "upstream",
        sel: { fields: ["source_column_key", "source_relation", "source_column", "via"] },
      },
    },
  };
}

function tableSel(f: ProductFilters): Selection {
  const dom = inDomains(f);
  return {
    fields: ["id", "domain_id", "table_name", "dq_contract", "product_id"],
    rels: {
      table_columns: {
        to: "table_columns",
        sel: columnSel(f),
        where: all([tagged(f), sensitiveColumn(f)]),
      },
      ...tagRel(f),
      profile_results: { alias: "profileResults", sel: { fields: ["id", "table_name"] } },
      checks: { alias: "checks", sel: { fields: ["id"] } },
      checked_by: {
        alias: "checkedBy",
        sel: {
          fields: ["id", "domain_id", "table_name", "dq_contract"],
          rels: { checks: { alias: "checks", sel: { fields: ["id"] } } },
        },
        where: dom,
      },
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
    fields: ["id", "name", "domain_id"],
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
          fields: [
            "kind",
            "direction",
            "target_product_id",
            "target_table_id",
            "target_relation",
            "hops",
            "path_count",
            "aliases",
          ],
          rels: {
            target_product: {
              alias: "targetProduct",
              sel: { fields: ["id", "name", "domain_id"] },
            },
          },
        },
        where: dom && { _or: [{ kind: { eq: "lineage" } }, { target_product: dom }] },
      },
    },
  };
}

interface Tagged {
  tags?: { tag_id: string }[] | null;
}

interface ColumnRow extends Tagged {
  column_name: string;
  column_key: string;
  upstream?:
    | {
        source_column_key: string;
        source_relation: string;
        source_column: string;
        via: string;
      }[]
    | null;
  glossary_ref?:
    | {
        term_id: number;
        term?: {
          id: number;
          name: string;
          edges_out?: (TermEdgeEnd & { target_term?: TermName | null })[] | null;
          edges_in?: (TermEdgeEnd & { source_term?: TermName | null })[] | null;
        } | null;
      }[]
    | null;
  // Its row in sensitive_coverage when it is sensitive; undefined when the role cannot read it.
  sensitivity?: { tags: string }[] | null;
}

interface TermName {
  id: number;
  name: string;
}

interface TermEdgeEnd {
  id: number;
  from_term_id: number;
  to_term_id: number;
  rel_type: string;
}

interface TableRow extends Tagged {
  id: number;
  domain_id: string;
  table_name: string;
  dq_contract?: string | null;
  product_id?: string | null;
  table_columns?: ColumnRow[] | null;
  profile_results?: { id: number; table_name: string }[] | null;
  checks?: { id: number } | null;
  checked_by?: TableRow[] | null;
  source_relationships?:
    | {
        id: number;
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
  kind: "relationship" | "lineage";
  direction: "upstream" | "downstream" | null;
  target_product_id: string | null;
  target_table_id: number | null;
  target_relation: string | null;
  hops: number;
  path_count: number;
  aliases: string | null;
  target_product?: { id: string; name: string; domain_id: string } | null;
}

export interface ProductRow extends Tagged {
  id: string;
  name: string;
  domain_id: string;
  member_tables?: TableRow[] | null;
  commands?: { name: string; domain_id: string }[] | null;
  edges?: EdgeRow[] | null;
}

export interface TermEdgeRow {
  id: number;
  from_term_id: number;
  to_term_id: number;
  rel_type: string;
  from_term: string;
  to_term: string;
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
  return field === null ? null : operation("DataProductGraph", [field]);
}

/**
 * The graph of the product graph's one query: its term edges and its columns' sensitivity read off
 * the columns, null when the role cannot read them (as toGraphData takes them).
 */
export function productGraph(products: ProductRow[]): GraphData {
  const columns = products.flatMap((p) =>
    (p.member_tables ?? []).flatMap((t) =>
      (t.table_columns ?? []).map((c) => ({ table: t.id, c })),
    ),
  );
  const readable = (pick: (c: ColumnRow) => unknown) =>
    columns.length === 0 || columns.every(({ c }) => pick(c) !== undefined);
  const terms = columns.flatMap(({ c }) => (c.glossary_ref ?? []).flatMap((r) => r.term ?? []));
  const termEdges: TermEdgeRow[] | null =
    terms.length > 0 && terms.every((t) => t.edges_out !== undefined && t.edges_in !== undefined)
      ? terms.flatMap((t) => [
          ...(t.edges_out ?? []).flatMap((e) =>
            e.target_term ? [{ ...e, from_term: t.name, to_term: e.target_term.name }] : [],
          ),
          ...(t.edges_in ?? []).flatMap((e) =>
            e.source_term ? [{ ...e, from_term: e.source_term.name, to_term: t.name }] : [],
          ),
        ])
      : terms.length === 0
        ? []
        : null;
  const sensitive: SensitiveRow[] | null = readable((c) => c.sensitivity)
    ? columns.flatMap(({ table, c }) =>
        (c.sensitivity ?? []).map((x) => ({ column_key: c.column_key, table_id: table, tags: x.tags })),
      )
    : null;
  return toGraphData(products, termEdges, sensitive);
}

export const productNode = (id: string) => `p:${id}`;
export const tableNode = (id: number) => `t:${id}`;

/**
 * ``members`` and the checker tables scanning them, each once. REQ-1443 clause 10: a checker
 * belongs to the product of the table it scans, so it is reached through that member's checkedBy;
 * one also assigned to the product directly is a member already.
 */
export function withCheckers<T extends { id: number; checked_by?: T[] | null }>(members: T[]): T[] {
  const seen = new Set(members.map((t) => t.id));
  const checkers = members
    .flatMap((t) => t.checked_by ?? [])
    .filter((k) => !seen.has(k.id) && seen.add(k.id));
  return [...members, ...checkers];
}

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
    const id = `${kind}:${a}>${b}`;
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
  const term = (id: number, name: string) => node({ id: `term:${id}`, type: "term", label: name });

  for (const p of products) {
    const pid = productNode(p.id);
    // A product another one links to is drawn before its own row is read: its row decides.
    nodes.set(pid, {
      id: pid,
      type: "product",
      label: p.name,
      domains: [p.domain_id],
      tags: tagIds(p),
      lane: 0,
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
      } else if (e.kind === "lineage" && e.target_relation) {
        // Upstream ancestry stands in lanes by its distance; a consumer one lane downstream.
        const xid = `l:${e.target_relation}`;
        const lane = e.direction === "upstream" ? -e.hops : 1;
        node({ id: xid, type: "relation", label: e.target_relation, lane });
        if (e.direction === "upstream") edge(xid, pid, "lineage");
        else edge(pid, xid, "lineage");
      }
    }
    for (const c of p.commands ?? []) {
      node({ id: `f:${c.name}`, type: "command", label: c.name, domains: [c.domain_id] });
      edge(pid, `f:${c.name}`, "command");
    }
    for (const t of withCheckers(p.member_tables ?? [])) {
      const label = `${t.domain_id}.${t.table_name}`;
      if (t.dq_contract != null) {
        // A checker belongs because the table it scans does; its rows are quality evidence.
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
        });
        edge(tid, cid, "has_column");
        tag(c, cid);
        for (const u of c.upstream ?? []) {
          const sid = `c:${u.source_column_key}`;
          // A column outside the product stands alone, named by its relation.
          node({
            id: sid,
            type: "column",
            label: `${u.source_relation}.${u.source_column}`,
            inProduct: false,
          });
          const id = `column_lineage:${sid}>${cid}`;
          if (!edges.has(id))
            edges.set(id, { id, a: sid, b: cid, kind: "column_lineage", label: u.via });
        }
        for (const r of c.glossary_ref ?? []) {
          if (!r.term) continue;
          term(r.term.id, r.term.name);
          edge(`term:${r.term.id}`, cid, "term_binding");
        }
      }
      for (const r of t.profile_results ?? []) {
        node({ id: `r:${r.id}`, type: "profile", label: r.table_name });
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
    term(e.from_term_id, e.from_term);
    term(e.to_term_id, e.to_term);
    edge(`term:${e.from_term_id}`, `term:${e.to_term_id}`, "term_edge", e.rel_type);
  }
  return { nodes: [...nodes.values()], edges: [...edges.values()] };
}
