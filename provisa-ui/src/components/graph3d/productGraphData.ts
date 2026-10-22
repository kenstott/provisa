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

import { rootQuery, rowsOf, type MetaReader, type Selection } from "../../api/metaGraph";
import type { GraphData, GraphEdge, GraphNode } from "./graphModel";

// Relationships by the GraphQL alias the meta domain declares for them (_meta_seed); the one
// declared without an alias (a table's columns) by the view it reaches.
const COLUMN: Selection = {
  fields: ["column_name", "column_key"],
  rels: {
    glossary_ref: {
      alias: "glossaryRef",
      sel: {
        fields: ["term_id"],
        rels: { term: { alias: "term", sel: { fields: ["id", "name"] } } },
      },
    },
    tags: { alias: "tags", sel: { fields: ["tag_id"] } },
    // REQ-2129: the column-level edges deriving this column (view_governance only).
    upstream: {
      alias: "upstream",
      sel: { fields: ["source_column_key", "source_relation", "source_column", "via"] },
    },
  },
};

const TABLE: Selection = {
  fields: ["id", "domain_id", "table_name", "dq_contract", "product_id"],
  rels: {
    table_columns: { to: "table_columns", sel: COLUMN },
    tags: { alias: "tags", sel: { fields: ["tag_id"] } },
    profile_results: { alias: "profileResults", sel: { fields: ["id", "table_name"] } },
    checks: { alias: "checks", sel: { fields: ["id"] } },
    checked_by: {
      alias: "checkedBy",
      sel: {
        fields: ["id", "domain_id", "table_name", "dq_contract"],
        rels: { checks: { alias: "checks", sel: { fields: ["id"] } } },
      },
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
    },
  },
};

export const PRODUCT: Selection = {
  fields: ["id", "name", "domain_id"],
  rels: {
    member_tables: { alias: "memberTables", sel: TABLE },
    commands: { alias: "commands", sel: { fields: ["name", "domain_id"] } },
    tags: { alias: "tags", sel: { fields: ["tag_id"] } },
    // REQ-2126: the product's links to the rest of the model.
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
          target_product: { alias: "targetProduct", sel: { fields: ["id", "name", "domain_id"] } },
        },
      },
    },
  },
};

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
  glossary_ref?: { term_id: number; term?: { id: number; name: string } | null }[] | null;
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

/** The product graph's queries: the products (one, or all) with their parts. */
export function productsQuery(m: MetaReader, product: string | null): string | null {
  return rootQuery(
    m,
    "DataProductGraph",
    "data_products",
    PRODUCT,
    product === null ? undefined : { id: { eq: product } },
  );
}

/** The term edges touching ``terms``, from either end; null when the role cannot read them. */
export function termEdgesQueries(m: MetaReader, terms: number[]): string[] | null {
  const sel = { fields: ["id", "from_term_id", "to_term_id", "rel_type", "from_term", "to_term"] };
  const out = (["from_term_id", "to_term_id"] as const).map((end) =>
    rootQuery(m, "TermEdges", "glossary_term_edges", sel, { [end]: { in: terms } }),
  );
  return out.every((q): q is string => q !== null) ? out : null;
}

export function sensitiveQuery(m: MetaReader, tables: number[]): string | null {
  return rootQuery(
    m,
    "SensitiveColumns",
    "sensitive_coverage",
    { fields: ["column_key", "table_id", "tags"] },
    { table_id: { in: tables } },
  );
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
