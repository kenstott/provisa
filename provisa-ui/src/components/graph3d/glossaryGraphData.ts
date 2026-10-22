// Copyright (c) 2026 Kenneth Stott
// Canary: 8f3a6d21-4c97-4e05-b1a8-2d7e9c5f4b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: the glossary's Ontology View data, read from the meta domain as the signed-in user --
// glossary_terms_meta (its standing columns), the domains each term is in, its refs to
// columns and the products owning those columns' tables, and glossary_term_edges_meta -- turned
// into the graph's nodes and edges.

import { rootQuery, type MetaReader, type Selection } from "../../api/metaGraph";
import type { GraphData, GraphEdge, GraphNode, TermStatus } from "./graphModel";

/** REQ-1593: a term in every domain, the org's shared vocabulary. */
export const ENTERPRISE_DOMAIN = "*";

const TERMS: Selection = {
  fields: ["id", "name", "is_abstract", "deprecated", "retired", "live"],
  rels: {
    domains: { alias: "domains", sel: { fields: ["domain_id"] } },
    refs: {
      alias: "refs",
      sel: {
        fields: ["column_key", "column_name", "table_id", "table_name", "domain_id"],
        rels: {
          table: {
            to: "registered_tables",
            sel: {
              fields: ["id"],
              rels: {
                product: { alias: "product", sel: { fields: ["id", "name", "domain_id"] } },
              },
            },
          },
        },
      },
    },
  },
};

export interface TermRow {
  id: number;
  name: string;
  is_abstract: boolean;
  deprecated: boolean;
  retired: boolean;
  live: boolean;
  // One-to-many: null when the term has none (see rowsOf).
  domains?: { domain_id: string }[] | null;
  refs?:
    | {
        column_key: string;
        column_name: string;
        table_id: number;
        table_name: string;
        domain_id: string;
        table?: {
          id: number;
          product?: { id: string; name: string; domain_id: string } | null;
        } | null;
      }[]
    | null;
}

export interface TermEdgeRow {
  id: number;
  from_term_id: number;
  to_term_id: number;
  rel_type: string;
}

export function termsQuery(m: MetaReader): string | null {
  return rootQuery(m, "GlossaryTerms", "glossary_terms", TERMS);
}

export function termEdgesQuery(m: MetaReader): string | null {
  return rootQuery(m, "GlossaryTermEdges", "glossary_term_edges", {
    fields: ["id", "from_term_id", "to_term_id", "rel_type"],
  });
}

/** A term's standing (REQ-1387: proposed is in service and not live). The graph names a term by its
 * title and marks its standing by style; it carries neither the term's definition nor reasons. */
export function standing(t: TermRow): TermStatus {
  if (t.retired) return "retired";
  if (t.deprecated) return "deprecated";
  if (t.live) return "live";
  return "proposed";
}

/** The glossary's graph. */
export function glossaryGraph(terms: TermRow[], edges: TermEdgeRow[]): GraphData {
  const nodes = new Map<string, GraphNode>();
  const out = new Map<string, GraphEdge>();
  const node = (n: GraphNode) => {
    if (!nodes.has(n.id)) nodes.set(n.id, n);
  };
  const edge = (a: string, b: string, kind: GraphEdge["kind"], label?: string) => {
    const id = `${kind}:${a}>${b}${label ? `:${label}` : ""}`;
    if (!out.has(id)) out.set(id, { id, a, b, kind, ...(label ? { label } : {}) });
  };
  const named = new Set<string>();
  for (const t of terms)
    for (const d of t.domains ?? []) if (d.domain_id !== ENTERPRISE_DOMAIN) named.add(d.domain_id);
  for (const d of named) node({ id: `d:${d}`, type: "domain", label: d });

  for (const t of terms) {
    const id = `term:${t.id}`;
    const status = standing(t);
    const domains = (t.domains ?? []).map((d) => d.domain_id);
    node({
      id,
      type: "term",
      label: t.name,
      status,
      hollow: t.is_abstract,
      // An enterprise-wide term is in every domain: no domain filter leaves it out.
      ...(domains.includes(ENTERPRISE_DOMAIN) ? {} : { domains }),
    });
    // An enterprise-wide term is joined to every domain, so it settles among them, at the centre.
    for (const d of domains.includes(ENTERPRISE_DOMAIN) ? named : domains)
      edge(id, `d:${d}`, "in_domain");
    for (const r of t.refs ?? []) {
      const tid = `t:${r.table_id}`;
      const cid = `c:${r.column_key}`;
      node({
        id: tid,
        type: "table",
        label: `${r.domain_id}.${r.table_name}`,
        domains: [r.domain_id],
      });
      node({
        id: cid,
        type: "column",
        label: `${r.table_name}.${r.column_name}`,
        parent: tid,
        domains: [r.domain_id],
      });
      edge(tid, cid, "has_column");
      edge(id, cid, "term_binding");
      const p = r.table?.product;
      if (p) {
        node({ id: `p:${p.id}`, type: "product", label: p.name, domains: [p.domain_id] });
        edge(`p:${p.id}`, tid, "member");
      }
    }
  }
  for (const e of edges) {
    const a = `term:${e.from_term_id}`;
    const b = `term:${e.to_term_id}`;
    // An edge to a term the role cannot read has no end to draw.
    if (nodes.has(a) && nodes.has(b)) edge(a, b, "term_edge", e.rel_type);
  }
  return { nodes: [...nodes.values()], edges: [...out.values()] };
}
