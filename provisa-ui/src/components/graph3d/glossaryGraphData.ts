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
// glossary_terms_meta (its standing columns), the domains each term is in, its refs to columns
// with those columns' tables and the relationships declared from them, and
// glossary_term_edges_meta; and the data products with their datasets, the datasets' columns and
// the terms those columns are -- turned into the graph's nodes and edges.

import { rootQuery, type MetaReader, type Selection } from "../../api/metaGraph";
import type { GraphData, GraphEdge, GraphNode, LayerId, TermStatus } from "./graphModel";

/** REQ-1593: a term in every domain, the org's shared vocabulary. */
export const ENTERPRISE_DOMAIN = "*";

/** A relationship declared from a table: its name is its alias, else its id. */
const RELATIONSHIP: Selection = { fields: ["id", "alias", "target_table_id"] };

const TERMS: Selection = {
  fields: ["id", "name", "definition", "is_abstract", "deprecated", "retired", "live"],
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
                source_relationships: { alias: "sourceRelationships", sel: RELATIONSHIP },
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
  definition: string | null;
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
          source_relationships?: RelationshipRow[] | null;
        } | null;
      }[]
    | null;
}

interface RelationshipRow {
  id: string;
  alias: string | null;
  target_table_id: number | null;
}

/** A data product with its datasets, their columns and the terms those columns are. */
const PRODUCTS: Selection = {
  fields: ["id", "name", "domain_id"],
  rels: {
    member_tables: {
      alias: "memberTables",
      sel: {
        fields: ["id", "domain_id", "table_name"],
        rels: {
          table_columns: {
            to: "table_columns",
            sel: {
              fields: ["column_name", "column_key"],
              rels: { glossary_ref: { alias: "glossaryRef", sel: { fields: ["term_id"] } } },
            },
          },
          source_relationships: { alias: "sourceRelationships", sel: RELATIONSHIP },
        },
      },
    },
  },
};

export interface ProductRow {
  id: string;
  name: string;
  domain_id: string;
  // One-to-many: null when there are none (see rowsOf).
  member_tables?:
    | {
        id: number;
        domain_id: string;
        table_name: string;
        table_columns?:
          | {
              column_name: string;
              column_key: string;
              glossary_ref?: { term_id: number }[] | null;
            }[]
          | null;
        source_relationships?: RelationshipRow[] | null;
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

/** The data products with their datasets and columns; null when the role cannot read them. */
export function productsQuery(m: MetaReader): string | null {
  return rootQuery(m, "OntologyProducts", "data_products", PRODUCTS);
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

/**
 * The glossary's graph. Each node and edge says which layers draw it where its kind's own layer is
 * not the whole answer:
 * - a column a term IS is Columns'; a data product's column is Data's too, and its datasets' other
 *   columns are Data's alone;
 * - a table is drawn only through a column drawn (Tables, with Columns) or as a product's dataset
 *   (Data); the relationships declared between the tables drawn are Tables' (ownerTables).
 */
export function glossaryGraph(
  terms: TermRow[],
  edges: TermEdgeRow[],
  products: ProductRow[] = [],
): GraphData {
  const nodes = new Map<string, GraphNode>();
  const out = new Map<string, GraphEdge>();
  // A node or edge met again (a term's column that is also a product's) is drawn by either's layers.
  const widen = (into: { layers?: LayerId[] }, layers: LayerId[] | undefined) => {
    if (!layers || !into.layers) return;
    into.layers = [...new Set([...into.layers, ...layers])];
  };
  const node = (n: GraphNode) => {
    const seen = nodes.get(n.id);
    if (!seen) nodes.set(n.id, n);
    else {
      widen(seen, n.layers);
      if (n.inProduct) seen.inProduct = true;
    }
  };
  const edge = (
    a: string,
    b: string,
    kind: GraphEdge["kind"],
    label?: string,
    layers?: LayerId[],
  ) => {
    const id = `${kind}:${a}>${b}${label ? `:${label}` : ""}`;
    const seen = out.get(id);
    if (!seen)
      out.set(id, { id, a, b, kind, ...(label ? { label } : {}), ...(layers ? { layers } : {}) });
    else widen(seen, layers);
  };
  const table = (id: number, domain: string, name: string, dataset: boolean) =>
    node({
      id: `t:${id}`,
      type: "table",
      label: `${domain}.${name}`,
      domains: [domain],
      stored: true,
      layers: dataset ? ["data"] : [],
    });
  const relationships = (from: number, rows: RelationshipRow[] | null | undefined) => {
    for (const r of rows ?? [])
      if (r.target_table_id !== null)
        edge(`t:${from}`, `t:${r.target_table_id}`, "relationship", r.alias ?? r.id, [
          "ownerTables",
        ]);
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
      abstract: t.is_abstract,
      // For the side panel only; a proposal with none has nothing said there.
      ...(t.definition ? { definition: t.definition } : {}),
      // An enterprise-wide term is in every domain: no domain filter leaves it out.
      ...(domains.includes(ENTERPRISE_DOMAIN) ? {} : { domains }),
    });
    // An enterprise-wide term is joined to every domain, so it settles among them, at the centre.
    for (const d of domains.includes(ENTERPRISE_DOMAIN) ? named : domains)
      edge(id, `d:${d}`, "in_domain");
    for (const r of t.refs ?? []) {
      const tid = `t:${r.table_id}`;
      const cid = `c:${r.column_key}`;
      const p = r.table?.product;
      // A column whose table a data product holds is the Data layer's too.
      const held: LayerId[] = p ? ["columns", "data"] : ["columns"];
      table(r.table_id, r.domain_id, r.table_name, p != null);
      node({
        id: cid,
        type: "column",
        label: `${r.table_name}.${r.column_name}`,
        parent: tid,
        domains: [r.domain_id],
        inProduct: p != null,
        layers: held,
      });
      edge(tid, cid, "has_column", undefined, p ? ["data", "ownerTables"] : ["ownerTables"]);
      // glossary.py's physical refs: the term IS this column.
      edge(id, cid, "term_binding", "IS", held);
      if (p) {
        node({ id: `p:${p.id}`, type: "product", label: p.name, domains: [p.domain_id] });
        edge(`p:${p.id}`, tid, "member");
      }
      relationships(r.table_id, r.table?.source_relationships);
    }
  }
  for (const p of products) {
    node({ id: `p:${p.id}`, type: "product", label: p.name, domains: [p.domain_id] });
    for (const m of p.member_tables ?? []) {
      const tid = `t:${m.id}`;
      table(m.id, m.domain_id, m.table_name, true);
      edge(`p:${p.id}`, tid, "member");
      for (const c of m.table_columns ?? []) {
        const cid = `c:${c.column_key}`;
        const bound = (c.glossary_ref ?? []).map((g) => `term:${g.term_id}`);
        // A dataset's column is the Data layer's; one a term IS is Columns' as well.
        node({
          id: cid,
          type: "column",
          label: `${m.table_name}.${c.column_name}`,
          parent: tid,
          domains: [m.domain_id],
          inProduct: true,
          layers: bound.length > 0 ? ["data", "columns"] : ["data"],
        });
        // Tables joins only the columns drawn by Columns: a column no term is stays Data's.
        edge(
          tid,
          cid,
          "has_column",
          undefined,
          bound.length > 0 ? ["data", "ownerTables"] : ["data"],
        );
        // A term the role cannot read has no end to draw.
        for (const term of bound)
          if (nodes.has(term)) edge(term, cid, "term_binding", "IS", ["data", "columns"]);
      }
      relationships(m.id, m.source_relationships);
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
