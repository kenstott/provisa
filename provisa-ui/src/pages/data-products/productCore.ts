// Copyright (c) 2026 Kenneth Stott
// Canary: 9e2b7c41-3d58-4a16-b0f7-c4a1e8d2f593
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2127: the Data Products page and the core of its detail panel, read from the meta domain
// through the governed pipeline as the signed-in user: the product list from data_products_meta,
// each panel section's summary from its relationships (REQ-2125) and data_product_edges
// (REQ-2126). The panel's queries are shown as it runs them: the nested GraphQL read, and the
// product's row by SQL and by JSON:API.

import { runSql } from "../../api/admin";
import {
  dataGraphql,
  rootQuery,
  rowsOf,
  type MetaReader,
  type Selection,
} from "../../api/metaGraph";
import {
  toGraphData,
  type ProductRow as GraphProductRow,
} from "../../components/graph3d/productGraphData";
import type { GraphData } from "../../components/graph3d/graphModel";
import { quoteIdent, tableSqlRef } from "../../naming";
import type { DataProduct } from "../../types/admin";
import type { LifecycleStatus } from "../../types/lifecycle";

/** The product row's own fields, as data_products_meta names them. */
export const PRODUCT_FIELDS = [
  "id",
  "domain_id",
  "name",
  "owner_role",
  "team_role",
  "purpose",
  "limitations",
  "usage",
  "version",
  "status",
  "status_reason",
  "retire_on",
  "successor_id",
  "sla",
  "support",
  "support_contact",
  "custom_properties",
] as const;

type ProductRow = Record<(typeof PRODUCT_FIELDS)[number], unknown>;

const TERM: Selection = { fields: ["id", "name", "definition", "is_abstract", "live"] };

// REQ-1443 clause 10: a checker table's rules, parsed from its contract at registration.
const DQ_RULES: NonNullable<Selection["rels"]>[string] = {
  alias: "dqRules",
  sel: { fields: ["position", "column_name", "check_type", "definition"] },
};

const PANEL: Selection = {
  fields: ["id"],
  rels: {
    member_tables: {
      alias: "memberTables",
      sel: {
        fields: [
          "id",
          "source_id",
          "domain_id",
          "schema_name",
          "table_name",
          "alias",
          "description",
          "dq_contract",
        ],
        rels: {
          checks: { alias: "checks", sel: { fields: ["id", "domain_id", "table_name", "alias"] } },
          dq_rules: DQ_RULES,
          table_columns: {
            to: "table_columns",
            sel: {
              fields: ["column_name", "column_key"],
              rels: {
                glossary_ref: {
                  alias: "glossaryRef",
                  sel: { fields: ["term_id"], rels: { term: { alias: "term", sel: TERM } } },
                },
              },
            },
          },
          profile_results: {
            alias: "profileResults",
            sel: { fields: ["id", "domain_id", "table_name", "alias"] },
          },
          // REQ-351: its relationships, under the effective names stored on each (the panel's
          // Relationships card and its Cypher click-through between member tables).
          source_relationships: {
            alias: "sourceRelationships",
            sel: {
              fields: [
                "id",
                "alias",
                "graphql_field",
                "cypher_type",
                "cardinality",
                "source_column",
                "target_column",
                "disable_cypher",
              ],
              rels: {
                target_table: {
                  alias: "targetTable",
                  sel: { fields: ["id", "domain_id", "table_name", "product_id"] },
                },
              },
            },
          },
        },
      },
    },
    commands: { alias: "commands", sel: { fields: ["name", "description", "domain_id"] } },
    tags: { alias: "tags", sel: { fields: ["tag_id"] } },
    edges: {
      alias: "edges",
      sel: {
        fields: [
          "id",
          "kind",
          "target_product_id",
          "target_table_id",
          "direct",
          "hops",
          "path_count",
          "aliases",
          "supporting_tables",
        ],
        rels: {
          target_product: { alias: "targetProduct", sel: { fields: ["id", "name", "domain_id"] } },
          target_table: {
            alias: "targetTable",
            sel: { fields: ["id", "domain_id", "table_name", "alias", "description"] },
          },
        },
      },
    },
  },
};

export interface CoreTerm {
  id: number;
  name: string;
  definition: string | null;
  is_abstract: boolean;
  live: boolean;
}

export interface CoreTable {
  id: number;
  source_id: string;
  domain_id: string;
  schema_name: string;
  table_name: string;
  alias: string | null;
  description: string | null;
  dq_contract: string | null;
  checks?: { id: number; domain_id: string; table_name: string; alias: string | null } | null;
  dq_rules?: CoreDqRule[] | null;
  table_columns?:
    | {
        column_name: string;
        column_key: string;
        glossary_ref?: { term_id: number; term?: CoreTerm | null }[] | null;
      }[]
    | null;
  profile_results?:
    | { id: number; domain_id: string; table_name: string; alias: string | null }[]
    | null;
  source_relationships?: CoreRelationship[] | null;
}

/** One check of a checker table's contract, as meta's dq_rules serves it (REQ-1443 clause 10). */
export interface CoreDqRule {
  position: number;
  column_name: string;
  check_type: string;
  definition: string;
}

/** A relationship of a member table, under its stored effective names (REQ-351). */
export interface CoreRelationship {
  id: string;
  alias: string | null;
  // NULL for a junction-backed edge, which is no GraphQL field (REQ-1586).
  graphql_field: string | null;
  // Every relationship has its served type stored (relationship_names.store_effective_names).
  cypher_type: string;
  cardinality: string;
  source_column: string;
  target_column: string | null;
  disable_cypher: boolean;
  target_table?: {
    id: number;
    domain_id: string;
    table_name: string;
    product_id?: string | null;
  } | null;
}

export interface CoreEdge {
  /** REQ-2126: joins (relationship, the graph's) and direct neighbours (related, the panel's).
   * A product's lineage is the product lineage endpoint's (REQ-2156). */
  kind: "relationship" | "related";
  /** The edge's key in data_product_edges, unique per product. */
  id: string;
  target_product_id: string | null;
  target_table_id: number | null;
  direct: boolean;
  hops: number;
  path_count: number;
  aliases: string | null;
  supporting_tables: string | null;
  target_product?: { id: string; name: string; domain_id: string } | null;
  target_table?: {
    id: number;
    domain_id: string;
    table_name: string;
    alias: string | null;
    description: string | null;
  } | null;
}

/** A registered table as the model names it: its domain, then its alias where it has one. Every
 * table the panel names is named so -- never by its physical schema or relation. */
export function modelName(t: {
  domain_id: string;
  table_name: string;
  alias: string | null;
}): string {
  return `${t.domain_id}.${t.alias || t.table_name}`;
}

interface PanelRow {
  id: string;
  member_tables?: CoreTable[] | null;
  commands?: { name: string; description: string | null; domain_id: string }[] | null;
  tags?: { tag_id: string }[] | null;
  edges?: CoreEdge[] | null;
}

/** What the panel shows of one product; a part the role cannot read is undefined. */
export interface ProductCore {
  product: DataProduct;
  tables?: CoreTable[];
  commands?: NonNullable<PanelRow["commands"]>;
  tags?: string[];
  terms?: CoreTerm[];
  edges?: CoreEdge[];
  queries: { graphql: string; sql: string; jsonapi: string };
}

const text = (v: unknown) => (v == null ? null : String(v));

/** A data_products_meta row as the page's DataProduct. */
export function asProduct(row: ProductRow): DataProduct {
  const custom = row.custom_properties;
  return {
    id: String(row.id),
    domainId: String(row.domain_id),
    name: String(row.name),
    ownerRole: text(row.owner_role),
    teamRole: text(row.team_role),
    purpose: String(row.purpose ?? ""),
    limitations: String(row.limitations ?? ""),
    usage: String(row.usage ?? ""),
    version: text(row.version),
    status: String(row.status) as LifecycleStatus,
    statusReason: text(row.status_reason),
    retireOn: text(row.retire_on),
    successorId: text(row.successor_id),
    sla: text(row.sla),
    support: text(row.support),
    supportContact: text(row.support_contact),
    customProperties:
      typeof custom === "string"
        ? (JSON.parse(custom) as Record<string, unknown>)
        : ((custom ?? {}) as Record<string, unknown>),
  };
}

/** The product list: every product the role reads. Null when it cannot read data_products_meta. */
export function listQuery(m: MetaReader): string | null {
  return rootQuery(m, "DataProducts", "data_products", { fields: [...PRODUCT_FIELDS] });
}

export async function loadProducts(m: MetaReader, role: string): Promise<DataProduct[] | null> {
  const q = listQuery(m);
  if (q === null) return null;
  const { data_products } = await dataGraphql<{ data_products: ProductRow[] }>(q, {}, role);
  return data_products.map(asProduct);
}

const PRODUCTS_REF = { domainId: "meta", tableName: "data_products" };

/** The panel's three reads of ``product``; null when the role cannot read the product view. */
export function panelQueries(m: MetaReader, product: string): ProductCore["queries"] | null {
  const graphql = rootQuery(m, "DataProductPanel", "data_products", PANEL, {
    id: { eq: product },
  });
  if (graphql === null) return null;
  const literal = `'${product.replace(/'/g, "''")}'`;
  const sql = `SELECT ${PRODUCT_FIELDS.map(quoteIdent).join(", ")} FROM ${tableSqlRef(PRODUCTS_REF)} WHERE ${quoteIdent("id")} = ${literal}`;
  const jsonapi = `/data/jsonapi/meta/data_products?filter[id]=${encodeURIComponent(product)}`;
  return { graphql, sql, jsonapi };
}

/** The terms the product's columns are bound to, live ones, with the abstract terms their edges
 * reach, followed until none is new (REQ-1634's closure, read from the meta domain). */
async function termClosure(m: MetaReader, role: string, bound: CoreTerm[]): Promise<CoreTerm[]> {
  const terms = new Map(bound.map((t) => [t.id, t]));
  let frontier = bound.map((t) => t.id);
  while (frontier.length > 0) {
    const eq = rootQuery(
      m,
      "TermEdges",
      "_term_edges",
      { fields: ["to_term_id"] },
      { from_term_id: { in: frontier } },
    );
    if (eq === null) break;
    const { _term_edges: out } = await dataGraphql<{
      _term_edges: { to_term_id: number }[];
    }>(eq, {}, role);
    const fresh = [...new Set(out.map((e) => e.to_term_id))].filter((id) => !terms.has(id));
    if (fresh.length === 0) break;
    const tq = rootQuery(m, "Terms", "_terms", TERM, { id: { in: fresh } });
    if (tq === null) break;
    const { _terms: found } = await dataGraphql<{ _terms: CoreTerm[] }>(tq, {}, role);
    const abstract = found.filter((t) => t.is_abstract);
    for (const t of abstract) terms.set(t.id, t);
    frontier = abstract.map((t) => t.id);
  }
  return [...terms.values()].filter((t) => t.live);
}

export async function loadProductCore(
  m: MetaReader,
  role: string,
  product: string,
): Promise<ProductCore | null> {
  const queries = panelQueries(m, product);
  if (queries === null) return null;
  const [nested, sql, jsonapi] = await Promise.all([
    dataGraphql<{ data_products: PanelRow[] }>(queries.graphql, {}, role),
    runSql(queries.sql, role),
    fetch(queries.jsonapi, { headers: { "X-Provisa-Role": role } }).then(async (r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`);
      return (await r.json()) as { data: { attributes: Record<string, unknown> }[] };
    }),
  ]);
  if (sql.error) throw new Error(sql.error);
  const [row] = nested.data_products;
  const [fields] = sql.rows;
  if (!row || !fields) throw new Error(`No data product ${product}`);
  const [doc] = jsonapi.data;
  const productRow = { ...(fields as ProductRow) };
  // JSON:API reads the same row under its physical column names; its custom properties come as
  // JSON, not a string.
  if (!doc) throw new Error(`No data product ${product} over JSON:API`);
  productRow.custom_properties = doc.attributes.custom_properties;
  const members = rowsOf(row.member_tables);
  const bound = new Map<number, CoreTerm>();
  for (const t of members ?? [])
    for (const c of t.table_columns ?? [])
      for (const r of c.glossary_ref ?? []) if (r.term) bound.set(r.term.id, r.term);
  return {
    product: asProduct(productRow),
    tables: members,
    commands: rowsOf(row.commands),
    tags: rowsOf(row.tags)?.map((t) => t.tag_id),
    terms: (members ?? []).some((t) => t.table_columns !== undefined)
      ? await termClosure(m, role, [...bound.values()])
      : undefined,
    edges: rowsOf(row.edges),
    queries,
  };
}

/** The product's graph as its core holds it -- the product, its member tables and checkers, their
 *  columns and the terms bound to them, its commands and its links -- built the way the product's
 *  graph query is (toGraphData), so the graph card is drawn from the panel's one read. */
export function coreGraph(core: ProductCore): GraphData {
  const row: GraphProductRow = {
    id: core.product.id,
    name: core.product.name,
    domain_id: core.product.domainId,
    member_tables: core.tables ?? [],
    commands: core.commands ?? [],
    edges: core.edges ?? [],
    tags: (core.tags ?? []).map((tag_id) => ({ tag_id })),
  };
  return toGraphData([row], null, null);
}
