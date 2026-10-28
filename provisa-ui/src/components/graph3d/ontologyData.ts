// Copyright (c) 2026 Kenneth Stott
// Canary: 8f3a6d21-4c97-4e05-b1a8-2d7e9c5f4b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28, ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT): the
// one graph's data, read from the ontology schema (REQ-2163) as the signed-in user --
// ontology._terms (its standing columns), the domains each term is in, its refs to columns with
// those columns' tables (and the products holding them) and the relationships declared from them,
// and the edges leaving it (ontology._term_edges, read with the term); and the data products with
// their datasets, the datasets' columns and the terms those columns are -- turned into the graph's
// nodes and edges. A focus (a term or a product) and a depth narrow the read to a snippet.

import { termRole } from "../../api/glossary";
import {
  operation,
  rootField,
  rowsOf,
  type MetaReader,
  type Selection,
  type Where,
} from "../../api/metaGraph";
import type { MaskEntry } from "../../lib/masks";
import type { ProductInput, ProductLineage } from "../../api/lineage";
import { catalogTableName, relationName } from "../../naming";
import type {
  GraphData,
  GraphEdge,
  GraphNode,
  LayerId,
  NodeInfo,
  QueryScope,
  TermStatus,
} from "./graphModel";

/** A product's node id. */
export const productNode = (id: string) => `p:${id}`;

/** A term's node id. */
export const termNode = (id: number) => `term:${id}`;

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

/** A column's or a table's tags, which the graph's tag filter judges. */
const TAGS: NonNullable<Selection["rels"]> = {
  tags: { alias: "tags", sel: { fields: ["tag_id"] } },
};

/** A column's facts as COLUMN_FACTS reads them. */
interface ColumnFacts {
  data_type?: string | null;
  description?: string | null;
  // table_columns.masks: the ordered mask list (REQ-2059), as JSON.
  masks?: unknown;
}

/** A table's facts as TABLE_FACTS reads them. */
interface TableFacts {
  source_id?: string | null;
  description?: string | null;
  checked_by?: { latest_outcome: "pass" | "fail" | null; latest_run_at: string | null }[] | null;
}

interface Tagged {
  tags?: { tag_id: string }[] | null;
}

/** ``info`` on a node, its facts not read left out; nothing when none was read. */
export function facts(info: NodeInfo): { info?: NodeInfo } {
  const kept = Object.fromEntries(
    Object.entries(info).filter(([, v]) => v !== undefined && v !== null && v !== ""),
  ) as NodeInfo;
  return Object.keys(kept).length === 0 ? {} : { info: kept };
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
export function tableFacts(t: TableFacts): NodeInfo {
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

/** The tags read on a row; absent when the role cannot read them. */
const tagIds = (row: Tagged) => rowsOf(row.tags)?.map((t) => t.tag_id);

/** REQ-1593: a term in every domain, the org's shared vocabulary. */
export const ENTERPRISE_DOMAIN = "*";

/** A relationship declared from a table: its name is the Cypher type it is served under
 * (relationships.cypher_type), as the Relationships page shows it. Its id is configuration, never
 * a label. */
const RELATIONSHIP: Selection = { fields: ["id", "cypher_type", "target_table_id"] };

/** The term standings ``hidden`` leaves out, as conditions on glossary_terms_meta's columns. */
const NOT_STATUS: Record<TermStatus, Where> = {
  retired: { retired: { eq: false } },
  deprecated: { _or: [{ retired: { eq: true } }, { deprecated: { eq: false } }] },
  live: { _or: [{ retired: { eq: true } }, { deprecated: { eq: true } }, { live: { eq: false } }] },
  draft: {
    _or: [{ retired: { eq: true } }, { deprecated: { eq: true } }, { live: { eq: true } }],
  },
};

const all = (conds: Where[]): Where | undefined =>
  conds.length === 0 ? undefined : conds.length === 1 ? conds[0] : { _and: conds };

/** A row whose domain is one of ``domains``; every row when there are none. */
const inDomains = (domains: string[], column = "domain_id"): Where | undefined =>
  domains.length === 0 ? undefined : { [column]: { in: domains } };

/** A relationship served under one of the Cypher ``types``. */
const named = (types: string[]): Where | undefined =>
  types.length === 0 ? undefined : { cypher_type: { in: types } };

function relationships(s: QueryScope): Selection["rels"] {
  return s.layers.has("ownerTables") && s.layers.has("columns")
    ? {
        source_relationships: {
          alias: "sourceRelationships",
          sel: RELATIONSHIP,
          where: named(s.edgeTypes),
        },
      }
    : {};
}

/** The terms ``s`` keeps by their own columns: their standing and their domains. */
function termKept(s: QueryScope): Where | undefined {
  const domainWhere: Where | undefined =
    s.domains.length === 0
      ? undefined
      : {
          // term_domains is the term's domains as |a|b| (ontology._domains); an
          // enterprise-wide term ("*") is in every one.
          _or: [...s.domains, ENTERPRISE_DOMAIN].map((d) => ({
            term_domains: { like: `%|${d}|%` },
          })),
        };
  return all([
    ...(domainWhere ? [domainWhere] : []),
    ...s.hiddenStatuses.map((st) => NOT_STATUS[st]),
  ]);
}

/** A term with a relationship drawn: an edge of a type kept to a term kept (either way, under
 * Ontology), a domain kept it is in (under Domains: its domains are its refs' and tables', so a
 * draft named for a column is in its table's domain whatever else is drawn), or a column it IS in
 * a domain kept (under Columns). With none of these layers on, none. */
function related(s: QueryScope): Where {
  const kept = termKept(s);
  const typed: Where | undefined =
    s.edgeTypes.length === 0 ? undefined : { rel_type: { in: s.edgeTypes } };
  const edge = (other: string): Where =>
    all([...(typed ? [typed] : []), ...(kept ? [{ [other]: kept }] : [])]) ?? {};
  return {
    _or: [
      ...(s.layers.has("ontology")
        ? [
            { edges_out: edge("target_term") },
            { edges_in: edge("source_term") },
            { tables: inDomains(s.domains) ?? {} },
          ]
        : []),
      ...(s.layers.has("domains")
        ? [
            {
              domains:
                inDomains(s.domains.length === 0 ? [] : [...s.domains, ENTERPRISE_DOMAIN]) ?? {},
            },
          ]
        : []),
      ...(s.layers.has("columns") ? [{ refs: inDomains(s.domains) ?? {} }] : []),
    ],
  };
}

/** A search item as an ILIKE pattern: anywhere in the text, its own % _ and \\ taken literally. */
const anywhere = (item: string) => `%${item.replace(/[\\%_]/g, (c) => `\\${c}`)}%`;

/** REQ-2143: the terms the page's search matches, searchItems' rule -- any item, in the name or the
 * definition, whatever its case. */
export function searchWhere(items: string[]): Where {
  const any: Where[] = [];
  for (const i of items)
    any.push({ name: { ilike: anywhere(i) } }, { definition: { ilike: anywhere(i) } });
  return { _or: any };
}

const TERM_FIELDS = [
  "id",
  "name",
  "definition",
  "ref_count",
  "deprecated",
  "retired",
  "live",
  "identity_gap",
];

/** A term's selection at ``level`` hops from the search's matches. Under a search the terms a term's
 * edges reach are read with it, a level further, to the neighbourhood's depth: a term at the
 * last level is read with its edges' ends alone, so an edge between two of them is drawn and
 * nothing past them is. With no search every term is read at level 0, its edges' ends by id. */
function termSelection(s: QueryScope, level: number): Selection {
  const depth = s.search.length > 0 || s.focus !== null ? s.depth : 0;
  const last = level >= depth;
  const kept = termKept(s);
  const rels: NonNullable<Selection["rels"]> = {};
  const end = (side: "target_term" | "source_term", alias: string) => ({
    [side]: { alias, sel: last ? { fields: ["id"] } : termSelection(s, level + 1) },
  });
  const edgeWhere = (other: "target_term" | "source_term"): Where | undefined =>
    all([
      ...(s.edgeTypes.length === 0 ? [] : [{ rel_type: { in: s.edgeTypes } }]),
      // Under a search the term an edge reaches is read: one the filters leave out is not.
      ...(depth > 0 && kept ? [{ [other]: kept }] : []),
    ]);
  // A term's edges are read with it, from the term they leave (each edge once), so the view is one
  // query: terms with their edges to other terms. An edge to a term not read has no end to draw.
  if (s.layers.has("ontology")) {
    rels.edges_out = {
      alias: "edgesOut",
      sel: {
        fields: ["id", "from_term_id", "to_term_id", "rel_type"],
        rels: end("target_term", "targetTerm"),
      },
      where: edgeWhere("target_term"),
    };
    // Under a search a match's neighbourhood runs both ways: the edges reaching it too.
    if (depth > 0)
      rels.edges_in = {
        alias: "edgesIn",
        sel: {
          fields: ["id", "from_term_id", "to_term_id", "rel_type"],
          rels: end("source_term", "sourceTerm"),
        },
        where: edgeWhere("source_term"),
      };
  }
  // What a term touches is a hop past it: the last level's is past the neighbourhood.
  if (!last || depth === 0) {
    // A class term's tables are the ontology's: what the term names the rows of.
    if (s.layers.has("ontology"))
      rels.tables = {
        alias: "tables",
        sel: { fields: ["id", "table_name", "domain_id"] },
        where: inDomains(s.domains),
      };
    if (s.layers.has("domains"))
      rels.domains = {
        alias: "domains",
        sel: { fields: ["domain_id"] },
        where: inDomains(s.domains.length === 0 ? [] : [...s.domains, ENTERPRISE_DOMAIN]),
      };
    if (s.layers.has("columns"))
      rels.refs = {
        alias: "refs",
        sel: {
          fields: ["column_key", "column_name", "table_id", "table_name", "domain_id"],
          rels: {
            // REQ-2174: the column's and its table's facts, for the hover card and detail panel.
            column: {
              alias: "column",
              sel: {
                fields: COLUMN_FACTS,
                rels: {
                  sensitivity: { alias: "sensitivity", sel: { fields: ["tags"] } },
                  ...TAGS,
                },
              },
            },
            table: {
              to: "registered_tables",
              sel: {
                fields: ["id", ...TABLE_FACTS.fields],
                rels: {
                  ...TABLE_FACTS.rels,
                  ...TAGS,
                  // With Data off, a column's product is read for its look alone: no product
                  // node -- but for the 2D lane diagram, whose product lane names it, and for a
                  // snippet, which draws the products its terms reach (REQ-2174).
                  product: {
                    alias: "product",
                    sel: {
                      fields:
                        s.layers.has("data") || s.lanes || s.focus !== null
                          ? ["id", "name", "domain_id", ...PRODUCT_FACTS]
                          : ["id"],
                    },
                  },
                  ...relationships(s),
                },
              },
            },
          },
        },
        where: inDomains(s.domains),
      };
  }
  // REQ-2174: its experts, its stewards on the graph's hover card and detail panel.
  rels.experts = { alias: "experts", sel: { fields: ["user_id", "kind"] } };
  return { fields: TERM_FIELDS, rels };
}

/** The product a focus names, or null for a term's focus or none. */
function focusProduct(s: QueryScope): string | null {
  return s.focus !== null && s.focus.startsWith("p:") ? s.focus.slice(2) : null;
}

/** The terms a focus starts from: the term itself, or a product's -- those its tables' columns
 * are, and those its tables are (a class). */
function focusWhere(focus: string): Where {
  if (focus.startsWith("term:")) return { id: { eq: Number(focus.slice("term:".length)) } };
  if (focus.startsWith("p:")) {
    const product = { product_id: { eq: focus.slice(2) } };
    return { _or: [{ refs: { registered_table: product } }, { tables: product }] };
  }
  throw new Error(`a graph's focus is a term or a product, not ${focus}`);
}

/** The terms ``s`` reads at the root: those its filters keep, under a search those it matches,
 * and under a focus those it starts from. */
function rootWhere(s: QueryScope): Where | undefined {
  const kept = termKept(s);
  return all([
    ...(kept ? [kept] : []),
    ...(s.orphans ? [] : [related(s)]),
    ...(s.search.length > 0 ? [searchWhere(s.search)] : []),
    ...(s.focus !== null ? [focusWhere(s.focus)] : []),
  ]);
}

function termsField(m: MetaReader, s: QueryScope): string | null {
  return rootField(m, "_terms", termSelection(s, 0), rootWhere(s));
}

/** REQ-2143: how many terms the view's query would read at its root -- the matches under a search,
 * else every term the filters keep -- read as their ids alone, before the graph is. */
export function ontologyCountQuery(m: MetaReader, s: QueryScope): string | null {
  // Each term's domains with it: the overview counts the terms in each.
  const field = rootField(
    m,
    "_terms",
    { fields: ["id"], rels: { domains: { alias: "domains", sel: { fields: ["domain_id"] } } } },
    rootWhere(s),
  );
  return field === null ? null : operation("OntologyCount", [field]);
}

/** What the count read answers: each term's id and domains. */
export interface CountRow {
  id: number;
  domains?: { domain_id: string }[] | null;
}

/** REQ-2143: the overview drawn when the whole ontology is past the budget -- each domain, named
 * with the number of terms in it; ``label`` words a domain and its count. */
export function overviewGraph(
  rows: CountRow[],
  label: (domain: string, count: number) => string,
): GraphData {
  const counts = new Map<string, number>();
  for (const r of rows)
    for (const d of r.domains ?? []) counts.set(d.domain_id, (counts.get(d.domain_id) ?? 0) + 1);
  return {
    nodes: [...counts]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([d, n]) => ({
        id: `d:${d}`,
        type: "domain" as const,
        label: label(d, n),
        domains: [d],
      })),
    edges: [],
  };
}

/** REQ-2174 (THE ONE GRAPH'S PRESETS AND LAYERS): the layers a product's own parts are drawn on,
 * read with the products only while they are on. */
export const PRODUCT_LAYERS: LayerId[] = ["commands", "quality", "productLinks", "lineage"];

/** A product's own parts its optional layers draw, as ``s`` has them on: its commands (Commands),
 * its links to other products (Product links). A linked product is a bare node: its own parts
 * are its own, never read through this one. */
function productParts(s: QueryScope): NonNullable<Selection["rels"]> {
  return {
    ...(s.layers.has("commands")
      ? { commands: { alias: "commands", sel: { fields: ["name", "domain_id"] } } }
      : {}),
    ...(s.layers.has("productLinks")
      ? {
          edges: {
            alias: "edges",
            sel: {
              fields: ["kind", "path_count", "aliases"],
              rels: {
                target_product: {
                  alias: "targetProduct",
                  sel: { fields: ["id", "name", "domain_id"] },
                },
              },
            },
            // REQ-2126: the links to other products are its relationship rows.
            where: { kind: { eq: "relationship" }, target_product_id: { is_null: false } },
          },
        }
      : {}),
  };
}

/** A member table's quality (Quality): the table a checker member checks (REQ-1443), and the
 * table's profiles. */
function memberQuality(s: QueryScope): NonNullable<Selection["rels"]> {
  return s.layers.has("quality")
    ? {
        checks: { alias: "checks", sel: { fields: ["id"] } },
        profile_results: { alias: "profileResults", sel: { fields: ["id", "table_name"] } },
      }
    : {};
}

function productsField(m: MetaReader, s: QueryScope): string | null {
  return rootField(
    m,
    "data_products",
    {
      fields: ["id", "name", "domain_id", ...PRODUCT_FACTS],
      rels: {
        ...productParts(s),
        member_tables: {
          alias: "memberTables",
          sel: {
            // A member with a contract is a checker (REQ-1443): quality evidence, not a dataset.
            fields: ["id", "domain_id", "table_name", "dq_contract", ...TABLE_FACTS.fields],
            rels: {
              ...TABLE_FACTS.rels,
              ...TAGS,
              ...memberQuality(s),
              table_columns: {
                to: "table_columns",
                sel: {
                  fields: ["column_name", "column_key", ...COLUMN_FACTS],
                  rels: {
                    glossary_ref: { alias: "glossaryRef", sel: { fields: ["term_id"] } },
                    ...TAGS,
                  },
                },
              },
              ...relationships(s),
            },
          },
          where: inDomains(s.domains),
        },
      },
    },
    // Under a product's focus, that product alone.
    all([
      ...(focusProduct(s) !== null ? [{ id: { eq: focusProduct(s) } }] : []),
      ...(s.domains.length > 0 ? [{ domain_id: { in: s.domains } }] : []),
    ]),
  );
}

/** Whether ``s`` reads the data products: the one a product's snippet is focused on; every one
 * for the whole ontology under Data or a layer of a product's own parts; none for a term's (its
 * columns' tables name theirs). */
export function readsProducts(s: QueryScope): boolean {
  return (
    focusProduct(s) !== null ||
    (s.focus === null && ["data", ...PRODUCT_LAYERS].some((l) => s.layers.has(l as LayerId)))
  );
}

/** The ontology's one query for ``s``: its terms, each with the edges leaving it under the
 * Ontology layer, and the data products readsProducts says; null when the role cannot read the
 * terms. A part the role cannot read is left out. Without ``terms`` (the count found none at the
 * root, so none nests under it) it reads the products alone, and is null when it reads none. */
export function ontologyQuery(m: MetaReader, s: QueryScope, terms = true): string | null {
  const termsRead = termsField(m, s);
  if (termsRead === null) return null;
  const products = readsProducts(s) ? productsField(m, s) : null;
  const fields = [terms ? termsRead : null, products].filter((f): f is string => f !== null);
  return fields.length === 0 ? null : operation("Ontology", fields);
}

/** Every term the query read, at whatever level of a search's neighbourhood, once: a term read at
 * two levels keeps what the fuller read holds. */
export function flatTerms(rows: TermRow[]): TermRow[] {
  const byId = new Map<number, TermRow>();
  const visit = (t: TermRow) => {
    const seen = byId.get(t.id);
    // The fuller read: a nearer level reads what a term touches, the last its edges' ends alone.
    if (!seen || Object.keys(t).length > Object.keys(seen).length) byId.set(t.id, t);
    for (const e of t.edges_out ?? [])
      if (e.target_term && "name" in e.target_term) visit(e.target_term as TermRow);
    for (const e of t.edges_in ?? [])
      if (e.source_term && "name" in e.source_term) visit(e.source_term as TermRow);
  };
  rows.forEach(visit);
  return [...byId.values()];
}

/** The edges the terms were read with, each once: from the term it leaves or the one it reaches. */
export function termEdges(terms: TermRow[]): TermEdgeRow[] {
  const out = new Map<string, TermEdgeRow>();
  for (const t of terms)
    for (const e of [...(t.edges_out ?? []), ...(t.edges_in ?? [])])
      out.set(`${e.from_term_id}>${e.to_term_id}:${e.rel_type}`, {
        id: e.id,
        from_term_id: e.from_term_id,
        to_term_id: e.to_term_id,
        rel_type: e.rel_type,
      });
  return [...out.values()];
}

/** What the Ontology View's query answers: a part not read is absent. */
export interface OntologyRows {
  _terms?: TermRow[];
  data_products?: ProductRow[];
}

export interface TermRow {
  id: number;
  name: string;
  definition: string | null;
  /** The columns the term names. */
  ref_count: number;
  deprecated: boolean;
  retired: boolean;
  live: boolean;
  // REQ-2130/2141: grounded in tables that same-entity relationships do not all join; absent
  // when the role cannot read it.
  identity_gap?: boolean;
  // One-to-many: null when the term has none (see rowsOf).
  domains?: { domain_id: string }[] | null;
  // Its experts (its stewards); null when it has none.
  experts?: { user_id: string; kind: string }[] | null;
  // Under Ontology: the edges leaving the term (null when none; see rowsOf).
  // Under a search, the term an edge reaches is read in full to the neighbourhood's depth.
  edges_out?: (TermEdgeRow & { target_term?: TermRow | { id: number } | null })[] | null;
  edges_in?: (TermEdgeRow & { source_term?: TermRow | { id: number } | null })[] | null;
  // REQ-2130 (class grounding): the tables whose rows the term names.
  tables?: { id: number; table_name: string; domain_id: string }[] | null;
  refs?:
    | {
        column_key: string;
        column_name: string;
        table_id: number;
        table_name: string;
        domain_id: string;
        column?: (ColumnFacts & Tagged & { sensitivity?: { tags: string }[] | null }) | null;
        table?:
          | (TableFacts &
              Tagged & {
                id: number;
                product?: {
                  id: string;
                  name?: string;
                  domain_id?: string;
                  status?: string | null;
                  owner_role?: string | null;
                } | null;
                source_relationships?: RelationshipRow[] | null;
              })
          | null;
      }[]
    | null;
}

interface RelationshipRow {
  id: string;
  // Every relationship has its served type stored (relationship_names.store_effective_names).
  cypher_type: string;
  target_table_id: number | null;
}

export interface ProductRow {
  id: string;
  name: string;
  domain_id: string;
  status?: string | null;
  owner_role?: string | null;
  // Under Commands.
  commands?: { name: string; domain_id: string }[] | null;
  // Under Product links: its relationship rows to other products.
  edges?:
    | {
        kind: string;
        path_count: number;
        aliases: string | null;
        target_product?: { id: string; name: string; domain_id: string } | null;
      }[]
    | null;
  // One-to-many: null when there are none (see rowsOf).
  member_tables?:
    | (TableFacts &
        Tagged & {
          id: number;
          domain_id: string;
          table_name: string;
          // Set on a checker member (REQ-1443).
          dq_contract?: string | null;
          // Under Quality: the table a checker checks, and the table's profiles.
          checks?: { id: number } | null;
          profile_results?: { id: number; table_name: string }[] | null;
          table_columns?:
            | (ColumnFacts &
                Tagged & {
                  column_name: string;
                  column_key: string;
                  glossary_ref?: { term_id: number }[] | null;
                })[]
            | null;
          source_relationships?: RelationshipRow[] | null;
        })[]
    | null;
}

export interface TermEdgeRow {
  id: number;
  from_term_id: number;
  to_term_id: number;
  rel_type: string;
}

/** A term's standing (REQ-1387: a draft is in service and not live). The graph names a term by its
 * title and marks its standing by style; it carries neither the term's definition nor reasons. */
export function standing(t: TermRow): TermStatus {
  if (t.retired) return "retired";
  if (t.deprecated) return "deprecated";
  if (t.live) return "live";
  return "draft";
}

/**
 * The glossary's graph. Each node and edge says which layers draw it where its kind's own layer is
 * not the whole answer:
 * - a column a term IS is Columns'; a data product's column is Data's too, and its datasets' other
 *   columns are Data's alone;
 * - a table is drawn only through a column drawn (Tables, with Columns) or as a product's dataset
 *   (Data); the relationships declared between the tables drawn are Tables' (ownerTables).
 */
export function ontologyGraph(
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
      // Met first where its facts were not read (a class term's table): they are read here.
      if (n.info && !seen.info) seen.info = n.info;
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
  const table = (
    id: number,
    domain: string,
    name: string,
    dataset: boolean,
    row: (TableFacts & Tagged) | null | undefined,
  ) =>
    node({
      id: `t:${id}`,
      type: "table",
      label: `${domain}.${name}`,
      domains: [domain],
      stored: true,
      layers: dataset ? ["data"] : [],
      ...(row ? { tags: tagIds(row), ...facts(tableFacts(row)) } : {}),
    });
  // A product is in its domain, as on the data product graph: drawn beside the domain's terms
  // even when none of its tables' columns is drawn.
  const product = (
    id: string,
    name: string,
    domain: string,
    row: { status?: string | null; owner_role?: string | null },
  ) => {
    node({
      id: `p:${id}`,
      type: "product",
      label: name,
      domains: [domain],
      ...facts({ status: row.status ?? undefined, owner: row.owner_role ?? undefined }),
    });
    if (nodes.has(`d:${domain}`)) edge(`p:${id}`, `d:${domain}`, "in_domain");
  };
  const relationships = (from: number, rows: RelationshipRow[] | null | undefined) => {
    for (const r of rows ?? [])
      if (r.target_table_id !== null)
        edge(`t:${from}`, `t:${r.target_table_id}`, "relationship", r.cypher_type, ["ownerTables"]);
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
      role: termRole({ table_count: (t.tables ?? []).length, ref_count: t.ref_count }),
      // For the side panel only; a proposal with none has nothing said there.
      ...(t.definition ? { definition: t.definition } : {}),
      // REQ-2130/2141: a class whose tables are not all declared one entity: a curation gap.
      ...(t.identity_gap
        ? { unlinkedTables: (t.tables ?? []).map((g) => `${g.domain_id}.${g.table_name}`) }
        : {}),
      // An enterprise-wide term is in every domain: no domain filter leaves it out.
      ...(domains.includes(ENTERPRISE_DOMAIN) ? {} : { domains }),
      // REQ-2174: its experts are its stewards (its status is its standing, above).
      ...facts({
        stewards: rowsOf(t.experts)
          ?.filter((x) => x.kind === "expert")
          .map((x) => x.user_id),
      }),
    });
    // An enterprise-wide term is joined to every domain, so it settles among them, at the centre.
    for (const d of domains.includes(ENTERPRISE_DOMAIN) ? named : domains)
      edge(id, `d:${d}`, "in_domain");
    // REQ-2130: the term IS each table it grounds (a class term): the table a cylinder, drawn
    // with the ontology.
    for (const g of t.tables ?? []) {
      node({
        id: `t:${g.id}`,
        type: "table",
        label: `${g.domain_id}.${g.table_name}`,
        domains: [g.domain_id],
        stored: true,
        layers: ["ontology"],
      });
      edge(id, `t:${g.id}`, "term_binding", "IS", ["ontology"]);
    }
    for (const r of t.refs ?? []) {
      const tid = `t:${r.table_id}`;
      const cid = `c:${r.column_key}`;
      const p = r.table?.product;
      // A column whose table a data product holds is the Data layer's too.
      const held: LayerId[] = p ? ["columns", "data"] : ["columns"];
      table(r.table_id, r.domain_id, r.table_name, p != null, r.table);
      // Its sensitivity: its row in sensitive_coverage when sensitive; absent when not read.
      const sensitivity = r.column ? rowsOf(r.column.sensitivity) : undefined;
      node({
        id: cid,
        type: "column",
        label: `${r.table_name}.${r.column_name}`,
        parent: tid,
        domains: [r.domain_id],
        inProduct: p != null,
        layers: held,
        ...(r.column ? { tags: tagIds(r.column) } : {}),
        // A fact, not a filter: the glossary's query is not narrowed by sensitivity.
        ...(r.column
          ? facts({
              ...columnFacts(r.column),
              sensitive: sensitivity?.flatMap((x) => x.tags.split(", ")),
            })
          : {}),
      });
      edge(tid, cid, "has_column", undefined, p ? ["data", "ownerTables"] : ["ownerTables"]);
      // glossary.py's physical refs: the term IS this column.
      edge(id, cid, "term_binding", "IS", held);
      // A product read for its column's look alone (Data off) has neither name nor domain and is
      // no node; under Data both are read.
      if (p?.name !== undefined && p.domain_id !== undefined) {
        product(p.id, p.name, p.domain_id, p);
        edge(`p:${p.id}`, tid, "member");
      }
      relationships(r.table_id, r.table?.source_relationships);
    }
  }
  for (const p of products) {
    const pid = `p:${p.id}`;
    product(p.id, p.name, p.domain_id, p);
    for (const c of p.commands ?? []) {
      node({ id: `f:${c.name}`, type: "command", label: c.name, domains: [c.domain_id] });
      edge(pid, `f:${c.name}`, "command");
    }
    for (const m of p.member_tables ?? []) {
      const tid = `t:${m.id}`;
      if (m.dq_contract != null) {
        // A checker member (REQ-1443): quality evidence, drawn under Quality, never as a dataset.
        const kid = `k:${m.id}`;
        node({
          id: kid,
          type: "checker",
          label: `${m.domain_id}.${m.table_name}`,
          domains: [m.domain_id],
        });
        edge(pid, kid, "member", undefined, ["quality"]);
        if (m.checks) edge(kid, `t:${m.checks.id}`, "checks");
        continue;
      }
      for (const r of m.profile_results ?? []) {
        // Its table is its parent: a profile is reached on its table's page.
        node({ id: `r:${r.id}`, type: "profile", label: r.table_name, parent: tid });
        edge(`r:${r.id}`, tid, "profiled");
      }
      table(m.id, m.domain_id, m.table_name, true, m);
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
          tags: tagIds(c),
          ...facts(columnFacts(c)),
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
  // Product links last: a linked product read as itself is Data's; one only linked to is a bare
  // node, drawn by its link alone.
  for (const p of products)
    for (const e of p.edges ?? []) {
      const to = e.target_product;
      // A link to a product the role cannot read has no end to draw.
      if (!to) continue;
      const b = productNode(to.id);
      node({
        id: b,
        type: "product",
        label: to.name,
        domains: [to.domain_id],
        layers: ["productLinks"],
      });
      edge(`p:${p.id}`, b, "product_link", e.aliases ?? undefined);
      const link = out.get(`product_link:p:${p.id}>${b}${e.aliases ? `:${e.aliases}` : ""}`);
      if (link) link.paths = e.path_count;
    }
  for (const e of edges) {
    const a = `term:${e.from_term_id}`;
    const b = `term:${e.to_term_id}`;
    // An edge to a term the role cannot read has no end to draw.
    if (nodes.has(a) && nodes.has(b)) edge(a, b, "term_edge", e.rel_type);
  }
  return { nodes: [...nodes.values()], edges: [...out.values()] };
}

/** REQ-2156: an input as its reader knows it -- a registered table or view by its catalog name, a
 * member read straight from its source by the source and the names it gives the object. */
function inputLabel(i: ProductInput): string {
  if (i.kind !== "source")
    return catalogTableName({ domainId: i.domain_id, tableName: i.table_name });
  return sourceObject(i);
}

/** An object read straight from its source, by the names the source gives it: the source, then
 * its schema and table where it has them. */
function sourceObject(n: {
  source_id?: string | null;
  source_schema?: string | null;
  source_table?: string | null;
}): string {
  const object = [n.source_schema, n.source_table].filter(Boolean).join(".");
  return object ? `${n.source_id} · ${object}` : (n.source_id as string);
}

/** REQ-2174 (THE ONE GRAPH'S PRESETS AND LAYERS): ``g`` with ``product``'s lineage on the Lineage
 * layer, from the product lineage endpoint (REQ-2156, cut to what the reader can read): each input
 * feeds the product; an input that is another product's member stands for that product, which
 * the product is built from. Its column lineage (REQ-2129) joins the columns it names: a member's
 * or an input's column by its table, any other by its relation. ``tables`` gives the relation each
 * of the product's member tables is served under. */
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
    // A column the ontology's read did not draw stands alone, named by where it is read from,
    // drawn by Lineage alone.
    if (!nodes.has(id))
      nodes.set(id, {
        id,
        type: "column",
        label:
          n.kind === "native"
            ? `${sourceObject(n)}.${n.column}`
            : `${n.relation === null ? "" : `${n.relation}.`}${n.column}`,
        inProduct: false,
        layers: ["lineage"],
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
