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
// ontology._terms (its standing columns), the domains each term is in, the tables it grounds, its
// refs to columns with those columns' tables and each table's data product, and the edges leaving
// it (ontology._term_edges, read with the term) -- turned into the graph's nodes and edges. A focus
// (a term or a product) and a depth narrow the read to a snippet.
// (Amended 2026-11-05, ONE GRAPH, RULES PER PRESET:) the presets carry their own rules. Under the
// Ontology preset's (REQ-2130, amended 2026-10-29) every read is nested under the terms, so the
// query returns only what a term reaches, and no relationship, join or lineage edge between tables
// or columns is drawn. Under the Product map's (QueryScope.products) the data products are read
// too -- their member tables, the tables' columns and the terms those columns are, the
// relationships declared between the tables, and the product layers (commands, quality, product
// links, lineage) -- so nothing the product graph showed is lost.

import { termRole, type TermGrounding } from "../../api/glossary";
import {
  operation,
  rootField,
  rowsOf,
  type MetaReader,
  type Selection,
  type Where,
} from "../../api/metaGraph";
import type { MaskEntry } from "../../lib/masks";
import { searchWhere } from "./ontologySearch";
import type { ProductInput, ProductLineage } from "../../api/lineage";
import { catalogTableName, relationName } from "../../naming";
import { TERM_STATUSES } from "./graphModel";
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
  rels: { experts: { sel: { fields: ["user_id", "kind"] } } },
};

/** A column's facts: its type, description and masks (its sensitivity is read beside them). */
export const COLUMN_FACTS = ["data_type", "description", "masks"];

/** A table's facts: its source and description, and its checkers' latest scans (its quality). */
export const TABLE_FACTS: Selection = {
  fields: ["source_id", "description"],
  rels: {
    checkedBy: { sel: { fields: ["latest_outcome", "latest_run_at"] } },
  },
};

/** A product's facts: its status and owner (its members are counted from the graph). */
export const PRODUCT_FACTS = ["status", "owner_role"];

/** A column's or a table's tags, which the Product map's tag filter judges. */
const TAGS: NonNullable<Selection["rels"]> = {
  tags: { sel: { fields: ["tag_id"] } },
};

/** A column's facts as COLUMN_FACTS reads them. */
interface ColumnFacts {
  dataType?: string | null;
  description?: string | null;
  // table_columns.masks: the ordered mask list (REQ-2059), as JSON.
  masks?: unknown;
}

/** A table's facts as TABLE_FACTS reads them. */
interface TableFacts {
  sourceId?: string | null;
  description?: string | null;
  checkedBy?: { latestOutcome: "pass" | "fail" | null; latestRunAt: string | null }[] | null;
}

interface Tagged {
  tags?: { tagId: string }[] | null;
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
    dataType: c.dataType ?? undefined,
    description: c.description ?? undefined,
    masked: masks == null ? undefined : masks.some((m) => m.mask !== null),
  };
}

/** A table's facts, as TABLE_FACTS reads them: its quality is its checkers' worst latest scan. */
export function tableFacts(t: TableFacts): NodeInfo {
  const runs = t.checkedBy ?? [];
  const outcomes = runs.map((r) => r.latestOutcome);
  const at = runs.map((r) => r.latestRunAt).filter((x): x is string => x !== null);
  return {
    source: t.sourceId ?? undefined,
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
const tagIds = (row: Tagged) => rowsOf(row.tags)?.map((t) => t.tagId);

/** REQ-1593: a term in every domain, the org's shared vocabulary. */
export const ENTERPRISE_DOMAIN = "*";

/** A relationship declared from a table: its name is the Cypher type it is served under
 * (relationships.cypher_type), as the Relationships page shows it. Its id is configuration, never
 * a label. REQ-2174 (amended 2026-11-07): the table it reaches, named, with the product that
 * holds it -- a related table is drawn with its products and nothing of its own. */
const RELATIONSHIP: Selection = {
  fields: ["id", "cypher_type", "target_table_id"],
  rels: {
    targetTable: {
      sel: {
        fields: ["id", "table_name", "domain_id"],
        rels: {
          dataProductTables: {
            sel: {
              fields: [],
              rels: {
                dataProduct: { sel: { fields: ["id", "name", "domain_id", ...PRODUCT_FACTS] } },
              },
            },
          },
        },
      },
    },
  },
};

/** The term standings ``hidden`` leaves out, as conditions on the ontology's status column
 * (REQ-1387: set by people). */
const notStatus = (st: TermStatus): Where => ({
  status: { in: TERM_STATUSES.filter((other) => other !== st) },
});

const all = (conds: Where[]): Where | undefined =>
  conds.length === 0 ? undefined : conds.length === 1 ? conds[0] : { _and: conds };

/** A row whose domain is one of ``domains``; every row when there are none. */
const inDomains = (domains: string[], column = "domain_id"): Where | undefined =>
  domains.length === 0 ? undefined : { [column]: { in: domains } };

/** The relationships declared from a table drawn: the Product map's, under Related tables (REQ-2174,
 * amended 2026-11-07), whatever the Term relationships filter keeps. The Ontology preset draws
 * none (REQ-2130, AN ONTOLOGY VIEW, NOT A DATA-FLOW VIEW). */
function relationships(s: QueryScope): NonNullable<Selection["rels"]> {
  return s.products && s.layers.has("related")
    ? { sourceRelationships: { sel: RELATIONSHIP } }
    : {};
}

/** Whether a reached table's data product is named: under Data, for the 2D lane diagram, whose
 * product lane names it, and for a snippet, which draws the products its terms reach (REQ-2174). */
const productsNamed = (s: QueryScope) =>
  s.layers.has("data") || s.lanes === true || s.focus !== null;

/** A reached table's data product: named (productsNamed), else read for the look of its columns
 * alone, with no product node. */
function productOf(s: QueryScope): NonNullable<Selection["rels"]> {
  return {
    dataProductTables: {
      sel: {
        fields: [],
        rels: {
          dataProduct: {
            sel: {
              fields: productsNamed(s) ? ["id", "name", "domain_id", ...PRODUCT_FACTS] : ["id"],
            },
          },
        },
      },
    },
  };
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
  return all([...(domainWhere ? [domainWhere] : []), ...s.hiddenStatuses.map(notStatus)]);
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
            { edgesOut: edge("target_term") },
            { edgesIn: edge("source_term") },
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

const TERM_FIELDS = [
  "id",
  "name",
  "definition",
  "ref_count",
  "deprecated",
  "retired",
  "status",
  "grounding",
  "identity_gap",
];

/** How many steps across the term links ``s``'s hops take from the terms at its root: a search's
 * matches, a term's focus, or a product's own terms (those its tables' columns are, and those its
 * tables are), each read at no hop (REQ-2174, amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS).
 * Infinity (All) reads to the end of the connected part. */
export function termLevels(s: Pick<QueryScope, "search" | "focus" | "depth">): number {
  if (s.search.length === 0 && s.focus === null) return 0;
  return s.depth;
}

/** A term's selection: its edges with their ends by id, and what it touches -- its grounding, drawn
 * with every term whatever the hop it is read at (REQ-2174, amended 2026-11-12). Under a search or
 * a focus a hop's terms are read with their edges both ways, each edge's end kept by the filters,
 * so the next hop reads the ends no hop has read (readOntology); an edge to a term no hop read has
 * no end to draw. With neither every term is read at the root, the edges leaving it. */
function termSelection(s: QueryScope): Selection {
  const depth = termLevels(s);
  const kept = termKept(s);
  const rels: NonNullable<Selection["rels"]> = {};
  const edgeWhere = (other: "target_term" | "source_term"): Where | undefined =>
    all([
      ...(s.edgeTypes.length === 0 ? [] : [{ rel_type: { in: s.edgeTypes } }]),
      // Under a search the term an edge reaches is read: one the filters leave out is not.
      ...(depth > 0 && kept ? [{ [other]: kept }] : []),
    ]);
  // A term's edges are read with it, each end by its id: an end the role cannot read is null, and
  // an edge to a term not read has no end to draw.
  if (s.layers.has("ontology")) {
    rels.edgesOut = {
      sel: {
        fields: ["id", "from_term_id", "to_term_id", "rel_type"],
        rels: { targetTerm: { sel: { fields: ["id"] } } },
      },
      where: edgeWhere("target_term"),
    };
    // Under a search a match's neighbourhood runs both ways: the edges reaching it too.
    if (depth > 0)
      rels.edgesIn = {
        sel: {
          fields: ["id", "from_term_id", "to_term_id", "rel_type"],
          rels: { sourceTerm: { sel: { fields: ["id"] } } },
        },
        where: edgeWhere("source_term"),
      };
  }
  // What a term touches comes with it at no hop: its grounding.
  // A class term's tables are the ontology's: what the term names the rows of.
  if (s.layers.has("ontology"))
    rels.tables = {
      // A class term's table has no columns drawn: its product is read only to be named.
      sel: {
        fields: ["id", "table_name", "domain_id"],
        ...(productsNamed(s) ? { rels: productOf(s) } : {}),
      },
      where: inDomains(s.domains),
    };
  if (s.layers.has("domains"))
    rels.domains = {
      sel: { fields: ["domain_id"] },
      where: inDomains(s.domains.length === 0 ? [] : [...s.domains, ENTERPRISE_DOMAIN]),
    };
  if (s.layers.has("columns"))
    rels.refs = {
      sel: {
        fields: ["column_key", "column_name", "table_id", "table_name", "domain_id"],
        rels: {
          // REQ-2174: the column's and its table's facts, for the hover card and detail panel;
          // under the Product map their tags too, which its tag filter judges.
          column: {
            sel: {
              fields: COLUMN_FACTS,
              rels: {
                sensitivity: { sel: { fields: ["tags"] } },
                ...(s.products ? TAGS : {}),
              },
            },
          },
          registeredTable: {
            to: "registered_tables",
            sel: {
              fields: ["id", ...TABLE_FACTS.fields],
              rels: {
                ...TABLE_FACTS.rels,
                ...(s.products ? TAGS : {}),
                ...productOf(s),
                ...relationships(s),
              },
            },
          },
        },
      },
      where: inDomains(s.domains),
    };
  // REQ-2174: its experts, its stewards on the graph's hover card and detail panel.
  rels.experts = { sel: { fields: ["user_id", "kind"] } };
  return { fields: TERM_FIELDS, rels };
}

/** The product a focus names, or null for a term's focus or none. */
function focusProduct(s: Pick<QueryScope, "focus">): string | null {
  return s.focus !== null && s.focus.startsWith("p:") ? s.focus.slice(2) : null;
}

/** The terms a focus starts from: the term itself, or a product's -- those its tables' columns
 * are, and those its tables are (a class). */
function focusWhere(focus: string): Where {
  if (focus.startsWith("term:")) return { id: { eq: Number(focus.slice("term:".length)) } };
  if (focus.startsWith("p:")) {
    const member = { dataProductTables: { product_id: { eq: focus.slice(2) } } };
    return { _or: [{ refs: { registeredTable: member } }, { tables: member }] };
  }
  throw new Error(`a graph's focus is a term or a product, not ${focus}`);
}

/** The terms ``s`` reads at the root: those its filters keep, under a search those it matches,
 * and under a focus those it starts from. */
function rootWhere(s: QueryScope): Where | undefined {
  const kept = termKept(s);
  const searched = searchWhere(s.search);
  return all([
    ...(kept ? [kept] : []),
    ...(s.orphans ? [] : [related(s)]),
    ...(searched ? [searched] : []),
    ...(s.focus !== null ? [focusWhere(s.focus)] : []),
  ]);
}

function termsField(m: MetaReader, s: QueryScope): string | null {
  // Under a search or a focus the root's terms are hop 0; else they are all there is.
  return rootField(m, "_terms", termSelection(s), rootWhere(s));
}

/** REQ-2174: the terms hop ``hop`` of ``s``'s neighbourhood reads -- ``ids``, the terms the last
 * hop's edges reach that no hop has read -- with their edges and what they touch; null when the
 * role cannot read the terms. */
export function ontologyHopQuery(
  m: MetaReader,
  s: QueryScope,
  hop: number,
  ids: number[],
): string | null {
  const field = hopField(m, s, ids);
  return field === null ? null : operation(`OntologyHop${hop}`, [field]);
}

function hopField(m: MetaReader, s: QueryScope, ids: number[]): string | null {
  return rootField(m, "_terms", termSelection(s), { id: { in: ids } });
}

/** REQ-2136, REQ-2174: what a snippet opens as -- one query of what it read: the root's fields
 * (its terms, whatever the count found) and each hop's terms, ``hops[i]`` the ids hop i + 1 read,
 * answered under ``_termsHop<n>``. Each term is named once, at the hop that read it, so the query
 * grows with what is drawn, as the reads do; null when the role cannot read the terms. */
export function ontologyOpenedQuery(m: MetaReader, s: QueryScope, hops: number[][]): string | null {
  const root = ontologyFields(m, s, true);
  const read = hops.map((ids) => hopField(m, s, ids));
  if (root === null || read.some((f) => f === null)) return null;
  return operation("Ontology", [
    ...root,
    ...read.map((f, i) => (f as string).replace(/^_terms:/, `_termsHop${i + 1}:`)),
  ]);
}

/** REQ-2143: how many terms the view's query would read at its root -- the matches under a search,
 * else every term the filters keep -- read as their ids alone, before the graph is. */
export function ontologyCountQuery(m: MetaReader, s: QueryScope): string | null {
  // Each term's domains with it: the overview counts the terms in each.
  const field = rootField(
    m,
    "_terms",
    { fields: ["id"], rels: { domains: { sel: { fields: ["domain_id"] } } } },
    rootWhere(s),
  );
  return field === null ? null : operation("OntologyCount", [field]);
}

/** What the count read answers: each term's id and domains. */
export interface CountRow {
  id: number;
  domains?: { domainId: string }[] | null;
}

/** REQ-2143: the overview drawn when the whole ontology is past the budget -- each domain, named
 * with the number of terms in it; ``label`` words a domain and its count. */
export function overviewGraph(
  rows: CountRow[],
  label: (domain: string, count: number) => string,
): GraphData {
  const counts = new Map<string, number>();
  for (const r of rows)
    for (const d of r.domains ?? []) counts.set(d.domainId, (counts.get(d.domainId) ?? 0) + 1);
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
 * read with the products only while they are on. The Product map's alone. */
export const PRODUCT_LAYERS: LayerId[] = ["commands", "quality", "productLinks", "lineage"];

/** A product's own parts its optional layers draw, as ``s`` has them on: its commands (Commands),
 * its links to other products (Product links). A linked product is a bare node: its own parts
 * are its own, never read through this one. */
function productParts(s: QueryScope): NonNullable<Selection["rels"]> {
  return {
    ...(s.layers.has("commands")
      ? {
          dataProductCommands: {
            sel: {
              fields: [],
              rels: { trackedFunction: { sel: { fields: ["name", "domain_id"] } } },
            },
          },
        }
      : {}),
    ...(s.layers.has("productLinks")
      ? {
          edges: {
            sel: {
              fields: ["kind", "path_count", "aliases"],
              rels: { targetProduct: { sel: { fields: ["id", "name", "domain_id"] } } },
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
        checks: { sel: { fields: ["id"] } },
        profileResults: { sel: { fields: ["id", "table_name"] } },
      }
    : {};
}

function productsField(m: MetaReader, s: QueryScope): string | null {
  const memberDomain = inDomains(s.domains);
  return rootField(
    m,
    "data_products",
    {
      fields: ["id", "name", "domain_id", ...PRODUCT_FACTS],
      rels: {
        ...productParts(s),
        dataProductTables: {
          sel: {
            fields: [],
            rels: {
              registeredTable: {
                sel: {
                  // A member with a contract is a checker (REQ-1443): quality evidence, not a
                  // dataset.
                  fields: ["id", "domain_id", "table_name", "dq_contract", ...TABLE_FACTS.fields],
                  rels: {
                    ...TABLE_FACTS.rels,
                    ...TAGS,
                    ...memberQuality(s),
                    tableColumns: {
                      to: "table_columns",
                      sel: {
                        fields: ["column_name", "column_key", ...COLUMN_FACTS],
                        rels: { glossaryRef: { sel: { fields: ["term_id"] } }, ...TAGS },
                      },
                    },
                    ...relationships(s),
                  },
                },
              },
            },
          },
          where: memberDomain ? { registeredTable: memberDomain } : undefined,
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

/** Whether ``s`` reads the data products: only under the Product map's rules -- the one a
 * product's snippet is focused on; every one for the whole map under Data or a layer of a
 * product's own parts; none for a term's (its columns' tables name theirs). The Ontology preset
 * reads none apart: a product is drawn only as a reached table's (REQ-2130). */
export function readsProducts(s: QueryScope): boolean {
  if (!s.products) return false;
  return (
    focusProduct(s) !== null ||
    (s.focus === null && ["data", ...PRODUCT_LAYERS].some((l) => s.layers.has(l as LayerId)))
  );
}

/** The ontology's one query for ``s``: its terms, each with the edges leaving it under the
 * Ontology layer, what it grounds, and those tables' data products under Data, with the data
 * products readsProducts says; null when the role cannot read the terms. A part the role cannot
 * read is left out. Without ``terms`` (the count found none at the root, so none nests under it)
 * it reads the products alone, and is null when it reads none. */
export function ontologyQuery(m: MetaReader, s: QueryScope, terms = true): string | null {
  const fields = ontologyFields(m, s, terms);
  return fields === null || fields.length === 0 ? null : operation("Ontology", fields);
}

/** ontologyQuery's root fields; null when the role cannot read the terms. */
function ontologyFields(m: MetaReader, s: QueryScope, terms: boolean): string[] | null {
  const termsRead = termsField(m, s);
  if (termsRead === null) return null;
  const products = readsProducts(s) ? productsField(m, s) : null;
  return [terms ? termsRead : null, products].filter((f): f is string => f !== null);
}

/** The edges the terms were read with, each once: from the term it leaves or the one it reaches. */
export function termEdges(terms: TermRow[]): TermEdgeRow[] {
  const out = new Map<string, TermEdgeRow>();
  for (const t of terms)
    for (const e of [...(t.edgesOut ?? []), ...(t.edgesIn ?? [])])
      out.set(`${e.fromTermId}>${e.toTermId}:${e.relType}`, {
        id: e.id,
        fromTermId: e.fromTermId,
        toTermId: e.toTermId,
        relType: e.relType,
      });
  return [...out.values()];
}

/** What the ontology's query answers: a part not read is absent. */
export interface OntologyRows {
  _terms?: TermRow[];
  data_products?: ProductRow[];
}

/** A reached table's data product, as read: its id alone when it is not named (see productOf). */
type ProductRef = {
  id: string;
  name?: string;
  domainId?: string;
  status?: string | null;
  ownerRole?: string | null;
} | null;

/** A table's rows of the membership junction, each reaching one of its data products. */
type Memberships = { dataProduct?: ProductRef }[] | null;

/** The data products a table's membership rows reach, those the role can read. */
const productsOf = (rows: Memberships | undefined): NonNullable<ProductRef>[] =>
  (rows ?? []).flatMap((m) => (m.dataProduct ? [m.dataProduct] : []));

interface RelationshipRow {
  id: string;
  // Every relationship has its served type stored (relationship_names.store_effective_names).
  cypherType: string;
  targetTableId: number | null;
  // Absent where the role cannot read the table it reaches.
  targetTable?: {
    id: number;
    tableName: string;
    domainId: string;
    dataProductTables?: Memberships;
  } | null;
}

export interface TermRow {
  id: number;
  name: string;
  definition: string | null;
  /** The columns the term names. */
  refCount: number;
  deprecated: boolean;
  retired: boolean;
  // REQ-1387: set by people, and computed from the graph.
  status: TermStatus;
  grounding: TermGrounding;
  // REQ-2130/2141: grounded in tables that same-entity relationships do not all join; absent
  // when the role cannot read it.
  identityGap?: boolean;
  // One-to-many: null when the term has none (see rowsOf).
  domains?: { domainId: string }[] | null;
  // Its experts (its stewards); null when it has none.
  experts?: { userId: string; kind: string }[] | null;
  // Under Ontology: the edges leaving the term (null when none; see rowsOf), each end by its id
  // (null where the role cannot read it). Under a search or a focus, the edges reaching it too.
  edgesOut?: (TermEdgeRow & { targetTerm?: { id: number } | null })[] | null;
  edgesIn?: (TermEdgeRow & { sourceTerm?: { id: number } | null })[] | null;
  // REQ-2130 (class grounding): the tables whose rows the term names.
  tables?:
    | { id: number; tableName: string; domainId: string; dataProductTables?: Memberships }[]
    | null;
  refs?:
    | {
        columnKey: string;
        columnName: string;
        tableId: number;
        tableName: string;
        domainId: string;
        column?: (ColumnFacts & Tagged & { sensitivity?: { tags: string }[] | null }) | null;
        registeredTable?:
          | (TableFacts &
              Tagged & {
                id: number;
                dataProductTables?: Memberships;
                // Under the Product map's Tables (with Columns).
                sourceRelationships?: RelationshipRow[] | null;
              })
          | null;
      }[]
    | null;
}

export interface ProductRow {
  id: string;
  name: string;
  domainId: string;
  status?: string | null;
  ownerRole?: string | null;
  // Under Commands.
  dataProductCommands?: { trackedFunction?: { name: string; domainId: string } | null }[] | null;
  // Under Product links: its relationship rows to other products.
  edges?:
    | {
        kind: string;
        pathCount: number;
        aliases: string | null;
        targetProduct?: { id: string; name: string; domainId: string } | null;
      }[]
    | null;
  // One-to-many: null when there are none (see rowsOf).
  dataProductTables?: { registeredTable?: MemberTable | null }[] | null;
}

/** A product's member table, as read through its membership row. */
export type MemberTable = TableFacts &
  Tagged & {
    id: number;
    domainId: string;
    tableName: string;
    // Set on a checker member (REQ-1443).
    dqContract?: string | null;
    // Under Quality: the table a checker checks, and the table's profiles.
    checks?: { id: number } | null;
    profileResults?: { id: number; tableName: string }[] | null;
    tableColumns?:
      | (ColumnFacts &
          Tagged & {
            columnName: string;
            columnKey: string;
            glossaryRef?: { termId: number }[] | null;
          })[]
      | null;
    sourceRelationships?: RelationshipRow[] | null;
  };

/** The member tables a product's membership rows reach, those the role can read. */
export const memberTablesOf = (p: Pick<ProductRow, "dataProductTables">): MemberTable[] =>
  (p.dataProductTables ?? []).flatMap((m) => (m.registeredTable ? [m.registeredTable] : []));

export interface TermEdgeRow {
  id: number;
  fromTermId: number;
  toTermId: number;
  relType: string;
}

/** A term's standing: its status, set by people (REQ-1387). The graph names a term by its title
 * and marks its standing by style; it carries neither the term's definition nor reasons. */
export function standing(t: TermRow): TermStatus {
  return t.status;
}

/** A graph being built: each node and edge once, one met again drawn by either's layers. */
function builder() {
  const nodes = new Map<string, GraphNode>();
  const out = new Map<string, GraphEdge>();
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
  const graph = (): GraphData => ({ nodes: [...nodes.values()], edges: [...out.values()] });
  return { nodes, out, node, edge, graph };
}

type Builder = ReturnType<typeof builder>;

/** The terms' domains, each a node (the enterprise-wide one is every domain, never a node of its
 * own), and each term with its domains; returns the domains named. */
function termNodes(b: Builder, terms: TermRow[]) {
  const named = new Set<string>();
  for (const t of terms)
    for (const d of t.domains ?? []) if (d.domainId !== ENTERPRISE_DOMAIN) named.add(d.domainId);
  for (const d of named) b.node({ id: `d:${d}`, type: "domain", label: d });
  for (const t of terms) {
    const id = termNode(t.id);
    const domains = (t.domains ?? []).map((d) => d.domainId);
    b.node({
      id,
      type: "term",
      label: t.name,
      status: standing(t),
      ...(t.grounding === "not_grounded" ? {} : { grounding: t.grounding }),
      role: termRole({ table_count: (t.tables ?? []).length, ref_count: t.refCount }),
      // For the side panel only; a proposal with none has nothing said there.
      ...(t.definition ? { definition: t.definition } : {}),
      // REQ-2130/2141: a class whose tables are not all declared one entity: a curation gap.
      ...(t.identityGap
        ? { unlinkedTables: (t.tables ?? []).map((g) => catalogTableName(g)) }
        : {}),
      // An enterprise-wide term is in every domain: no domain filter leaves it out.
      ...(domains.includes(ENTERPRISE_DOMAIN) ? {} : { domains }),
      // REQ-2174: its experts are its stewards (its status is its standing, above).
      ...facts({
        stewards: rowsOf(t.experts)
          ?.filter((x) => x.kind === "expert")
          .map((x) => x.userId),
      }),
    });
    // An enterprise-wide term is joined to every domain, so it settles among them, at the centre.
    for (const d of domains.includes(ENTERPRISE_DOMAIN) ? named : domains)
      b.edge(id, `d:${d}`, "in_domain");
  }
}

/** The terms' relationships to one another: an edge to a term the role cannot read has no end to
 * draw. */
function termEdgesDrawn(b: Builder, edges: TermEdgeRow[]) {
  for (const e of edges) {
    const a = termNode(e.fromTermId);
    const z = termNode(e.toTermId);
    if (b.nodes.has(a) && b.nodes.has(z)) b.edge(a, z, "term_edge", e.relType);
  }
}

/** A product's node, its facts as read. */
function productNodeOf(
  id: string,
  name: string,
  domain: string,
  row: { status?: string | null; ownerRole?: string | null },
): GraphNode {
  return {
    id: productNode(id),
    type: "product",
    label: name,
    domains: [domain],
    ...facts({ status: row.status ?? undefined, owner: row.ownerRole ?? undefined }),
  };
}

/**
 * The Ontology preset's graph (REQ-2130, amended 2026-10-29): the terms, and what each reaches --
 * the terms its edges reach, the domains it is in, the table it grounds (Ontology) or the columns
 * it IS (Columns) with each column's table, and each such table's data product (Data). Its edges
 * are the terms' relationships and that grounding chain only: no relationship, join or lineage
 * edge between tables or columns is drawn. A table is drawn through the edge reaching it, never by
 * a layer of its own.
 */
export function glossaryGraph(terms: TermRow[], edges: TermEdgeRow[]): GraphData {
  const b = builder();
  // A reached table's data product (Data): read named under Data, else for its columns' look alone
  // (no node).
  const productOf = (tid: string, p: ProductRef | undefined) => {
    if (p?.name === undefined || p.domainId === undefined) return;
    b.node(productNodeOf(p.id, p.name, p.domainId, p));
    b.edge(productNode(p.id), tid, "member");
  };
  termNodes(b, terms);
  for (const t of terms) {
    const id = termNode(t.id);
    // REQ-2130: the term IS each table it grounds (a class term): the table a cylinder, drawn
    // with the ontology.
    for (const g of t.tables ?? []) {
      const tid = `t:${g.id}`;
      b.node({
        id: tid,
        type: "table",
        label: catalogTableName(g),
        domains: [g.domainId],
        stored: true,
        layers: ["ontology"],
      });
      b.edge(id, tid, "term_binding", "IS", ["ontology"]);
      for (const p of productsOf(g.dataProductTables)) productOf(tid, p);
    }
    for (const r of t.refs ?? []) {
      const tid = `t:${r.tableId}`;
      const cid = `c:${r.columnKey}`;
      const held = productsOf(r.registeredTable?.dataProductTables);
      // The column's table: drawn through the column, never by a layer of its own.
      b.node({
        id: tid,
        type: "table",
        label: catalogTableName(r),
        domains: [r.domainId],
        stored: true,
        layers: [],
        ...(r.registeredTable ? facts(tableFacts(r.registeredTable)) : {}),
      });
      // Its sensitivity: its row in sensitive_coverage when sensitive; absent when not read.
      const sensitivity = r.column ? rowsOf(r.column.sensitivity) : undefined;
      b.node({
        id: cid,
        type: "column",
        label: `${r.tableName}.${r.columnName}`,
        parent: tid,
        domains: [r.domainId],
        // A column whose table a data product holds is coloured apart (nodeLook).
        inProduct: held.length > 0,
        // A fact, not a filter: the glossary's query is not narrowed by sensitivity.
        ...(r.column
          ? facts({
              ...columnFacts(r.column),
              sensitive: sensitivity?.flatMap((x) => x.tags.split(", ")),
            })
          : {}),
      });
      // The grounding chain runs term, column, table: from the column, so a layered layout hangs
      // the table below it.
      b.edge(cid, tid, "has_column");
      // glossary.py's physical refs: the term IS this column.
      b.edge(id, cid, "term_binding", "IS");
      for (const p of held) productOf(tid, p);
    }
  }
  termEdgesDrawn(b, edges);
  return b.graph();
}

/** The graph the ontology's query answer draws, under the rules ``s`` reads it by: each term once
 * (readOntology reads a search's or a focus's neighbourhood a hop at a time), with its edges --
 * the Ontology preset's term reach, or the Product map's with its products. */
export function ontologyGraph(rows: OntologyRows, s?: Pick<QueryScope, "products">): GraphData {
  const terms = rows._terms ?? [];
  return s?.products
    ? productMapGraph(terms, termEdges(terms), rows.data_products ?? [])
    : glossaryGraph(terms, termEdges(terms));
}

/**
 * The Product map's graph (REQ-2174, ONE GRAPH, RULES PER PRESET): the ontology with what the
 * product graph showed. Each node and edge says which layers draw it where its kind's own layer is
 * not the whole answer:
 * - a column a term IS is Columns'; a data product's column is Data's too, and its datasets' other
 *   columns are Data's alone;
 * - a table is drawn only through a column drawn (Tables, with Columns) or as a product's dataset
 *   (Data); the relationships declared between the tables drawn are Tables' (ownerTables).
 */
export function productMapGraph(
  terms: TermRow[],
  edges: TermEdgeRow[],
  products: ProductRow[] = [],
): GraphData {
  const b = builder();
  const { node, edge, nodes, out } = b;
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
      label: catalogTableName({ domainId: domain, tableName: name }),
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
    row: { status?: string | null; ownerRole?: string | null },
  ) => {
    node(productNodeOf(id, name, domain, row));
    if (nodes.has(`d:${domain}`)) edge(productNode(id), `d:${domain}`, "in_domain");
  };
  // Related tables are drawn last, so a table or product read as itself keeps its own layers.
  const related: [number, RelationshipRow][] = [];
  const relationships = (from: number, rows: RelationshipRow[] | null | undefined) => {
    for (const r of rows ?? []) related.push([from, r]);
  };

  termNodes(b, terms);
  for (const t of terms) {
    const id = termNode(t.id);
    // REQ-2130: the term IS each table it grounds (a class term): the table a cylinder, drawn
    // with the ontology.
    for (const g of t.tables ?? []) {
      node({
        id: `t:${g.id}`,
        type: "table",
        label: catalogTableName(g),
        domains: [g.domainId],
        stored: true,
        layers: ["ontology"],
      });
      edge(id, `t:${g.id}`, "term_binding", "IS", ["ontology"]);
    }
    for (const r of t.refs ?? []) {
      const tid = `t:${r.tableId}`;
      const cid = `c:${r.columnKey}`;
      const owners = productsOf(r.registeredTable?.dataProductTables);
      const inProduct = owners.length > 0;
      // A column whose table a data product holds is the Data layer's too.
      const held: LayerId[] = inProduct ? ["columns", "data"] : ["columns"];
      table(r.tableId, r.domainId, r.tableName, inProduct, r.registeredTable);
      // Its sensitivity: its row in sensitive_coverage when sensitive; absent when not read.
      const sensitivity = r.column ? rowsOf(r.column.sensitivity) : undefined;
      node({
        id: cid,
        type: "column",
        label: `${r.tableName}.${r.columnName}`,
        parent: tid,
        domains: [r.domainId],
        inProduct,
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
      edge(
        tid,
        cid,
        "has_column",
        undefined,
        inProduct ? ["data", "ownerTables"] : ["ownerTables"],
      );
      // glossary.py's physical refs: the term IS this column.
      edge(id, cid, "term_binding", "IS", held);
      // A product read for its column's look alone (Data off) has neither name nor domain and is
      // no node; under Data both are read.
      for (const p of owners)
        if (p.name !== undefined && p.domainId !== undefined) {
          product(p.id, p.name, p.domainId, p);
          edge(productNode(p.id), tid, "member");
        }
      relationships(r.tableId, r.registeredTable?.sourceRelationships);
    }
  }
  for (const p of products) {
    const pid = productNode(p.id);
    product(p.id, p.name, p.domainId, p);
    for (const c of (p.dataProductCommands ?? []).flatMap((x) =>
      x.trackedFunction ? [x.trackedFunction] : [],
    )) {
      node({ id: `f:${c.name}`, type: "command", label: c.name, domains: [c.domainId] });
      edge(pid, `f:${c.name}`, "command");
    }
    for (const m of memberTablesOf(p)) {
      const tid = `t:${m.id}`;
      if (m.dqContract != null) {
        // A checker member (REQ-1443): quality evidence, drawn under Quality, never as a dataset.
        const kid = `k:${m.id}`;
        node({ id: kid, type: "checker", label: catalogTableName(m), domains: [m.domainId] });
        edge(pid, kid, "member", undefined, ["quality"]);
        if (m.checks) edge(kid, `t:${m.checks.id}`, "checks");
        continue;
      }
      for (const r of m.profileResults ?? []) {
        // Its table is its parent: a profile is reached on its table's page.
        node({ id: `r:${r.id}`, type: "profile", label: r.tableName, parent: tid });
        edge(`r:${r.id}`, tid, "profiled");
      }
      table(m.id, m.domainId, m.tableName, true, m);
      edge(pid, tid, "member");
      for (const c of m.tableColumns ?? []) {
        const cid = `c:${c.columnKey}`;
        const bound = (c.glossaryRef ?? []).map((g) => termNode(g.termId));
        // A dataset's column is the Data layer's; one a term IS is Columns' as well.
        node({
          id: cid,
          type: "column",
          label: `${m.tableName}.${c.columnName}`,
          parent: tid,
          domains: [m.domainId],
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
      relationships(m.id, m.sourceRelationships);
    }
  }
  // Product links last: a linked product read as itself is Data's; one only linked to is a bare
  // node, drawn by its link alone.
  for (const p of products)
    for (const e of p.edges ?? []) {
      const to = e.targetProduct;
      // A link to a product the role cannot read has no end to draw.
      if (!to) continue;
      const z = productNode(to.id);
      node({
        id: z,
        type: "product",
        label: to.name,
        domains: [to.domainId],
        layers: ["productLinks"],
      });
      edge(productNode(p.id), z, "product_link", e.aliases ?? undefined);
      const link = out.get(
        `product_link:${productNode(p.id)}>${z}${e.aliases ? `:${e.aliases}` : ""}`,
      );
      if (link) link.paths = e.pathCount;
    }
  // REQ-2174 (amended 2026-11-07): each table related to a table drawn, named by the relationship,
  // with the product holding it: Related tables', one step, nothing of the related table's own.
  for (const [from, r] of related) {
    const to = r.targetTable;
    // A table the role cannot read has no end to draw.
    if (!to) continue;
    const tid = `t:${to.id}`;
    node({
      id: tid,
      type: "table",
      label: catalogTableName(to),
      domains: [to.domainId],
      stored: true,
      layers: [],
    });
    edge(`t:${from}`, tid, "relationship", r.cypherType, ["related"]);
    for (const p of productsOf(to.dataProductTables)) {
      // The relationship's read names every product it reaches.
      node({
        ...productNodeOf(p.id, p.name as string, p.domainId as string, p),
        layers: ["related"],
      });
      edge(productNode(p.id), tid, "member", undefined, ["related"]);
    }
  }
  termEdgesDrawn(b, edges);
  return b.graph();
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

/** REQ-2174 (amended 2026-11-07, WHAT THE GRAPH DRAWS): ``g`` with ``product``'s inputs on the
 * Inputs layer (LayerId "lineage"), from the product lineage endpoint (REQ-2156, cut to what the
 * reader can read): where the product comes from, never who consumes it. Each input links to the
 * member table that reads it (the member whose lineage, walked upstream, reaches it first); an
 * input that is other products' member stands for each of them. Its column lineage (REQ-2129)
 * joins the columns it names: a member's or an input's column by its table, any other by its
 * relation. ``tables`` gives the relation each of the product's member tables is served under. */
export function withProductLineage(
  g: GraphData,
  product: string,
  lineage: ProductLineage,
  tables: { id: number; domainId: string; tableName: string }[],
): GraphData {
  const nodes = new Map(g.nodes.map((n) => [n.id, n]));
  const edges = new Map(g.edges.map((e) => [e.id, e]));
  const readers = inputReaders(lineage);
  const memberTable = new Map(tables.map((t) => [relationName(t), t.id]));
  for (const i of lineage.inputs) {
    // An input that is another product's member stands for each such product; the focus product
    // is not drawn as its own input. An input in no other product stands for itself.
    const others = i.product_ids.filter((p) => p !== product);
    const sources = others.length > 0 ? others.map(productNode) : [`l:${i.relation}`];
    const members = readers.get(i.relation);
    // The endpoint finds each input upstream of a member: one no member reaches is not its input.
    if (!members || members.size === 0)
      throw new Error(`product ${product}'s input ${i.relation} is read by none of its members`);
    for (const from of sources) {
      if (!nodes.has(from)) {
        const other = others.find((p) => productNode(p) === from);
        nodes.set(
          from,
          other === undefined
            ? { id: from, type: "relation", label: inputLabel(i), domains: [i.domain_id] }
            : { id: from, type: "product", label: other, domains: [i.domain_id] },
        );
      }
      for (const m of members) {
        const tid = memberTable.get(m);
        // A member the read leaves out (another domain's, under the domain filter) has no end to
        // draw.
        if (tid === undefined) continue;
        const id = `lineage:${from}>t:${tid}`;
        if (!edges.has(id)) edges.set(id, { id, a: from, b: `t:${tid}`, kind: "lineage" });
      }
    }
  }
  const tableOf = new Map<string, number>([
    ...tables.map((t) => [relationName(t), t.id] as [string, number]),
    ...lineage.inputs.map((i) => [i.relation, i.table_id] as [string, number]),
  ]);
  const columnOf = new Map<string, string>();
  for (const n of lineage.nodes) {
    const tid = n.relation === null ? undefined : tableOf.get(n.relation);
    const id = tid === undefined ? `lc:${n.id}` : `c:${tid}:${n.column}`;
    columnOf.set(n.id, id);
    // A column the ontology's read did not draw stands alone, named by where it is read from,
    // drawn only as the end of a column lineage edge (on an unfolded table).
    if (!nodes.has(id))
      nodes.set(id, {
        id,
        type: "column",
        label:
          n.kind === "native"
            ? `${sourceObject(n)}.${n.column}`
            : `${n.relation === null ? "" : `${n.relation}.`}${n.column}`,
        inProduct: false,
        layers: [],
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

/** Each input's readers: the member relations whose lineage, walked upstream, reaches the input
 * before any other member (the endpoint's nearest-inputs walk, REQ-2126). */
export function inputReaders(lineage: ProductLineage): Map<string, Set<string>> {
  const members = new Set(lineage.members);
  const inputs = new Set(lineage.inputs.map((i) => i.relation));
  const relation = new Map(lineage.nodes.map((n) => [n.id, n.relation]));
  const up = new Map<string, string[]>();
  for (const e of lineage.edges) up.set(e.target, [...(up.get(e.target) ?? []), e.source]);
  const out = new Map<string, Set<string>>();
  for (const n of lineage.nodes) {
    if (n.relation === null || !members.has(n.relation)) continue;
    const member = n.relation;
    const seen = new Set([n.id]);
    const stack = [n.id];
    while (stack.length > 0) {
      for (const p of up.get(stack.pop() as string) ?? []) {
        if (seen.has(p)) continue;
        seen.add(p);
        const r = relation.get(p) ?? null;
        if (r !== null && members.has(r)) continue;
        if (r !== null && inputs.has(r)) out.set(r, (out.get(r) ?? new Set()).add(member));
        else stack.push(p);
      }
    }
  }
  return out;
}
