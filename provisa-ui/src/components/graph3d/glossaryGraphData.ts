// Copyright (c) 2026 Kenneth Stott
// Canary: 8f3a6d21-4c97-4e05-b1a8-2d7e9c5f4b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: the glossary's Ontology View data, read from the ontology schema (REQ-2163) as the
// signed-in user -- ontology._terms (its standing columns), the domains each term is in, the tables it
// grounds, its refs to columns with those columns' tables, each table's data product, and the edges
// leaving it (ontology._term_edges, read with the term) -- turned into the graph's nodes and edges.
// (Amended 2026-10-29, THE ONTOLOGY VIEW DRAWS ONLY WHAT A TERM REACHES:) every read is nested under
// the terms, so the query returns only what a term reaches: no table, column or product apart.

import { termRole, type TermGrounding } from "../../api/glossary";
import { catalogTableName } from "../../naming";
import { NEIGHBOURHOOD } from "./neighbourhood";
import { TERM_STATUSES } from "./graphModel";
import {
  operation,
  rootField,
  rowsOf,
  type MetaReader,
  type Selection,
  type Where,
} from "../../api/metaGraph";
import type {
  GraphData,
  GraphEdge,
  GraphNode,
  LayerId,
  QueryScope,
  TermStatus,
} from "./graphModel";
import {
  COLUMN_FACTS,
  PRODUCT_FACTS,
  TABLE_FACTS,
  columnFacts,
  facts,
  tableFacts,
} from "./productGraphData";

/** REQ-1593: a term in every domain, the org's shared vocabulary. */
export const ENTERPRISE_DOMAIN = "*";

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

/** Whether a reached table's data product is named: under Data, and for the 2D lane diagram,
 * whose product lane names it (REQ-2174). */
const productsNamed = (s: QueryScope) => s.layers.has("data") || s.lanes === true;

/** A reached table's data product: named (productsNamed), else read for the look of its columns
 * alone, with no product node. */
function productOf(s: QueryScope): NonNullable<Selection["rels"]> {
  return {
    product: {
      sel: { fields: productsNamed(s) ? ["id", "name", "domain_id", ...PRODUCT_FACTS] : ["id"] },
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
  return all([
    ...(domainWhere ? [domainWhere] : []),
    ...s.hiddenStatuses.map(notStatus),
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
  "status",
  "grounding",
  "identity_gap",
];

/** A term's selection at ``level`` hops from the search's matches. Under a search the terms a term's
 * edges reach are read with it, a level further, to the neighbourhood's depth: a term at the
 * last level is read with its edges' ends alone, so an edge between two of them is drawn and
 * nothing past them is. With no search every term is read at level 0, its edges' ends by id. */
function termSelection(s: QueryScope, level: number): Selection {
  const depth = s.search.length > 0 ? NEIGHBOURHOOD.depth : 0;
  const last = level >= depth;
  const kept = termKept(s);
  const rels: NonNullable<Selection["rels"]> = {};
  const end = (side: "targetTerm" | "sourceTerm") => ({
    [side]: { sel: last ? { fields: ["id"] } : termSelection(s, level + 1) },
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
    rels.edgesOut = {
      sel: {
        fields: ["id", "from_term_id", "to_term_id", "rel_type"],
        rels: end("targetTerm"),
      },
      where: edgeWhere("target_term"),
    };
    // Under a search a match's neighbourhood runs both ways: the edges reaching it too.
    if (depth > 0)
      rels.edgesIn = {
        sel: {
          fields: ["id", "from_term_id", "to_term_id", "rel_type"],
          rels: end("sourceTerm"),
        },
        where: edgeWhere("source_term"),
      };
  }
  // What a term touches is a hop past it: the last level's is past the neighbourhood.
  if (!last || depth === 0) {
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
            // REQ-2174: the column's and its table's facts, for the hover card and detail panel.
            column: {
              sel: {
                fields: COLUMN_FACTS,
                rels: { sensitivity: { sel: { fields: ["tags"] } } },
              },
            },
            registeredTable: {
              to: "registered_tables",
              sel: {
                fields: ["id", ...TABLE_FACTS.fields],
                rels: { ...TABLE_FACTS.rels, ...productOf(s) },
              },
            },
          },
        },
        where: inDomains(s.domains),
      };
  }
  // REQ-2174: its experts, its stewards on the graph's hover card and detail panel.
  rels.experts = { sel: { fields: ["user_id", "kind"] } };
  return { fields: TERM_FIELDS, rels };
}

/** The terms ``s`` reads at the root: those its filters keep, and under a search those it matches. */
function rootWhere(s: QueryScope): Where | undefined {
  const kept = termKept(s);
  return all([
    ...(kept ? [kept] : []),
    ...(s.orphans ? [] : [related(s)]),
    ...(s.search.length > 0 ? [searchWhere(s.search)] : []),
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

/** The Ontology View's one query for ``s``: its terms, each with the edges leaving it under the
 * Ontology layer, what it grounds, and those tables' data products under Data; null when the role
 * cannot read the terms. A part the role cannot read is left out. */
export function ontologyQuery(m: MetaReader, s: QueryScope): string | null {
  const terms = termsField(m, s);
  return terms === null ? null : operation("Ontology", [terms]);
}

/** Every term the query read, at whatever level of a search's neighbourhood, once: a term read at
 * two levels keeps what the fuller read holds. */
export function flatTerms(rows: TermRow[]): TermRow[] {
  const byId = new Map<number, TermRow>();
  const visit = (t: TermRow) => {
    const seen = byId.get(t.id);
    // The fuller read: a nearer level reads what a term touches, the last its edges' ends alone.
    if (!seen || Object.keys(t).length > Object.keys(seen).length) byId.set(t.id, t);
    for (const e of t.edgesOut ?? [])
      if (e.targetTerm && "name" in e.targetTerm) visit(e.targetTerm as TermRow);
    for (const e of t.edgesIn ?? [])
      if (e.sourceTerm && "name" in e.sourceTerm) visit(e.sourceTerm as TermRow);
  };
  rows.forEach(visit);
  return [...byId.values()];
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

/** What the Ontology View's query answers. */
export interface OntologyRows {
  _terms: TermRow[];
}

/** The graph the Ontology View's query answer draws: each term once (a search's neighbourhood is
 * read nested, a level to each hop), with its edges. */
export function ontologyGraph(rows: OntologyRows): GraphData {
  const terms = flatTerms(rows._terms);
  return glossaryGraph(terms, termEdges(terms));
}

/** A reached table's data product, as read: its id alone with Data off (see productOf). */
type ProductRef = {
  id: string;
  name?: string;
  domainId?: string;
  status?: string | null;
  ownerRole?: string | null;
} | null;

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
  // Under Ontology: the edges leaving the term (null when none; see rowsOf).
  // Under a search, the term an edge reaches is read in full to the neighbourhood's depth.
  edgesOut?: (TermEdgeRow & { targetTerm?: TermRow | { id: number } | null })[] | null;
  edgesIn?: (TermEdgeRow & { sourceTerm?: TermRow | { id: number } | null })[] | null;
  // REQ-2130 (class grounding): the tables whose rows the term names.
  tables?: { id: number; tableName: string; domainId: string; product?: ProductRef }[] | null;
  refs?:
    | {
        columnKey: string;
        columnName: string;
        tableId: number;
        tableName: string;
        domainId: string;
        column?: (ColumnFactsRow & { sensitivity?: { tags: string }[] | null }) | null;
        registeredTable?:
          | (TableFactsRow & { id: number; product?: ProductRef })
          | null;
      }[]
    | null;
}

/** A column's facts (COLUMN_FACTS) and a table's (TABLE_FACTS), as productGraphData reads them. */
type ColumnFactsRow = Parameters<typeof columnFacts>[0];
type TableFactsRow = Parameters<typeof tableFacts>[0];

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

/**
 * The glossary's graph: the terms, and what each reaches (REQ-2130, amended 2026-10-29) -- the
 * terms its edges reach, the domains it is in, the table it grounds (Ontology) or the columns it IS
 * (Columns) with each column's table, and each such table's data product (Data). Its edges are the
 * terms' relationships and that grounding chain only: no relationship between tables is drawn.
 * A table is drawn through the edge reaching it, never by a layer of its own.
 */
export function glossaryGraph(terms: TermRow[], edges: TermEdgeRow[]): GraphData {
  const nodes = new Map<string, GraphNode>();
  const out = new Map<string, GraphEdge>();
  // A node or edge met again (a class term's table that is also a column's) is drawn by either's layers.
  const widen = (into: { layers?: LayerId[] }, layers: LayerId[] | undefined) => {
    if (!layers || !into.layers) return;
    into.layers = [...new Set([...into.layers, ...layers])];
  };
  const node = (n: GraphNode) => {
    const seen = nodes.get(n.id);
    if (!seen) nodes.set(n.id, n);
    else {
      widen(seen, n.layers);
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
  // A reached table's data product (Data): read named under Data, else for its columns' look alone
  // (no node).
  const productOf = (tid: string, p: ProductRef | undefined) => {
    if (p?.name === undefined || p.domainId === undefined) return;
    node({
      id: `p:${p.id}`,
      type: "product",
      label: p.name,
      domains: [p.domainId],
      ...facts({ status: p.status ?? undefined, owner: p.ownerRole ?? undefined }),
    });
    edge(`p:${p.id}`, tid, "member");
  };

  const named = new Set<string>();
  for (const t of terms)
    for (const d of t.domains ?? []) if (d.domainId !== ENTERPRISE_DOMAIN) named.add(d.domainId);
  for (const d of named) node({ id: `d:${d}`, type: "domain", label: d });

  for (const t of terms) {
    const id = `term:${t.id}`;
    const status = standing(t);
    const domains = (t.domains ?? []).map((d) => d.domainId);
    node({
      id,
      type: "term",
      label: t.name,
      status,
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
      edge(id, `d:${d}`, "in_domain");
    // REQ-2130: the term IS each table it grounds (a class term): the table a cylinder, drawn
    // with the ontology.
    for (const g of t.tables ?? []) {
      const tid = `t:${g.id}`;
      node({
        id: tid,
        type: "table",
        label: catalogTableName(g),
        domains: [g.domainId],
        stored: true,
        layers: ["ontology"],
      });
      edge(id, tid, "term_binding", "IS", ["ontology"]);
      productOf(tid, g.product);
    }
    for (const r of t.refs ?? []) {
      const tid = `t:${r.tableId}`;
      const cid = `c:${r.columnKey}`;
      const p = r.registeredTable?.product;
      // The column's table: drawn through the column, never by a layer of its own.
      node({
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
      node({
        id: cid,
        type: "column",
        label: `${r.tableName}.${r.columnName}`,
        parent: tid,
        domains: [r.domainId],
        // A column whose table a data product holds is coloured apart (nodeLook).
        inProduct: p != null,
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
      edge(cid, tid, "has_column");
      // glossary.py's physical refs: the term IS this column.
      edge(id, cid, "term_binding", "IS");
      productOf(tid, p);
    }
  }
  for (const e of edges) {
    const a = `term:${e.fromTermId}`;
    const b = `term:${e.toTermId}`;
    // An edge to a term the role cannot read has no end to draw.
    if (nodes.has(a) && nodes.has(b)) edge(a, b, "term_edge", e.relType);
  }
  return { nodes: [...nodes.values()], edges: [...out.values()] };
}
