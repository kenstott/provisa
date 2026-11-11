// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0f7c1e-2b3a-4c8e-9a41-7e6d2f0b8c13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187: a focus of the ERD, and it is a link: `domains` the only domains shown, `objects` the
// tables highlighted and fitted into view. The one domain list and the one table list both pages
// read, so a focus crosses between them. A table is named as the catalog names it, `domain.table`.

import { catalogTableName } from "../../naming";
import type { ListedTable } from "../../types/admin";
import type { GraphFilter } from "../provisa-graph/marks";
import type { GraphSpec } from "../provisa-graph/model";
import { matchesSearch, searchItems } from "../../lib/searchItems";

export interface ErdFocus {
  domains: string[];
  objects: string[];
}

/** What a focus comes to against the tables and domains that exist. */
export interface ResolvedErdFocus {
  /** The only domains shown; null with no focus, when the page's filter decides. */
  shownDomains: Set<string> | null;
  /** The domains and the tables it names that exist, as the bar names them. */
  domains: string[];
  tables: string[];
  /** The ids of the tables to highlight: those named, in a domain shown. */
  highlighted: number[];
  /** What it names that does not exist. */
  missing: string[];
}

const named = (list: string | null): string[] => [
  ...new Set((list ?? "").split(",").filter((name) => name !== "")),
];

export function readErdFocus(params: URLSearchParams): ErdFocus {
  return { domains: named(params.get("domains")), objects: named(params.get("objects")) };
}

/** Both lists named in `params`; a list with nothing in it is not in the link. */
export function writeErdFocus(params: URLSearchParams, focus: ErdFocus): void {
  for (const key of ["domains", "objects"] as const) {
    if (focus[key].length > 0) params.set(key, focus[key].join(","));
    else params.delete(key);
  }
}

/** `params` as a link's query, its lists comma-separated as they are written by hand. */
export function erdSearch(params: URLSearchParams): string {
  return `?${params.toString().replace(/%2C/g, ",")}`;
}

export const focusIsOn = (focus: ErdFocus): boolean =>
  focus.domains.length > 0 || focus.objects.length > 0;

export function resolveErdFocus(
  focus: ErdFocus,
  tables: ListedTable[],
  domainIds: string[],
): ResolvedErdFocus {
  if (!focusIsOn(focus)) {
    return { shownDomains: null, domains: [], tables: [], highlighted: [], missing: [] };
  }
  const byName = new Map(tables.map((t) => [catalogTableName(t), t]));
  const existing = new Set([...domainIds, ...tables.map((t) => t.domainId)]);
  const objects = focus.objects.flatMap((name) => byName.get(name) ?? []);
  // Only objects given: their own domains are shown.
  const shown = focus.domains.length > 0 ? focus.domains : objects.map((t) => t.domainId);
  const domains = [...new Set(shown)].filter((d) => existing.has(d));
  const shownDomains = new Set(domains);
  return {
    shownDomains,
    domains,
    tables: objects.map(catalogTableName),
    highlighted: objects.filter((t) => shownDomains.has(t.domainId)).map((t) => t.id),
    missing: [
      ...focus.domains.filter((d) => !existing.has(d)),
      ...focus.objects.filter((name) => !byName.has(name)),
    ],
  };
}

type FocusTable = Pick<ListedTable, "domainId" | "tableName">;

/** The panel's Focus: the table's domain, and the table. */
export function focusOnTable(table: FocusTable): ErdFocus {
  return { domains: [table.domainId], objects: [catalogTableName(table)] };
}

/** The panel's Add to focus: the table, and its domain if new. A link of objects alone lists no
 * domains and stays that way: its objects' own domains are the ones shown. */
export function addTableToFocus(focus: ErdFocus, table: FocusTable): ErdFocus {
  return {
    domains:
      focus.domains.length > 0 ? [...new Set([...focus.domains, table.domainId])] : focus.domains,
    objects: [...new Set([...focus.objects, catalogTableName(table)])],
  };
}

/** The elements a focus marks on the graph: its tables' nodes, and the lines of its relationships
 * (a relationship drawn as two legs through its junction is both). */
export function erdFocusIds(
  graph: GraphSpec,
  tableIds: number[],
  relationshipIds: string[] = [],
): string[] {
  const named = new Set(relationshipIds);
  return [
    ...tableIds.map((id) => `t:${id}`),
    ...graph.edges.filter((e) => named.has(String(e.data?.relationshipId))).map((e) => e.id),
  ];
}

/** The tables a focus marked: the table ids among the ids the graph held. */
export const heldTables = (held: string[]): number[] =>
  held.filter((id) => id.startsWith("t:")).map((id) => Number(id.slice(2)));

/** The page's filter box on the diagram, as a search focuses the Ontology graph (searchItems' rule):
 * what matches is marked and the rest dimmed -- nothing moved, nothing rebuilt. On Tables a table
 * matches by name or domain, and its lines and domain stay lit; on Relationships a line matches by
 * its source or its target, and its two tables and their domains stay lit. An empty search is no
 * filter. */
export function erdSearchMarks(
  graph: GraphSpec,
  search: string,
  page: "tables" | "relationships",
): GraphFilter | null {
  const items = searchItems(search);
  if (items.length === 0) return null;
  const marked = new Set<string>();
  const lit = new Set<string>();
  const nodes = new Map(graph.nodes.map((n) => [n.id, n]));
  const tableName = (id: string): string => nodes.get(id)?.name ?? "";
  if (page === "tables") {
    for (const n of graph.nodes)
      if (matchesSearch([n.name, String(n.data?.domainId ?? "")], items)) marked.add(n.id);
    marked.forEach((id) => lit.add(id));
    for (const e of graph.edges)
      if (marked.has(e.source) || marked.has(e.target)) lit.add(e.id);
  } else {
    for (const e of graph.edges) {
      const ends = [e.source, e.target];
      if (!matchesSearch(ends.map(tableName), items)) continue;
      marked.add(e.id);
      lit.add(e.id);
      ends.forEach((id) => lit.add(id));
    }
  }
  for (const n of graph.nodes) if (lit.has(n.id) && n.parent !== undefined) lit.add(n.parent);
  return { marked: [...marked], lit: [...lit] };
}
