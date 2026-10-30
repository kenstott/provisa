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
import type { CyInstance } from "../graph/cytoscape-types";
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

const FOCUSED = "erd-focused";
const FIT_PADDING = 40;
/** The closest a fit comes: one table is read in its place, not blown up to the whole canvas. */
const MOST_ZOOM = 1.5;

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

/** Highlight the tables the diagram holds and fit them into view; the ids it held. */
export function highlightErdTables(cy: CyInstance, tableIds: number[]): number[] {
  cy.$(`.${FOCUSED}`).removeClass(FOCUSED);
  const held = tableIds.filter((id) => {
    const node = cy.getElementById(`t:${id}`);
    if (node.empty()) return false;
    node.addClass(FOCUSED);
    return true;
  });
  if (held.length === 0) return held;
  const focused = cy.$(`.${FOCUSED}`);
  cy.fit(focused, FIT_PADDING);
  if (cy.zoom() > MOST_ZOOM) {
    cy.zoom(MOST_ZOOM);
    cy.center(focused);
  }
  return held;
}

const DIM = "erd-dim";
const MATCH = "erd-match";

const text = (el: { data(key: string): unknown }, key: string): string | undefined => {
  const v = el.data(key);
  return typeof v === "string" ? v : undefined;
};

/** The page's filter box on the diagram, as a search focuses the Ontology graph (searchItems' rule):
 * what matches is marked and the rest dimmed, by classes -- nothing moved, nothing rebuilt. On
 * Tables a table matches by name or domain, and its lines and domain stay lit; on Relationships a
 * line matches by its source or its target, and its two tables and their domains stay lit. An empty
 * search clears the classes. Returns the ids of what matched. */
export function applyErdSearch(
  cy: CyInstance,
  search: string,
  page: "tables" | "relationships",
): string[] {
  const items = searchItems(search);
  const marked = new Set<string>();
  const lit = new Set<string>();
  const tableName = (id: string): string => text(cy.getElementById(id), "tableName") ?? "";
  if (items.length > 0) {
    if (page === "tables") {
      cy.nodes(".erd-table").forEach((n) => {
        if (matchesSearch([text(n, "tableName"), text(n, "domainId")], items)) marked.add(n.id());
      });
      marked.forEach((id) => lit.add(id));
      cy.edges().forEach((e) => {
        const ends = [String(e.data("source")), String(e.data("target"))];
        if (ends.some((id) => marked.has(id))) lit.add(e.id());
      });
    } else {
      cy.edges().forEach((e) => {
        const ends = [String(e.data("source")), String(e.data("target"))];
        if (!matchesSearch(ends.map(tableName), items)) return;
        marked.add(e.id());
        lit.add(e.id());
        ends.forEach((id) => lit.add(id));
      });
    }
    cy.nodes(".erd-table").forEach((n) => {
      const parent = n.data("parent");
      if (lit.has(n.id()) && typeof parent === "string") lit.add(parent);
    });
  }
  cy.batch(() => {
    cy.elements().removeClass(DIM).removeClass(MATCH);
    if (items.length === 0) return;
    cy.elements().forEach((el) => {
      if (!lit.has(el.id())) el.addClass(DIM);
      if (marked.has(el.id())) el.addClass(MATCH);
    });
  });
  return [...marked];
}
