// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4a2f71-3e8c-4b16-b0d5-62c1e7f3a984
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2189: a saved layout against the live model. A layout holds only the arrangement -- the
// selection, the options, what is collapsed and where each domain and table stands -- by id, so a
// renamed table keeps its place; what is drawn comes from the model. A table the layout does not
// hold, in a domain it shows, is new since the save: it waits in the tray rather than being placed.

import { catalogTableName } from "../../naming";
import type { ListedTable } from "../../types/admin";
import type { CyInstance } from "../graph/cytoscape-types";
import type { DomainPlace, LayoutArrangement, SavedLayout, TablePlace } from "../../api/erdLayouts";
import type { ErdFocus } from "./erd-focus";
import type { ErdOptions } from "./useErdOptions";

type Pt = { x: number; y: number };

/** Where the diagram stands now: what `captureArrangement` reads off the canvas. */
export interface ErdPlaces {
  options: ErdOptions;
  collapsed: Set<string>;
  /** Each drawn table's position, by id. */
  tables: Map<number, Pt>;
  /** Each drawn domain's box: its centre and its inner size. */
  domains: Map<string, Pt & { w: number; h: number }>;
}

/** The focus a layout's selection comes to: its domains, and its tables by the names they have
 * now. A table gone from the model is not named. */
export function layoutFocus(layout: SavedLayout, tables: ListedTable[]): ErdFocus {
  const byId = new Map(tables.map((t) => [t.id, t]));
  return {
    domains: layout.domains.filter((d) => d.selected).map((d) => d.domainId),
    objects: layout.tables.flatMap((place) => {
      const table = byId.get(place.tableId);
      return place.selected && table ? [catalogTableName(table)] : [];
    }),
  };
}

export function sameFocus(a: ErdFocus, b: ErdFocus): boolean {
  const same = (x: string[], y: string[]) =>
    x.length === y.length && new Set([...x, ...y]).size === x.length;
  return same(a.domains, b.domains) && same(a.objects, b.objects);
}

/** The tables new since the layout was saved: in a domain shown (`shown`; null, every one), and
 * not held by the layout. */
export function newSinceSaved(
  layout: SavedLayout,
  tables: ListedTable[],
  shown: Set<string> | null,
): ListedTable[] {
  const held = new Set(layout.tables.map((t) => t.tableId));
  return tables.filter((t) => !held.has(t.id) && (shown === null || shown.has(t.domainId)));
}

/** The layout to save: what the canvas shows now (`places`), the focus, and every table of a
 * domain shown (`members`) -- a table of a collapsed domain keeps the place `before` held. */
export function arrangementOf(
  places: ErdPlaces,
  focus: ErdFocus,
  members: ListedTable[],
  before: SavedLayout | null,
): LayoutArrangement {
  const selected = new Set(focus.objects);
  const held = new Map((before?.tables ?? []).map((t) => [t.tableId, t]));
  const tables: TablePlace[] = members.map((t) => {
    const at = places.tables.get(t.id) ?? held.get(t.id) ?? null;
    return {
      tableId: t.id,
      selected: selected.has(catalogTableName(t)),
      x: at?.x ?? null,
      y: at?.y ?? null,
    };
  });
  const domainIds = [...new Set(members.map((t) => t.domainId))].sort();
  const domains: DomainPlace[] = domainIds.map((domainId) => {
    const box = places.domains.get(domainId) ?? null;
    return {
      domainId,
      selected: focus.domains.includes(domainId),
      collapsed: places.collapsed.has(domainId),
      x: box?.x ?? null,
      y: box?.y ?? null,
      w: box?.w ?? null,
      h: box?.h ?? null,
    };
  });
  return { options: places.options, domains, tables };
}

/** Read the diagram's places off the canvas. */
export function readPlaces(cy: CyInstance, options: ErdOptions, collapsed: Set<string>): ErdPlaces {
  const tables = new Map<number, Pt>();
  cy.nodes(".erd-table").forEach((n) => {
    tables.set(Number(n.data("tableId")), n.position());
  });
  const domains = new Map<string, Pt & { w: number; h: number }>();
  cy.nodes(".erd-domain").forEach((n) => {
    domains.set(String(n.data("domainId")), { ...n.position(), w: n.width(), h: n.height() });
  });
  return { options, collapsed, tables, domains };
}

/** Stand the diagram as the layout holds it, with what was moved since (`moved`, by node id)
 * over it. A table with no saved position keeps the one the automatic layout gave it. A domain
 * keeps at least its saved size; a collapsed one stands where it was saved. */
export function applyPlaces(cy: CyInstance, layout: SavedLayout, moved: Map<string, Pt>): void {
  cy.batch(() => {
    for (const place of layout.tables) {
      const node = cy.getElementById(`t:${place.tableId}`);
      if (node.empty() || place.x === null || place.y === null) continue;
      node.position({ x: place.x, y: place.y });
    }
    for (const place of layout.domains) {
      const node = cy.getElementById(`d:${place.domainId}`);
      if (node.empty()) continue;
      if (node.children().empty()) {
        if (place.x !== null && place.y !== null) node.position({ x: place.x, y: place.y });
      } else if (place.w !== null && place.h !== null) {
        node.style({ "min-width": place.w, "min-height": place.h });
      }
    }
    moved.forEach((at, id) => {
      const node = cy.getElementById(id);
      if (!node.empty()) node.position(at);
    });
  });
}
