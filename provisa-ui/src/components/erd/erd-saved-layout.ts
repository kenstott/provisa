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
import type { GraphPlaces, StandPlaces } from "../provisa-graph/places";
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

/** The ERD's places, from the graph's: its tables by id, its domains by id. */
export function erdPlaces(
  graph: GraphPlaces,
  options: ErdOptions,
  collapsed: Set<string>,
): ErdPlaces {
  const tables = new Map<number, Pt>();
  graph.nodes.forEach((at, id) => {
    if (id.startsWith("t:")) tables.set(Number(id.slice(2)), at);
  });
  const domains = new Map<string, Pt & { w: number; h: number }>();
  graph.groups.forEach((box, id) => {
    if (id.startsWith("d:")) domains.set(id.slice(2), box);
  });
  return { options, collapsed, tables, domains };
}

/** A saved layout's places, by the ids the graph draws them with. */
export function standOf(layout: SavedLayout): StandPlaces {
  return {
    nodes: layout.tables.map((p) => ({ id: `t:${p.tableId}`, x: p.x, y: p.y })),
    groups: layout.domains.map((p) => ({ id: `d:${p.domainId}`, x: p.x, y: p.y, w: p.w, h: p.h })),
  };
}
