// Copyright (c) 2026 Kenneth Stott
// Canary: 4c8e1f27-9a3d-4b60-8e15-2d7f6a0c93b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useMemo, useState } from "react";

/**
 * REQ-1940: sort and group are standard on every list, taken from the Register Tables page.
 * A page declares its columns; the hook owns the state and produces the ordered, grouped items.
 *
 * Sort cycles ascending, descending, off. Group is multi-level (click order sets the level);
 * groups are ordered by key and a group header collapses its rows.
 */
export interface ListColumn<T> {
  key: string;
  /** Column header text; also names the column in a group header ("Source: acme"). */
  label: string;
  /** Present when the column sorts. */
  sortValue?: (row: T) => string | number;
  /** Present when the column groups (categorical columns). */
  groupValue?: (row: T) => string;
}

export type ListItem<T> =
  | { type: "header"; level: number; key: string; label: string; count: number }
  | { type: "row"; row: T };

export interface ListSortGroup<T> {
  columns: ListColumn<T>[];
  testPrefix: string;
  sortCol: string | null;
  sortDir: "asc" | "desc";
  toggleSort: (key: string) => void;
  groupBy: string[];
  toggleGroup: (key: string) => void;
  collapsed: Set<string>;
  toggleCollapsed: (key: string) => void;
  /** Sorted, then grouped; collapsed groups contribute only their header. */
  items: ListItem<T>[];
}

/**
 * The items one page shows. A grouped list shows every group on one page (the group headers are
 * the navigation), so paging applies only while ungrouped.
 */
export function pageItems<T>(
  state: ListSortGroup<T>,
  page: number,
  size: number,
): ListItem<T>[] {
  if (state.groupBy.length > 0) return state.items;
  return state.items.slice(page * size, (page + 1) * size);
}

export function useListSortGroup<T>(
  rows: T[],
  columns: ListColumn<T>[],
  testPrefix: string,
  /** A page that opens already sorted (the user can still cycle it off). */
  initialSortCol: string | null = null,
): ListSortGroup<T> {
  const [sortCol, setSortCol] = useState<string | null>(initialSortCol);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [groupBy, setGroupBy] = useState<string[]>([]);
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());

  const toggleSort = useCallback(
    (key: string) => {
      if (sortCol !== key) {
        setSortCol(key);
        setSortDir("asc");
      } else if (sortDir === "asc") {
        setSortDir("desc");
      } else {
        setSortCol(null);
        setSortDir("asc");
      }
    },
    [sortCol, sortDir],
  );

  const toggleGroup = useCallback(
    (key: string) => {
      setGroupBy((prev) => (prev.includes(key) ? prev.filter((g) => g !== key) : [...prev, key]));
      // A different grouping has different group keys: collapsed state from the old one is stale.
      setCollapsed(new Set());
    },
    [],
  );

  const toggleCollapsed = useCallback(
    (key: string) =>
      setCollapsed((prev) => {
        const next = new Set(prev);
        if (next.has(key)) next.delete(key);
        else next.add(key);
        return next;
      }),
    [],
  );

  const items = useMemo(() => {
    const byKey = new Map(columns.map((c) => [c.key, c]));
    let ordered = rows;
    if (sortCol) {
      const sortValue = byKey.get(sortCol)?.sortValue;
      if (!sortValue) throw new Error(`list column "${sortCol}" is not sortable`);
      ordered = [...rows].sort((a, b) => {
        const av = sortValue(a);
        const bv = sortValue(b);
        const cmp =
          typeof av === "number" && typeof bv === "number"
            ? av - bv
            : String(av).localeCompare(String(bv));
        return sortDir === "asc" ? cmp : -cmp;
      });
    }
    if (groupBy.length === 0) {
      return ordered.map((row): ListItem<T> => ({ type: "row", row }));
    }
    const out: ListItem<T>[] = [];
    const emit = (subset: T[], level: number, parentKey: string) => {
      const col = byKey.get(groupBy[level]);
      const groupValue = col?.groupValue;
      if (!col || !groupValue) throw new Error(`list column "${groupBy[level]}" is not groupable`);
      const buckets = new Map<string, T[]>();
      for (const row of subset) {
        const k = groupValue(row);
        const bucket = buckets.get(k);
        if (bucket) bucket.push(row);
        else buckets.set(k, [row]);
      }
      const keys = [...buckets.keys()].sort((a, b) => a.localeCompare(b));
      for (const k of keys) {
        const members = buckets.get(k)!;
        const compositeKey = parentKey ? `${parentKey}|${k}` : k;
        out.push({
          type: "header",
          level: level + 1,
          key: compositeKey,
          label: `${col.label}: ${k}`,
          count: members.length,
        });
        if (collapsed.has(compositeKey)) continue;
        if (level + 1 < groupBy.length) emit(members, level + 1, compositeKey);
        else for (const row of members) out.push({ type: "row", row });
      }
    };
    emit(ordered, 0, "");
    return out;
  }, [rows, columns, sortCol, sortDir, groupBy, collapsed]);

  return {
    columns,
    testPrefix,
    sortCol,
    sortDir,
    toggleSort,
    groupBy,
    toggleGroup,
    collapsed,
    toggleCollapsed,
    items,
  };
}
