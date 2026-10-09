// Copyright (c) 2026 Kenneth Stott
// Canary: 4576fb2d-f7c6-459d-99ef-7073b56b6be0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Client-side results-grid state shared by the SQL workbench, the admin Reports
// viewer, and the table preview modal: filter -> group -> sort -> page, plus
// column widths, clipboard copy, and column profiling. File exports live in
// ResultsGrid — REQ-1444 reads past the page, which this hook cannot do.

import React, { useState, useCallback, useEffect, useMemo, useRef } from "react";
import { CHAR_PX, COL_MAX, COL_MIN, PAGE_SIZE } from "./types";
import type { ColumnProfile } from "./types";
import {
  distinctValues,
  effectiveSpec,
  isUsable,
  matchesFilter,
  kindFromType,
  type ActiveFilter,
  type ColumnKind,
  type FilterSpec,
} from "./columnFilter";

/** A group header row (collapsible) or a data row, in render order. */
export type GridItem =
  | { type: "group"; level: number; col: string; value: string; count: number; key: string }
  | { type: "row"; row: Record<string, unknown> };

export interface ResultsGridState {
  sorts: { col: string; dir: "asc" | "desc" }[];
  filters: Record<string, string>;
  setFilters: React.Dispatch<React.SetStateAction<Record<string, string>>>;
  /** REQ-1937: filters chosen from a column's filter menu, one per column. A column carries either
      one of these or the quick syntax typed in its box, never both. */
  filterSpecs: Record<string, FilterSpec>;
  /** Set (or with null, drop) a column's menu filter; it replaces whatever was typed in the box. */
  setFilterSpec: (col: string, spec: FilterSpec | null) => void;
  /** Type into a column's filter box; it replaces the column's menu filter. */
  setFilterText: (col: string, text: string) => void;
  /** Each column's filter kind: its declared type, else read off the values held. */
  columnKinds: Record<string, ColumnKind>;
  /** The filters in force, usable ones only: what the rows are narrowed by and the chips show. */
  activeFilters: ActiveFilter[];
  /** Distinct values of a column in the rows the grid holds, with counts. */
  valuesOf: (col: string) => { value: string; count: number }[];
  /** How many rows the grid holds before any filter. */
  rowsHeld: number;
  /** REQ-1442: true when at least one column filter is narrowing the result. */
  hasFilters: boolean;
  /** REQ-1442: drop every column filter at once and return to the first page. */
  clearFilters: () => void;
  /** Ordered multi-level grouping columns; empty = ungrouped. */
  groupBy: string[];
  toggleGroupBy: (col: string) => void;
  collapsedGroups: Set<string>;
  toggleGroupCollapsed: (key: string) => void;
  /** Columns eligible for the header group-by toggle (the raw result columns). */
  baseColumns: string[];
  page: number;
  setPage: React.Dispatch<React.SetStateAction<number>>;
  /** REQ-1438: rows per page, chosen by the reader and retained with the other grid choices. */
  pageSize: number;
  setPageSize: (n: number) => void;
  displayColumns: string[];
  displayRows: Record<string, unknown>[];
  pagedItems: GridItem[];
  totalPages: number;
  colWidths: Record<string, number>;
  autoWidths: Record<string, number>;
  copiedResults: boolean;
  profile: ColumnProfile[];
  handleSort: (col: string) => void;
  handleResizeStart: (col: string, e: React.MouseEvent) => void;
  handleCopyResults: () => void;
  handleDownloadProfile: () => void;
  resetGrid: () => void;
  /** Server-paged mode: rows are ONE page fetched upstream; the hook does no
      client filter/sort/slice, and the pager advances on hasMore. */
  serverPaged: boolean;
  hasMore: boolean;
}

interface PersistedGridChoices {
  sorts?: { col: string; dir: "asc" | "desc" }[];
  /** REQ-1937: the one stored shape of column filters. An older `filters` value is not read. */
  columnFilters?: { text: Record<string, string>; specs: Record<string, FilterSpec> };
  groupBy?: string[];
  colWidths?: Record<string, number>;
  pageSize?: number;
}

function loadChoices(storageKey: string | undefined): PersistedGridChoices {
  if (!storageKey) return {};
  try {
    return JSON.parse(localStorage.getItem(`provisa.grid.${storageKey}`) ?? "{}");
  } catch {
    return {};
  }
}

export function useResultsGrid(
  resultRows: Record<string, unknown>[],
  resultColumns: string[],
  /** When set, filter/group/sort choices and column widths persist to localStorage under this key
      and restore on the next mount (per-report / per-table retention). */
  storageKey?: string,
  /** Present = server-paged: the caller fetches one page at a time (pushing
      filters/sorts/grouping into the query) and reports whether more exist. */
  server?: { hasMore: boolean },
  /** REQ-1937: each column's declared type, when the caller knows it. A column without one is typed
      from its values. */
  columnTypes?: Record<string, string | null | undefined>,
): ResultsGridState {
  const [sorts, setSorts] = useState<{ col: string; dir: "asc" | "desc" }[]>(
    () => loadChoices(storageKey).sorts ?? [],
  );
  const [filters, setFilters] = useState<Record<string, string>>(
    () => loadChoices(storageKey).columnFilters?.text ?? {},
  );
  const [filterSpecs, setFilterSpecs] = useState<Record<string, FilterSpec>>(
    () => loadChoices(storageKey).columnFilters?.specs ?? {},
  );
  const [colWidths, setColWidths] = useState<Record<string, number>>(
    () => loadChoices(storageKey).colWidths ?? {},
  );
  const [groupBy, setGroupBy] = useState<string[]>(() => loadChoices(storageKey).groupBy ?? []);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [page, setPage] = useState(0);
  // REQ-1438: retained per report/table exactly like the other choices — a reader who works a
  // report at 500 rows a page should not reset it to 100 on every visit.
  const [pageSize, setPageSizeState] = useState<number>(
    () => loadChoices(storageKey).pageSize ?? PAGE_SIZE,
  );
  const [copiedResults, setCopiedResults] = useState(false);
  const resizingRef = useRef<{ col: string; startX: number; startW: number } | null>(null);

  useEffect(() => {
    if (!storageKey) return;
    localStorage.setItem(
      `provisa.grid.${storageKey}`,
      JSON.stringify({
        sorts,
        columnFilters: { text: filters, specs: filterSpecs },
        groupBy,
        colWidths,
        pageSize,
      }),
    );
  }, [storageKey, sorts, filters, filterSpecs, groupBy, colWidths, pageSize]);

  const baseColumns = useMemo(
    () => (resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {})),
    [resultRows, resultColumns],
  );

  // Grouping never changes the column set — group values become collapsible
  // row headers within the table, not aggregates.
  const displayColumns = baseColumns;

  // REQ-1937: a column's filter family comes from the type the server reports for it, through the
  // one table in columnFilter.ts. A column with no reported type is text.
  const columnKinds = useMemo(() => {
    const out: Record<string, ColumnKind> = {};
    for (const c of baseColumns) out[c] = kindFromType(columnTypes?.[c]);
    return out;
  }, [baseColumns, columnTypes]);

  const activeFilters = useMemo(() => {
    const out: ActiveFilter[] = [];
    for (const col of baseColumns) {
      const kind = columnKinds[col];
      const spec = effectiveSpec(kind, filterSpecs[col], filters[col]);
      if (spec && isUsable(kind, spec)) out.push({ col, kind, spec });
    }
    return out;
  }, [baseColumns, columnKinds, filterSpecs, filters]);

  const serverPaged = server != null;
  const serverHasMore = server?.hasMore ?? false;

  const displayRows = useMemo(() => {
    let rows = [...resultRows];
    if (serverPaged) return rows; // filtering/sorting already happened in the query
    for (const { col, kind, spec } of activeFilters) {
      rows = rows.filter((r) => matchesFilter(kind, spec, r[col]));
    }
    if (sorts.length > 0) {
      rows.sort((a, b) => {
        for (const { col, dir } of sorts) {
          const av = a[col],
            bv = b[col];
          let cmp: number;
          if (av == null && bv == null) continue;
          if (av == null) {
            cmp = 1;
          } else if (bv == null) {
            cmp = -1;
          } else if (typeof av === "number" && typeof bv === "number") {
            cmp = av - bv;
          } else {
            cmp = String(av).localeCompare(String(bv));
          }
          if (cmp !== 0) return dir === "asc" ? cmp : -cmp;
        }
        return 0;
      });
    }
    return rows;
  }, [resultRows, activeFilters, sorts, serverPaged]);

  // Flattened group tree in render order: a header item per group value at each
  // level, its rows (or sub-groups) nested beneath, collapsed subtrees omitted.
  const items = useMemo((): GridItem[] => {
    if (groupBy.length === 0) return displayRows.map((row) => ({ type: "row", row }));
    const build = (rows: Record<string, unknown>[], level: number, prefix: string): GridItem[] => {
      if (level === groupBy.length) return rows.map((row) => ({ type: "row", row }));
      const col = groupBy[level];
      const buckets = new Map<string, Record<string, unknown>[]>();
      for (const r of rows) {
        const val = r[col] == null ? "null" : String(r[col]);
        const bucket = buckets.get(val);
        if (bucket) bucket.push(r);
        else buckets.set(val, [r]);
      }
      const out: GridItem[] = [];
      for (const [value, bucketRows] of buckets) {
        const key = `${prefix}\u0000${value}`;
        out.push({ type: "group", level, col, value, count: bucketRows.length, key });
        if (!collapsedGroups.has(key)) out.push(...build(bucketRows, level + 1, key));
      }
      return out;
    };
    return build(displayRows, 0, "");
  }, [displayRows, groupBy, collapsedGroups]);

  const autoWidths = useMemo(() => {
    const widths: Record<string, number> = {};
    for (const col of displayColumns) {
      const headerLen = col.length;
      const maxDataLen = displayRows.slice(0, 50).reduce((m, r) => {
        const v = r[col];
        return Math.max(m, v == null ? 4 : String(v).length);
      }, 0);
      widths[col] = Math.min(
        COL_MAX,
        Math.max(COL_MIN, Math.max(headerLen, maxDataLen) * CHAR_PX + 24),
      );
    }
    return widths;
  }, [displayRows, displayColumns]);

  const pagedItems = useMemo(
    () => (serverPaged ? items : items.slice(page * pageSize, (page + 1) * pageSize)),
    [items, page, pageSize, serverPaged],
  );

  // Server mode never knows the total; the pager runs on hasMore instead.
  const totalPages = serverPaged
    ? page + 1 + (serverHasMore ? 1 : 0)
    : Math.max(1, Math.ceil(items.length / pageSize));

  // A resize renumbers every page, so the reader goes back to the first one rather than to a
  // page number that now points somewhere else in the relation.
  const setPageSize = useCallback((n: number) => {
    setPageSizeState(n);
    setPage(0);
  }, []);

  // REQ-1442: clearing filters one input at a time is the only way back from a filter that emptied
  // the grid, and a reader who narrowed six columns has to find all six. This drops them together.
  const hasFilters = useMemo(
    () => Object.values(filters).some((f) => f !== "") || Object.keys(filterSpecs).length > 0,
    [filters, filterSpecs],
  );

  const clearFilters = useCallback(() => {
    setFilters({});
    setFilterSpecs({});
    setPage(0);
  }, []);

  const setFilterSpec = useCallback((col: string, spec: FilterSpec | null) => {
    setFilterSpecs((prev) => {
      const next = { ...prev };
      if (spec) next[col] = spec;
      else delete next[col];
      return next;
    });
    setFilters((prev) => {
      if (!(col in prev)) return prev;
      const next = { ...prev };
      delete next[col];
      return next;
    });
    setPage(0);
  }, []);

  const setFilterText = useCallback((col: string, text: string) => {
    setFilters((prev) => ({ ...prev, [col]: text }));
    setFilterSpecs((prev) => {
      if (!(col in prev)) return prev;
      const next = { ...prev };
      delete next[col];
      return next;
    });
    setPage(0);
  }, []);

  const valuesOf = useCallback((col: string) => distinctValues(resultRows, col), [resultRows]);

  const toggleGroupBy = useCallback((col: string) => {
    setGroupBy((prev) => (prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]));
    setCollapsedGroups(new Set());
    setPage(0);
  }, []);

  const toggleGroupCollapsed = useCallback((key: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
    setPage(0);
  }, []);

  const handleSort = useCallback((col: string) => {
    setPage(0);
    setSorts((prev) => {
      const idx = prev.findIndex((s) => s.col === col);
      if (idx === -1) return [...prev, { col, dir: "asc" }];
      if (prev[idx].dir === "asc")
        return prev.map((s, i) => (i === idx ? { ...s, dir: "desc" } : s));
      return prev.filter((_, i) => i !== idx);
    });
  }, []);

  const handleResizeStart = useCallback((col: string, e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    const th = (e.currentTarget as HTMLElement).closest("th") as HTMLElement;
    const startW = th.offsetWidth;
    resizingRef.current = { col, startX: e.clientX, startW };
    const onMove = (ev: MouseEvent) => {
      if (!resizingRef.current) return;
      const delta = ev.clientX - resizingRef.current.startX;
      const newW = Math.max(60, resizingRef.current.startW + delta);
      setColWidths((prev) => ({ ...prev, [resizingRef.current!.col]: newW }));
    };
    const onUp = () => {
      resizingRef.current = null;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  const handleCopyResults = useCallback(() => {
    const lines = [displayColumns.join("\t")];
    for (const row of displayRows)
      lines.push(displayColumns.map((c) => (row[c] == null ? "" : String(row[c]))).join("\t"));
    navigator.clipboard.writeText(lines.join("\n")).then(() => {
      setCopiedResults(true);
      setTimeout(() => setCopiedResults(false), 1500);
    });
  }, [displayRows, displayColumns]);

  const profile = useMemo((): ColumnProfile[] => {
    if (resultRows.length === 0) return [];
    return baseColumns.map((col) => {
      const vals = resultRows.map((r) => r[col]);
      const nullCount = vals.filter((v) => v === null || v === undefined).length;
      const blankCount = vals.filter((v) => typeof v === "string" && v.trim() === "").length;
      const nonNull = vals.filter((v) => v !== null && v !== undefined);
      const freq: Map<string, number> = new Map();
      for (const v of vals) {
        const k = v === null || v === undefined ? "NULL" : String(v);
        freq.set(k, (freq.get(k) ?? 0) + 1);
      }
      const distinctCount = freq.size;
      const constantValue = distinctCount === 1 ? vals[0] : undefined;
      const numbers = nonNull.filter((v) => typeof v === "number") as number[];
      const mean = numbers.length > 0 ? numbers.reduce((a, b) => a + b, 0) / numbers.length : null;
      const sorted = [...nonNull].sort((a, b) => (a! < b! ? -1 : a! > b! ? 1 : 0));
      const min = sorted.length > 0 ? (sorted[0] as string | number) : null;
      const max = sorted.length > 0 ? (sorted[sorted.length - 1] as string | number) : null;
      const topValues = [...freq.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([value, count]) => ({ value, count }));
      return {
        col,
        nullCount,
        blankCount,
        distinctCount,
        constantValue,
        min,
        max,
        mean,
        topValues,
      };
    });
  }, [resultRows, baseColumns]);

  const handleDownloadProfile = useCallback(() => {
    const blob = new Blob([JSON.stringify(profile, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "profile.json";
    a.click();
    URL.revokeObjectURL(url);
  }, [profile]);

  const resetGrid = useCallback(() => {
    setSorts([]);
    setFilters({});
    setFilterSpecs({});
    setColWidths({});
    setGroupBy([]);
    setCollapsedGroups(new Set());
    setPage(0);
    setPageSizeState(PAGE_SIZE);
  }, []);

  return {
    sorts,
    filters,
    setFilters,
    filterSpecs,
    setFilterSpec,
    setFilterText,
    columnKinds,
    activeFilters,
    valuesOf,
    rowsHeld: resultRows.length,
    hasFilters,
    clearFilters,
    groupBy,
    toggleGroupBy,
    collapsedGroups,
    toggleGroupCollapsed,
    baseColumns,
    page,
    setPage,
    pageSize,
    setPageSize,
    displayColumns,
    displayRows,
    pagedItems,
    totalPages,
    colWidths,
    autoWidths,
    copiedResults,
    profile,
    handleSort,
    handleResizeStart,
    handleCopyResults,
    handleDownloadProfile,
    resetGrid,
    serverPaged,
    hasMore: serverHasMore,
  };
}
