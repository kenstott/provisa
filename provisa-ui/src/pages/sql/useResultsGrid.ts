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
// column widths, CSV/clipboard export, and column profiling.

import React, { useState, useCallback, useEffect, useMemo, useRef } from "react";
import { CHAR_PX, COL_MAX, COL_MIN, PAGE_SIZE } from "./types";
import type { ColumnProfile } from "./types";

/** A group header row (collapsible) or a data row, in render order. */
export type GridItem =
  | { type: "group"; level: number; col: string; value: string; count: number; key: string }
  | { type: "row"; row: Record<string, unknown> };

export interface ResultsGridState {
  sorts: { col: string; dir: "asc" | "desc" }[];
  filters: Record<string, string>;
  setFilters: React.Dispatch<React.SetStateAction<Record<string, string>>>;
  /** Ordered multi-level grouping columns; empty = ungrouped. */
  groupBy: string[];
  toggleGroupBy: (col: string) => void;
  collapsedGroups: Set<string>;
  toggleGroupCollapsed: (key: string) => void;
  /** Columns eligible for the header group-by toggle (the raw result columns). */
  baseColumns: string[];
  page: number;
  setPage: React.Dispatch<React.SetStateAction<number>>;
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
  handleDownloadCsv: () => void;
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
  filters?: Record<string, string>;
  groupBy?: string[];
  colWidths?: Record<string, number>;
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
): ResultsGridState {
  const [sorts, setSorts] = useState<{ col: string; dir: "asc" | "desc" }[]>(
    () => loadChoices(storageKey).sorts ?? [],
  );
  const [filters, setFilters] = useState<Record<string, string>>(
    () => loadChoices(storageKey).filters ?? {},
  );
  const [colWidths, setColWidths] = useState<Record<string, number>>(
    () => loadChoices(storageKey).colWidths ?? {},
  );
  const [groupBy, setGroupBy] = useState<string[]>(() => loadChoices(storageKey).groupBy ?? []);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [page, setPage] = useState(0);
  const [copiedResults, setCopiedResults] = useState(false);
  const resizingRef = useRef<{ col: string; startX: number; startW: number } | null>(null);

  useEffect(() => {
    if (!storageKey) return;
    localStorage.setItem(
      `provisa.grid.${storageKey}`,
      JSON.stringify({ sorts, filters, groupBy, colWidths }),
    );
  }, [storageKey, sorts, filters, groupBy, colWidths]);

  const baseColumns = useMemo(
    () => (resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {})),
    [resultRows, resultColumns],
  );

  // Grouping never changes the column set — group values become collapsible
  // row headers within the table, not aggregates.
  const displayColumns = baseColumns;

  const serverPaged = server != null;
  const serverHasMore = server?.hasMore ?? false;

  const displayRows = useMemo(() => {
    let rows = [...resultRows];
    if (serverPaged) return rows; // filtering/sorting already happened in the query
    for (const col of baseColumns) {
      const f = filters[col];
      if (!f) continue;
      const lower = f.toLowerCase();
      rows = rows.filter((r) => {
        const v = r[col];
        return v != null && String(v).toLowerCase().includes(lower);
      });
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
  }, [resultRows, baseColumns, filters, sorts, serverPaged]);

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
    () => (serverPaged ? items : items.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)),
    [items, page, serverPaged],
  );

  // Server mode never knows the total; the pager runs on hasMore instead.
  const totalPages = serverPaged
    ? page + 1 + (serverHasMore ? 1 : 0)
    : Math.max(1, Math.ceil(items.length / PAGE_SIZE));

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

  const handleDownloadCsv = useCallback(() => {
    const escape = (v: unknown) => {
      const s = v == null ? "" : String(v);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const lines = [displayColumns.map(escape).join(",")];
    for (const row of displayRows)
      lines.push(displayColumns.map((c) => escape(row[c])).join(","));
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "results.csv";
    a.click();
    URL.revokeObjectURL(url);
  }, [displayRows, displayColumns]);

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
    setColWidths({});
    setGroupBy([]);
    setCollapsedGroups(new Set());
    setPage(0);
  }, []);

  return {
    sorts,
    filters,
    setFilters,
    groupBy,
    toggleGroupBy,
    collapsedGroups,
    toggleGroupCollapsed,
    baseColumns,
    page,
    setPage,
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
    handleDownloadCsv,
    handleCopyResults,
    handleDownloadProfile,
    resetGrid,
    serverPaged,
    hasMore: serverHasMore,
  };
}
