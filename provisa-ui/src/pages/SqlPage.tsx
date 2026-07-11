// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
// Canary: PLACEHOLDER
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useCallback, useMemo, useRef, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { get as idbGet, set as idbSet, del as idbDel } from "idb-keyval";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { EditorView } from "@codemirror/view";
import { useDomainFilter } from "../context/DomainFilterContext";
import { runSql } from "../api/admin";
import {
  useRoles,
  useDomains,
  useTables,
  useRelationships,
  useRegisterTable,
  useUpdateTable,
} from "../hooks/useAdminQueries";
import type { RegisteredTable } from "../types/admin";
import { useCapability } from "../hooks/useCapability";
import {
  PAGE_SIZE,
  COL_MAX,
  COL_MIN,
  CHAR_PX,
  tabResultsKey,
  tabSqlKey,
  tabNlKey,
} from "./sql/types";
import type { ResultTab, TopTab, SqlTab, SqlResults, ViewColumnConfig, ColumnProfile } from "./sql/types";
import { loadHistory, saveHistory } from "./sql/historyHelpers";
import { autoAliasConflicts, normalizeDomain } from "./sql/sqlHelpers";
import { newTabId, emptyTab, loadTabsMeta, persistTabsMeta, nextTabTitle } from "./sql/tabHelpers";
import { SchemaBrowser } from "./sql/SchemaBrowser";
import { JoinCanvas } from "./sql/JoinCanvas";
import { SqlEditorPanel } from "./sql/SqlEditorPanel";
import { ResultsPanel } from "./sql/ResultsPanel";
import { ViewModal } from "./sql/ViewModal";

// ── SqlPage ──────────────────────────────────────────────────────────────────

export function SqlPage() {
  const { checkedDomains } = useDomainFilter();
  const location = useLocation();
  const navigate = useNavigate();
  const canCreateView = useCapability("create_view");
  const canRequestView = useCapability("query_development");
  const { roles: rolesData } = useRoles();
  const { domains: domainsData } = useDomains();
  const { tables: tablesData, refetch: refetchTables } = useTables();
  const { relationships: relsData, refetch: refetchRelationships } = useRelationships();
  const { registerTable } = useRegisterTable();
  const { updateTable } = useUpdateTable();
  const [viewModal, setViewModal] = useState(false);
  const [viewId, setViewId] = useState("");
  const [viewDescription, setViewDescription] = useState("");
  const [viewDomainId, setViewDomainId] = useState("");
  const [viewSaving, setViewSaving] = useState(false);
  const [viewMsg, setViewMsg] = useState("");
  const [viewColumns, setViewColumns] = useState<ViewColumnConfig[]>([]);
  const [savedViewId, setSavedViewId] = useState<number | null>(null);
  const tables = tablesData;
  const existingRels = relsData;
  const [topTab, setTopTab] = useState<TopTab>("sql");
  const viewTable = (location.state as { viewTable?: RegisteredTable } | null)?.viewTable ?? null;

  // Query tabs. Working state (sqlText/nlText/result*) mirrors the active tab; inactive
  // tabs retain their content in the `tabs` array and are persisted per-tab.
  const initialTabs = useMemo(() => {
    const loaded = loadTabsMeta();
    const locSql = (location.state as { sql?: string } | null)?.sql;
    if (locSql != null) {
      const id = newTabId();
      const title = nextTabTitle(loaded.tabs);
      const newTab = emptyTab(id, title, locSql);
      loaded.tabs = [...loaded.tabs, newTab];
      loaded.activeId = id;
    }
    return loaded;
    // eslint-disable-next-line react-hooks/exhaustive-deps -- intentional mount-only memo; location.state is consumed once on mount, not tracked reactively
  }, []);
  const active0 = initialTabs.tabs.find((t) => t.id === initialTabs.activeId)!;
  const [tabs, setTabs] = useState<SqlTab[]>(initialTabs.tabs);
  const [activeTabId, setActiveTabId] = useState<string>(initialTabs.activeId);
  const [editingTabId, setEditingTabId] = useState<string | null>(null);
  const [editingTitle, setEditingTitle] = useState("");

  const [sqlText, setSqlText] = useState(active0.sqlText);
  const [role, setRole] = useState("admin");
  const roles = useMemo(
    () => (rolesData.length ? rolesData.map((r) => r.id) : ["admin"]),
    [rolesData],
  );
  const [running, setRunning] = useState(false);
  const [sampleMode, setSampleMode] = useState<"first" | "last" | "random">("first");
  const [sampleSize, setSampleSize] = useState(100);
  const [resultTab, setResultTab] = useState<ResultTab>("results");
  const [resultColumns, setResultColumns] = useState<string[]>(active0.resultColumns);
  const [resultRows, setResultRows] = useState<Record<string, unknown>[]>(active0.resultRows);
  const [resultError, setResultError] = useState(active0.resultError);
  const [execMs, setExecMs] = useState<number | null>(active0.execMs);
  const [statsEnabled, setStatsEnabled] = useState(() => localStorage.getItem("sql:statsEnabled") === "true");
  const [queryStats, setQueryStats] = useState<unknown>(null);
  const [errors, _setErrors] = useState<string[]>([]);
  const [expandedDomains, setExpandedDomains] = useState<Set<string>>(new Set());
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [domainPages, setDomainPages] = useState<Record<string, number>>({});
  const [history, setHistory] = useState<ReturnType<typeof loadHistory>>(loadHistory);
  const [copied, setCopied] = useState(false);
  const [copiedResults, setCopiedResults] = useState(false);
  const [sorts, setSorts] = useState<{ col: string; dir: "asc" | "desc" }[]>([]);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [colWidths, setColWidths] = useState<Record<string, number>>({});
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const resizingRef = useRef<{ col: string; startX: number; startW: number } | null>(null);
  const editorViewRef = useRef<EditorView | null>(null);
  const pendingAutoRunRef = useRef(
    (location.state as { autoRun?: boolean } | null)?.autoRun === true,
  );
  const [nlText, setNlText] = useState(active0.nlText);
  const [nlLoading, setNlLoading] = useState(false);
  const [nlError, setNlError] = useState("");
  const resultsHydrated = useRef(false);

  // Hydrate each tab's last-run results from IndexedDB on mount.
  useEffect(() => {
    let cancelled = false;
    Promise.all(
      initialTabs.tabs.map((t) =>
        idbGet<SqlResults>(tabResultsKey(t.id)).then((r) => ({ id: t.id, r })),
      ),
    ).then((loaded) => {
      if (cancelled) return;
      const byId = new Map(loaded.map((x) => [x.id, x.r]));
      setTabs((prev) =>
        prev.map((t) => {
          const r = byId.get(t.id);
          return r
            ? { ...t, resultColumns: r.columns, resultRows: r.rows, resultError: r.error }
            : t;
        }),
      );
      const ar = byId.get(initialTabs.activeId);
      if (ar) {
        setResultColumns(ar.columns);
        setResultRows(ar.rows);
        setResultError(ar.error);
      }
      resultsHydrated.current = true;
    });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- intentional mount-only effect; idb hydration runs once, initialTabs captured at mount
  }, []);

  useEffect(() => {
    localStorage.removeItem("provisa.sql.pending_query");
  }, []);

  // Persist tab metadata + per-tab sql/nl whenever the active tab's text changes.
  useEffect(() => {
    const merged = tabs.map((t) =>
      t.id === activeTabId ? { ...t, sqlText, nlText } : t,
    );
    persistTabsMeta(merged, activeTabId);
  }, [tabs, activeTabId, sqlText, nlText]);

  const domainMap = useMemo(
    () => Object.fromEntries(domainsData.map((d) => [normalizeDomain(d.id), d])),
    [domainsData],
  );

  const sqlSchema = useMemo(() => {
    const schema: Record<string, string[] | Record<string, string[]>> = {};
    for (const t of tables) {
      const cols = t.columns.flatMap((c) =>
        c.nativeFilterType ? [c.computedSqlAlias, `_nf_${c.computedSqlAlias}`] : [c.computedSqlAlias],
      );
      schema[t.tableName] = cols;
      if (t.alias) schema[t.alias] = cols;
      if (t.schemaName) {
        const schemaEntry = schema[t.schemaName] as Record<string, string[]> | undefined;
        if (!schemaEntry || Array.isArray(schemaEntry)) {
          schema[t.schemaName] = { [t.tableName]: cols };
        } else {
          schemaEntry[t.tableName] = cols;
        }
      }
    }
    return schema;
  }, [tables]);

  const sqlExtensions = useMemo(
    () => [sql({ dialect: PostgreSQL, schema: sqlSchema })],
    [sqlSchema],
  );

  const viewSqlExtensions = useMemo(
    () => [sql({ dialect: PostgreSQL }), EditorView.lineWrapping],
    [],
  );

  const viewSqlNormalized = useMemo(() => {
    const COMMENT_PREFIX = "-- provisa-params:";
    const PARAM_RE = /\$(\d+)=(NULL|TRUE|FALSE|-?\d+(?:\.\d+)?|'(?:[^']|'')*')/g;

    const lines = sqlText.trim().replace(/;+$/, "").split("\n");
    const params: Record<number, string> = {};
    let filtered = lines;
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].trim().startsWith(COMMENT_PREFIX)) {
        for (const m of lines[i].matchAll(PARAM_RE)) params[parseInt(m[1])] = m[2];
        filtered = [...lines.slice(0, i), ...lines.slice(i + 1)];
        break;
      }
    }
    let sql = filtered.join("\n");
    if (Object.keys(params).length > 0) {
      sql = sql.replace(/\$(\d+)/g, (_, n) => params[parseInt(n)] ?? `$${n}`);
    }
    // Strip trailing LIMIT (and optional OFFSET) — views must not have a fixed limit
    return sql
      .replace(/\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?$/i, "")
      .replace(/\s+OFFSET\s+\d+\s+LIMIT\s+\d+$/i, "")
      .trim();
  }, [sqlText]);

  const viewHasParams = useMemo(() => /\$\d+/.test(viewSqlNormalized), [viewSqlNormalized]);

  const domainGroups = useMemo(() => {
    const groups: Record<string, RegisteredTable[]> = {};
    for (const t of tables) {
      const isImplicitDomain = t.domainId === "meta" || t.domainId === "ops";
      if (
        !isImplicitDomain &&
        checkedDomains.size > 0 &&
        t.domainId &&
        !checkedDomains.has(t.domainId)
      )
        continue;
      const d = t.domainId ? normalizeDomain(t.domainId) : "(no domain)";
      (groups[d] = groups[d] || []).push(t);
    }
    return groups;
  }, [tables, checkedDomains]);

  const insertAtCursor = useCallback((text: string) => {
    const view = editorViewRef.current;
    if (!view) {
      setSqlText((prev) => prev + text);
      return;
    }
    const { from, to } = view.state.selection.main;
    view.dispatch({
      changes: { from, to, insert: text },
      selection: { anchor: from + text.length },
    });
    view.focus();
  }, []);

  const toggleDomain = (d: string) =>
    setExpandedDomains((prev) => {
      const next = new Set(prev);
      if (next.has(d)) {
        next.delete(d);
        setDomainPages((p) => {
          const n = { ...p };
          delete n[d];
          return n;
        });
      } else {
        next.add(d);
      }
      return next;
    });

  const toggleTable = (t: string) =>
    setExpandedTables((prev) => {
      const next = new Set(prev);
      if (next.has(t)) next.delete(t);
      else next.add(t);
      return next;
    });

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(sqlText).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [sqlText]);

  const handleSort = useCallback((col: string) => {
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

  const [page, setPage] = useState(0);

  // ----- Query tab actions -----
  const mergeActive = useCallback(
    (): SqlTab[] =>
      tabs.map((t) =>
        t.id === activeTabId
          ? { ...t, sqlText, nlText, resultColumns, resultRows, resultError, execMs }
          : t,
      ),
    [tabs, activeTabId, sqlText, nlText, resultColumns, resultRows, resultError, execMs],
  );

  const loadTabIntoWorkingState = useCallback((t: SqlTab) => {
    setSqlText(t.sqlText);
    setNlText(t.nlText);
    setResultColumns(t.resultColumns);
    setResultRows(t.resultRows);
    setResultError(t.resultError);
    setExecMs(t.execMs);
    setQueryStats(null);
    setNlError("");
    setSorts([]);
    setFilters({});
    setColWidths({});
    setPage(0);
    setResultTab("results");
  }, []);

  const switchTab = useCallback(
    (id: string) => {
      if (id === activeTabId) return;
      const merged = mergeActive();
      const target = merged.find((t) => t.id === id);
      if (!target) return;
      setTabs(merged);
      setActiveTabId(id);
      loadTabIntoWorkingState(target);
    },
    [activeTabId, mergeActive, loadTabIntoWorkingState],
  );

  const addTab = useCallback(() => {
    const merged = mergeActive();
    const tab = emptyTab(newTabId(), nextTabTitle(merged));
    setTabs([...merged, tab]);
    setActiveTabId(tab.id);
    loadTabIntoWorkingState(tab);
  }, [mergeActive, loadTabIntoWorkingState]);

  const closeTab = useCallback(
    (id: string) => {
      const merged = mergeActive();
      if (merged.length <= 1) {
        // Never drop the last tab — reset it to blank instead.
        const blank = emptyTab(merged[0].id, merged[0].title);
        setTabs([blank]);
        setActiveTabId(blank.id);
        loadTabIntoWorkingState(blank);
        idbDel(tabResultsKey(blank.id));
        return;
      }
      const idx = merged.findIndex((t) => t.id === id);
      const remaining = merged.filter((t) => t.id !== id);
      localStorage.removeItem(tabSqlKey(id));
      localStorage.removeItem(tabNlKey(id));
      idbDel(tabResultsKey(id));
      if (id === activeTabId) {
        const next = remaining[Math.min(idx, remaining.length - 1)];
        setActiveTabId(next.id);
        loadTabIntoWorkingState(next);
      }
      setTabs(remaining);
    },
    [activeTabId, mergeActive, loadTabIntoWorkingState],
  );

  const renameTab = useCallback((id: string, title: string) => {
    setTabs((prev) => prev.map((t) => (t.id === id ? { ...t, title } : t)));
  }, []);

  const autoWidths = useMemo(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    const widths: Record<string, number> = {};
    for (const col of cols) {
      const headerLen = col.length;
      const maxDataLen = resultRows.slice(0, 50).reduce((m, r) => {
        const v = r[col];
        return Math.max(m, v == null ? 4 : String(v).length);
      }, 0);
      widths[col] = Math.min(
        COL_MAX,
        Math.max(COL_MIN, Math.max(headerLen, maxDataLen) * CHAR_PX + 24),
      );
    }
    return widths;
  }, [resultRows, resultColumns]);

  const displayRows = useMemo(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    let rows = [...resultRows];
    for (const col of cols) {
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
  }, [resultRows, resultColumns, filters, sorts]);

  const pagedRows = useMemo(
    () => displayRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE),
    [displayRows, page],
  );

  const totalPages = Math.max(1, Math.ceil(displayRows.length / PAGE_SIZE));

  const handleDownloadCsv = useCallback(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    const escape = (v: unknown) => {
      const s = v == null ? "" : String(v);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const lines = [cols.map(escape).join(",")];
    for (const row of displayRows) lines.push(cols.map((c) => escape(row[c])).join(","));
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "results.csv";
    a.click();
    URL.revokeObjectURL(url);
  }, [displayRows, resultColumns, resultRows]);

  const handleCopyResults = useCallback(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    const lines = [cols.join("\t")];
    for (const row of displayRows)
      lines.push(cols.map((c) => (row[c] == null ? "" : String(row[c]))).join("\t"));
    navigator.clipboard.writeText(lines.join("\n")).then(() => {
      setCopiedResults(true);
      setTimeout(() => setCopiedResults(false), 1500);
    });
  }, [displayRows, resultColumns, resultRows]);

  const profile = useMemo((): ColumnProfile[] => {
    if (resultRows.length === 0) return [];
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    return cols.map((col) => {
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
  }, [resultRows, resultColumns]);

  const handleDownloadProfile = useCallback(() => {
    const blob = new Blob([JSON.stringify(profile, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "profile.json";
    a.click();
    URL.revokeObjectURL(url);
  }, [profile]);

  const handleSaveView = useCallback(async () => {
    if (!viewId.trim() || !viewDomainId.trim()) return;
    setViewSaving(true);
    setViewMsg("");
    try {
      const sql = viewSqlNormalized;
      const columns = viewColumns.map((c) => ({
        name: c.name,
        alias: c.alias || undefined,
        description: c.description || undefined,
        scope: c.scope,
        visibleTo: c.visibleTo,
        unmaskedTo: c.unmaskedTo
          ? c.unmaskedTo
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
          : undefined,
        maskType: c.maskType || undefined,
        maskPattern: c.maskPattern || undefined,
        maskReplace: c.maskReplace || undefined,
        maskValue: c.maskValue || undefined,
        maskPrecision: c.maskPrecision || undefined,
      }));
      const result = await registerTable({
        sourceId: "__provisa__",
        domainId: viewDomainId.trim(),
        schemaName: "views",
        tableName: viewId.trim(),
        alias: viewId.trim(),
        description: viewDescription.trim() || undefined,
        viewSql: sql,
        columns,
      });
      const idMatch = result.message.match(/\(id=(\d+)\)/);
      const newTableId = idMatch ? parseInt(idMatch[1], 10) : null;
      setViewMsg(canCreateView ? "View created." : "View request submitted.");
      setSavedViewId(newTableId);
      refetchTables();
      refetchRelationships();
      localStorage.setItem("provisa.schema.version", String(Date.now()));
      window.dispatchEvent(new StorageEvent("storage", { key: "provisa.schema.version" }));
    } catch (e) {
      setViewMsg(`Error: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setViewSaving(false);
    }
  }, [
    viewId,
    viewDescription,
    viewDomainId,
    viewSqlNormalized,
    canCreateView,
    viewColumns,
    registerTable,
    refetchTables,
    refetchRelationships,
  ]);

  const handleRun = useCallback(async () => {
    if (!sqlText.trim()) return;
    const aliased = autoAliasConflicts(sqlText);
    if (aliased !== sqlText) setSqlText(aliased);
    setRunning(true);
    setResultError("");
    const t0 = performance.now();
    const inner = aliased.trim().replace(/;+$/, "");
    const sampledSql =
      sampleMode === "first"
        ? `SELECT * FROM (\n${inner}\n) _sample LIMIT ${sampleSize}`
        : sampleMode === "last"
          ? `SELECT * FROM (\n${inner}\n) _sample ORDER BY 1 DESC LIMIT ${sampleSize}`
          : `SELECT * FROM (\n${inner}\n) _sample ORDER BY random() LIMIT ${sampleSize}`;
    const result = await runSql(sampledSql, role, false, statsEnabled);
    const durationMs = Math.round(performance.now() - t0);
    setExecMs(durationMs);
    setQueryStats(result.provisa_stats ?? null);
    if (result.error) {
      setResultError(result.error);
      setResultColumns([]);
      setResultRows([]);
      idbSet(tabResultsKey(activeTabId), { columns: [], rows: [], error: result.error });
    } else {
      setResultColumns(result.columns);
      setResultRows(result.rows);
      idbSet(tabResultsKey(activeTabId), { columns: result.columns, rows: result.rows, error: "" });
    }
    setSorts([]);
    setFilters({});
    setColWidths({});
    setPage(0);
    setResultTab("results");
    setRunning(false);
    const entry = {
      sql: aliased.trim(),
      role,
      executedAt: Date.now(),
      durationMs,
      rowCount: result.error ? 0 : result.rows.length,
      error: result.error ?? "",
    };
    setHistory((prev) => {
      const next = [entry, ...prev.filter((h) => h.sql !== entry.sql || h.role !== entry.role)];
      saveHistory(next);
      return next;
    });
  }, [sqlText, role, sampleMode, sampleSize, activeTabId, statsEnabled]);

  useEffect(() => {
    if (pendingAutoRunRef.current && sqlText.trim()) {
      pendingAutoRunRef.current = false;
      handleRun();
    }
  }, [sqlText, handleRun]);

  const handleOpenViewModal = useCallback(() => {
    setViewId("");
    setViewDescription("");
    setViewDomainId("");
    setViewMsg("");
    // Build a lookup of column descriptions from all registered tables.
    // Build description lookup keyed by both raw column name and tableName_columnName
    // so aliased result columns like "users_id" still find "id"'s description.
    const colDescMap = new Map<string, string>();
    for (const t of tables) {
      for (const c of t.columns) {
        if (c.description) {
          if (!colDescMap.has(c.columnName))
            colDescMap.set(c.columnName, c.description);
          const aliased = `${t.tableName}_${c.columnName}`;
          if (!colDescMap.has(aliased)) colDescMap.set(aliased, c.description);
        }
      }
    }
    setViewColumns(
      resultColumns.map((name) => ({
        name,
        alias: "",
        description: colDescMap.get(name) ?? "",
        scope: "domain" as const,
        visibleTo: roles,
        maskType: "" as const,
        maskPattern: "",
        maskReplace: "",
        maskValue: "",
        maskPrecision: "",
        unmaskedTo: "",
      })),
    );
    setViewModal(true);
  }, [tables, resultColumns, roles]);

  const handleCloseConfirmation = useCallback(() => {
    setViewMsg("");
    setViewId("");
    setViewDescription("");
    setViewDomainId("");
  }, []);

  return (
    <div
      style={{
        flex: 1,
        minHeight: 0,
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        background: "var(--bg)",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0.75rem 1rem",
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
          <span style={{ fontWeight: 600, fontSize: "0.9rem", letterSpacing: "0.02em" }}>
            SQL Explorer
          </span>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 0,
              border: "1px solid var(--border)",
              borderRadius: "5px",
              overflow: "hidden",
              marginLeft: "0.5rem",
            }}
          >
            {(["sql", "canvas"] as TopTab[]).map((tab) => (
              <button
                key={tab}
                onClick={() => setTopTab(tab)}
                style={{
                  padding: "0.2rem 0.6rem",
                  fontSize: "0.75rem",
                  background: topTab === tab ? "var(--primary)" : "none",
                  color: topTab === tab ? "#fff" : "var(--text-muted)",
                  border: "none",
                  cursor: "pointer",
                  textTransform: "capitalize",
                  fontWeight: topTab === tab ? 600 : 400,
                }}
              >
                {tab}
              </button>
            ))}
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem", marginLeft: "auto" }}>
          {execMs !== null && (
            <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
              {execMs} ms
            </span>
          )}
          <label style={{ display: "flex", alignItems: "center", gap: "0.3rem", fontSize: "0.8rem", cursor: "pointer" }}>
            <input
              type="checkbox"
              checked={statsEnabled}
              onChange={(e) => {
                setStatsEnabled(e.target.checked);
                localStorage.setItem("sql:statsEnabled", String(e.target.checked));
              }}
              style={{ marginRight: 2 }}
            />
            Query Stats
          </label>
        </div>
      </div>

      {/* Body: sidebar + right pane */}
      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {/* Schema browser drawer */}
        <SchemaBrowser
          sidebarOpen={sidebarOpen}
          setSidebarOpen={setSidebarOpen}
          domainGroups={domainGroups}
          domainMap={domainMap}
          expandedDomains={expandedDomains}
          expandedTables={expandedTables}
          domainPages={domainPages}
          topTab={topTab}
          insertAtCursor={insertAtCursor}
          toggleDomain={toggleDomain}
          toggleTable={toggleTable}
          setDomainPages={setDomainPages}
        />

        {/* Right pane */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
          <div
            style={{
              display: topTab === "canvas" ? "flex" : "none",
              flex: 1,
              overflow: "hidden",
              flexDirection: "column",
            }}
          >
            <JoinCanvas
              tables={tables}
              existingRels={existingRels}
              onGenerateSql={(generatedSql) => {
                setSqlText(generatedSql);
                setTopTab("sql");
              }}
            />
          </div>

          <div
            style={{
              display: topTab === "sql" ? "flex" : "none",
              flex: 1,
              overflow: "hidden",
              flexDirection: "column",
            }}
          >
            <SqlEditorPanel
              tabs={tabs}
              activeTabId={activeTabId}
              editingTabId={editingTabId}
              editingTitle={editingTitle}
              setEditingTabId={setEditingTabId}
              setEditingTitle={setEditingTitle}
              switchTab={switchTab}
              addTab={addTab}
              closeTab={closeTab}
              renameTab={renameTab}
              nlText={nlText}
              setNlText={setNlText}
              nlLoading={nlLoading}
              setNlLoading={setNlLoading}
              nlError={nlError}
              setNlError={setNlError}
              setSqlText={setSqlText}
              role={role}
              sqlText={sqlText}
              sqlExtensions={sqlExtensions}
              editorViewRef={editorViewRef}
              copied={copied}
              handleCopy={handleCopy}
              running={running}
              handleRun={handleRun}
              sampleMode={sampleMode}
              setSampleMode={setSampleMode}
              sampleSize={sampleSize}
              setSampleSize={setSampleSize}
              roles={roles}
              setRole={setRole}
              viewTable={viewTable}
              viewSaving={viewSaving}
              setViewSaving={setViewSaving}
              updateTable={updateTable}
              canCreateView={canCreateView}
              canRequestView={canRequestView}
              onOpenViewModal={handleOpenViewModal}
            />
            <ResultsPanel
              resultTab={resultTab}
              setResultTab={setResultTab}
              running={running}
              resultError={resultError}
              resultRows={resultRows}
              resultColumns={resultColumns}
              sorts={sorts}
              filters={filters}
              setFilters={setFilters}
              page={page}
              setPage={setPage}
              displayRows={displayRows}
              pagedRows={pagedRows}
              totalPages={totalPages}
              colWidths={colWidths}
              autoWidths={autoWidths}
              copiedResults={copiedResults}
              profile={profile}
              errors={errors}
              history={history}
              queryStats={queryStats}
              sqlText={sqlText}
              setSqlText={setSqlText}
              setRole={setRole}
              handleDownloadCsv={handleDownloadCsv}
              handleCopyResults={handleCopyResults}
              handleSort={handleSort}
              handleResizeStart={handleResizeStart}
              handleDownloadProfile={handleDownloadProfile}
            />
          </div>
        </div>
      </div>

      <ViewModal
        viewModal={viewModal}
        setViewModal={setViewModal}
        viewMsg={viewMsg}
        canCreateView={canCreateView}
        handleSaveView={handleSaveView}
        viewSaving={viewSaving}
        viewId={viewId}
        setViewId={setViewId}
        viewDomainId={viewDomainId}
        setViewDomainId={setViewDomainId}
        viewHasParams={viewHasParams}
        viewDescription={viewDescription}
        setViewDescription={setViewDescription}
        viewSqlNormalized={viewSqlNormalized}
        viewSqlExtensions={viewSqlExtensions}
        domainMap={domainMap}
        savedViewId={savedViewId}
        setSavedViewId={setSavedViewId}
        setViewColumns={setViewColumns}
        onNavigateToViews={() => navigate("/views")}
        onCloseConfirmation={handleCloseConfirmation}
      />
    </div>
  );
}
