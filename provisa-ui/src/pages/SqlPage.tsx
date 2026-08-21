// Copyright (c) 2026 Kenneth Stott
// Canary: d4f9e388-0149-4766-b2aa-9a87874b2e9c
// Canary: PLACEHOLDER
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback, useMemo, useRef, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { get as idbGet, set as idbSet, del as idbDel } from "idb-keyval";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { format as formatSql } from "sql-formatter";
import { EditorView } from "@codemirror/view";
import {
  Button,
  Drawer,
  Group,
  Modal,
  SegmentedControl,
  Checkbox,
  Text,
  Title,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import { useDomainFilter } from "../context/DomainFilterContext";
import { runSql, explainSql } from "../api/admin";
import type { ExplainResponse } from "../api/admin";
import {
  useRoles,
  useDomains,
  useTables,
  useRelationships,
  useMetrics,
  useRegisterTable,
  useUpdateTable,
} from "../hooks/useAdminQueries";
import type { RegisteredTable } from "../types/admin";
import { DERIVED_SOURCE_ID } from "../types/admin";
import { useCapability } from "../hooks/useCapability";
import { tabResultsKey, tabSqlKey, tabNlKey } from "./sql/types";
import type { ResultTab, TopTab, SqlTab, SqlResults, ViewColumnConfig } from "./sql/types";
import { useResultsGrid } from "./sql/useResultsGrid";
import { loadHistory, saveHistory } from "./sql/historyHelpers";
import { autoAliasConflicts, normalizeDomain, parseSemanticMetricQuery } from "./sql/sqlHelpers";
import { newTabId, emptyTab, loadTabsMeta, persistTabsMeta, nextTabTitle } from "./sql/tabHelpers";
import { SchemaBrowser } from "./sql/SchemaBrowser";
import { JoinCanvas } from "./sql/JoinCanvas";
import { SqlEditorPanel } from "./sql/SqlEditorPanel";
import { ResultsPanel } from "./sql/ResultsPanel";
import { ViewModal } from "./sql/ViewModal";

// ── SqlPage ──────────────────────────────────────────────────────────────────

export function SqlPage() {
  const { t } = useTranslation();
  const { checkedDomains, ensureDomainChecked } = useDomainFilter();
  const location = useLocation();
  const navigate = useNavigate();
  const canCreateView = useCapability("create_view");
  const canRequestView = useCapability("query_development");
  const { roles: rolesData } = useRoles();
  const { domains: domainsData } = useDomains();
  const { tables: tablesData, refetch: refetchTables } = useTables();
  const { relationships: relsData, refetch: refetchRelationships } = useRelationships();
  const { metrics } = useMetrics();
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
  // REQ-1322: read-only expansion preview + one-way detach for metric-referencing SQL.
  const [expansionOpen, setExpansionOpen] = useState(false);
  const [expansionText, setExpansionText] = useState("");
  const [expansionLoading, setExpansionLoading] = useState(false);
  const [detachConfirmOpen, setDetachConfirmOpen] = useState(false);
  // REQ-1318: pure semantic metric queries can save as a metric view.
  const [saveAsMetricView, setSaveAsMetricView] = useState(true);
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
  const [statsEnabled, setStatsEnabled] = useState(
    () => localStorage.getItem("sql:statsEnabled") === "true",
  );
  const [queryStats, setQueryStats] = useState<unknown>(null);
  // REQ-1519: the plan Analyze last described, and the reason a description was refused.
  const [analyzePlan, setAnalyzePlan] = useState<ExplainResponse | null>(null);
  const [analyzeError, setAnalyzeError] = useState("");
  const [analyzing, setAnalyzing] = useState(false);
  const [errors, _setErrors] = useState<string[]>([]);
  const [expandedDomains, setExpandedDomains] = useState<Set<string>>(new Set());
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [domainPages, setDomainPages] = useState<Record<string, number>>({});
  const [history, setHistory] = useState<ReturnType<typeof loadHistory>>(loadHistory);
  const [copied, setCopied] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const editorViewRef = useRef<EditorView | null>(null);
  const pendingAutoRunRef = useRef(
    (location.state as { autoRun?: boolean } | null)?.autoRun === true,
  );
  const pendingRunAfterFormatRef = useRef(false);
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
    const merged = tabs.map((t) => (t.id === activeTabId ? { ...t, sqlText, nlText } : t));
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
        c.nativeFilterType
          ? [c.computedSqlAlias, `_nf_${c.computedSqlAlias}`]
          : [c.computedSqlAlias],
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

  // REQ-1322: the editor references the semantic metrics schema.
  const sqlReferencesMetrics = useMemo(() => /\bmetrics\.\w+/i.test(sqlText), [sqlText]);
  // REQ-1318: non-null when the view SQL is a pure semantic metric query.
  const metricViewInfo = useMemo(
    () => parseSemanticMetricQuery(viewSqlNormalized),
    [viewSqlNormalized],
  );

  // REQ-1322: server-side expansion of a metric query via EXPLAIN on the /data/sql
  // surface — the server compiler is the single generator (REQ-1321).
  const fetchExpansion = useCallback(async (): Promise<{ text: string; isError: boolean }> => {
    const inner = sqlText.trim().replace(/;+$/, "");
    const result = await runSql(`EXPLAIN ${inner}`, role);
    if (result.error) return { text: result.error, isError: true };
    const text = result.rows.map((r) => Object.values(r).map(String).join(" ")).join("\n");
    return { text, isError: false };
  }, [sqlText, role]);

  const handleShowExpansion = useCallback(async () => {
    setExpansionOpen(true);
    setExpansionLoading(true);
    const { text } = await fetchExpansion();
    setExpansionText(text);
    setExpansionLoading(false);
  }, [fetchExpansion]);

  // REQ-1322: one-way detach — replaces the editor text with the server expansion
  // and severs the metric link permanently (no re-ingestion path).
  const handleDetach = useCallback(async () => {
    const { text, isError } = await fetchExpansion();
    setDetachConfirmOpen(false);
    if (isError) {
      setResultError(text);
      return;
    }
    setSqlText(text);
    setTabs((prev) => prev.map((t2) => (t2.id === activeTabId ? { ...t2, detached: true } : t2)));
  }, [fetchExpansion, activeTabId]);

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

  // Client-side grid state (sort/filter/group/page/widths/export) — shared hook.
  const grid = useResultsGrid(resultRows, resultColumns);
  const { resetGrid } = grid;

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

  const loadTabIntoWorkingState = useCallback(
    (t: SqlTab) => {
      setSqlText(t.sqlText);
      setNlText(t.nlText);
      setResultColumns(t.resultColumns);
      setResultRows(t.resultRows);
      setResultError(t.resultError);
      setExecMs(t.execMs);
      setQueryStats(null);
      setNlError("");
      resetGrid();
      setResultTab("results");
    },
    [resetGrid],
  );

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

  const handleSaveView = useCallback(async () => {
    if (!viewId.trim() || !viewDomainId.trim()) return;
    setViewSaving(true);
    setViewMsg("");
    try {
      // REQ-1318: a metric view registers through the real viewMetrics input — the server
      // generates the view SQL from the spec and regenerates it when the metric changes.
      const asMetricView = metricViewInfo !== null && saveAsMetricView;
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
        sourceId: DERIVED_SOURCE_ID,
        domainId: viewDomainId.trim(),
        schemaName: "views",
        tableName: viewId.trim(),
        alias: viewId.trim(),
        description: viewDescription.trim() || undefined,
        viewSql: asMetricView ? undefined : viewSqlNormalized,
        viewMetrics:
          asMetricView && metricViewInfo
            ? {
                metrics: [metricViewInfo.metric],
                dimensions: metricViewInfo.dimensions,
                filters: [],
              }
            : undefined,
        columns,
      });
      const idMatch = result.message.match(/\(id=(\d+)\)/);
      const newTableId = idMatch ? parseInt(idMatch[1], 10) : null;
      setViewMsg(canCreateView ? "View created." : "View request submitted.");
      setSavedViewId(newTableId);
      // Reveal the view's domain in the filter so it shows in the schema sidebar / Views list
      // immediately (the created view is registered under viewDomainId).
      ensureDomainChecked(viewDomainId.trim());
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
    metricViewInfo,
    saveAsMetricView,
    canCreateView,
    viewColumns,
    registerTable,
    refetchTables,
    refetchRelationships,
    ensureDomainChecked,
  ]);

  // REQ-1519: describe the statement through the ONE pipeline instead of running it. The plan
  // comes back governed, optimized and routed, so the tree is the tree for the SQL that would run.
  const handleAnalyze = useCallback(async () => {
    if (!sqlText.trim()) return;
    setAnalyzing(true);
    setAnalyzeError("");
    try {
      const plan = await explainSql(sqlText.trim().replace(/;+$/, ""), role, false);
      setAnalyzePlan(plan);
    } catch (e) {
      setAnalyzePlan(null);
      setAnalyzeError(e instanceof Error ? e.message : String(e));
    }
    setAnalyzing(false);
    setResultTab("analyze");
  }, [sqlText, role]);

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
    const result = await runSql(sampledSql, role, statsEnabled);
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
    resetGrid();
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
  }, [sqlText, role, sampleMode, sampleSize, activeTabId, statsEnabled, resetGrid]);

  useEffect(() => {
    if (pendingAutoRunRef.current && sqlText.trim()) {
      pendingAutoRunRef.current = false;
      // REQ-1359: "Open in SQL" must copy the SQL in, prettify it, then execute.
      const pretty = formatSql(sqlText, { language: "postgresql" });
      if (pretty !== sqlText) {
        pendingRunAfterFormatRef.current = true;
        // eslint-disable-next-line react-hooks/set-state-in-effect -- one-time nav-forwarded format, guarded by pendingAutoRunRef
        setSqlText(pretty);
      } else {
        handleRun();
      }
    }
  }, [sqlText, handleRun]);

  useEffect(() => {
    if (pendingRunAfterFormatRef.current && sqlText.trim()) {
      pendingRunAfterFormatRef.current = false;
      handleRun();
    }
  }, [sqlText, handleRun]);

  const handleOpenViewModal = useCallback(() => {
    setViewId("");
    setViewDescription("");
    setViewDomainId("");
    setViewMsg("");
    setSaveAsMetricView(true); // REQ-1318: default on
    // Build a lookup of column descriptions from all registered tables.
    // Build description lookup keyed by both raw column name and tableName_columnName
    // so aliased result columns like "users_id" still find "id"'s description.
    const colDescMap = new Map<string, string>();
    for (const t of tables) {
      for (const c of t.columns) {
        if (c.description) {
          if (!colDescMap.has(c.columnName)) colDescMap.set(c.columnName, c.description);
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
      <Group
        justify="space-between"
        wrap="nowrap"
        style={{
          padding: "0.75rem 1rem",
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
        }}
      >
        <Group gap="0.75rem" wrap="nowrap">
          <Text fw={600} size="0.9rem" style={{ letterSpacing: "0.02em" }}>
            {t("sqlPage.title")}
          </Text>
          <SegmentedControl
            size="xs"
            value={topTab}
            onChange={(v) => setTopTab(v as TopTab)}
            data={[
              { label: t("sqlPage.tabSql"), value: "sql" },
              { label: t("sqlPage.tabCanvas"), value: "canvas" },
            ]}
            data-testid="sql-page-top-tabs"
          />
        </Group>
        <Group gap="0.75rem" wrap="nowrap" ml="auto">
          {sqlReferencesMetrics && (
            <>
              <Button
                size="compact-xs"
                variant="default"
                onClick={handleShowExpansion}
                data-testid="sql-show-expansion"
              >
                {t("sqlPage.showExpansion")}
              </Button>
              <Button
                size="compact-xs"
                color="orange"
                variant="light"
                onClick={() => setDetachConfirmOpen(true)}
                data-testid="sql-detach"
              >
                {t("sqlPage.detachToPhysical")}
              </Button>
            </>
          )}
          {execMs !== null && (
            <Text size="0.75rem" c="var(--text-muted)">
              {t("sqlPage.execMs", { ms: execMs })}
            </Text>
          )}
          <Checkbox
            size="xs"
            label={t("sqlPage.queryStats")}
            checked={statsEnabled}
            onChange={(e) => {
              setStatsEnabled(e.currentTarget.checked);
              localStorage.setItem("sql:statsEnabled", String(e.currentTarget.checked));
            }}
            data-testid="sql-page-stats-checkbox"
          />
        </Group>
      </Group>

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
          metrics={metrics}
          tables={tables}
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
              metrics={metrics}
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
              handleAnalyze={handleAnalyze}
              analyzing={analyzing}
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
              grid={grid}
              errors={errors}
              history={history}
              queryStats={queryStats}
              analyzePlan={analyzePlan}
              analyzeError={analyzeError}
              sqlText={sqlText}
              setSqlText={setSqlText}
              setRole={setRole}
            />
          </div>
        </div>
      </div>

      {/* REQ-1322: read-only server expansion of the metric query */}
      <Drawer
        opened={expansionOpen}
        onClose={() => setExpansionOpen(false)}
        position="right"
        size="lg"
        title={<Title order={4}>{t("sqlPage.expansionTitle")}</Title>}
        data-testid="sql-expansion-drawer"
      >
        {expansionLoading ? (
          <Text size="sm" c="var(--text-muted)">
            {t("sqlPage.expansionLoading")}
          </Text>
        ) : (
          <pre
            data-testid="sql-expansion-text"
            style={{
              whiteSpace: "pre-wrap",
              fontSize: "0.75rem",
              fontFamily: "monospace",
              margin: 0,
            }}
          >
            {expansionText}
          </pre>
        )}
      </Drawer>

      {/* REQ-1322: one-way detach confirmation */}
      <Modal
        opened={detachConfirmOpen}
        onClose={() => setDetachConfirmOpen(false)}
        title={<Title order={4}>{t("sqlPage.detachConfirmTitle")}</Title>}
        centered
        data-testid="sql-detach-confirm-modal"
      >
        <Text mb="lg" size="sm">
          {t("sqlPage.detachConfirmBody")}
        </Text>
        <Group justify="flex-end" gap="sm">
          <Button
            variant="default"
            onClick={() => setDetachConfirmOpen(false)}
            data-testid="sql-detach-cancel"
          >
            {t("sqlPage.detachCancel")}
          </Button>
          <Button color="orange" onClick={handleDetach} data-testid="sql-detach-confirm">
            {t("sqlPage.detachConfirm")}
          </Button>
        </Group>
      </Modal>

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
        metricViewInfo={metricViewInfo}
        saveAsMetricView={saveAsMetricView}
        setSaveAsMetricView={setSaveAsMetricView}
        savedViewId={savedViewId}
        setSavedViewId={setSavedViewId}
        setViewColumns={setViewColumns}
        onNavigateToViews={() => navigate("/views")}
        onCloseConfirmation={handleCloseConfirmation}
      />
    </div>
  );
}
