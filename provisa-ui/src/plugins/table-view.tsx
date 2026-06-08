// Copyright (c) 2026 Kenneth Stott
// Canary: 449553eb-096c-40e9-bb1e-4ccdf6e987bf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * GraphiQL Response Table View
 *
 * Overlays the response panel with a sortable table.
 * Supports multiple root fields with tabs.
 * Toggled via a button in the response toolbar area.
 */

import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import { get as idbGet, set as idbSet } from "idb-keyval";
import { Copy } from "lucide-react";
import { createPortal } from "react-dom";
import { useEditorContext } from "@graphiql/react";
import { parse, print, visit, Kind } from "graphql";
import { lastQueryElapsedMs } from "../query-timing";
import { setCurrentQueryStats, subscribeQueryStats, type QueryStats } from "../query-stats";
import { useAuth } from "../context/AuthContext";

type ViewMode = "json" | "table" | "stats";

const PAGE_SIZE = 50;

function injectLimitOffset(query: string, limit: number, offset: number): string {
  const doc = parse(query);
  const patched = visit(doc, {
    OperationDefinition(node) {
      const patchedSelections = node.selectionSet.selections.map((sel) => {
        if (sel.kind !== Kind.FIELD) return sel;
        const filteredArgs = (sel.arguments ?? []).filter(
          (a) => a.name.value !== "limit" && a.name.value !== "offset",
        );
        return {
          ...sel,
          arguments: [
            ...filteredArgs,
            {
              kind: Kind.ARGUMENT,
              name: { kind: Kind.NAME, value: "limit" },
              value: { kind: Kind.INT, value: String(limit) },
            },
            {
              kind: Kind.ARGUMENT,
              name: { kind: Kind.NAME, value: "offset" },
              value: { kind: Kind.INT, value: String(offset) },
            },
          ],
        };
      });
      return { ...node, selectionSet: { ...node.selectionSet, selections: patchedSelections } };
    },
  });
  return print(patched);
}

let _mermaidDagSeq = 0;

function MermaidDiagram({ chart }: { chart: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const idPrefixRef = useRef(`mermaid-dag-${++_mermaidDagSeq}`);
  useEffect(() => {
    let cancelled = false;
    const charts = chart.split(/\n\n(?=flowchart)/).filter(Boolean);
    import("mermaid").then((m) => {
      if (cancelled || !ref.current) return;
      m.default.initialize({ startOnLoad: false, theme: "dark" });
      const renders = charts.map((c, i) =>
        m.default.render(`${idPrefixRef.current}-${i}`, c).then(({ svg }) => svg),
      );
      Promise.all(renders).then((svgs) => {
        if (!cancelled && ref.current) ref.current.innerHTML = svgs.join("");
      });
    });
    return () => {
      cancelled = true;
    };
  }, [chart]);
  return <div ref={ref} className="stats-mermaid" />;
}

function flattenObject(obj: Record<string, unknown>, prefix: string, out: Record<string, unknown>) {
  for (const [key, val] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    if (val !== null && typeof val === "object" && !Array.isArray(val)) {
      flattenObject(val as Record<string, unknown>, fullKey, out);
    } else if (Array.isArray(val)) {
      out[fullKey] = JSON.stringify(val);
    } else if (typeof val === "string" && val.startsWith("{") && val.endsWith("}")) {
      try {
        const parsed = JSON.parse(val);
        if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
          flattenObject(parsed as Record<string, unknown>, fullKey, out);
          continue;
        }
      } catch {
        /* not JSON — treat as plain string */
      }
      out[fullKey] = val;
    } else {
      out[fullKey] = val;
    }
  }
}

interface ParsedTable {
  key: string;
  columns: string[];
  rows: Record<string, unknown>[];
  arrayColumns: string[];
}

function parseArrayLen(val: unknown): number {
  if (!val || typeof val !== "string" || !val.startsWith("[")) return 0;
  try {
    return (JSON.parse(val) as unknown[]).length;
  } catch {
    return 0;
  }
}

function normalizeForCsv(
  rows: Record<string, unknown>[],
  arrayColumns: string[],
  columns: string[],
): { normColumns: string[]; normRows: Record<string, unknown>[] } {
  const nonArrayCols = columns.filter((c) => !arrayColumns.includes(c));
  const normColSet = new Set<string>(nonArrayCols);
  const normRows: Record<string, unknown>[] = [];

  for (const row of rows) {
    const base: Record<string, unknown> = {};
    for (const c of nonArrayCols) base[c] = row[c];

    let seeds: Record<string, unknown>[] = [base];
    for (const col of arrayColumns) {
      const raw = row[col];
      let items: Record<string, unknown>[] = [];
      if (raw && typeof raw === "string" && raw.startsWith("[")) {
        try {
          items = (JSON.parse(raw) as unknown[]).map((item) => {
            const flat: Record<string, unknown> = {};
            flattenObject(item as Record<string, unknown>, col, flat);
            return flat;
          });
        } catch {
          /* leave items empty */
        }
      }
      if (items.length === 0) continue;
      for (const key of Object.keys(items[0] ?? {})) normColSet.add(key);
      const next: Record<string, unknown>[] = [];
      for (const seed of seeds) {
        for (const item of items) next.push({ ...seed, ...item });
      }
      seeds = next;
    }
    normRows.push(...seeds);
  }

  return { normColumns: Array.from(normColSet), normRows };
}

function computeNormalizedRowCount(
  rows: Record<string, unknown>[],
  arrayColumns: string[],
): number {
  return rows.reduce((sum, row) => {
    const product = arrayColumns.reduce((p, col) => p * Math.max(1, parseArrayLen(row[col])), 1);
    return sum + product;
  }, 0);
}

function parseResponse(text: string): ParsedTable[] {
  try {
    const parsed = JSON.parse(text);
    if (!parsed?.data) return [];
    const data = parsed.data as Record<string, unknown>;
    const rootKeys = Object.keys(data).filter((k) => !k.startsWith("__"));
    if (rootKeys.length === 0) return [];

    const tables: ParsedTable[] = [];
    for (const rootKey of rootKeys) {
      const rootVal = data[rootKey];
      if (rootVal === null) continue; // redirected field
      const items = Array.isArray(rootVal) ? rootVal : rootVal ? [rootVal] : [];
      if (items.length === 0) {
        tables.push({ key: rootKey, columns: [], rows: [], arrayColumns: [] });
        continue;
      }

      const allRows: Record<string, unknown>[] = [];
      const columnSet = new Set<string>();
      for (const item of items) {
        const flat: Record<string, unknown> = {};
        flattenObject(item as Record<string, unknown>, "", flat);
        for (const key of Object.keys(flat)) columnSet.add(key);
        allRows.push(flat);
      }
      const columns = Array.from(columnSet).filter(
        (col) => !Array.from(columnSet).some((other) => other.startsWith(col + ".")),
      );
      const arrayColumns = columns.filter((col) => allRows.some((r) => parseArrayLen(r[col]) > 0));
      tables.push({ key: rootKey, columns, rows: allRows, arrayColumns });
    }
    return tables;
  } catch {
    return [];
  }
}

export function ResponseTableOverlay() {
  const [viewMode, setViewMode] = useState<ViewMode>("json");
  const [queryStats, setQueryStats] = useState<QueryStats | null>(null);
  const [activeTab, setActiveTab] = useState(0);
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [responseText, setResponseText] = useState("");
  const [copiedJson, setCopiedJson] = useState(false);
  const [copiedCsv, setCopiedCsv] = useState(false);
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set());
  const [expandedStatRows, setExpandedStatRows] = useState<Set<number>>(new Set());
  const [elapsedMs, setElapsedMs] = useState<number | null>(null);
  const strippingRef = useRef(false);
  const lastStrippedRef = useRef<string | null>(null);
  const editorContext = useEditorContext();
  const { role } = useAuth();
  const [page, setPage] = useState(0);
  const [pagedResponseText, setPagedResponseText] = useState<string | null>(null);

  useEffect(() => subscribeQueryStats(setQueryStats), []);

  // Restore persisted response on first editor mount
  useEffect(() => {
    const editor = editorContext.responseEditor;
    if (!editor) return;
    if (!editor.getValue()) {
      idbGet<string>("provisa.graphql.response").then((saved) => {
        if (saved) {
          (editor as { setValue?: (v: string) => void }).setValue?.(saved);
          setResponseText(saved);
        }
      });
    }
  }, [editorContext.responseEditor]);

  // Poll for response editor value since GraphiQL doesn't re-render on content change
  useEffect(() => {
    const editor = editorContext.responseEditor;
    if (!editor) return;
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       initial sync of React state from external GraphiQL editor (external-system subscription pattern) */
    setResponseText(editor.getValue() ?? "");
    const cm = (
      editor as unknown as {
        editor?: {
          on?: (event: string, cb: () => void) => void;
          off?: (event: string, cb: () => void) => void;
        };
      }
    ).editor;
    const applyStats = (text: string) => {
      setElapsedMs(lastQueryElapsedMs);
      try {
        const parsed = JSON.parse(text);
        const stats = parsed?.extensions?.provisa_stats ?? null;
        setCurrentQueryStats(stats);
        if (stats && parsed.extensions) {
          const ext = { ...parsed.extensions };
          delete ext.provisa_stats;
          const stripped =
            Object.keys(ext).length === 0
              ? (() => {
                  const c = { ...parsed };
                  delete c.extensions;
                  return c;
                })()
              : { ...parsed, extensions: ext };
          const strippedText = JSON.stringify(stripped, null, 2);
          if (strippedText !== text) {
            lastStrippedRef.current = strippedText;
            strippingRef.current = true;
            (editor as { setValue?: (v: string) => void }).setValue?.(strippedText);
            strippingRef.current = false;
          }
        }
      } catch {
        setCurrentQueryStats(null);
      }
    };
    const handler = () => {
      if (strippingRef.current) return;
      const val = editor.getValue() ?? "";
      setResponseText(val);
      applyStats(val);
    };
    if (cm?.on) {
      cm.on("change", handler);
      return () => cm.off?.("change", handler);
    }
    const interval = setInterval(() => {
      const val = editor.getValue() ?? "";
      setResponseText((prev) => {
        if (prev !== val && val !== lastStrippedRef.current) applyStats(val);
        return prev !== val ? val : prev;
      });
    }, 300);
    return () => clearInterval(interval);
  }, [editorContext.responseEditor]);

  // Persist response and reset sort/tab/expanded rows when response changes
  useEffect(() => {
    if (responseText) {
      idbSet("provisa.graphql.response", responseText);
    }
    /* eslint-disable react-hooks/set-state-in-effect -- reset user interaction state (sort/tab/expansion/page) when underlying response data changes */
    setSortCol(null);
    setActiveTab(0);
    setExpandedRows(new Set());
    setPage(0);
    setPagedResponseText(null);
    /* eslint-enable react-hooks/set-state-in-effect */
  }, [responseText]);

  const displayText = pagedResponseText ?? responseText;
  const tables = useMemo(() => parseResponse(displayText), [displayText]);
  const currentTable = tables[activeTab] ?? null;
  const columns = currentTable?.columns ?? [];
  const rows = useMemo(() => currentTable?.rows ?? [], [currentTable]);

  const sortedRows = useMemo(() => {
    if (!sortCol) return rows;
    return [...rows].sort((a, b) => {
      const av = a[sortCol];
      const bv = b[sortCol];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number")
        return sortDir === "asc" ? av - bv : bv - av;
      return sortDir === "asc"
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
  }, [rows, sortCol, sortDir]);

  const handleToggleRow = useCallback((i: number) => {
    setExpandedRows((prev) => {
      const next = new Set(prev);
      if (next.has(i)) next.delete(i);
      else next.add(i);
      return next;
    });
  }, []);

  const handleSort = useCallback((col: string) => {
    setSortCol((prev) => {
      if (prev === col) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return col;
      }
      setSortDir("asc");
      return col;
    });
  }, []);

  const hasData = tables.some((t) => t.columns.length > 0);

  const downloadFile = useCallback((content: string, filename: string, mime: string) => {
    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }, []);

  const handleDownloadJSON = useCallback(() => {
    if (!responseText) return;
    downloadFile(responseText, "response.json", "application/json");
  }, [responseText, downloadFile]);

  const handleCopyJSON = useCallback(() => {
    if (!responseText) return;
    navigator.clipboard.writeText(responseText).then(() => {
      setCopiedJson(true);
      setTimeout(() => setCopiedJson(false), 2000);
    });
  }, [responseText]);

  const fetchServerCsv = useCallback(async (): Promise<string> => {
    const query = editorContext.queryEditor?.getValue() ?? "";
    const varsRaw = editorContext.variableEditor?.getValue() ?? "";
    const variables = varsRaw.trim() ? JSON.parse(varsRaw) : null;
    const res = await fetch("/data/graphql", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "text/csv",
        "X-Provisa-Role": role?.id ?? "",
      },
      body: JSON.stringify({ query, variables }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.text();
  }, [editorContext.queryEditor, editorContext.variableEditor, role?.id]);

  const handleCopyCSV = useCallback(() => {
    void fetchServerCsv().then((csv) => {
      void navigator.clipboard.writeText(csv).then(() => {
        setCopiedCsv(true);
        setTimeout(() => setCopiedCsv(false), 2000);
      });
    });
  }, [fetchServerCsv]);

  const handleDownloadCSV = useCallback(() => {
    void fetchServerCsv().then((csv) => {
      const filename = tables.length > 1 ? `${currentTable?.key ?? "response"}.csv` : "response.csv";
      downloadFile(csv, filename, "text/csv");
    });
  }, [fetchServerCsv, tables, currentTable, downloadFile]);

  const fetchPage = useCallback(async (p: number) => {
    if (p === 0) { setPagedResponseText(null); return; }
    const query = editorContext.queryEditor?.getValue() ?? "";
    const varsRaw = editorContext.variableEditor?.getValue() ?? "";
    const variables = varsRaw.trim() ? JSON.parse(varsRaw) : null;
    const res = await fetch("/data/graphql", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        "X-Provisa-Role": role?.id ?? "",
      },
      body: JSON.stringify({ query: injectLimitOffset(query, PAGE_SIZE, p * PAGE_SIZE), variables }),
    });
    setPagedResponseText(await res.text());
  }, [editorContext.queryEditor, editorContext.variableEditor, role?.id]);

  const handleNextPage = useCallback(() => {
    const next = page + 1;
    setPage(next);
    void fetchPage(next);
  }, [page, fetchPage]);

  const handlePrevPage = useCallback(() => {
    if (page === 0) return;
    const prev = page - 1;
    setPage(prev);
    void fetchPage(prev);
  }, [page, fetchPage]);

  const handleDownloadNormalizedCSV = useCallback(() => {
    if (!currentTable || currentTable.columns.length === 0) return;
    if (currentTable.arrayColumns.length === 0) return;
    const count = computeNormalizedRowCount(currentTable.rows, currentTable.arrayColumns);
    if (count > 10_000) {
      const ok = window.confirm(
        `Normalized export will produce ~${count.toLocaleString()} rows. Continue?`,
      );
      if (!ok) return;
    }
    const escape = (v: unknown) => {
      const s = v != null ? String(v) : "";
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const { normColumns, normRows } = normalizeForCsv(
      currentTable.rows,
      currentTable.arrayColumns,
      currentTable.columns,
    );
    const header = normColumns.map(escape).join(",");
    const body = normRows
      .map((row) => normColumns.map((col) => escape(row[col])).join(","))
      .join("\n");
    const basename = tables.length > 1 ? currentTable.key : "response";
    downloadFile(`${header}\n${body}`, `${basename}.normalized.csv`, "text/csv");
  }, [currentTable, tables, downloadFile]);

  const portalRef = useRef<HTMLElement | null>(null);
  const [portalReady, setPortalReady] = useState(false);

  useEffect(() => {
    const responseSection = document.querySelector(".graphiql-response") as HTMLElement | null;
    if (!responseSection) return;
    let wrapper = responseSection.querySelector(".response-table-wrapper") as HTMLElement | null;
    if (!wrapper) {
      wrapper = document.createElement("div");
      wrapper.className = "response-table-wrapper";
      responseSection.insertBefore(wrapper, responseSection.firstChild);
    }
    portalRef.current = wrapper;
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       portal target DOM node created imperatively on mount; flag triggers the createPortal render */
    setPortalReady(true);
  }, []);

  const overlayActive =
    (viewMode === "table" && hasData) || (viewMode === "stats" && queryStats != null);

  useEffect(() => {
    const responseSection = document.querySelector(".graphiql-response") as HTMLElement | null;
    if (!responseSection) return;
    if (overlayActive) {
      responseSection.classList.add("response-table-active");
      let el: HTMLElement | null = responseSection.parentElement;
      while (el && !el.classList.contains("graphiql-container")) {
        el.style.overflow = "hidden";
        el = el.parentElement;
      }
    } else {
      responseSection.classList.remove("response-table-active");
      let el: HTMLElement | null = responseSection.parentElement;
      while (el && !el.classList.contains("graphiql-container")) {
        el.style.overflow = "";
        el = el.parentElement;
      }
    }
  }, [overlayActive]);

  /* eslint-disable-next-line react-hooks/refs --
     portal target ref read during render to gate createPortal (standard imperative portal pattern); portalReady forces re-render after assignment */
  if (!portalReady || !portalRef.current) return null;

  return createPortal(
    <>
      <div className="response-view-toggle">
        <button
          className={viewMode === "json" ? "active" : ""}
          onClick={() => setViewMode("json")}
          title="JSON"
        >
          {"{ }"}
        </button>
        <button
          className={viewMode === "table" ? "active" : ""}
          onClick={() => setViewMode("table")}
          disabled={!hasData}
          title="Table"
        >
          <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
            <path d="M0 2a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V2zm15 2h-4v3h4V4zm0 4h-4v3h4V8zm0 4h-4v3h3a1 1 0 0 0 1-1v-2zM10 4H6v3h4V4zm0 4H6v3h4V8zm0 4H6v3h4v-3zM5 4H1v3h4V4zm0 4H1v3h4V8zm0 4H1v2a1 1 0 0 0 1 1h3v-3z" />
          </svg>
        </button>
        <button
          className={viewMode === "stats" ? "active" : ""}
          onClick={() => setViewMode("stats")}
          disabled={!queryStats}
          title="Query Stats"
        >
          ⚡
        </button>
        <span className="response-toggle-separator" />
        <button onClick={handleDownloadJSON} disabled={!responseText} title="Download JSON">
          <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
            <path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z" />
            <path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708l3 3z" />
          </svg>
          {" JSON"}
        </button>
        <button onClick={handleCopyJSON} disabled={!responseText} title="Copy JSON to clipboard">
          {copiedJson ? (
            <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
              <path d="M13.854 3.646a.5.5 0 0 1 0 .708l-7 7a.5.5 0 0 1-.708 0l-3.5-3.5a.5.5 0 1 1 .708-.708L6.5 10.293l6.646-6.647a.5.5 0 0 1 .708 0z" />
            </svg>
          ) : (
            <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
              <path d="M4 1.5H3a2 2 0 0 0-2 2V14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V3.5a2 2 0 0 0-2-2h-1v1h1a1 1 0 0 1 1 1V14a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V3.5a1 1 0 0 1 1-1h1v-1z" />
              <path d="M9.5 1a.5.5 0 0 1 .5.5v1a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-1a.5.5 0 0 1 .5-.5h3zm-3-1A1.5 1.5 0 0 0 5 1.5v1A1.5 1.5 0 0 0 6.5 4h3A1.5 1.5 0 0 0 11 2.5v-1A1.5 1.5 0 0 0 9.5 0h-3z" />
            </svg>
          )}
        </button>
        <button onClick={handleDownloadCSV} disabled={!hasData} title="Download CSV">
          <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
            <path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z" />
            <path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708l3 3z" />
          </svg>
          {" CSV"}
        </button>
        <button
          onClick={handleDownloadNormalizedCSV}
          disabled={!hasData || !currentTable?.arrayColumns.length}
          title="Normalized CSV — arrays expanded into rows (cross-join if multiple arrays)"
        >
          <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
            <path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z" />
            <path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708l3 3z" />
          </svg>
          {" CSV±"}
        </button>
        <button onClick={handleCopyCSV} disabled={!hasData} title="Copy CSV to clipboard">
          {copiedCsv ? (
            <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
              <path d="M13.854 3.646a.5.5 0 0 1 0 .708l-7 7a.5.5 0 0 1-.708 0l-3.5-3.5a.5.5 0 1 1 .708-.708L6.5 10.293l6.646-6.647a.5.5 0 0 1 .708 0z" />
            </svg>
          ) : (
            <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
              <path d="M4 1.5H3a2 2 0 0 0-2 2V14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V3.5a2 2 0 0 0-2-2h-1v1h1a1 1 0 0 1 1 1V14a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V3.5a1 1 0 0 1 1-1h1v-1z" />
              <path d="M9.5 1a.5.5 0 0 1 .5.5v1a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-1a.5.5 0 0 1 .5-.5h3zm-3-1A1.5 1.5 0 0 0 5 1.5v1A1.5 1.5 0 0 0 6.5 4h3A1.5 1.5 0 0 0 11 2.5v-1A1.5 1.5 0 0 0 9.5 0h-3z" />
            </svg>
          )}
        </button>
      </div>
      {viewMode === "stats" && queryStats && (
        <div className="response-table-overlay">
          <div className="response-table-info">
            Query Stats — {queryStats.total_elapsed_ms} ms total
          </div>
          {queryStats.mermaid && <MermaidDiagram chart={queryStats.mermaid} />}
          <div className="response-table-scroll">
            <table className="response-table">
              <thead>
                <tr>
                  <th>field</th>
                  <th>source</th>
                  <th>strategy</th>
                  <th>ms</th>
                  <th>rows</th>
                  <th>cache</th>
                  <th>sql</th>
                </tr>
              </thead>
              <tbody>
                {queryStats.sources.map((s, i) => (
                  <>
                    <tr
                      key={i}
                      className={s.physical_sql ? "stats-row-clickable" : undefined}
                      onClick={
                        s.physical_sql
                          ? () =>
                              setExpandedStatRows((prev) => {
                                const next = new Set(prev);
                                if (next.has(i)) next.delete(i);
                                else next.add(i);
                                return next;
                              })
                          : undefined
                      }
                    >
                      <td>{s.field}</td>
                      <td>{s.source}</td>
                      <td>{s.strategy}</td>
                      <td className="stats-num">{s.elapsed_ms}</td>
                      <td className="stats-num">{s.rows}</td>
                      <td>{s.cache_hit ? "✓" : ""}</td>
                      <td>{s.physical_sql ? (expandedStatRows.has(i) ? "▲" : "▼") : ""}</td>
                    </tr>
                    {s.physical_sql && expandedStatRows.has(i) && (
                      <tr key={`${i}-sql`} className="stats-sql-row">
                        <td colSpan={7}>
                          <div className="stats-sql-wrap">
                            <pre className="stats-sql">{s.physical_sql}</pre>
                            <button
                              className="stats-sql-copy"
                              onClick={(e) => {
                                e.stopPropagation();
                                navigator.clipboard.writeText(s.physical_sql!);
                              }}
                              title="Copy SQL"
                            >
                              <Copy size={12} />
                            </button>
                          </div>
                        </td>
                      </tr>
                    )}
                  </>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {viewMode === "table" && hasData && (
        <div className="response-table-overlay">
          {tables.length > 1 && (
            <div className="response-table-tabs">
              {tables.map((t, i) => (
                <button
                  key={t.key}
                  className={`response-table-tab${i === activeTab ? " active" : ""}`}
                  onClick={() => {
                    setActiveTab(i);
                    setSortCol(null);
                    setPage(0);
                    setPagedResponseText(null);
                  }}
                >
                  {t.key}
                  <span className="response-table-tab-count">{t.rows.length}</span>
                </button>
              ))}
            </div>
          )}
          <div className="response-table-info">
            {rows.length} row{rows.length !== 1 ? "s" : ""}
            {tables.length === 1 && currentTable ? ` in ${currentTable.key}` : ""}
            {elapsedMs !== null && page === 0 && (
              <span className="response-table-elapsed"> · {Math.round(elapsedMs)} ms</span>
            )}
            {hasData && (
              <span className="response-table-pager">
                <button onClick={handlePrevPage} disabled={page === 0} title="Previous page">‹</button>
                {` p${page + 1} `}
                <button onClick={handleNextPage} disabled={rows.length < PAGE_SIZE} title="Next page">›</button>
              </span>
            )}
          </div>
          <div className="response-table-scroll">
            <table className="response-table">
              <thead>
                <tr>
                  {currentTable?.arrayColumns.length ? <th style={{ width: 24 }} /> : null}
                  {columns.map((col) => (
                    <th
                      key={col}
                      onClick={() => handleSort(col)}
                      className={sortCol === col ? "sorted" : ""}
                    >
                      {col}
                      {sortCol === col && (
                        <span className="sort-arrow">
                          {sortDir === "asc" ? " \u25B2" : " \u25BC"}
                        </span>
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, i) => {
                  const hasArrays = currentTable?.arrayColumns.length
                    ? currentTable.arrayColumns.some((c) => parseArrayLen(row[c]) > 0)
                    : false;
                  const isExpanded = expandedRows.has(i);
                  return (
                    <>
                      <tr key={i}>
                        {currentTable?.arrayColumns.length ? (
                          <td
                            style={{
                              width: 24,
                              cursor: hasArrays ? "pointer" : "default",
                              textAlign: "center",
                              userSelect: "none",
                            }}
                            onClick={() => hasArrays && handleToggleRow(i)}
                          >
                            {hasArrays ? (isExpanded ? "▼" : "▶") : ""}
                          </td>
                        ) : null}
                        {columns.map((col) => {
                          const len = parseArrayLen(row[col]);
                          return (
                            <td key={col}>
                              {len > 0 ? (
                                <span className="array-badge">
                                  [{len} item{len !== 1 ? "s" : ""}]
                                </span>
                              ) : row[col] != null ? (
                                String(row[col])
                              ) : (
                                ""
                              )}
                            </td>
                          );
                        })}
                      </tr>
                      {isExpanded &&
                        currentTable?.arrayColumns.map((col) => {
                          const len = parseArrayLen(row[col]);
                          if (!len) return null;
                          let subItems: Record<string, unknown>[];
                          try {
                            subItems = JSON.parse(row[col] as string) as Record<string, unknown>[];
                          } catch {
                            return null;
                          }
                          const subColSet = new Set<string>();
                          const subRows = subItems.map((item) => {
                            const flat: Record<string, unknown> = {};
                            flattenObject(item, "", flat);
                            Object.keys(flat).forEach((k) => subColSet.add(k));
                            return flat;
                          });
                          const subCols = Array.from(subColSet);
                          return (
                            <tr key={`${i}-${col}`}>
                              <td />
                              <td colSpan={columns.length} style={{ padding: "4px 8px 8px" }}>
                                <div className="sub-table-label">{col}</div>
                                <table className="response-table sub-table">
                                  <thead>
                                    <tr>
                                      {subCols.map((c) => (
                                        <th key={c}>{c}</th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {subRows.map((sr, si) => (
                                      <tr key={si}>
                                        {subCols.map((c) => (
                                          <td key={c}>{sr[c] != null ? String(sr[c]) : ""}</td>
                                        ))}
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </td>
                            </tr>
                          );
                        })}
                    </>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </>,
    /* eslint-disable-next-line react-hooks/refs --
       passing the imperatively-created portal container ref to createPortal (standard portal pattern); render gated by portalReady above */
    portalRef.current,
  );
}
