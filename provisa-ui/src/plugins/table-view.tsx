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
import { createPortal } from "react-dom";
import { useEditorContext } from "@graphiql/react";

function flattenObject(obj: Record<string, unknown>, prefix: string, out: Record<string, unknown>) {
  for (const [key, val] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    if (val !== null && typeof val === "object" && !Array.isArray(val)) {
      flattenObject(val as Record<string, unknown>, fullKey, out);
    } else if (Array.isArray(val)) {
      out[fullKey] = JSON.stringify(val);
    } else {
      out[fullKey] = val;
    }
  }
}

interface ParsedTable {
  key: string;
  columns: string[];
  rows: Record<string, unknown>[];
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
        tables.push({ key: rootKey, columns: [], rows: [] });
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
        (col) => !Array.from(columnSet).some((other) => other.startsWith(col + "."))
      );
      tables.push({ key: rootKey, columns, rows: allRows });
    }
    return tables;
  } catch {
    return [];
  }
}

export function ResponseTableOverlay() {
  const [showTable, setShowTable] = useState(false);
  const [activeTab, setActiveTab] = useState(0);
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [responseText, setResponseText] = useState("");
  const [copiedJson, setCopiedJson] = useState(false);
  const [copiedCsv, setCopiedCsv] = useState(false);
  const editorContext = useEditorContext();

  // Poll for response editor value since GraphiQL doesn't re-render on content change
  useEffect(() => {
    const editor = editorContext.responseEditor;
    if (!editor) return;
    setResponseText(editor.getValue() ?? "");
    const cm = (editor as unknown as { editor?: { on?: (event: string, cb: () => void) => void; off?: (event: string, cb: () => void) => void } }).editor;
    const handler = () => setResponseText(editor.getValue() ?? "");
    if (cm?.on) {
      cm.on("change", handler);
      return () => cm.off?.("change", handler);
    }
    const interval = setInterval(() => {
      const val = editor.getValue() ?? "";
      setResponseText((prev) => (prev !== val ? val : prev));
    }, 300);
    return () => clearInterval(interval);
  }, [editorContext.responseEditor]);

  // Reset sort and tab when response changes
  useEffect(() => {
    setSortCol(null);
    setActiveTab(0);
  }, [responseText]);

  const tables = useMemo(() => parseResponse(responseText), [responseText]);
  const currentTable = tables[activeTab] ?? null;
  const columns = currentTable?.columns ?? [];
  const rows = currentTable?.rows ?? [];

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

  const handleCopyCSV = useCallback(() => {
    if (!currentTable || currentTable.columns.length === 0) return;
    const escape = (v: unknown) => {
      const s = v != null ? String(v) : "";
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const header = currentTable.columns.map(escape).join(",");
    const body = currentTable.rows.map((row) => currentTable.columns.map((col) => escape(row[col])).join(",")).join("\n");
    navigator.clipboard.writeText(`${header}\n${body}`).then(() => {
      setCopiedCsv(true);
      setTimeout(() => setCopiedCsv(false), 2000);
    });
  }, [currentTable]);

  const handleDownloadCSV = useCallback(() => {
    if (!currentTable || currentTable.columns.length === 0) return;
    const escape = (v: unknown) => {
      const s = v != null ? String(v) : "";
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const header = currentTable.columns.map(escape).join(",");
    const body = currentTable.rows.map((row) => currentTable.columns.map((col) => escape(row[col])).join(",")).join("\n");
    const filename = tables.length > 1 ? `${currentTable.key}.csv` : "response.csv";
    downloadFile(`${header}\n${body}`, filename, "text/csv");
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
    setPortalReady(true);
  }, []);

  useEffect(() => {
    const responseSection = document.querySelector(".graphiql-response") as HTMLElement | null;
    if (!responseSection) return;
    if (showTable && hasData) {
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
  }, [showTable, hasData]);

  if (!portalReady || !portalRef.current) return null;

  return createPortal(
    <>
      <div className="response-view-toggle">
        <button
          className={!showTable ? "active" : ""}
          onClick={() => setShowTable(false)}
          title="JSON"
        >
          {"{ }"}
        </button>
        <button
          className={showTable ? "active" : ""}
          onClick={() => setShowTable(true)}
          disabled={!hasData}
          title="Table"
        >
          <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
            <path d="M0 2a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V2zm15 2h-4v3h4V4zm0 4h-4v3h4V8zm0 4h-4v3h3a1 1 0 0 0 1-1v-2zM10 4H6v3h4V4zm0 4H6v3h4V8zm0 4H6v3h4v-3zM5 4H1v3h4V4zm0 4H1v3h4V8zm0 4H1v2a1 1 0 0 0 1 1h3v-3z" />
          </svg>
        </button>
        <span className="response-toggle-separator" />
        <button
          onClick={handleDownloadJSON}
          disabled={!responseText}
          title="Download JSON"
        >
          <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
            <path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z" />
            <path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708l3 3z" />
          </svg>
          {" JSON"}
        </button>
        <button
          onClick={handleCopyJSON}
          disabled={!responseText}
          title="Copy JSON to clipboard"
        >
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
        <button
          onClick={handleDownloadCSV}
          disabled={!hasData}
          title="Download CSV"
        >
          <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
            <path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z" />
            <path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708l3 3z" />
          </svg>
          {" CSV"}
        </button>
        <button
          onClick={handleCopyCSV}
          disabled={!hasData}
          title="Copy CSV to clipboard"
        >
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
      {showTable && hasData && (
        <div className="response-table-overlay">
          {tables.length > 1 && (
            <div className="response-table-tabs">
              {tables.map((t, i) => (
                <button
                  key={t.key}
                  className={`response-table-tab${i === activeTab ? " active" : ""}`}
                  onClick={() => { setActiveTab(i); setSortCol(null); }}
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
          </div>
          <div className="response-table-scroll">
            <table className="response-table">
              <thead>
                <tr>
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
                {sortedRows.map((row, i) => (
                  <tr key={i}>
                    {columns.map((col) => (
                      <td key={col}>
                        {row[col] != null ? String(row[col]) : ""}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </>,
    portalRef.current
  );
}
