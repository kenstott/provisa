/**
 * GraphiQL Response Table View
 *
 * Overlays the response panel with a sortable table.
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

function parseResponse(text: string): { columns: string[]; rows: Record<string, unknown>[] } {
  try {
    const parsed = JSON.parse(text);
    if (!parsed?.data) return { columns: [], rows: [] };
    const data = parsed.data as Record<string, unknown>;
    const rootKey = Object.keys(data).find((k) => !k.startsWith("__"));
    if (!rootKey) return { columns: [], rows: [] };
    const rootVal = data[rootKey];
    const items = Array.isArray(rootVal) ? rootVal : rootVal ? [rootVal] : [];
    if (items.length === 0) return { columns: [], rows: [] };

    const allRows: Record<string, unknown>[] = [];
    const columnSet = new Set<string>();
    for (const item of items) {
      const flat: Record<string, unknown> = {};
      flattenObject(item as Record<string, unknown>, "", flat);
      for (const key of Object.keys(flat)) columnSet.add(key);
      allRows.push(flat);
    }
    // Remove parent keys that have child keys (e.g. "a.b" when "a.b.c" exists)
    const columns = Array.from(columnSet).filter(
      (col) => !Array.from(columnSet).some((other) => other.startsWith(col + "."))
    );
    return { columns, rows: allRows };
  } catch {
    return { columns: [], rows: [] };
  }
}

export function ResponseTableOverlay() {
  const [showTable, setShowTable] = useState(false);
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [responseText, setResponseText] = useState("");
  const editorContext = useEditorContext({ nonNull: true });

  // Poll for response editor value since GraphiQL doesn't re-render on content change
  useEffect(() => {
    const editor = editorContext.responseEditor;
    if (!editor) return;
    // Set initial value
    setResponseText(editor.getValue() ?? "");
    // Listen for changes via CodeMirror's onDidChangeModelContent (Monaco) or onChange
    const cm = (editor as unknown as { editor?: { on?: (event: string, cb: () => void) => void; off?: (event: string, cb: () => void) => void } }).editor;
    const handler = () => setResponseText(editor.getValue() ?? "");
    if (cm?.on) {
      cm.on("change", handler);
      return () => cm.off?.("change", handler);
    }
    // Fallback: poll for changes
    const interval = setInterval(() => {
      const val = editor.getValue() ?? "";
      setResponseText((prev) => (prev !== val ? val : prev));
    }, 300);
    return () => clearInterval(interval);
  }, [editorContext.responseEditor]);

  // Reset sort when response changes
  useEffect(() => {
    setSortCol(null);
  }, [responseText]);

  const { columns, rows } = useMemo(() => parseResponse(responseText), [responseText]);

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

  const hasData = columns.length > 0;

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

  const handleDownloadCSV = useCallback(() => {
    if (!hasData) return;
    const escape = (v: unknown) => {
      const s = v != null ? String(v) : "";
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const header = columns.map(escape).join(",");
    const body = rows.map((row) => columns.map((col) => escape(row[col])).join(",")).join("\n");
    downloadFile(`${header}\n${body}`, "response.csv", "text/csv");
  }, [columns, rows, hasData, downloadFile]);

  const portalRef = useRef<HTMLElement | null>(null);
  const [portalReady, setPortalReady] = useState(false);

  // Find the response section and inject our container at the top
  useEffect(() => {
    const responseSection = document.querySelector(".graphiql-response") as HTMLElement | null;
    if (!responseSection) return;

    // Create a wrapper that sits at the top of the response section
    let wrapper = responseSection.querySelector(".response-table-wrapper") as HTMLElement | null;
    if (!wrapper) {
      wrapper = document.createElement("div");
      wrapper.className = "response-table-wrapper";
      responseSection.insertBefore(wrapper, responseSection.firstChild);
    }
    portalRef.current = wrapper;
    setPortalReady(true);
  }, []);

  // Toggle a class on the response section and constrain parent height
  useEffect(() => {
    const responseSection = document.querySelector(".graphiql-response") as HTMLElement | null;
    if (!responseSection) return;
    if (showTable && hasData) {
      responseSection.classList.add("response-table-active");
      // Walk up the DOM and find the graphiql-session or editors container
      // and ensure it doesn't overflow beyond the viewport
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
      </div>
      {showTable && hasData && (
        <div className="response-table-overlay">
          <div className="response-table-info">
            {rows.length} row{rows.length !== 1 ? "s" : ""}
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
