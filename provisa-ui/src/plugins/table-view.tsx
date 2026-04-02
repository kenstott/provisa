/**
 * GraphiQL Response Table View
 *
 * Overlays the response panel with a sortable table.
 * Toggled via a button in the response toolbar area.
 */

import { useState, useMemo, useCallback, useEffect } from "react";
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
    return { columns: Array.from(columnSet), rows: allRows };
  } catch {
    return { columns: [], rows: [] };
  }
}

export function ResponseTableOverlay() {
  const [showTable, setShowTable] = useState(false);
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const editorContext = useEditorContext({ nonNull: true });

  const responseText = editorContext.responseEditor?.getValue() ?? "";

  // Reset table view when response changes
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

  return (
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
    </>
  );
}
