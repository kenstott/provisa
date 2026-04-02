/**
 * GraphiQL plugin: Table View
 *
 * Renders query results as a sortable table instead of raw JSON.
 * Reads the response from GraphiQL's editor state.
 */

import { useState, useMemo } from "react";
import { useEditorContext } from "@graphiql/react";
import type { GraphiQLPlugin } from "graphiql";

function flattenRows(data: unknown): { columns: string[]; rows: Record<string, unknown>[] } {
  if (!data || typeof data !== "object") return { columns: [], rows: [] };

  const obj = data as Record<string, unknown>;
  const rootKey = Object.keys(obj).find((k) => !k.startsWith("__"));
  if (!rootKey) return { columns: [], rows: [] };

  const rootVal = obj[rootKey];
  if (!Array.isArray(rootVal)) {
    // Single object — wrap in array
    if (rootVal && typeof rootVal === "object") {
      return flattenSingle(rootVal as Record<string, unknown>);
    }
    return { columns: [], rows: [] };
  }

  if (rootVal.length === 0) return { columns: [], rows: [] };

  // Flatten nested objects (relationships) into dot-notation columns
  const allRows: Record<string, unknown>[] = [];
  const columnSet = new Set<string>();

  for (const row of rootVal) {
    const flat: Record<string, unknown> = {};
    flattenObject(row as Record<string, unknown>, "", flat);
    for (const key of Object.keys(flat)) columnSet.add(key);
    allRows.push(flat);
  }

  return { columns: Array.from(columnSet), rows: allRows };
}

function flattenSingle(obj: Record<string, unknown>): { columns: string[]; rows: Record<string, unknown>[] } {
  const flat: Record<string, unknown> = {};
  flattenObject(obj, "", flat);
  return { columns: Object.keys(flat), rows: [flat] };
}

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

function TableViewContent() {
  const editorContext = useEditorContext({ nonNull: true });
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const responseText = editorContext.responseEditor?.getValue() ?? "";

  const { columns, rows } = useMemo(() => {
    try {
      const parsed = JSON.parse(responseText);
      if (parsed?.data) return flattenRows(parsed.data);
    } catch {}
    return { columns: [], rows: [] };
  }, [responseText]);

  const sortedRows = useMemo(() => {
    if (!sortCol) return rows;
    return [...rows].sort((a, b) => {
      const av = a[sortCol];
      const bv = b[sortCol];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number") {
        return sortDir === "asc" ? av - bv : bv - av;
      }
      const sa = String(av);
      const sb = String(bv);
      return sortDir === "asc" ? sa.localeCompare(sb) : sb.localeCompare(sa);
    });
  }, [rows, sortCol, sortDir]);

  const handleSort = (col: string) => {
    if (sortCol === col) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortCol(col);
      setSortDir("asc");
    }
  };

  if (columns.length === 0) {
    return (
      <div className="table-view-empty">
        Run a query to see results as a table.
      </div>
    );
  }

  return (
    <div className="table-view">
      <div className="table-view-info">
        {rows.length} row{rows.length !== 1 ? "s" : ""}
      </div>
      <div className="table-view-scroll">
        <table className="table-view-table">
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
  );
}

export const tableViewPlugin: GraphiQLPlugin = {
  title: "Table",
  icon: () => (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
      <rect x="3" y="3" width="18" height="18" rx="2" />
      <line x1="3" y1="9" x2="21" y2="9" />
      <line x1="3" y1="15" x2="21" y2="15" />
      <line x1="9" y1="3" x2="9" y2="21" />
      <line x1="15" y1="3" x2="15" y2="21" />
    </svg>
  ),
  content: () => <TableViewContent />,
};
