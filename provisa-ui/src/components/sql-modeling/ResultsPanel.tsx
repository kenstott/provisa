// Copyright (c) 2026 Kenneth Stott
// Canary: e2f4e259-305e-44c6-8b2a-d8b8c5facaac
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useMemo } from "react";
import { PAGE_SIZE, COL_MAX, COL_MIN, CHAR_PX } from "./types";

interface ResultsPanelProps {
  resultError: string;
  resultRows: Record<string, unknown>[];
  resultColumns: string[];
  displayRows: Record<string, unknown>[];
  page: number;
  setPage: React.Dispatch<React.SetStateAction<number>>;
  sorts: { col: string; dir: "asc" | "desc" }[];
  colWidths: Record<string, number>;
  filters: Record<string, string>;
  setFilters: React.Dispatch<React.SetStateAction<Record<string, string>>>;
  handleSort: (col: string) => void;
  handleResizeStart: (col: string, e: React.MouseEvent) => void;
  handleDownloadCsv: () => void;
  sqlText: string;
}

export function ResultsPanel({
  resultError,
  resultRows,
  resultColumns,
  displayRows,
  page,
  setPage,
  sorts,
  colWidths,
  filters,
  setFilters,
  handleSort,
  handleResizeStart,
  handleDownloadCsv,
  sqlText,
}: ResultsPanelProps) {
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

  const totalPages = Math.max(1, Math.ceil(displayRows.length / PAGE_SIZE));
  const pagedRows = displayRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  if (resultError) {
    return (
      <pre
        style={{
          margin: "0.75rem",
          fontSize: "0.8rem",
          color: "var(--destructive)",
          whiteSpace: "pre-wrap",
          fontFamily: "monospace",
        }}
      >
        {resultError}
      </pre>
    );
  }

  if (resultRows.length === 0) {
    return (
      <div
        style={{
          padding: "1.5rem",
          textAlign: "center",
          color: "var(--text-muted)",
          fontSize: "0.85rem",
        }}
      >
        {sqlText.trim() ? "No results." : "Write SQL and click Sample to execute."}
      </div>
    );
  }

  const displayCols =
    resultColumns.length > 0
      ? resultColumns
      : resultRows[0] != null
        ? Object.keys(resultRows[0] as object)
        : [];

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      {/* Download + pagination bar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
          padding: "0.25rem 0.75rem",
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
          background: "var(--surface)",
          fontSize: "0.72rem",
          color: "var(--text-muted)",
        }}
      >
        <button
          onClick={handleDownloadCsv}
          style={{
            fontSize: "0.72rem",
            padding: "0.15rem 0.45rem",
            background: "none",
            border: "1px solid var(--border)",
            borderRadius: "3px",
            color: "var(--text-muted)",
            cursor: "pointer",
          }}
        >
          ↓ CSV
        </button>
        <span>
          {displayRows.length} row{displayRows.length !== 1 ? "s" : ""}
          {displayRows.length < resultRows.length
            ? ` (filtered from ${resultRows.length})`
            : ""}
        </span>
        <div style={{ flex: 1 }} />
        {totalPages > 1 && (
          <>
            <button
              onClick={() => setPage(0)}
              disabled={page === 0}
              style={{
                background: "none",
                border: "none",
                cursor: "pointer",
                color: page === 0 ? "var(--text-muted)" : "var(--text)",
                fontSize: "0.75rem",
              }}
            >
              «
            </button>
            <button
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              style={{
                background: "none",
                border: "none",
                cursor: "pointer",
                color: page === 0 ? "var(--text-muted)" : "var(--text)",
                fontSize: "0.75rem",
              }}
            >
              ‹
            </button>
            <span>
              Page {page + 1} / {totalPages}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              style={{
                background: "none",
                border: "none",
                cursor: "pointer",
                color: page >= totalPages - 1 ? "var(--text-muted)" : "var(--text)",
                fontSize: "0.75rem",
              }}
            >
              ›
            </button>
            <button
              onClick={() => setPage(totalPages - 1)}
              disabled={page >= totalPages - 1}
              style={{
                background: "none",
                border: "none",
                cursor: "pointer",
                color: page >= totalPages - 1 ? "var(--text-muted)" : "var(--text)",
                fontSize: "0.75rem",
              }}
            >
              »
            </button>
          </>
        )}
      </div>
      <div style={{ flex: 1, overflow: "auto" }}>
        <table
          className="data-table sql-results-table"
          style={{
            fontSize: "0.78rem",
            tableLayout: "fixed",
            width: "max-content",
            minWidth: "100%",
          }}
        >
          <thead>
            <tr>
              {displayCols.map((c) => {
                const sortIdx = sorts.findIndex((s) => s.col === c);
                const sortEntry = sortIdx !== -1 ? sorts[sortIdx] : null;
                return (
                  <th
                    key={c}
                    className={sortEntry ? "col-sorted" : undefined}
                    style={{
                      width: colWidths[c] ?? autoWidths[c] ?? 140,
                      minWidth: COL_MIN,
                      position: "relative",
                    }}
                  >
                    <div className="th-label" onClick={() => handleSort(c)}>
                      <span
                        style={{
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          flex: 1,
                        }}
                      >
                        {c}
                      </span>
                      {sortEntry ? (
                        <span
                          style={{
                            display: "flex",
                            alignItems: "center",
                            gap: "0.1rem",
                            flexShrink: 0,
                            fontSize: "0.62rem",
                            color: "var(--primary)",
                          }}
                        >
                          {sorts.length > 1 && (
                            <span style={{ opacity: 0.7 }}>{sortIdx + 1}</span>
                          )}
                          <span>{sortEntry.dir === "asc" ? "▲" : "▼"}</span>
                        </span>
                      ) : (
                        <span
                          style={{
                            fontSize: "0.6rem",
                            color: "var(--text-muted)",
                            opacity: 0.3,
                          }}
                        >
                          ⇅
                        </span>
                      )}
                    </div>
                    <input
                      className="th-filter"
                      value={filters[c] ?? ""}
                      onChange={(e) => {
                        setFilters((prev) => ({
                          ...prev,
                          [c]: e.target.value,
                        }));
                        setPage(0);
                      }}
                      onClick={(e) => e.stopPropagation()}
                      placeholder="filter…"
                    />
                    <div
                      onMouseDown={(e) => handleResizeStart(c, e)}
                      style={{
                        position: "absolute",
                        right: 0,
                        top: 0,
                        bottom: 0,
                        width: "5px",
                        cursor: "col-resize",
                      }}
                    />
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {pagedRows.map((row, i) => (
              <tr key={i}>
                {displayCols.map((c) => {
                  const v = row[c];
                  const isNum = typeof v === "number";
                  return (
                    <td
                      key={c}
                      className={isNum ? "col-num" : undefined}
                      style={{
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {v != null ? (
                        String(v)
                      ) : (
                        <span className="null-val">null</span>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
