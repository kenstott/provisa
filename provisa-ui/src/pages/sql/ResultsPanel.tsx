// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { History, BarChart2, Copy, Check } from "lucide-react";
import { COL_MIN, PAGE_SIZE } from "./types";
import type { ResultTab, HistoryEntry, ColumnProfile } from "./types";

interface ResultsPanelProps {
  resultTab: ResultTab;
  setResultTab: React.Dispatch<React.SetStateAction<ResultTab>>;
  running: boolean;
  resultError: string;
  resultRows: Record<string, unknown>[];
  resultColumns: string[];
  sorts: { col: string; dir: "asc" | "desc" }[];
  filters: Record<string, string>;
  setFilters: React.Dispatch<React.SetStateAction<Record<string, string>>>;
  page: number;
  setPage: React.Dispatch<React.SetStateAction<number>>;
  displayRows: Record<string, unknown>[];
  pagedRows: Record<string, unknown>[];
  totalPages: number;
  colWidths: Record<string, number>;
  autoWidths: Record<string, number>;
  copiedResults: boolean;
  profile: ColumnProfile[];
  errors: string[];
  history: HistoryEntry[];
  queryStats: unknown;
  sqlText: string;
  setSqlText: React.Dispatch<React.SetStateAction<string>>;
  setRole: React.Dispatch<React.SetStateAction<string>>;
  handleDownloadCsv: () => void;
  handleCopyResults: () => void;
  handleSort: (col: string) => void;
  handleResizeStart: (col: string, e: React.MouseEvent) => void;
  handleDownloadProfile: () => void;
}

export function ResultsPanel({
  resultTab,
  setResultTab,
  running,
  resultError,
  resultRows,
  resultColumns,
  sorts,
  filters,
  setFilters,
  page,
  setPage,
  displayRows,
  pagedRows,
  totalPages,
  colWidths,
  autoWidths,
  copiedResults,
  profile,
  errors,
  history,
  queryStats,
  sqlText,
  setSqlText,
  setRole,
  handleDownloadCsv,
  handleCopyResults,
  handleSort,
  handleResizeStart,
  handleDownloadProfile,
}: ResultsPanelProps) {
  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 0,
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
          background: "var(--surface)",
        }}
      >
        {(["results", "profile", "errors", "history", "stats"] as ResultTab[]).map((tab) => {
          const count =
            tab === "results"
              ? resultRows.length
              : tab === "profile"
                ? profile.length
                : tab === "errors"
                  ? errors.length
                  : tab === "stats"
                    ? 0
                    : history.length;
          const active = resultTab === tab;
          if (tab === "stats" && !queryStats) return null;
          return (
            <button
              key={tab}
              onClick={() => setResultTab(tab)}
              style={{
                padding: "0.35rem 0.8rem",
                fontSize: "0.75rem",
                background: "none",
                border: "none",
                borderBottom: active
                  ? "2px solid var(--primary)"
                  : "2px solid transparent",
                color: active ? "var(--text)" : "var(--text-muted)",
                cursor: "pointer",
                textTransform: "capitalize",
                display: "flex",
                alignItems: "center",
                gap: "0.3rem",
              }}
            >
              {tab === "history" ? (
                <History size={11} />
              ) : tab === "profile" ? (
                <BarChart2 size={11} />
              ) : null}
              {tab}
              {count > 0 && (
                <span
                  style={{
                    background:
                      tab === "errors" ? "var(--destructive)" : "var(--primary)",
                    color: "#fff",
                    borderRadius: "8px",
                    fontSize: "0.65rem",
                    padding: "0 0.35rem",
                    lineHeight: "1.4",
                  }}
                >
                  {count}
                </span>
              )}
            </button>
          );
        })}
      </div>

      <div style={{ flex: 1, overflow: "auto" }}>
        {resultTab === "results" &&
          (running ? (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                padding: "2rem",
                gap: "0.5rem",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              <span className="btn-spinner" style={{ flexShrink: 0 }} />
              Running…
            </div>
          ) : resultError ? (
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
          ) : resultRows.length === 0 ? (
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
          ) : (
            (() => {
              const displayCols =
                resultColumns.length > 0
                  ? resultColumns
                  : resultRows[0] != null
                    ? Object.keys(resultRows[0] as object)
                    : [];
              return (
                <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
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
                    <button
                      onClick={handleCopyResults}
                      title="Copy results as TSV"
                      style={{
                        fontSize: "0.72rem",
                        padding: "0.15rem 0.45rem",
                        background: "none",
                        border: "1px solid var(--border)",
                        borderRadius: "3px",
                        color: "var(--text-muted)",
                        cursor: "pointer",
                        display: "flex",
                        alignItems: "center",
                        gap: "0.25rem",
                      }}
                    >
                      {copiedResults ? (
                        <Check size={11} style={{ color: "var(--approve)" }} />
                      ) : (
                        <Copy size={11} />
                      )}
                      {copiedResults ? "Copied" : "Copy"}
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
                            color:
                              page >= totalPages - 1
                                ? "var(--text-muted)"
                                : "var(--text)",
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
                            color:
                              page >= totalPages - 1
                                ? "var(--text-muted)"
                                : "var(--text)",
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
                                        <span style={{ opacity: 0.7 }}>
                                          {sortIdx + 1}
                                        </span>
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
            })()
          ))}

        {resultTab === "profile" &&
          (profile.length === 0 ? (
            <div
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              Sample a query to profile the result columns.
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  padding: "0.25rem 0.75rem",
                  borderBottom: "1px solid var(--border)",
                  flexShrink: 0,
                  background: "var(--surface)",
                }}
              >
                <button
                  onClick={handleDownloadProfile}
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
                  ↓ JSON
                </button>
              </div>
              <div style={{ flex: 1, overflow: "auto" }}>
                <table className="data-table" style={{ fontSize: "0.75rem" }}>
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th title="Rows where value is NULL">Nulls</th>
                      <th title="Rows where value is empty string">Blanks</th>
                      <th title="Number of unique values">Distinct</th>
                      <th title="Column has only one unique value">Constant?</th>
                      <th>Min</th>
                      <th>Max</th>
                      <th title="Mean of numeric values">Mean</th>
                      <th>Top values</th>
                    </tr>
                  </thead>
                  <tbody>
                    {profile.map((p) => {
                      const total = resultRows.length;
                      const nullPct =
                        total > 0 ? Math.round((p.nullCount / total) * 100) : 0;
                      const isHighNull = nullPct >= 50;
                      const isConstant = p.constantValue !== undefined;
                      return (
                        <tr key={p.col}>
                          <td style={{ fontFamily: "monospace", fontWeight: 600 }}>
                            {p.col}
                          </td>
                          <td>
                            <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
                              <div
                                style={{
                                  width: 52,
                                  height: 5,
                                  borderRadius: 3,
                                  background: "var(--border)",
                                  position: "relative",
                                  flexShrink: 0,
                                }}
                              >
                                {p.nullCount > 0 && (
                                  <div
                                    style={{
                                      position: "absolute",
                                      left: 0,
                                      top: 0,
                                      bottom: 0,
                                      width: `${nullPct}%`,
                                      borderRadius: 3,
                                      background: isHighNull
                                        ? "var(--destructive)"
                                        : "var(--text-muted)",
                                    }}
                                  />
                                )}
                              </div>
                              <span
                                style={{
                                  color: isHighNull
                                    ? "var(--destructive)"
                                    : p.nullCount > 0
                                      ? "var(--text)"
                                      : "var(--text-muted)",
                                  fontSize: "0.7rem",
                                }}
                              >
                                {p.nullCount > 0 ? `${nullPct}%` : "—"}
                              </span>
                            </div>
                          </td>
                          <td
                            style={{
                              color:
                                p.blankCount > 0 ? "var(--text)" : "var(--text-muted)",
                            }}
                          >
                            {p.blankCount > 0 ? (
                              p.blankCount
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>—</span>
                            )}
                          </td>
                          <td
                            style={{
                              color: isConstant ? "var(--text-muted)" : "var(--text)",
                            }}
                          >
                            {p.distinctCount}
                          </td>
                          <td
                            style={{
                              color: isConstant
                                ? "var(--destructive)"
                                : "var(--text-muted)",
                            }}
                          >
                            {isConstant ? (
                              <span title={String(p.constantValue)}>
                                Yes ({String(p.constantValue).slice(0, 12)})
                              </span>
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>—</span>
                            )}
                          </td>
                          <td style={{ fontFamily: "monospace" }}>
                            {p.min !== null ? (
                              String(p.min).slice(0, 16)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>—</span>
                            )}
                          </td>
                          <td style={{ fontFamily: "monospace" }}>
                            {p.max !== null ? (
                              String(p.max).slice(0, 16)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>—</span>
                            )}
                          </td>
                          <td style={{ fontFamily: "monospace" }}>
                            {p.mean !== null ? (
                              p.mean.toFixed(2)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>—</span>
                            )}
                          </td>
                          <td>
                            <div style={{ display: "flex", flexDirection: "column", gap: "0.18rem", minWidth: 140 }}>
                              {p.topValues.map(({ value, count }) => {
                                const barPct = p.topValues[0].count > 0
                                  ? (count / p.topValues[0].count) * 100
                                  : 0;
                                return (
                                  <div key={value} style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
                                    <div
                                      style={{
                                        width: 52,
                                        height: 5,
                                        borderRadius: 2,
                                        background: "var(--border)",
                                        position: "relative",
                                        flexShrink: 0,
                                      }}
                                    >
                                      <div
                                        style={{
                                          position: "absolute",
                                          left: 0,
                                          top: 0,
                                          bottom: 0,
                                          width: `${barPct}%`,
                                          borderRadius: 2,
                                          background: "var(--primary)",
                                        }}
                                      />
                                    </div>
                                    <span
                                      style={{
                                        fontFamily: "monospace",
                                        fontSize: "0.68rem",
                                        whiteSpace: "nowrap",
                                        overflow: "hidden",
                                        maxWidth: 110,
                                        textOverflow: "ellipsis",
                                      }}
                                      title={value}
                                    >
                                      {value.slice(0, 22)}
                                    </span>
                                    <span style={{ color: "var(--text-muted)", fontSize: "0.65rem", marginLeft: "auto", flexShrink: 0 }}>
                                      ×{count}
                                    </span>
                                  </div>
                                );
                              })}
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          ))}

        {resultTab === "errors" &&
          (errors.length === 0 ? (
            <div
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              No unsupported conditions.
            </div>
          ) : (
            <div style={{ padding: "0.75rem" }}>
              <p
                style={{
                  color: "var(--destructive)",
                  fontSize: "0.8rem",
                  fontWeight: 600,
                  marginBottom: "0.5rem",
                }}
              >
                Unsupported ON conditions — simplify using a view:
              </p>
              <ul
                style={{
                  margin: 0,
                  paddingLeft: "1.25rem",
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.3rem",
                }}
              >
                {errors.map((e, i) => (
                  <li
                    key={i}
                    style={{
                      fontSize: "0.8rem",
                      color: "var(--destructive)",
                      fontFamily: "monospace",
                    }}
                  >
                    {e}
                  </li>
                ))}
              </ul>
            </div>
          ))}

        {resultTab === "history" &&
          (history.length === 0 ? (
            <div
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              No queries run yet. History persists across sessions.
            </div>
          ) : (
            <table className="data-table" style={{ fontSize: "0.75rem" }}>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Role</th>
                  <th>Duration</th>
                  <th>Rows</th>
                  <th style={{ width: "50%" }}>SQL</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {history.map((h, i) => {
                  const ts = new Date(h.executedAt);
                  const timeLabel = ts.toLocaleTimeString([], {
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                  });
                  const dateLabel = ts.toLocaleDateString([], {
                    month: "short",
                    day: "numeric",
                  });
                  const isToday = ts.toDateString() === new Date().toDateString();
                  return (
                    <tr key={i} style={{ verticalAlign: "top" }}>
                      <td style={{ whiteSpace: "nowrap", color: "var(--text-muted)" }}>
                        <div>{timeLabel}</div>
                        {!isToday && (
                          <div style={{ fontSize: "0.68rem" }}>{dateLabel}</div>
                        )}
                      </td>
                      <td style={{ color: "var(--text-muted)", whiteSpace: "nowrap" }}>
                        {h.role}
                      </td>
                      <td
                        style={{
                          whiteSpace: "nowrap",
                          color: h.error ? "var(--destructive)" : "var(--text-muted)",
                        }}
                      >
                        {h.durationMs}ms
                      </td>
                      <td
                        style={{
                          whiteSpace: "nowrap",
                          color: h.error ? "var(--destructive)" : "var(--text)",
                        }}
                      >
                        {h.error ? <span title={h.error}>error</span> : h.rowCount}
                      </td>
                      <td>
                        <pre
                          style={{
                            margin: 0,
                            fontSize: "0.72rem",
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-all",
                            color: "var(--text)",
                            maxHeight: "4.5em",
                            overflow: "hidden",
                          }}
                        >
                          {h.sql}
                        </pre>
                      </td>
                      <td style={{ whiteSpace: "nowrap" }}>
                        <button
                          className="btn-secondary"
                          style={{ fontSize: "0.7rem", padding: "0.15rem 0.45rem" }}
                          onClick={() => {
                            setSqlText(h.sql);
                            setRole(h.role);
                            setResultTab("results");
                          }}
                        >
                          Restore
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ))}
        {resultTab === "stats" &&
          (() => {
            type StatsSource = {
              field: string;
              source: string;
              strategy: string;
              elapsed_ms: number;
              rows: number;
              cache_hit?: boolean;
              physical_sql?: string;
            };
            const stats = queryStats as {
              total_elapsed_ms?: number;
              sources?: StatsSource[];
            } | null;
            if (!stats) return null;
            return (
              <div style={{ padding: "0.75rem 1rem", fontSize: "0.8rem" }}>
                <div style={{ marginBottom: "0.5rem", color: "var(--text-muted)" }}>
                  Total:{" "}
                  <strong style={{ color: "var(--text)" }}>
                    {stats.total_elapsed_ms} ms
                  </strong>
                </div>
                {(stats.sources ?? []).map((s, i) => (
                  <div
                    key={i}
                    style={{
                      marginBottom: "0.75rem",
                      borderLeft: "2px solid var(--primary)",
                      paddingLeft: "0.75rem",
                    }}
                  >
                    <div
                      style={{
                        display: "flex",
                        gap: "1rem",
                        flexWrap: "wrap",
                        marginBottom: "0.25rem",
                      }}
                    >
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>field:</span>{" "}
                        {s.field}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>source:</span>{" "}
                        {s.source}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>strategy:</span>{" "}
                        {s.strategy}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>elapsed:</span>{" "}
                        {s.elapsed_ms} ms
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>rows:</span> {s.rows}
                      </span>
                      {s.cache_hit && (
                        <span style={{ color: "#4ade80" }}>cache hit</span>
                      )}
                    </div>
                    {s.physical_sql && (
                      <pre
                        style={{
                          margin: "0.25rem 0 0",
                          fontSize: "0.72rem",
                          color: "var(--text-muted)",
                          whiteSpace: "pre-wrap",
                          wordBreak: "break-all",
                          maxHeight: "6em",
                          overflow: "auto",
                          background: "var(--surface)",
                          padding: "0.4rem",
                          borderRadius: "4px",
                        }}
                      >
                        {s.physical_sql}
                      </pre>
                    )}
                  </div>
                ))}
              </div>
            );
          })()}
      </div>
    </div>
  );
}

// PAGE_SIZE is re-exported for consumers that need it (unused here but kept for tree-shaking)
export { PAGE_SIZE };
