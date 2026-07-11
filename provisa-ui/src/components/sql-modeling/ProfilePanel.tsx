// Copyright (c) 2026 Kenneth Stott
// Canary: 605384b5-29e5-47b7-a96f-644456fe1de9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ColumnProfile } from "./types";

interface ProfilePanelProps {
  profile: ColumnProfile[];
  resultRows: Record<string, unknown>[];
  handleDownloadProfile: () => void;
}

export function ProfilePanel({ profile, resultRows, handleDownloadProfile }: ProfilePanelProps) {
  if (profile.length === 0) {
    return (
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
    );
  }

  return (
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
              const nullPct = total > 0 ? Math.round((p.nullCount / total) * 100) : 0;
              const isHighNull = nullPct >= 50;
              const isConstant = p.constantValue !== undefined;
              return (
                <tr key={p.col}>
                  <td style={{ fontFamily: "monospace", fontWeight: 600 }}>{p.col}</td>
                  <td
                    style={{
                      color: isHighNull
                        ? "var(--destructive)"
                        : p.nullCount > 0
                          ? "var(--text)"
                          : "var(--text-muted)",
                    }}
                  >
                    {p.nullCount > 0 ? (
                      `${p.nullCount} (${nullPct}%)`
                    ) : (
                      <span style={{ color: "var(--text-muted)" }}>—</span>
                    )}
                  </td>
                  <td
                    style={{
                      color: p.blankCount > 0 ? "var(--text)" : "var(--text-muted)",
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
                      color: isConstant ? "var(--destructive)" : "var(--text-muted)",
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
                    <div
                      style={{
                        display: "flex",
                        flexWrap: "wrap",
                        gap: "0.2rem",
                      }}
                    >
                      {p.topValues.map(({ value, count }) => (
                        <span
                          key={value}
                          style={{
                            background: "var(--surface)",
                            border: "1px solid var(--border)",
                            borderRadius: "3px",
                            padding: "0 0.3rem",
                            fontSize: "0.7rem",
                            fontFamily: "monospace",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {value.slice(0, 20)}
                          <span style={{ color: "var(--text-muted)" }}>×{count}</span>
                        </span>
                      ))}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
