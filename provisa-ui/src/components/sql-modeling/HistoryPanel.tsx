// Copyright (c) 2026 Kenneth Stott
// Canary: ed102b59-16aa-4d17-912b-875dbfe96bba
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import type { HistoryEntry, ResultTab } from "./types";

interface HistoryPanelProps {
  history: HistoryEntry[];
  setSqlText: React.Dispatch<React.SetStateAction<string>>;
  setRole: React.Dispatch<React.SetStateAction<string>>;
  setResultTab: React.Dispatch<React.SetStateAction<ResultTab>>;
}

export function HistoryPanel({ history, setSqlText, setRole, setResultTab }: HistoryPanelProps) {
  if (history.length === 0) {
    return (
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
    );
  }

  return (
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
                {!isToday && <div style={{ fontSize: "0.68rem" }}>{dateLabel}</div>}
              </td>
              <td style={{ color: "var(--text-muted)", whiteSpace: "nowrap" }}>{h.role}</td>
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
  );
}
