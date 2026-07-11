// Copyright (c) 2026 Kenneth Stott
// Canary: a2bae5fe-7f63-4cca-be80-6fa7252e362a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import type { ModelingCandidate } from "./types";

interface CandidatesPanelProps {
  candidates: ModelingCandidate[];
  setCandidates: React.Dispatch<React.SetStateAction<ModelingCandidate[]>>;
  tableNameSet: Set<string>;
  onPromote: ((candidate: ModelingCandidate) => Promise<void>) | undefined;
  handlePromote: (idx: number) => void;
}

export function CandidatesPanel({
  candidates,
  setCandidates,
  tableNameSet,
  onPromote,
  handlePromote,
}: CandidatesPanelProps) {
  if (candidates.length === 0) {
    return (
      <div
        style={{
          padding: "1.5rem",
          textAlign: "center",
          color: "var(--text-muted)",
          fontSize: "0.85rem",
        }}
      >
        Click "Extract Joins" to find new relationships. JOIN conditions that already have a
        registered relationship are excluded.
      </div>
    );
  }

  return (
    <table className="data-table" style={{ fontSize: "0.78rem" }}>
      <thead>
        <tr>
          <th>ID</th>
          <th>Source</th>
          <th>Target</th>
          <th>Cardinality</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {candidates.map((c, idx) => (
          <tr key={idx}>
            <td>
              <input
                value={c.id}
                onChange={(e) =>
                  setCandidates((prev) =>
                    prev.map((item, i) =>
                      i === idx ? { ...item, id: e.target.value } : item,
                    ),
                  )
                }
                style={{ width: "100%", fontSize: "0.78rem" }}
              />
            </td>
            <td>
              <span
                style={{
                  color: tableNameSet.has(c.sourceTable)
                    ? "var(--text)"
                    : "var(--destructive)",
                }}
              >
                {c.sourceTable}
              </span>
              <span style={{ color: "var(--text-muted)" }}>.</span>
              {c.sourceCol}
            </td>
            <td>
              <span
                style={{
                  color: tableNameSet.has(c.targetTable)
                    ? "var(--text)"
                    : "var(--destructive)",
                }}
              >
                {c.targetTable}
              </span>
              <span style={{ color: "var(--text-muted)" }}>.</span>
              {c.targetCol}
            </td>
            <td>
              <select
                value={c.cardinality}
                onChange={(e) =>
                  setCandidates((prev) =>
                    prev.map((item, i) =>
                      i === idx ? { ...item, cardinality: e.target.value } : item,
                    ),
                  )
                }
                style={{ fontSize: "0.78rem" }}
              >
                <option value="many-to-one">many-to-one</option>
                <option value="one-to-many">one-to-many</option>
              </select>
            </td>
            <td>
              {c.promoted ? (
                <span style={{ color: "var(--approve)", fontSize: "0.78rem" }}>✓ Promoted</span>
              ) : onPromote ? (
                <button
                  className="btn-primary"
                  style={{ fontSize: "0.72rem", padding: "0.15rem 0.5rem" }}
                  onClick={() => handlePromote(idx)}
                >
                  Promote
                </button>
              ) : null}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
