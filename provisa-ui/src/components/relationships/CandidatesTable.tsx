// Copyright (c) 2026 Kenneth Stott
// Canary: c8be322e-5e5c-4922-8e69-c46fb2d330bf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import type { Candidate } from "./relationship-types";

interface CandidatesTableProps {
  candidates: Candidate[];
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  onAccept: (id: number, name: string) => void;
  onReject: (id: number) => void;
}

export function CandidatesTable({
  candidates,
  tableDomainById,
  tableNameById,
  onAccept,
  onReject,
}: CandidatesTableProps) {
  return (
    <>
      <h3 style={{ marginTop: "2rem" }}>AI-Suggested Relationships</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Source</th>
            <th>Target</th>
            <th>Cardinality</th>
            <th>Confidence</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {candidates.map((c) => {
            const srcDomain = tableDomainById[c.source_table_id];
            const tgtDomain = tableDomainById[c.target_table_id];
            const srcTable = tableNameById[c.source_table_id] ?? String(c.source_table_id);
            const tgtTable = tableNameById[c.target_table_id] ?? String(c.target_table_id);
            const srcLabel = srcDomain
              ? `${srcDomain}.${srcTable}.${c.source_column}`
              : `${srcTable}.${c.source_column}`;
            const tgtLabel = tgtDomain
              ? `${tgtDomain}.${tgtTable}.${c.target_column}`
              : `${tgtTable}.${c.target_column}`;
            const suggestedName =
              c.suggested_name || `${srcTable}-${c.source_column}-to-${tgtTable}`;
            return (
              <React.Fragment key={c.id}>
                <tr>
                  <td>
                    <code>{suggestedName}</code>
                  </td>
                  <td>{srcLabel}</td>
                  <td>{tgtLabel}</td>
                  <td>{c.cardinality}</td>
                  <td>{(c.confidence * 100).toFixed(0)}%</td>
                  <td>
                    <div style={{ display: "flex", gap: "0.5rem" }}>
                      <button className="btn-primary" onClick={() => onAccept(c.id, suggestedName)}>
                        Accept
                      </button>
                      <button className="btn-danger" onClick={() => onReject(c.id)}>
                        Reject
                      </button>
                    </div>
                  </td>
                </tr>
                <tr>
                  <td
                    colSpan={8}
                    style={{
                      padding: "0.25rem 1rem 0.75rem",
                      color: "var(--text-muted)",
                      fontSize: "0.85rem",
                      fontStyle: "italic",
                      borderTop: "none",
                    }}
                  >
                    {c.reasoning}
                  </td>
                </tr>
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
    </>
  );
}
