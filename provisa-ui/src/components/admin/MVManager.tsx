// Copyright (c) 2026 Kenneth Stott
// Canary: f84bad38-f12c-4ecf-93da-d96cce939dde
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useMVList, useRefreshMV, useToggleMV } from "../../hooks/useAdminQueries";

const PAGE_SIZE = 50;

export function MVManager() {
  const navigate = useNavigate();
  const { mvList: mvs, loading } = useMVList();
  const { refreshMV } = useRefreshMV();
  const { toggleMV } = useToggleMV();
  const [refreshing, setRefreshing] = useState<string | null>(null);
  const [mvPage, setMvPage] = useState(0);

  const handleRefresh = async (id: string) => {
    setRefreshing(id);
    await refreshMV(id);
    setRefreshing(null);
  };

  const handleToggle = async (id: string, enabled: boolean) => {
    await toggleMV(id, enabled);
  };

  if (loading) return <p>Loading materialized views...</p>;

  if (mvs.length === 0)
    return (
      <div>
        <p>No materialized views configured.</p>
        <button onClick={() => navigate("/sql")}>+ View</button>
      </div>
    );

  const totalPages = Math.max(1, Math.ceil(mvs.length / PAGE_SIZE));
  const paged = mvs.slice(mvPage * PAGE_SIZE, (mvPage + 1) * PAGE_SIZE);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: "0.5rem" }}>
        <button onClick={() => navigate("/sql")}>+ View</button>
      </div>
      <table className="data-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Source Tables</th>
            <th>Target</th>
            <th>Status</th>
            <th>Rows</th>
            <th>Last Refresh</th>
            <th>Interval</th>
            <th>Error</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {paged.map((mv) => (
            <tr key={mv.id}>
              <td>
                <code>{mv.id}</code>
              </td>
              <td>{mv.sourceTables.join(", ")}</td>
              <td>
                <code>{mv.targetTable}</code>
              </td>
              <td>
                <span className={`status-badge status-${mv.status}`}>{mv.status}</span>
              </td>
              <td>{mv.rowCount ?? "—"}</td>
              <td>
                {mv.lastRefreshAt
                  ? new Date(mv.lastRefreshAt * 1000).toLocaleTimeString()
                  : "never"}
              </td>
              <td>{mv.refreshInterval}s</td>
              <td className="reasoning-cell" style={{ color: "var(--error)", maxWidth: 200 }}>
                {mv.lastError || ""}
              </td>
              <td>
                <div style={{ display: "flex", gap: "0.25rem" }}>
                  <button
                    onClick={() => handleRefresh(mv.id)}
                    disabled={refreshing === mv.id}
                    style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }}
                  >
                    {refreshing === mv.id ? "..." : "Refresh"}
                  </button>
                  <button
                    onClick={() => handleToggle(mv.id, !mv.enabled)}
                    style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }}
                  >
                    {mv.enabled ? "Disable" : "Enable"}
                  </button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {totalPages > 1 && (
        <div
          style={{
            display: "flex",
            gap: "0.5rem",
            alignItems: "center",
            justifyContent: "flex-end",
            padding: "0.5rem 0",
          }}
        >
          <button onClick={() => setMvPage(0)} disabled={mvPage === 0}>
            «
          </button>
          <button onClick={() => setMvPage((p) => p - 1)} disabled={mvPage === 0}>
            ‹
          </button>
          <span>
            Page {mvPage + 1} / {totalPages}
          </span>
          <button onClick={() => setMvPage((p) => p + 1)} disabled={mvPage >= totalPages - 1}>
            ›
          </button>
          <button onClick={() => setMvPage(totalPages - 1)} disabled={mvPage >= totalPages - 1}>
            »
          </button>
        </div>
      )}
    </div>
  );
}
