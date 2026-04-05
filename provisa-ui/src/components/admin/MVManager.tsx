// Copyright (c) 2025 Kenneth Stott
// Canary: f84bad38-f12c-4ecf-93da-d96cce939dde
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { fetchMVList, refreshMV, toggleMV } from "../../api/admin";
import type { MVInfo } from "../../api/admin";

export function MVManager() {
  const [mvs, setMvs] = useState<MVInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    fetchMVList().then(setMvs).finally(() => setLoading(false));
  };

  useEffect(load, []);

  const handleRefresh = async (id: string) => {
    setRefreshing(id);
    await refreshMV(id);
    load();
    setRefreshing(null);
  };

  const handleToggle = async (id: string, enabled: boolean) => {
    await toggleMV(id, enabled);
    load();
  };

  if (loading) return <p>Loading materialized views...</p>;
  if (mvs.length === 0) return <p>No materialized views configured.</p>;

  return (
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
        {mvs.map((mv) => (
          <tr key={mv.id}>
            <td><code>{mv.id}</code></td>
            <td>{mv.sourceTables.join(", ")}</td>
            <td><code>{mv.targetTable}</code></td>
            <td>
              <span className={`status-badge status-${mv.status}`}>{mv.status}</span>
            </td>
            <td>{mv.rowCount ?? "—"}</td>
            <td>{mv.lastRefreshAt ? new Date(mv.lastRefreshAt * 1000).toLocaleTimeString() : "never"}</td>
            <td>{mv.refreshInterval}s</td>
            <td className="reasoning-cell" style={{ color: "var(--error)", maxWidth: 200 }}>
              {mv.lastError || ""}
            </td>
            <td style={{ display: "flex", gap: "0.25rem" }}>
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
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
