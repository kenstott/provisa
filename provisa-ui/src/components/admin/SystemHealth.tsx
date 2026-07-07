// Copyright (c) 2026 Kenneth Stott
// Canary: 38295f0d-fd9a-40c3-aba0-27d3a27ce193
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect } from "react";
import { useSystemHealth } from "../../hooks/useAdminQueries";

// null = neutral/disabled (grey), true = ok (green), false = down (red)
function StatusDot({ ok }: { ok: boolean | null }) {
  const color = ok === null ? "var(--text-muted, #6b7280)" : ok ? "var(--success, #22c55e)" : "var(--error, #ef4444)";
  return (
    <span style={{
      display: "inline-block",
      width: 10,
      height: 10,
      borderRadius: "50%",
      background: color,
      marginRight: "0.5rem",
    }} />
  );
}

export function SystemHealth() {
  const { systemHealth: health, refetch } = useSystemHealth();

  useEffect(() => {
    const interval = setInterval(() => refetch(), 10000);
    return () => clearInterval(interval);
  }, [refetch]);

  if (!health) return <p>Loading system health...</p>;

  return (
    <table className="data-table">
      <thead>
        <tr><th>Component</th><th>Status</th><th>Details</th></tr>
      </thead>
      <tbody>
        <tr>
          <td>Federation Engine</td>
          <td><StatusDot ok={health.engineConnected} /> {health.engineConnected ? "Connected" : "Disconnected"}</td>
          <td>{health.engineConnected ? `${health.engineWorkerCount} worker${health.engineWorkerCount !== 1 ? "s" : ""} (${health.engineActiveWorkers} active)` : ""}</td>
        </tr>
        <tr>
          <td>PostgreSQL Pool</td>
          <td><StatusDot ok={health.pgPoolSize > 0} /> {health.pgPoolSize > 0 ? "Active" : "No pool"}</td>
          <td>{health.pgPoolSize} connections ({health.pgPoolFree} idle)</td>
        </tr>
        <tr>
          <td>Cache</td>
          <td>
            <StatusDot ok={health.cacheMode === "disabled" ? null : health.cacheConnected} />{" "}
            {health.cacheMode === "disabled"
              ? "Disabled"
              : health.cacheMode === "embedded"
                ? "Embedded (in-memory)"
                : health.cacheConnected
                  ? "Server connected"
                  : "Server unreachable"}
          </td>
          <td>{health.cacheMode === "server" ? "Redis server" : health.cacheMode === "embedded" ? "fakeredis (in-process)" : ""}</td>
        </tr>
        {health.protocols.map((p) => (
          <tr key={p.name}>
            <td>{p.name}</td>
            <td>
              <StatusDot ok={p.status === "disabled" ? null : p.status === "running"} />{" "}
              {p.status === "disabled" ? "Disabled" : p.status === "running" ? "Running" : "Unreachable"}
            </td>
            <td>{p.port != null ? `port ${p.port}` : ""}</td>
          </tr>
        ))}
        <tr>
          <td>MV Refresh Loop</td>
          <td><StatusDot ok={health.mvRefreshLoopRunning} /> {health.mvRefreshLoopRunning ? "Running" : "Stopped"}</td>
          <td></td>
        </tr>
      </tbody>
    </table>
  );
}
