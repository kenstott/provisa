// Copyright (c) 2026 Kenneth Stott
// Canary: 38295f0d-fd9a-40c3-aba0-27d3a27ce193
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { fetchSystemHealth } from "../../api/admin";
import type { SystemHealth as HealthInfo } from "../../api/admin";

function StatusDot({ ok }: { ok: boolean }) {
  return (
    <span style={{
      display: "inline-block",
      width: 10,
      height: 10,
      borderRadius: "50%",
      background: ok ? "var(--success, #22c55e)" : "var(--error, #ef4444)",
      marginRight: "0.5rem",
    }} />
  );
}

export function SystemHealth() {
  const [health, setHealth] = useState<HealthInfo | null>(null);

  useEffect(() => {
    fetchSystemHealth().then(setHealth);
    const interval = setInterval(() => fetchSystemHealth().then(setHealth), 10000);
    return () => clearInterval(interval);
  }, []);

  if (!health) return <p>Loading system health...</p>;

  return (
    <table className="data-table">
      <thead>
        <tr><th>Component</th><th>Status</th><th>Details</th></tr>
      </thead>
      <tbody>
        <tr>
          <td>Trino</td>
          <td><StatusDot ok={health.trinoConnected} /> {health.trinoConnected ? "Connected" : "Disconnected"}</td>
          <td>{health.trinoConnected ? `${health.trinoWorkerCount} worker${health.trinoWorkerCount !== 1 ? "s" : ""} (${health.trinoActiveWorkers} active)` : ""}</td>
        </tr>
        <tr>
          <td>PostgreSQL Pool</td>
          <td><StatusDot ok={health.pgPoolSize > 0} /> {health.pgPoolSize > 0 ? "Active" : "No pool"}</td>
          <td>{health.pgPoolSize} connections ({health.pgPoolFree} idle)</td>
        </tr>
        <tr>
          <td>Cache (Redis)</td>
          <td><StatusDot ok={health.cacheConnected} /> {health.cacheConnected ? "Connected" : "Not connected"}</td>
          <td></td>
        </tr>
        <tr>
          <td>Arrow Flight Server</td>
          <td><StatusDot ok={health.flightServerRunning} /> {health.flightServerRunning ? "Running" : "Not running"}</td>
          <td></td>
        </tr>
        <tr>
          <td>MV Refresh Loop</td>
          <td><StatusDot ok={health.mvRefreshLoopRunning} /> {health.mvRefreshLoopRunning ? "Running" : "Stopped"}</td>
          <td></td>
        </tr>
      </tbody>
    </table>
  );
}
