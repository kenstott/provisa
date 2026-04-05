// Copyright (c) 2025 Kenneth Stott
// Canary: a8e46a16-e119-41fb-96ab-bdbd3e691995
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { fetchCacheStats, purgeCache, purgeCacheByTable, fetchTables } from "../../api/admin";
import type { CacheStats } from "../../api/admin";
import type { RegisteredTable } from "../../types/admin";

export function CacheManager() {
  const [stats, setStats] = useState<CacheStats | null>(null);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [purging, setPurging] = useState(false);
  const [msg, setMsg] = useState("");

  const load = () => {
    Promise.all([fetchCacheStats(), fetchTables()]).then(([s, t]) => {
      setStats(s);
      setTables(t);
    });
  };

  useEffect(load, []);

  const handlePurgeAll = async () => {
    setPurging(true);
    setMsg("");
    const result = await purgeCache();
    setMsg(result.message);
    setPurging(false);
    load();
  };

  const handlePurgeTable = async (tableId: number, tableName: string) => {
    setMsg("");
    const result = await purgeCacheByTable(tableId);
    setMsg(`${tableName}: ${result.message}`);
    load();
  };

  if (!stats) return <p>Loading cache stats...</p>;

  const hitRate = stats.hitCount + stats.missCount > 0
    ? ((stats.hitCount / (stats.hitCount + stats.missCount)) * 100).toFixed(1)
    : "—";

  return (
    <div>
      <div className="stats-grid" style={{ marginBottom: "1rem" }}>
        <div className="stat-card">
          <div className="stat-count">{stats.totalKeys}</div>
          <div className="stat-label">Cached Keys</div>
        </div>
        <div className="stat-card">
          <div className="stat-count">{hitRate}%</div>
          <div className="stat-label">Hit Rate</div>
        </div>
        <div className="stat-card">
          <div className="stat-count">{stats.hitCount}</div>
          <div className="stat-label">Hits</div>
        </div>
        <div className="stat-card">
          <div className="stat-count">{stats.missCount}</div>
          <div className="stat-label">Misses</div>
        </div>
        <div className="stat-card">
          <div className="stat-count">{stats.storeType}</div>
          <div className="stat-label">Store</div>
        </div>
      </div>

      <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginBottom: "1rem" }}>
        <button className="destructive" onClick={handlePurgeAll} disabled={purging}>
          {purging ? "Purging..." : "Purge All Cache"}
        </button>
        {msg && <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>{msg}</span>}
      </div>

      {tables.length > 0 && (
        <table className="data-table">
          <thead>
            <tr><th>Table</th><th>Domain</th><th></th></tr>
          </thead>
          <tbody>
            {tables.map((t) => (
              <tr key={t.id}>
                <td>{t.alias || t.tableName}</td>
                <td>{t.domainId}</td>
                <td>
                  <button
                    onClick={() => handlePurgeTable(t.id, t.tableName)}
                    style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }}
                  >
                    Purge
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
