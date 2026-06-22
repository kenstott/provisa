// Copyright (c) 2026 Kenneth Stott
// Canary: a8e46a16-e119-41fb-96ab-bdbd3e691995
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import {
  useCacheStats,
  usePurgeCache,
  usePurgeCacheByTable,
  useTables,
} from "../../hooks/useAdminQueries";
import { FilterInput } from "./FilterInput";

const PAGE_SIZE = 50;

export function CacheManager() {
  const { cacheStats: stats, refetch: refetchStats } = useCacheStats();
  const { tables } = useTables();
  const { purgeCache } = usePurgeCache();
  const { purgeCacheByTable } = usePurgeCacheByTable();
  const [purging, setPurging] = useState(false);
  const [msg, setMsg] = useState("");
  const [tableSearch, setTableSearch] = useState("");
  const [tablePage, setTablePage] = useState(0);

  const handlePurgeAll = async () => {
    setPurging(true);
    setMsg("");
    const result = await purgeCache();
    setMsg(result.message);
    setPurging(false);
    await refetchStats();
  };

  const handlePurgeTable = async (tableId: number, tableName: string) => {
    setMsg("");
    const result = await purgeCacheByTable(tableId);
    setMsg(`${tableName}: ${result.message}`);
    await refetchStats();
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

      {tables.length > 0 && (() => {
        const q = tableSearch.toLowerCase();
        const filtered = tables.filter(
          (t) =>
            (t.alias || t.tableName).toLowerCase().includes(q) ||
            (t.domainId ?? "").toLowerCase().includes(q),
        );
        const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
        const safePage = Math.min(tablePage, totalPages - 1);
        const paged = filtered.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);
        return (
          <div>
            <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginBottom: "0.5rem" }}>
              <FilterInput
                value={tableSearch}
                onChange={(v) => { setTableSearch(v); setTablePage(0); }}
                placeholder="Filter by table or domain…"
              />
              {msg && <span style={{ color: "var(--text-muted)", fontSize: "0.85rem", whiteSpace: "nowrap" }}>{msg}</span>}
              <button className="destructive" onClick={handlePurgeAll} disabled={purging} style={{ whiteSpace: "nowrap" }}>
                {purging ? "Purging..." : "Purge All Cache"}
              </button>
            </div>
            <table className="data-table">
              <thead>
                <tr><th>Table</th><th>Domain</th><th></th></tr>
              </thead>
              <tbody>
                {paged.map((t) => (
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
            {totalPages > 1 && (
              <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0" }}>
                <button onClick={() => setTablePage(0)} disabled={safePage === 0}>«</button>
                <button onClick={() => setTablePage(p => p - 1)} disabled={safePage === 0}>‹</button>
                <span>Page {safePage + 1} / {totalPages}</span>
                <button onClick={() => setTablePage(p => p + 1)} disabled={safePage >= totalPages - 1}>›</button>
                <button onClick={() => setTablePage(totalPages - 1)} disabled={safePage >= totalPages - 1}>»</button>
              </div>
            )}
          </div>
        );
      })()}
    </div>
  );
}
