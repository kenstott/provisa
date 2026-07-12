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
import { useNavigate } from "react-router-dom";
import {
  useCacheStats,
  useCacheTableStats,
  useHotTables,
  useMaterializeStoreInfo,
  useMVList,
  usePurgeCache,
  usePurgeCacheByTable,
  useRefreshMV,
  useTables,
  useToggleMV,
} from "../../hooks/useAdminQueries";
import { CacheStorageTab } from "./CacheStorageTab";
import { FilterInput } from "./FilterInput";

const PAGE_SIZE = 50;

function fmtBytes(n: number | null): string {
  if (n == null) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

type TabKey = "response" | "hot" | "materialized" | "setup";

const TABS: { key: TabKey; label: string }[] = [
  { key: "response", label: "Response Cache" },
  { key: "hot", label: "Hot Tables" },
  { key: "materialized", label: "Materialized Store" },
  { key: "setup", label: "Setup" },
];

export function CacheManager() {
  const [tab, setTab] = useState<TabKey>("response");
  return (
    <div>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1rem" }}>
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            style={{
              fontSize: "0.8125rem",
              padding: "0.25rem 0.75rem",
              borderRadius: "4px",
              background: tab === t.key ? "var(--primary)" : "transparent",
              color: tab === t.key ? "#fff" : "var(--text-muted)",
              border: "1px solid var(--border)",
            }}
          >
            {t.label}
          </button>
        ))}
      </div>
      {tab === "response" && <ResponseCacheTab />}
      {tab === "hot" && <HotTablesTab />}
      {tab === "materialized" && <MaterializedStoreTab />}
      {tab === "setup" && <CacheStorageTab />}
    </div>
  );
}

function ResponseCacheTab() {
  const { cacheStats: stats, refetch: refetchStats } = useCacheStats();
  const { cacheTableStats, refetch: refetchTableStats } = useCacheTableStats();
  const { tables } = useTables();
  const entriesByTable = new Map(cacheTableStats.map((s) => [s.tableId, s.cachedEntries]));
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
    await refetchTableStats();
  };

  const handlePurgeTable = async (tableId: number, tableName: string) => {
    setMsg("");
    const result = await purgeCacheByTable(tableId);
    setMsg(`${tableName}: ${result.message}`);
    await refetchStats();
    await refetchTableStats();
  };

  if (!stats) return <p>Loading cache stats...</p>;

  const hitRate = stats.hitCount + stats.missCount > 0
    ? ((stats.hitCount / (stats.hitCount + stats.missCount)) * 100).toFixed(1)
    : "—";

  // Logical cached-result count. stats.totalKeys is a raw Redis DBSIZE (data + :meta
  // per entry + one table-index set per referenced table), so it overcounts entries by
  // a non-constant factor. The per-table index sums to the real entry total.
  const totalEntries = cacheTableStats.reduce((n, s) => n + s.cachedEntries, 0);

  const isRedis = stats.storeType === "redis";
  // "memory" = embedded fakeredis: an enabled store, just without Redis INFO metrics.
  const isEnabled = stats.storeType !== "noop";
  const memUsed = fmtBytes(stats.usedMemoryBytes);
  const memPct = stats.usedMemoryBytes != null && stats.maxMemoryBytes
    ? ` / ${((stats.usedMemoryBytes / stats.maxMemoryBytes) * 100).toFixed(0)}%`
    : "";

  return (
    <div>
      {!isEnabled && (
        <div
          style={{
            marginBottom: "1rem",
            padding: "0.6rem 0.9rem",
            borderRadius: "6px",
            background: "var(--warning-bg, rgba(255,196,0,0.1))",
            color: "var(--text-muted)",
            fontSize: "0.9rem",
          }}
        >
          Query-response cache disabled (store: <strong>{stats.storeType}</strong>). Query results
          are not cached in Redis, so the counters below stay at zero. Hot-table and
          materialized-store caching are separate layers (see the other tabs) and are unaffected.
        </div>
      )}
      <div className="stats-grid" style={{ marginBottom: "1rem" }}>
        <div className="stat-card">
          <div className="stat-count">{totalEntries}</div>
          <div className="stat-label">Cached Entries</div>
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
        {isRedis && (
          <>
            <div className="stat-card">
              <div className="stat-count">{stats.totalKeys}</div>
              <div className="stat-label">Redis Keys (raw)</div>
            </div>
            <div className="stat-card">
              <div className="stat-count">{memUsed}{memPct}</div>
              <div className="stat-label">Memory</div>
            </div>
            <div className="stat-card">
              <div className="stat-count">{stats.evictedKeys ?? "—"}</div>
              <div className="stat-label">Evicted</div>
            </div>
            <div className="stat-card">
              <div className="stat-count">{stats.expiredKeys ?? "—"}</div>
              <div className="stat-label">Expired</div>
            </div>
            <div className="stat-card">
              <div className="stat-count">{stats.connectedClients ?? "—"}</div>
              <div className="stat-label">Clients</div>
            </div>
            <div className="stat-card">
              <div className="stat-count">{stats.opsPerSec ?? "—"}</div>
              <div className="stat-label">Ops/sec</div>
            </div>
          </>
        )}
      </div>

      {tables.length > 0 && (() => {
        const q = tableSearch.toLowerCase();
        // Hide Provisa's own internal catalog (meta/ops system views) — matches TablesPage.
        const userTables = tables.filter(
          (t) => t.sourceId !== "provisa-admin" && t.sourceId !== "provisa-otel",
        );
        const filtered = userTables.filter(
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
                <tr><th>Table</th><th>Domain</th><th>Cached Entries</th><th></th></tr>
              </thead>
              <tbody>
                {paged.map((t) => (
                  <tr key={t.id}>
                    <td>{t.alias || t.tableName}</td>
                    <td>{t.domainId}</td>
                    <td>{entriesByTable.get(t.id) ?? 0}</td>
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

function HotTablesTab() {
  const { hotTables } = useHotTables();
  const loaded = hotTables.filter((h) => h.loaded);
  const totalRows = loaded.reduce((n, h) => n + h.rowCount, 0);
  return (
    <div>
      <p style={{ color: "var(--text-muted)", fontSize: "0.9rem", marginTop: 0 }}>
        Small lookup tables mirrored in Redis (or in-memory) and inlined as VALUES CTEs to optimize
        JOINs. Always available regardless of the response-cache store.
      </p>
      <div className="stats-grid" style={{ marginBottom: "1rem" }}>
        <div className="stat-card">
          <div className="stat-count">{loaded.length}</div>
          <div className="stat-label">Loaded Tables</div>
        </div>
        <div className="stat-card">
          <div className="stat-count">{hotTables.length - loaded.length}</div>
          <div className="stat-label">Candidates</div>
        </div>
        <div className="stat-card">
          <div className="stat-count">{totalRows}</div>
          <div className="stat-label">Cached Rows</div>
        </div>
      </div>
      {hotTables.length === 0 ? (
        <p style={{ color: "var(--text-muted)" }}>No hot tables loaded or registered as candidates.</p>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>Table</th><th>Catalog</th><th>Schema</th><th>Rows</th><th>Kind</th><th>State</th>
            </tr>
          </thead>
          <tbody>
            {hotTables.map((h) => (
              <tr key={`${h.catalog}.${h.schemaName}.${h.tableName}`}>
                <td>{h.tableName}</td>
                <td>{h.catalog}</td>
                <td>{h.schemaName}</td>
                <td>{h.loaded ? h.rowCount : "—"}</td>
                <td>{h.isApi ? "API" : "engine"}</td>
                <td>{h.loaded ? "loaded" : "candidate"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function MaterializedStoreTab() {
  const navigate = useNavigate();
  const { materializeStoreInfo: info } = useMaterializeStoreInfo();
  const { mvList } = useMVList();
  const { refreshMV } = useRefreshMV();
  const { toggleMV } = useToggleMV();
  const [refreshing, setRefreshing] = useState<string | null>(null);
  const [mvPage, setMvPage] = useState(0);

  const handleRefresh = async (id: string) => {
    setRefreshing(id);
    await refreshMV(id);
    setRefreshing(null);
  };

  const totalPages = Math.max(1, Math.ceil(mvList.length / PAGE_SIZE));
  const paged = mvList.slice(mvPage * PAGE_SIZE, (mvPage + 1) * PAGE_SIZE);

  return (
    <div>
      <p style={{ color: "var(--text-muted)", fontSize: "0.9rem", marginTop: 0 }}>
        Durable store where non-attachable sources land and materialized views are written. Distinct
        from the response cache and the hot tier; a query can use any combination of the three.
      </p>
      <div className="stats-grid" style={{ marginBottom: "1rem" }}>
        <div className="stat-card">
          <div className="stat-count">{info?.engineName ?? "—"}</div>
          <div className="stat-label">Federation Engine</div>
        </div>
        <div className="stat-card">
          <div className="stat-count">{info?.mvCount ?? "—"}</div>
          <div className="stat-label">Materialized Views</div>
        </div>
      </div>
      {info?.storeRef && (
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
          Store: <code>{info.storeRef}</code>
        </p>
      )}
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: "0.5rem" }}>
        <button onClick={() => navigate("/sql")}>+ View</button>
      </div>
      {mvList.length === 0 ? (
        <p style={{ color: "var(--text-muted)" }}>No materialized views defined.</p>
      ) : (
        <>
          <table className="data-table">
            <thead>
              <tr>
                <th>View</th>
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
                        onClick={() => toggleMV(mv.id, !mv.enabled)}
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
              <button
                onClick={() => setMvPage(totalPages - 1)}
                disabled={mvPage >= totalPages - 1}
              >
                »
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
