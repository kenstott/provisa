// Copyright (c) 2026 Kenneth Stott
// Canary: e1499f85-6ff4-44b7-aad6-327499acea72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, Fragment, useCallback } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Network } from "lucide-react";
import { ErdModal } from "../components/erd/ErdModal";
import { fetchSettings, profileTable } from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import {
  useTables,
  useSources,
  useDomains,
  useRoles,
  useAvailableColumnsMetadataLazy,
  useGenerateTableDescription,
  useGenerateColumnDescription,
  useRegisterTable,
  useUpdateTable,
  useDeleteTable,
  useUpdateTableCache,
  useUpdateTablePreferMaterialized,
  useUpdateTableNaming,
  usePurgeCacheByTable,
  useInvalidateFileSource,
  useDeployViewToDb,
  useSuggestTableAlias,
  useAllRelationships,
} from "../hooks/useAdminQueries";
import type { RegisteredTable } from "../types/admin";
import { FilterInput } from "../components/admin/FilterInput";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useAuth } from "../context/AuthContext";
import { NAMING_CONVENTIONS } from "./tables/constants";
import { normalizeDomain } from "./tables/helpers";
import { RegisterTableForm } from "./tables/RegisterTableForm";
import { TableReadView } from "./tables/TableReadView";
import { TableEditForm } from "./tables/TableEditForm";

export function TablesPage({ viewsOnly = false }: { viewsOnly?: boolean } = {}) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { tables, loading: tablesLoading, refetch: refetchTables } = useTables();
  const { sources, refetch: refetchSources } = useSources();
  const { domains, refetch: refetchDomains } = useDomains();
  const { relationships } = useAllRelationships();
  const [showErd, setShowErd] = useState(false);
  const { roles, refetch: refetchRoles } = useRoles();
  const domainHints = domains.map((d) => d.id);
  const getAvailableColumnsMetadata = useAvailableColumnsMetadataLazy();
  const { generateTableDescription } = useGenerateTableDescription();
  const { generateColumnDescription } = useGenerateColumnDescription();
  const { registerTable } = useRegisterTable();
  const { updateTable } = useUpdateTable();
  const { deleteTable } = useDeleteTable();
  const { updateTableCache } = useUpdateTableCache();
  const { updateTablePreferMaterialized } = useUpdateTablePreferMaterialized();
  const { updateTableNaming } = useUpdateTableNaming();
  const { purgeCacheByTable } = usePurgeCacheByTable();
  const { invalidateFileSource } = useInvalidateFileSource();
  const { deployViewToDb } = useDeployViewToDb();
  const { suggestTableAlias } = useSuggestTableAlias();
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tableSearch, setTableSearch] = useState(() => searchParams.get("source") ?? "");
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 50;
  const [groupBy, setGroupBy] = useState<Array<"source" | "domain">>([]);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const toggleGroupBy = (col: "source" | "domain") =>
    setGroupBy((prev) => (prev.includes(col) ? prev.filter((g) => g !== col) : [...prev, col]));
  const [sortCol, setSortCol] = useState<"source" | "domain" | "table" | "cols" | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const { checkedDomains, domainsEnabled } = useDomainFilter();
  const { domainAccess, role: activeRole } = useAuth();

  // Per-table profile state: tableId → profile result or "loading"
  const [tableProfiles, setTableProfiles] = useState<
    Record<
      number,
      { columns: string[]; rows: Record<string, unknown>[]; rowCount: number } | "loading" | string
    >
  >({});

  // Inline edit state for expanded table
  const [editingTable, setEditingTable] = useState<RegisteredTable | null>(null);
  const [editingColumnTypes, setEditingColumnTypes] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [generatingDesc, setGeneratingDesc] = useState(false);
  const [generatingColDesc, setGeneratingColDesc] = useState<string | null>(null);

  // Cache state
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [cacheTtlEdits, setCacheTtlEdits] = useState<
    Record<number, { value: string; dirty: boolean; saving: boolean }>
  >({});
  const [purging, setPurging] = useState<Record<number, boolean>>({});
  const [invalidating, setInvalidating] = useState<Record<number, boolean>>({});
  const [deploying, setDeploying] = useState<Record<number, boolean>>({});
  const [deployMsg, setDeployMsg] = useState<Record<number, { success: boolean; message: string }>>(
    {},
  );

  const reload = useCallback(() => {
    setLoading(true);
    // tables/sources/domains/roles come from useQuery subscribers; refetch revalidates
    // their caches. Settings is still an imperative REST read (fetch(), not GraphQL).
    refetchTables();
    refetchSources();
    refetchDomains();
    refetchRoles();
    fetchSettings()
      .then((st) => setSettings(st))
      .finally(() => setLoading(false));
  }, [refetchTables, refetchSources, refetchDomains, refetchRoles]);

  // Seed/refresh per-table TTL edits whenever the tables list changes. Preserve any
  // in-progress dirty edit so a background cache-and-network refetch can't clobber it.
  useEffect(() => {
    setCacheTtlEdits((prev) => {
      const next: Record<number, { value: string; dirty: boolean; saving: boolean }> = {};
      for (const tbl of tables) {
        const existing = prev[tbl.id];
        next[tbl.id] =
          existing && existing.dirty
            ? existing
            : {
                value: tbl.cacheTtl != null ? String(tbl.cacheTtl) : "",
                dirty: false,
                saving: false,
              };
      }
      return next;
    });
  }, [tables]);

  const getEffectiveTableTtl = (t: RegisteredTable): string => {
    if (t.cacheTtl != null) return `${t.cacheTtl}s (custom)`;
    const source = sources.find((s) => s.id === t.sourceId);
    if (source?.cacheTtl != null) return `${source.cacheTtl}s (from source)`;
    if (settings) return `${settings.cache.default_ttl}s (global)`;
    return "default";
  };

  const handleSaveTableCache = async (tableId: number) => {
    const edit = cacheTtlEdits[tableId];
    setCacheTtlEdits((prev) => ({ ...prev, [tableId]: { ...prev[tableId], saving: true } }));
    setError(null);
    try {
      const ttlValue = edit.value.trim() === "" ? null : parseInt(edit.value, 10);
      if (ttlValue !== null && isNaN(ttlValue)) throw new Error("TTL must be a number");
      const result = await updateTableCache(tableId, ttlValue);
      if (!result.success) throw new Error(result.message);
      setCacheTtlEdits((prev) => ({
        ...prev,
        [tableId]: { ...prev[tableId], dirty: false, saving: false },
      }));
      reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setCacheTtlEdits((prev) => ({ ...prev, [tableId]: { ...prev[tableId], saving: false } }));
    }
  };

  const handlePurgeTableCache = async (tableId: number) => {
    setPurging((prev) => ({ ...prev, [tableId]: true }));
    setError(null);
    try {
      const result = await purgeCacheByTable(tableId);
      if (!result.success) throw new Error(result.message);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setPurging((prev) => ({ ...prev, [tableId]: false }));
    }
  };

  const handleInvalidateFileSource = async (tableId: number) => {
    setInvalidating((prev) => ({ ...prev, [tableId]: true }));
    setError(null);
    try {
      const result = await invalidateFileSource(tableId);
      if (!result.success) throw new Error(result.message);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setInvalidating((prev) => ({ ...prev, [tableId]: false }));
    }
  };

  const handleNamingChange = async (tableId: number, value: string) => {
    setError(null);
    try {
      const result = await updateTableNaming(tableId, value === "" ? null : value);
      if (!result.success) throw new Error(result.message);
      reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    // useQuery hooks (tables/sources/domains/roles) fetch on mount automatically.
    // Only the imperative REST call (fetchSettings) needs explicit initialization.
    fetchSettings()
      .then((st) => setSettings(st))
      .finally(() => setLoading(false));
  }, []);

  // Reset pagination when filters change
  useEffect(() => {
    setPage(0);
  }, [tableSearch, checkedDomains, groupBy]);

  const groupByKey = groupBy.join(",");
  useEffect(() => {
    setCollapsedGroups(new Set());
  }, [groupByKey]);

  const handleDelete = async (id: number) => {
    if (!confirm("Delete this table registration?")) return;
    try {
      await deleteTable(id);
      reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleProfile = async (tableId: number) => {
    if (!activeRole) {
      setTableProfiles((prev) => ({ ...prev, [tableId]: "No active role" }));
      return;
    }
    setTableProfiles((prev) => ({ ...prev, [tableId]: "loading" }));
    try {
      const result = await profileTable(tableId, activeRole.id);
      setTableProfiles((prev) => ({ ...prev, [tableId]: result }));
    } catch (e) {
      setTableProfiles((prev) => ({
        ...prev,
        [tableId]: e instanceof Error ? e.message : String(e),
      }));
    }
  };

  const startEditing = (t: RegisteredTable) => {
    setEditingTable(JSON.parse(JSON.stringify(t)));
    setEditingColumnTypes({});
    getAvailableColumnsMetadata(t.sourceId, t.schemaName, t.tableName)
      .then((meta) => {
        const map: Record<string, string> = {};
        for (const c of meta) map[c.name] = c.dataType;
        setEditingColumnTypes(map);
      })
      .catch(() => {});
  };

  const cancelEditing = () => {
    setEditingTable(null);
  };

  const updateEditCol = (i: number, key: string, value: string | string[] | boolean) => {
    if (!editingTable) return;
    const next = { ...editingTable };
    next.columns = [...next.columns];
    next.columns[i] = { ...next.columns[i], [key]: value };
    setEditingTable(next);
  };

  const handleSaveEdit = async () => {
    if (!editingTable) return;
    setError(null);
    setSaving(true);
    try {
      const result = await updateTable({
        sourceId: editingTable.sourceId,
        domainId: editingTable.domainId,
        schemaName: editingTable.schemaName,
        tableName: editingTable.tableName,
        alias: editingTable.alias || undefined,
        description: editingTable.description || undefined,
        watermarkColumn: editingTable.watermarkColumn || null,
        changeSignal: editingTable.changeSignal || null,
        probeQuery: editingTable.probeQuery || null,
        probeType: editingTable.probeType || null,
        viewSql: editingTable.viewSql || undefined,
        materialize: editingTable.materialize,
        mvRefreshInterval: editingTable.mvRefreshInterval,
        mvDebounceQuiet: editingTable.mvDebounceQuiet,
        mvDebounceMaxDelay: editingTable.mvDebounceMaxDelay,
        mvConsistency: editingTable.mvConsistency,
        dataProduct: editingTable.dataProduct,
        enableAggregates: editingTable.enableAggregates,
        enableGroupBy: editingTable.enableGroupBy,
        live: editingTable.live
          ? {
              queryId: editingTable.live.queryId ?? undefined,
              watermarkColumn: editingTable.live.watermarkColumn ?? undefined,
              pollInterval: editingTable.live.pollInterval,
              strategy: editingTable.live.strategy,
              kafka:
                editingTable.live.strategy === "kafka" && editingTable.live.kafka
                  ? {
                      topic: editingTable.live.kafka.topic,
                      format: editingTable.live.kafka.format ?? undefined,
                      keyColumn: editingTable.live.kafka.keyColumn ?? undefined,
                    }
                  : undefined,
              outputs: editingTable.live.outputs.map((o) => ({
                type: o.type,
                topic: o.topic ?? undefined,
                keyColumn: o.keyColumn ?? undefined,
                bootstrapServers: o.bootstrapServers ?? undefined,
              })),
            }
          : null,
        columnPresets: editingTable.columnPresets,
        columns: editingTable.columns.map((c) => ({
          name: c.columnName,
          visibleTo: c.visibleTo,
          writableBy: c.writableBy,
          unmaskedTo: c.unmaskedTo,
          maskType: c.maskType || undefined,
          maskPattern: c.maskPattern || undefined,
          maskReplace: c.maskReplace || undefined,
          maskValue: c.maskValue || undefined,
          maskPrecision: c.maskPrecision || undefined,
          alias: c.alias || undefined,
          description: c.description || undefined,
          nativeFilterType: c.nativeFilterType || undefined,
          isPrimaryKey: c.isPrimaryKey || undefined,
          isForeignKey: c.isForeignKey || undefined,
          isAlternateKey: c.isAlternateKey || undefined,
          scope: c.scope || "domain",
        })),
      });
      if (!result.success) {
        setError(result.message);
        return;
      }
      await handleNamingChange(editingTable.id, editingTable.gqlNamingConvention ?? "");
      const ttlEdit = cacheTtlEdits[editingTable.id];
      if (ttlEdit?.dirty) await handleSaveTableCache(editingTable.id);
      const preferResult = await updateTablePreferMaterialized(
        editingTable.id,
        editingTable.preferMaterialized,
      );
      if (!preferResult.success) {
        setError(preferResult.message);
        return;
      }
      setEditingTable(null);
      reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  if (loading || tablesLoading) return <div className="page">Loading tables...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>{viewsOnly ? "Views" : "Registered Tables"}</h2>
        <FilterInput
          value={tableSearch}
          onChange={setTableSearch}
          placeholder={viewsOnly ? "Filter views…" : "Filter by source, domain, or table…"}
        />
        <div className="page-actions">
          {!viewsOnly && (
            <button data-tour="tables-add" onClick={() => setShowForm(!showForm)}>
              {showForm ? "✕" : "+ Table"}
            </button>
          )}
          <button onClick={() => navigate("/sql")} title="Create a new view in the SQL Explorer">
            + View
          </button>
          <button
            data-tour="tables-erd"
            className="btn-icon"
            title="View ERD"
            onClick={() => setShowErd(true)}
          >
            <Network size={14} />
          </button>
        </div>
      </div>

      {error && <div className="error">{error}</div>}

      {showForm && !viewsOnly && (
        <RegisterTableForm
          sources={sources}
          domainHints={domainHints}
          domainAccess={domainAccess}
          checkedDomains={checkedDomains}
          domainsEnabled={domainsEnabled}
          tables={tables}
          roles={roles}
          getAvailableColumnsMetadata={getAvailableColumnsMetadata}
          suggestTableAlias={suggestTableAlias}
          registerTable={registerTable}
          onSuccess={() => {
            setShowForm(false);
            reload();
          }}
          setError={setError}
        />
      )}

      <table className="data-table">
        <thead>
          <tr>
            {(
              [
                ["source", "Source"],
                ["domain", "Domain"],
                ["table", "Table"],
              ] as const
            )
              .filter(([col]) => domainsEnabled || col !== "domain")
              .map(([col, label]) => {
                const isGroupable = col === "source" || col === "domain";
                const groupLevel = groupBy.indexOf(col as "source" | "domain");
                const isGrouped = groupLevel !== -1;
                return (
                  <th key={col} style={{ whiteSpace: "nowrap" }}>
                    <span
                      onClick={() => {
                        if (sortCol !== col) {
                          setSortCol(col);
                          setSortDir("asc");
                        } else if (sortDir === "asc") setSortDir("desc");
                        else {
                          setSortCol(null);
                          setSortDir("asc");
                        }
                      }}
                      style={{ cursor: "pointer", userSelect: "none" }}
                    >
                      {label}{" "}
                      <span style={{ color: "var(--text-muted)", fontSize: "0.7rem" }}>
                        {sortCol === col ? (sortDir === "asc" ? "▲" : "▼") : "⇅"}
                      </span>
                    </span>
                    {isGroupable && (
                      <span
                        title={
                          isGrouped ? `Ungroup (level ${groupLevel + 1})` : `Group by ${label}`
                        }
                        onClick={() => toggleGroupBy(col)}
                        style={{
                          marginLeft: "0.3rem",
                          fontSize: "0.65rem",
                          cursor: "pointer",
                          userSelect: "none",
                          opacity: isGrouped ? 1 : 0.35,
                          color: isGrouped ? "var(--primary, #6366f1)" : undefined,
                        }}
                      >
                        {isGrouped ? `⊞${groupLevel + 1}` : "⊞"}
                      </span>
                    )}
                  </th>
                );
              })}
            <th>Naming</th>
            <th>Cache TTL</th>
            <th>Effective TTL</th>
            <th
              onClick={() => {
                if (sortCol !== "cols") {
                  setSortCol("cols");
                  setSortDir("asc");
                } else if (sortDir === "asc") setSortDir("desc");
                else {
                  setSortCol(null);
                  setSortDir("asc");
                }
              }}
              style={{ cursor: "pointer", userSelect: "none", whiteSpace: "nowrap" }}
            >
              Cols{" "}
              <span style={{ color: "var(--text-muted)", fontSize: "0.7rem" }}>
                {sortCol === "cols" ? (sortDir === "asc" ? "▲" : "▼") : "⇅"}
              </span>
            </th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {(() => {
            const filtered = tables.filter((t) => {
              if (t.sourceId === "provisa-admin" || t.sourceId === "provisa-otel") return false;
              if (viewsOnly && !t.viewSql) return false;
              if (t.domainId && checkedDomains.size > 0 && !checkedDomains.has(t.domainId))
                return false;
              const terms = tableSearch.trim().toLowerCase().split(/\s+/).filter(Boolean);
              if (terms.length === 0) return true;
              const haystack = [t.sourceId, t.tableName, t.domainId ?? ""].join(" ").toLowerCase();
              return terms.every((term) => haystack.includes(term));
            });

            if (sortCol) {
              filtered.sort((a, b) => {
                let cmp = 0;
                if (sortCol === "source") cmp = a.sourceId.localeCompare(b.sourceId);
                else if (sortCol === "domain")
                  cmp = (a.domainId ?? "").localeCompare(b.domainId ?? "");
                else if (sortCol === "table")
                  cmp = (a.alias || a.tableName).localeCompare(b.alias || b.tableName);
                else if (sortCol === "cols") cmp = a.columns.length - b.columns.length;
                return sortDir === "asc" ? cmp : -cmp;
              });
            }

            const getGroupKey = (t: RegisteredTable, col: "source" | "domain") =>
              col === "source" ? t.sourceId : t.domainId ? normalizeDomain(t.domainId) : "(none)";

            const colLabel = (col: "source" | "domain") =>
              col === "source" ? "Source" : "Domain";

            type GroupItem =
              | { type: "header"; level: 1 | 2; key: string; label: string; count: number }
              | { type: "row"; t: RegisteredTable };

            let items: GroupItem[];

            if (groupBy.length === 0) {
              items = filtered
                .slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
                .map((t) => ({ type: "row" as const, t }));
            } else {
              items = [];
              const l1Col = groupBy[0];
              const l2Col = groupBy[1];
              const l1Map = new Map<string, RegisteredTable[]>();
              for (const t of filtered) {
                const k = getGroupKey(t, l1Col);
                if (!l1Map.has(k)) l1Map.set(k, []);
                l1Map.get(k)!.push(t);
              }
              for (const [l1Key, l1Tables] of [...l1Map.entries()].sort(([a], [b]) =>
                a.localeCompare(b),
              )) {
                items.push({
                  type: "header",
                  level: 1,
                  key: l1Key,
                  label: `${colLabel(l1Col)}: ${l1Key}`,
                  count: l1Tables.length,
                });
                if (collapsedGroups.has(l1Key)) continue;
                if (!l2Col) {
                  for (const t of l1Tables) items.push({ type: "row", t });
                } else {
                  const l2Map = new Map<string, RegisteredTable[]>();
                  for (const t of l1Tables) {
                    const k = getGroupKey(t, l2Col);
                    if (!l2Map.has(k)) l2Map.set(k, []);
                    l2Map.get(k)!.push(t);
                  }
                  for (const [l2Key, l2Tables] of [...l2Map.entries()].sort(([a], [b]) =>
                    a.localeCompare(b),
                  )) {
                    const compositeKey = `${l1Key}|${l2Key}`;
                    items.push({
                      type: "header",
                      level: 2,
                      key: compositeKey,
                      label: `${colLabel(l2Col)}: ${l2Key}`,
                      count: l2Tables.length,
                    });
                    if (collapsedGroups.has(compositeKey)) continue;
                    for (const t of l2Tables) items.push({ type: "row", t });
                  }
                }
              }
            }

            return items.map((item) => {
              if (item.type === "header") {
                const isL1 = item.level === 1;
                return (
                  <tr key={`grp-${item.key}`}>
                    <td
                      colSpan={domainsEnabled ? 9 : 8}
                      onClick={() =>
                        setCollapsedGroups((prev) => {
                          const next = new Set(prev);
                          if (next.has(item.key)) next.delete(item.key);
                          else next.add(item.key);
                          return next;
                        })
                      }
                      style={{
                        fontWeight: isL1 ? 600 : 500,
                        fontSize: isL1 ? "0.8rem" : "0.75rem",
                        padding: isL1 ? "0.35rem 0.75rem" : "0.25rem 1.5rem",
                        color: isL1 ? "var(--text-muted)" : "var(--text-muted)",
                        background: isL1
                          ? "var(--surface)"
                          : "var(--surface-raised, var(--surface))",
                        borderTop: isL1 ? "2px solid var(--border)" : "1px solid var(--border)",
                        cursor: "pointer",
                        userSelect: "none",
                      }}
                    >
                      {collapsedGroups.has(item.key) ? "▶" : "▼"} {item.label}{" "}
                      <span style={{ fontWeight: "normal", opacity: 0.7 }}>({item.count})</span>
                    </td>
                  </tr>
                );
              }
              const t = item.t;
              const isEditing = editingTable?.id === t.id;
              const row = (
                <Fragment key={t.id}>
                  <tr
                    onClick={() => {
                      setExpanded(expanded === t.id ? null : t.id);
                      if (expanded === t.id) cancelEditing();
                    }}
                    className="clickable"
                  >
                    <td>{t.sourceId}</td>
                    {domainsEnabled && <td>{t.domainId ? normalizeDomain(t.domainId) : ""}</td>}
                    <td
                      style={{ fontFamily: "monospace", fontSize: "0.9rem" }}
                      title={t.description || undefined}
                    >
                      {t.alias || t.tableName}
                    </td>
                    <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {NAMING_CONVENTIONS.find((nc) => nc.value === (t.gqlNamingConvention ?? ""))
                        ?.label ??
                        t.gqlNamingConvention ??
                        "Inherit (source)"}
                    </td>
                    <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {t.cacheTtl != null ? `${t.cacheTtl}s` : "inherit"}
                    </td>
                    <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {getEffectiveTableTtl(t)}
                    </td>
                    <td>{t.columns.length}</td>
                    <td onClick={(e) => e.stopPropagation()}>
                      <div style={{ display: "flex", gap: "0.25rem" }}>
                        {(() => {
                          const srcType = sources.find((s) => s.id === t.sourceId)?.type;
                          const hasCacheable =
                            srcType === "graphql_remote" ||
                            srcType === "openapi" ||
                            srcType === "grpc_remote";
                          const isFileBacked = srcType === "sqlite";
                          return (
                            <>
                              {hasCacheable && (
                                <button
                                  onClick={() => handlePurgeTableCache(t.id)}
                                  disabled={purging[t.id]}
                                  style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                                >
                                  {purging[t.id] ? "Purging..." : "Invalidate Cache"}
                                </button>
                              )}
                              {isFileBacked && (
                                <button
                                  onClick={() => handleInvalidateFileSource(t.id)}
                                  disabled={invalidating[t.id]}
                                  style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                                >
                                  {invalidating[t.id] ? "Refreshing..." : "Refresh Data"}
                                </button>
                              )}
                            </>
                          );
                        })()}
                      </div>
                    </td>
                  </tr>
                  {expanded === t.id && (
                    <tr key={`${t.id}-cols`}>
                      <td colSpan={domainsEnabled ? 12 : 11} style={{ padding: 0 }}>
                        {!isEditing ? (
                          <TableReadView
                            t={t}
                            navigate={navigate}
                            viewsOnly={viewsOnly}
                            deploying={deploying}
                            setDeploying={setDeploying}
                            deployMsg={deployMsg}
                            setDeployMsg={setDeployMsg}
                            tableProfiles={tableProfiles}
                            deployViewToDb={deployViewToDb}
                            reload={reload}
                            startEditing={startEditing}
                            handleDelete={handleDelete}
                            handleProfile={handleProfile}
                          />
                        ) : (
                          editingTable && (
                            <TableEditForm
                              editingTable={editingTable}
                              setEditingTable={setEditingTable}
                              editingColumnTypes={editingColumnTypes}
                              cacheTtlEdits={cacheTtlEdits}
                              setCacheTtlEdits={setCacheTtlEdits}
                              sources={sources}
                              roles={roles}
                              settings={settings}
                              saving={saving}
                              generatingDesc={generatingDesc}
                              setGeneratingDesc={setGeneratingDesc}
                              generatingColDesc={generatingColDesc}
                              setGeneratingColDesc={setGeneratingColDesc}
                              generateTableDescription={generateTableDescription}
                              generateColumnDescription={generateColumnDescription}
                              cancelEditing={cancelEditing}
                              handleSaveEdit={handleSaveEdit}
                              updateEditCol={updateEditCol}
                            />
                          )
                        )}
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
              return row;
            });
          })()}
        </tbody>
      </table>

      {(() => {
        const filtered = tables.filter((t) => {
          if (t.sourceId === "provisa-admin" || t.sourceId === "provisa-otel") return false;
          if (viewsOnly && !t.viewSql) return false;
          if (t.domainId && checkedDomains.size > 0 && !checkedDomains.has(t.domainId))
            return false;
          const terms = tableSearch.trim().toLowerCase().split(/\s+/).filter(Boolean);
          if (terms.length === 0) return true;
          const haystack = [t.sourceId, t.tableName, t.domainId ?? ""].join(" ").toLowerCase();
          return terms.every((term) => haystack.includes(term));
        });
        if (groupBy.length > 0) return null;
        const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
        if (totalPages === 1) return null;
        return (
          <div
            style={{
              display: "flex",
              gap: "0.5rem",
              alignItems: "center",
              justifyContent: "flex-end",
              padding: "0.5rem 0",
            }}
          >
            <button onClick={() => setPage(0)} disabled={page === 0}>
              «
            </button>
            <button onClick={() => setPage((p) => p - 1)} disabled={page === 0}>
              ‹
            </button>
            <span>
              Page {page + 1} / {totalPages}
            </span>
            <button onClick={() => setPage((p) => p + 1)} disabled={page >= totalPages - 1}>
              ›
            </button>
            <button onClick={() => setPage(totalPages - 1)} disabled={page >= totalPages - 1}>
              »
            </button>
          </div>
        );
      })()}
      {showErd && (
        <ErdModal
          tables={tables}
          relationships={relationships}
          domains={domains}
          activeDomain={checkedDomains.size === 1 ? [...checkedDomains][0] : null}
          onClose={() => setShowErd(false)}
        />
      )}
    </div>
  );
}
