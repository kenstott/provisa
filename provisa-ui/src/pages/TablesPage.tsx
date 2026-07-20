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
import { useTranslation } from "react-i18next";
import { Network, ArrowUp, ArrowDown, ArrowUpDown, Layers, X } from "lucide-react";
import { ActionIcon, Alert, Button, Group, Modal, Table, Text, Title } from "@mantine/core";
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
  useUpdateTableLoadProtection,
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
import { ModelingForm } from "./tables/ModelingForm";
import { TableReadView } from "./tables/TableReadView";
import { TableEditForm } from "./tables/TableEditForm";

export function TablesPage({ viewsOnly = false }: { viewsOnly?: boolean } = {}) {
  const { t: translate } = useTranslation();
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
  const { updateTableLoadProtection } = useUpdateTableLoadProtection();
  const { updateTableNaming } = useUpdateTableNaming();
  const { purgeCacheByTable } = usePurgeCacheByTable();
  const { invalidateFileSource } = useInvalidateFileSource();
  const { deployViewToDb } = useDeployViewToDb();
  const { suggestTableAlias } = useSuggestTableAlias();
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [showModeling, setShowModeling] = useState(false); // REQ-1164: entity/fact modeling modal
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
    const existing = tableProfiles[tableId];
    if (existing !== undefined && existing !== "loading") {
      setTableProfiles((prev) => {
        const next = { ...prev };
        delete next[tableId];
        return next;
      });
      return;
    }
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
        mvPreprocess: editingTable.mvPreprocess || null, // REQ-957
        mvBitemporalMode: editingTable.mvBitemporalMode || null, // REQ-1162
        mvBitemporalKey: editingTable.mvBitemporalKey, // REQ-1162
        mvPersist: editingTable.mvPersist, // REQ-965
        mvPrimaryKey: editingTable.mvPrimaryKey, // REQ-970
        mvIncremental: editingTable.mvIncremental, // REQ-969
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
        // REQ-1093: persist edited UNIQUE constraints; drop empty/incomplete rows.
        uniqueConstraints: (editingTable.uniqueConstraints ?? [])
          .filter((u) => u.name.trim() && u.columns.length > 0)
          .map((u) => ({ name: u.name.trim(), columns: u.columns })),
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
      // REQ-1141: persist load protection + off-peak window (validated server-side ≥1-gate rule).
      const lpResult = await updateTableLoadProtection(
        editingTable.id,
        editingTable.loadProtected,
        editingTable.offPeakWindow,
        editingTable.offPeakTz,
      );
      if (!lpResult.success) {
        setError(lpResult.message);
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

  if (loading || tablesLoading) return <div className="page">{translate("tablesPage.loading")}</div>;

  return (
    <div className="page">
      <div className="page-header">
        <Title order={2}>{viewsOnly ? translate("tablesPage.titleViews") : translate("tablesPage.titleTables")}</Title>
        <FilterInput
          value={tableSearch}
          onChange={setTableSearch}
          placeholder={viewsOnly ? translate("tablesPage.filterPlaceholderViews") : translate("tablesPage.filterPlaceholderTables")}
        />
        <div className="page-actions">
          {!viewsOnly && (
            <Button
              data-tour="tables-add"
              data-testid="tables-add-toggle"
              variant={showForm ? "outline" : "filled"}
              onClick={() => setShowForm(!showForm)}
              aria-label={showForm ? translate("tablesPage.closeForm") : undefined}
            >
              {showForm ? <X size={14} /> : translate("tablesPage.addTable")}
            </Button>
          )}
          <Button variant="default" onClick={() => navigate("/sql")} title={translate("tablesPage.addViewTitle")}>
            {translate("tablesPage.addView")}
          </Button>
          {!viewsOnly && (
            <Button
              variant="default"
              data-testid="tables-model-toggle"
              onClick={() => setShowModeling(true)}
              title={translate("tablesPage.modelTitle")}
            >
              {translate("tablesPage.model")}
            </Button>
          )}
          <ActionIcon
            data-tour="tables-erd"
            variant="subtle"
            aria-label={translate("tablesPage.viewErd")}
            title={translate("tablesPage.viewErd")}
            onClick={() => setShowErd(true)}
          >
            <Network size={14} />
          </ActionIcon>
        </div>
      </div>

      {error && (
        <Alert color="red" mb="md" data-testid="tables-error">
          {error}
        </Alert>
      )}

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

      <Table className="data-table">
        <Table.Thead>
          <Table.Tr>
            {(
              [
                ["source", "tablesPage.colSource"],
                ["domain", "tablesPage.colDomain"],
                ["table", "tablesPage.colTable"],
              ] as const
            )
              .filter(([col]) => domainsEnabled || col !== "domain")
              .map(([col, labelKey]) => {
                const label = translate(labelKey);
                const isGroupable = col === "source" || col === "domain";
                const groupLevel = groupBy.indexOf(col as "source" | "domain");
                const isGrouped = groupLevel !== -1;
                const sortActive = sortCol === col;
                const sortLabel = sortActive
                  ? sortDir === "asc"
                    ? translate("tablesPage.sortAscending")
                    : translate("tablesPage.sortDescending")
                  : translate("tablesPage.sortNone");
                return (
                  <Table.Th key={col} style={{ whiteSpace: "nowrap" }}>
                    <Group gap={4} wrap="nowrap" component="span">
                      <button
                        type="button"
                        data-testid={`tables-sort-${col}`}
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
                        aria-label={`${label}, ${sortLabel}`}
                        style={{
                          cursor: "pointer",
                          userSelect: "none",
                          background: "none",
                          border: "none",
                          padding: 0,
                          display: "inline-flex",
                          alignItems: "center",
                          gap: "0.25rem",
                          font: "inherit",
                          color: "inherit",
                        }}
                      >
                        {label}
                        {sortActive ? (
                          sortDir === "asc" ? (
                            <ArrowUp size={11} color="var(--text-muted)" aria-hidden="true" />
                          ) : (
                            <ArrowDown size={11} color="var(--text-muted)" aria-hidden="true" />
                          )
                        ) : (
                          <ArrowUpDown size={11} color="var(--text-muted)" aria-hidden="true" />
                        )}
                      </button>
                      {isGroupable && (
                        <ActionIcon
                          variant="transparent"
                          size="xs"
                          data-testid={`tables-group-${col}`}
                          aria-label={
                            isGrouped
                              ? translate("tablesPage.ungroupLevel", { level: groupLevel + 1 })
                              : translate("tablesPage.groupBy", { label })
                          }
                          title={
                            isGrouped
                              ? translate("tablesPage.ungroupLevel", { level: groupLevel + 1 })
                              : translate("tablesPage.groupBy", { label })
                          }
                          onClick={() => toggleGroupBy(col)}
                          style={{ opacity: isGrouped ? 1 : 0.35 }}
                        >
                          <Layers
                            size={11}
                            color={isGrouped ? "var(--primary, #6366f1)" : undefined}
                            aria-hidden="true"
                          />
                        </ActionIcon>
                      )}
                      {isGroupable && isGrouped && (
                        <Text span fz="0.65rem" c="var(--primary, #6366f1)">
                          {groupLevel + 1}
                        </Text>
                      )}
                    </Group>
                  </Table.Th>
                );
              })}
            <Table.Th>{translate("tablesPage.colNaming")}</Table.Th>
            <Table.Th>{translate("tablesPage.colCacheTtl")}</Table.Th>
            <Table.Th>{translate("tablesPage.colEffectiveTtl")}</Table.Th>
            <Table.Th style={{ whiteSpace: "nowrap" }}>
              {(() => {
                const label = translate("tablesPage.colCols");
                const sortActive = sortCol === "cols";
                const sortLabel = sortActive
                  ? sortDir === "asc"
                    ? translate("tablesPage.sortAscending")
                    : translate("tablesPage.sortDescending")
                  : translate("tablesPage.sortNone");
                return (
                  <button
                    type="button"
                    data-testid="tables-sort-cols"
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
                    aria-label={`${label}, ${sortLabel}`}
                    style={{
                      cursor: "pointer",
                      userSelect: "none",
                      background: "none",
                      border: "none",
                      padding: 0,
                      display: "inline-flex",
                      alignItems: "center",
                      gap: "0.25rem",
                      font: "inherit",
                      color: "inherit",
                    }}
                  >
                    {label}
                    {sortActive ? (
                      sortDir === "asc" ? (
                        <ArrowUp size={11} color="var(--text-muted)" aria-hidden="true" />
                      ) : (
                        <ArrowDown size={11} color="var(--text-muted)" aria-hidden="true" />
                      )
                    ) : (
                      <ArrowUpDown size={11} color="var(--text-muted)" aria-hidden="true" />
                    )}
                  </button>
                );
              })()}
            </Table.Th>
            <Table.Th></Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
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
                const isCollapsed = collapsedGroups.has(item.key);
                const toggleCollapsed = () =>
                  setCollapsedGroups((prev) => {
                    const next = new Set(prev);
                    if (next.has(item.key)) next.delete(item.key);
                    else next.add(item.key);
                    return next;
                  });
                return (
                  <Table.Tr key={`grp-${item.key}`}>
                    <Table.Td
                      colSpan={domainsEnabled ? 9 : 8}
                      role="button"
                      tabIndex={0}
                      aria-expanded={!isCollapsed}
                      onClick={toggleCollapsed}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault();
                          toggleCollapsed();
                        }
                      }}
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
                      {isCollapsed ? "▶" : "▼"} {item.label}{" "}
                      <span style={{ fontWeight: "normal", opacity: 0.7 }}>({item.count})</span>
                    </Table.Td>
                  </Table.Tr>
                );
              }
              const t = item.t;
              const isEditing = editingTable?.id === t.id;
              const row = (
                <Fragment key={t.id}>
                  <Table.Tr
                    onClick={() => {
                      setExpanded(expanded === t.id ? null : t.id);
                      if (expanded === t.id) cancelEditing();
                    }}
                    className="clickable"
                  >
                    <Table.Td>{t.sourceId}</Table.Td>
                    {domainsEnabled && (
                      <Table.Td>{t.domainId ? normalizeDomain(t.domainId) : ""}</Table.Td>
                    )}
                    <Table.Td
                      style={{ fontFamily: "monospace", fontSize: "0.9rem" }}
                      title={t.description || undefined}
                    >
                      {t.alias || t.tableName}
                    </Table.Td>
                    <Table.Td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {NAMING_CONVENTIONS.find((nc) => nc.value === (t.gqlNamingConvention ?? ""))
                        ?.label ??
                        t.gqlNamingConvention ??
                        translate("tablesPage.inheritSource")}
                    </Table.Td>
                    <Table.Td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {t.cacheTtl != null ? `${t.cacheTtl}s` : translate("tablesPage.inherit")}
                    </Table.Td>
                    <Table.Td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {getEffectiveTableTtl(t)}
                    </Table.Td>
                    <Table.Td>{t.columns.length}</Table.Td>
                    <Table.Td onClick={(e) => e.stopPropagation()}>
                      <Group gap="xs" wrap="nowrap">
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
                                <Button
                                  size="compact-xs"
                                  variant="default"
                                  onClick={() => handlePurgeTableCache(t.id)}
                                  disabled={purging[t.id]}
                                >
                                  {purging[t.id]
                                    ? translate("tablesPage.purging")
                                    : translate("tablesPage.invalidateCache")}
                                </Button>
                              )}
                              {isFileBacked && (
                                <Button
                                  size="compact-xs"
                                  variant="default"
                                  onClick={() => handleInvalidateFileSource(t.id)}
                                  disabled={invalidating[t.id]}
                                >
                                  {invalidating[t.id]
                                    ? translate("tablesPage.refreshing")
                                    : translate("tablesPage.refreshData")}
                                </Button>
                              )}
                            </>
                          );
                        })()}
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                  {expanded === t.id && (
                    <Table.Tr key={`${t.id}-cols`}>
                      <Table.Td colSpan={domainsEnabled ? 12 : 11} style={{ padding: 0 }}>
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
                      </Table.Td>
                    </Table.Tr>
                  )}
                </Fragment>
              );
              return row;
            });
          })()}
        </Table.Tbody>
      </Table>

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
          <Group gap="sm" justify="flex-end" py="sm">
            <ActionIcon
              variant="default"
              aria-label={translate("tablesPage.firstPage")}
              onClick={() => setPage(0)}
              disabled={page === 0}
            >
              «
            </ActionIcon>
            <ActionIcon
              variant="default"
              aria-label={translate("tablesPage.prevPage")}
              onClick={() => setPage((p) => p - 1)}
              disabled={page === 0}
            >
              ‹
            </ActionIcon>
            <Text fz="sm">{translate("tablesPage.pageOf", { page: page + 1, totalPages })}</Text>
            <ActionIcon
              variant="default"
              aria-label={translate("tablesPage.nextPage")}
              onClick={() => setPage((p) => p + 1)}
              disabled={page >= totalPages - 1}
            >
              ›
            </ActionIcon>
            <ActionIcon
              variant="default"
              aria-label={translate("tablesPage.lastPage")}
              onClick={() => setPage(totalPages - 1)}
              disabled={page >= totalPages - 1}
            >
              »
            </ActionIcon>
          </Group>
        );
      })()}
      <Modal
        opened={showModeling}
        onClose={() => setShowModeling(false)}
        title={translate("tablesPage.modelTitle")}
        size="lg"
      >
        <ModelingForm
          domains={domains}
          onSuccess={() => {
            setShowModeling(false);
            reload();
          }}
          onCancel={() => setShowModeling(false)}
        />
      </Modal>
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
