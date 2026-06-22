// Copyright (c) 2026 Kenneth Stott
// Canary: e1499f85-6ff4-44b7-aad6-327499acea72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { toSnakeCase, toCamelCase, toPascalCase, applyConvention } from "../naming";
import { useState, useEffect, Fragment, useCallback } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Trash2, Pencil, Sparkles, Save, X, Loader2, Network } from "lucide-react";
import { ErdModal } from "../components/erd/ErdModal";
import { CopyButton } from "../components/CopyButton";
import { MultiSelect } from "../components/MultiSelect";
import { fetchSettings, profileTable } from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import {
  useTables,
  useSources,
  useDomains,
  useRoles,
  useAvailableSchemas,
  useAvailableTables,
  useAvailableColumnsMetadataLazy,
  useGenerateTableDescription,
  useGenerateColumnDescription,
  useRegisterTable,
  useUpdateTable,
  useDeleteTable,
  useUpdateTableCache,
  useUpdateTableNaming,
  usePurgeCacheByTable,
  useInvalidateFileSource,
  useDeployViewToDb,
  useSuggestTableAlias,
  useRelationships,
} from "../hooks/useAdminQueries";
import type { RegisteredTable } from "../types/admin";
import { ColumnPresetsEditor } from "../components/admin/ColumnPresetsEditor";
import { FilterInput } from "../components/admin/FilterInput";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useAuth } from "../context/AuthContext";

function DescriptionField({
  value,
  onChange,
  placeholder,
  rows = 2,
  onGenerate,
  generating,
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  rows?: number;
  onGenerate?: () => void;
  generating?: boolean;
}) {
  const [focused, setFocused] = useState(false);
  return (
    <div className="desc-field">
      <textarea
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        rows={rows}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        style={
          focused
            ? { height: 300, transition: "height 0.15s ease" }
            : { transition: "height 0.15s ease" }
        }
      />
      <div className="desc-field-toolbar">
        <CopyButton text={value} size={11} />
        {onGenerate && (
          <button type="button" title="Generate with AI" onClick={onGenerate} disabled={generating}>
            <Sparkles size={11} />
          </button>
        )}
        <button type="button" title="Clear" onClick={() => onChange("")}>
          <X size={11} />
        </button>
      </div>
    </div>
  );
}



function normalizeDomain(domain: string): string {
  return domain.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
}

const NAMING_CONVENTIONS = [
  { value: "", label: "Inherit (source)" },
  { value: "none", label: "none" },
  { value: "snake_case", label: "snake_case" },
  { value: "camelCase", label: "camelCase" },
  { value: "PascalCase", label: "PascalCase" },
];

const CDC_TYPES = new Set(["postgresql", "mongodb", "kafka", "debezium"]);

function isWatermarkEligible(dataType: string): boolean {
  const t = dataType.toLowerCase();
  return (
    t.includes("timestamp") ||
    t.includes("datetime") ||
    t.includes("date") ||
    t === "bigint" ||
    t === "int8" ||
    t === "integer" ||
    t === "int4" ||
    t === "int" ||
    t === "int2" ||
    t === "smallint" ||
    t === "long" ||
    t === "numeric" ||
    t === "number"
  );
}

interface ColumnForm {
  name: string;
  visibleTo: string[];
  writableBy: string[];
  unmaskedTo: string;
  maskType: string;
  maskPattern: string;
  maskReplace: string;
  maskValue: string;
  maskPrecision: string;
  alias: string;
  description: string;
  selected: boolean;
  nativeFilterType: string | null;
  dataType: string;
  isPrimaryKey: boolean;
  scope: string;
}

export function TablesPage({ viewsOnly = false }: { viewsOnly?: boolean } = {}) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { tables, loading: tablesLoading, refetch: refetchTables } = useTables();
  const { sources, refetch: refetchSources } = useSources();
  const { domains, refetch: refetchDomains } = useDomains();
  const { relationships } = useRelationships();
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
  const [sortCol, setSortCol] = useState<"source" | "domain" | "table" | "cols">("source");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const { checkedDomains, domainsEnabled } = useDomainFilter();
  const { domainAccess } = useAuth();

  // Form state
  const [sourceId, setSourceId] = useState("");
  const [domainId, setDomainId] = useState("");
  const [schemaName, setSchemaName] = useState("");
  const [tableName, setTableName] = useState("");
  const [tableAlias, setTableAlias] = useState("");
  const [tableDescription, setTableDescription] = useState("");
  const [columns, setColumns] = useState<ColumnForm[]>([]);
  const [watermarkColumn, setWatermarkColumn] = useState<string>("");
  const [dataProduct, setDataProduct] = useState(false);

  // Per-table profile state: tableId → profile result or "loading"
  const [tableProfiles, setTableProfiles] = useState<
    Record<
      number,
      { columns: string[]; rows: Record<string, unknown>[]; rowCount: number } | "loading" | string
    >
  >({});

  // Discovery state
  const { schemas: availableSchemas, loading: loadingSchemas } = useAvailableSchemas(
    sourceId || null,
  );
  const isFixedSchema = availableSchemas.length === 1;
  const { tables: availableTables, loading: loadingTables } = useAvailableTables(
    sourceId && schemaName ? sourceId : null,
    schemaName || null,
  );
  const [loadingColumns, setLoadingColumns] = useState(false);

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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    setSchemaName("");
    setTableName("");
    setTableDescription("");
    setColumns([]);
  }, [sourceId]);

  useEffect(() => {
    if (availableSchemas.length === 1) {
      setSchemaName(availableSchemas[0]);
    }
  }, [availableSchemas, sourceId]);

  useEffect(() => {
    setTableName("");
    setTableDescription("");
    setColumns([]);
  }, [sourceId, schemaName]);

  // Auto-populate table description from physical database comment
  useEffect(() => {
    if (!tableName) return;
    const meta = availableTables.find((t) => t.name === tableName);
    if (meta?.comment) setTableDescription(meta.comment);
  }, [tableName, availableTables]);

  // Auto-generate alias from table name using snake_case convention
  useEffect(() => {
    if (!tableName || !domainId || !sourceId) {
      setTableAlias("");
      return;
    }
    suggestTableAlias(tableName, domainId, sourceId).then(setTableAlias);
  }, [tableName, domainId, sourceId]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    setColumns([]);
    setWatermarkColumn("");
    if (!sourceId || !schemaName || !tableName) return;
    setLoadingColumns(true);
    getAvailableColumnsMetadata(sourceId, schemaName, tableName)
      .then((cols) => {
        const formed = cols.map((c) => {
          const snake = toSnakeCase(c.name);
          return {
            name: c.name,
            visibleTo: roles.map((r) => r.id),
            writableBy: [],
            unmaskedTo: "",
            maskType: "",
            maskPattern: "",
            maskReplace: "",
            maskValue: "",
            maskPrecision: "",
            alias: snake !== c.name ? snake : "",
            description: c.comment || "",
            selected: true,
            nativeFilterType: c.nativeFilterType,
            dataType: c.dataType,
            isPrimaryKey: c.isPrimaryKey ?? false,
            scope: c.nativeFilterType ? "public" : "domain",
          };
        });
        setColumns(formed);
        const sourceType = sources.find((s) => s.id === sourceId)?.type ?? "";
        if (!CDC_TYPES.has(sourceType)) {
          const autoWm = formed.find(
            (c) =>
              (c.name === "updated_at" || c.name === "updated") && isWatermarkEligible(c.dataType),
          );
          if (autoWm) setWatermarkColumn(autoWm.name);
        }
      })
      .catch(() => setColumns([]))
      .finally(() => setLoadingColumns(false));
    /* eslint-disable-next-line react-hooks/exhaustive-deps --
       refetch columns only when the table selection changes; roles/sources are read for default seeding and must not retrigger a column fetch */
  }, [sourceId, schemaName, tableName]);

  // Reset pagination when filters change
  useEffect(() => {
    setPage(0);
  }, [tableSearch, checkedDomains, groupBy]);

  useEffect(() => {
    setCollapsedGroups(new Set());
  }, [groupBy.join(",")]);

  const handleSubmit = async () => {
    setError(null);
    const selectedCols = columns
      .filter((c) => c.selected)
      .map((c) => ({
        name: c.name,
        visibleTo: c.visibleTo,
        writableBy: c.writableBy,
        unmaskedTo: c.unmaskedTo.trim() ? c.unmaskedTo.split(",").map((s) => s.trim()) : [],
        maskType: c.maskType || undefined,
        maskPattern: c.maskPattern || undefined,
        maskReplace: c.maskReplace || undefined,
        maskValue: c.maskValue || undefined,
        maskPrecision: c.maskPrecision || undefined,
        alias: c.alias || undefined,
        description: c.description || undefined,
        nativeFilterType: c.nativeFilterType || undefined,
        isPrimaryKey: c.isPrimaryKey || undefined,
        scope: c.scope || "domain",
      }));
    if (!sourceId || !schemaName || !tableName) {
      setError("Source, schema, and table name are required.");
      return;
    }
    if (selectedCols.length === 0) {
      setError("At least one column must be selected.");
      return;
    }
    try {
      const result = await registerTable({
        sourceId,
        domainId,
        schemaName: domainId ? normalizeDomain(domainId) : schemaName,
        tableName,
        alias: tableAlias || undefined,
        description: tableDescription || undefined,
        watermarkColumn: watermarkColumn || null,
        dataProduct,
        columns: selectedCols,
      });
      if (!result.success) {
        setError(result.message);
        return;
      }
      setShowForm(false);
      setSourceId("");
      setDomainId("");
      setSchemaName("");
      setTableName("");
      setTableAlias("");
      setTableDescription("");
      setGovernance("pre-approved");
      setColumns([]);
      setWatermarkColumn("");
      setDataProduct(false);
      reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

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
    setTableProfiles((prev) => ({ ...prev, [tableId]: "loading" }));
    try {
      const result = await profileTable(tableId);
      setTableProfiles((prev) => ({ ...prev, [tableId]: result }));
    } catch (e) {
      setTableProfiles((prev) => ({
        ...prev,
        [tableId]: e instanceof Error ? e.message : String(e),
      }));
    }
  };

  function computeProfile(columns: string[], rows: Record<string, unknown>[]) {
    return columns.map((col) => {
      const vals = rows.map((r) => r[col]);
      const nullCount = vals.filter((v) => v === null || v === undefined).length;
      const blankCount = vals.filter((v) => typeof v === "string" && v.trim() === "").length;
      const nonNull = vals.filter((v) => v !== null && v !== undefined);
      const freq = new Map<string, number>();
      for (const v of vals) {
        const k = v === null || v === undefined ? "NULL" : String(v);
        freq.set(k, (freq.get(k) ?? 0) + 1);
      }
      const distinctCount = freq.size;
      const constantValue = distinctCount === 1 ? vals[0] : undefined;
      const numbers = nonNull.filter((v) => typeof v === "number") as number[];
      const mean = numbers.length > 0 ? numbers.reduce((a, b) => a + b, 0) / numbers.length : null;
      const sorted = [...nonNull].sort((a, b) => (a! < b! ? -1 : a! > b! ? 1 : 0));
      const min = sorted.length > 0 ? (sorted[0] as string | number) : null;
      const max = sorted.length > 0 ? (sorted[sorted.length - 1] as string | number) : null;
      const topValues = [...freq.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([value, count]) => ({ value, count }));
      return {
        col,
        nullCount,
        blankCount,
        distinctCount,
        constantValue,
        min,
        max,
        mean,
        topValues,
      };
    });
  }

  const updateCol = (i: number, key: keyof ColumnForm, value: string | boolean | string[]) => {
    const next = [...columns];
    next[i] = { ...next[i], [key]: value };
    setColumns(next);
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
        viewSql: editingTable.viewSql || undefined,
        materialize: editingTable.materialize,
        mvRefreshInterval: editingTable.mvRefreshInterval,
        dataProduct: editingTable.dataProduct,
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
            <button onClick={() => setShowForm(!showForm)}>{showForm ? "Cancel" : "+ Table"}</button>
          )}
          <button onClick={() => navigate("/sql")} title="Create a new view in the SQL Explorer">
            + View
          </button>
          <button className="btn-icon" title="View ERD" onClick={() => setShowErd(true)}>
            <Network size={14} />
          </button>
        </div>
      </div>

      {error && <div className="error">{error}</div>}

      {showForm && !viewsOnly && (
        <div className="form-card">
          <label>
            Source
            <select value={sourceId} onChange={(e) => setSourceId(e.target.value)}>
              <option value="">Select source...</option>
              {sources
                .filter(
                  (s) =>
                    s.allowedDomains.length === 0 ||
                    s.allowedDomains.some((d) => checkedDomains.has(d)),
                )
                .map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.id}
                  </option>
                ))}
            </select>
          </label>
          {domainsEnabled && (
            <label>
              Domain
              <select value={domainId} onChange={(e) => setDomainId(e.target.value)}>
                <option value="">Select domain...</option>
                {domainHints
                  .filter((d) => d !== "" && d !== "meta" && d !== "ops")
                  .filter((d) => domainAccess.includes("*") || domainAccess.includes(d))
                  .map((d) => (
                    <option key={d} value={d}>
                      {d}
                    </option>
                  ))}
              </select>
            </label>
          )}
          <label>
            Schema
            {(() => {
              return (
                <select
                  value={schemaName}
                  onChange={(e) => setSchemaName(e.target.value)}
                  disabled={!sourceId || loadingSchemas || isFixedSchema}
                  style={isFixedSchema ? { opacity: 0.5, cursor: "not-allowed" } : undefined}
                >
                  <option value="">{loadingSchemas ? "Loading..." : "Select schema..."}</option>
                  {availableSchemas.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              );
            })()}
          </label>
          <label>
            Table
            {(() => {
              const isRegistered = (t: { name: string }) =>
                tables.some(
                  (rt) =>
                    rt.sourceId === sourceId &&
                    toSnakeCase(rt.tableName) === toSnakeCase(t.name),
                );
              const allRegistered =
                !loadingTables &&
                schemaName &&
                availableTables.length > 0 &&
                availableTables.every(isRegistered);
              return (
                <select
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  disabled={!schemaName || loadingTables || !!allRegistered}
                >
                  <option value="">
                    {loadingTables
                      ? "Loading..."
                      : allRegistered
                        ? "All tables already registered"
                        : "Select table..."}
                  </option>
                  {availableTables.map((t) => (
                    <option key={t.name} value={t.name} disabled={isRegistered(t)}>
                      {t.name}
                    </option>
                  ))}
                </select>
              );
            })()}
          </label>
          <label>
            SQL Alias{" "}
            <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>(optional)</span>
            <input
              value={tableAlias}
              onChange={(e) => setTableAlias(e.target.value)}
              placeholder="Semantic name override"
            />
          </label>
          <label>
            Description{" "}
            <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>(optional)</span>
            <input
              value={tableDescription}
              onChange={(e) => setTableDescription(e.target.value)}
              placeholder="Appears in SDL docs"
            />
          </label>
          <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem" }}>
            <input
              type="checkbox"
              checked={dataProduct}
              onChange={(e) => setDataProduct(e.target.checked)}
              style={{ width: "auto" }}
            />
            Data Product
            <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>
              (publish to catalog / export to Atlas, Atlan, etc.)
            </span>
          </label>
          {sourceId && (
            <label>
              Watermark Column{" "}
              <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>
                {CDC_TYPES.has(sources.find((s) => s.id === sourceId)?.type ?? "")
                  ? "(optional — polling fallback if triggers unavailable)"
                  : "(required for subscriptions)"}
              </span>
              <select
                value={watermarkColumn}
                onChange={(e) => setWatermarkColumn(e.target.value)}
                disabled={columns.length === 0}
              >
                <option value="">
                  {CDC_TYPES.has(sources.find((s) => s.id === sourceId)?.type ?? "")
                    ? "None (use triggers)"
                    : "None (no subscriptions)"}
                </option>
                {columns
                  .filter((c) => c.selected && isWatermarkEligible(c.dataType))
                  .map((c) => (
                    <option key={c.name} value={c.name}>
                      {c.name} ({c.dataType})
                    </option>
                  ))}
              </select>
            </label>
          )}
          <label style={{ gridColumn: "1 / -1" }}>
            Columns {loadingColumns && "(loading...)"}
            {columns.length > 0 && (
              <div className="column-editor">
                <div className="column-editor-header">
                  <span style={{ width: 28 }}></span>
                  <span className="col-name-header">Column</span>
                  <span style={{ width: 32, textAlign: "center" }}>PK</span>
                  <span className="col-flex-header">Visible To (Read)</span>
                  <span className="col-flex-header">Writable By (R/W)</span>
                  <span className="col-flex-header">Masking</span>
                  <span className="col-flex-header">SQL Alias</span>
                  <span className="col-flex-header">Description</span>
                  <span className="col-flex-header">Scope</span>
                </div>
                {columns.map((col, i) => (
                  <Fragment key={col.name}>
                    <div className="column-editor-row">
                      <input
                        type="checkbox"
                        checked={col.selected}
                        onChange={(e) => updateCol(i, "selected", e.target.checked)}
                      />
                      <span className="col-name">{col.name}</span>
                      <input
                        type="checkbox"
                        title="Primary Key"
                        checked={col.isPrimaryKey}
                        onChange={(e) => updateCol(i, "isPrimaryKey", e.target.checked)}
                        style={{ width: 32, justifySelf: "center" }}
                      />
                      <MultiSelect
                        options={roles.map((r) => ({ id: r.id, label: r.id }))}
                        value={col.visibleTo}
                        onChange={(selected) => updateCol(i, "visibleTo", selected)}
                        className="col-flex-input"
                      />
                      <MultiSelect
                        options={roles.map((r) => ({ id: r.id, label: r.id }))}
                        value={col.writableBy}
                        onChange={(selected) => updateCol(i, "writableBy", selected)}
                        className="col-flex-input"
                      />
                      <select
                        value={col.maskType}
                        onChange={(e) => updateCol(i, "maskType", e.target.value)}
                        className="col-flex-input"
                      >
                        <option value="">None</option>
                        <option value="regex">Regex</option>
                        <option value="constant">Constant</option>
                        <option value="truncate">Truncate</option>
                      </select>
                      <input
                        value={col.alias || ""}
                        onChange={(e) => updateCol(i, "alias", e.target.value)}
                        className="col-flex-input"
                      />
                      <input
                        value={col.description}
                        onChange={(e) => updateCol(i, "description", e.target.value)}
                        placeholder="description"
                        className="col-flex-input"
                      />
                      <select
                        value={col.scope}
                        onChange={(e) => updateCol(i, "scope", e.target.value)}
                        className="col-flex-input"
                      >
                        <option value="domain">domain</option>
                        <option value="public">public</option>
                        <option value="restricted">restricted</option>
                      </select>
                    </div>
                    {col.maskType && (
                      <div className="column-editor-row column-mask-row">
                        <span style={{ width: 28 }}></span>
                        <span
                          className="col-name"
                          style={{ color: "var(--text-muted)", fontSize: "0.75rem" }}
                        >
                          ↳ masking
                        </span>
                        {col.maskType === "regex" && (
                          <>
                            <input
                              value={col.maskPattern}
                              onChange={(e) => updateCol(i, "maskPattern", e.target.value)}
                              placeholder="regex pattern"
                              className="col-flex-input"
                            />
                            <input
                              value={col.maskReplace}
                              onChange={(e) => updateCol(i, "maskReplace", e.target.value)}
                              placeholder="replacement"
                              className="col-flex-input"
                            />
                          </>
                        )}
                        {col.maskType === "constant" && (
                          <input
                            value={col.maskValue}
                            onChange={(e) => updateCol(i, "maskValue", e.target.value)}
                            placeholder="constant value (NULL, 0, ***)"
                            className="col-flex-input"
                          />
                        )}
                        {col.maskType === "truncate" && (
                          <select
                            value={col.maskPrecision}
                            onChange={(e) => updateCol(i, "maskPrecision", e.target.value)}
                            className="col-flex-input"
                          >
                            <option value="">Select precision...</option>
                            <option value="year">Year</option>
                            <option value="month">Month</option>
                            <option value="day">Day</option>
                            <option value="hour">Hour</option>
                          </select>
                        )}
                        <input
                          value={col.unmaskedTo}
                          onChange={(e) => updateCol(i, "unmaskedTo", e.target.value)}
                          placeholder="unmasked roles (csv)"
                          className="col-flex-input"
                        />
                      </div>
                    )}
                  </Fragment>
                ))}
              </div>
            )}
          </label>
          <button onClick={handleSubmit}>+ Table</button>
        </div>
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
              const groupLevel = groupBy.indexOf(col);
              const isGrouped = groupLevel !== -1;
              return (
                <th key={col} style={{ whiteSpace: "nowrap" }}>
                  <span
                    onClick={() => {
                      if (sortCol === col) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                      else { setSortCol(col); setSortDir("asc"); }
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
                      title={isGrouped ? `Ungroup (level ${groupLevel + 1})` : `Group by ${label}`}
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
                if (sortCol === "cols") setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                else { setSortCol("cols"); setSortDir("asc"); }
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

            filtered.sort((a, b) => {
              let cmp = 0;
              if (sortCol === "source") cmp = a.sourceId.localeCompare(b.sourceId);
              else if (sortCol === "domain") cmp = (a.domainId ?? "").localeCompare(b.domainId ?? "");
              else if (sortCol === "table") cmp = (a.alias || a.tableName).localeCompare(b.alias || b.tableName);
              else if (sortCol === "cols") cmp = a.columns.length - b.columns.length;
              return sortDir === "asc" ? cmp : -cmp;
            });

            const getGroupKey = (t: RegisteredTable, col: "source" | "domain") =>
              col === "source" ? t.sourceId : (t.domainId ? normalizeDomain(t.domainId) : "(none)");

            const colLabel = (col: "source" | "domain") => col === "source" ? "Source" : "Domain";

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
              for (const [l1Key, l1Tables] of [...l1Map.entries()].sort(([a], [b]) => a.localeCompare(b))) {
                items.push({ type: "header", level: 1, key: l1Key, label: `${colLabel(l1Col)}: ${l1Key}`, count: l1Tables.length });
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
                  for (const [l2Key, l2Tables] of [...l2Map.entries()].sort(([a], [b]) => a.localeCompare(b))) {
                    const compositeKey = `${l1Key}|${l2Key}`;
                    items.push({ type: "header", level: 2, key: compositeKey, label: `${colLabel(l2Col)}: ${l2Key}`, count: l2Tables.length });
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
                        background: isL1 ? "var(--surface)" : "var(--surface-raised, var(--surface))",
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
                    {domainsEnabled && (
                      <td>{t.domainId ? normalizeDomain(t.domainId) : ""}</td>
                    )}
                    <td style={{ fontFamily: "monospace", fontSize: "0.9rem" }}>
                      {(() => {
                        const ns = domainsEnabled && t.domainId ? normalizeDomain(t.domainId) : "";
                        return [ns, t.alias || t.tableName].filter(Boolean).join(".");
                      })()}
                      {t.description && (
                        <div
                          style={{
                            fontFamily: "inherit",
                            fontSize: "0.8rem",
                            color: "var(--text-muted)",
                            marginTop: "0.2rem",
                          }}
                        >
                          {t.description}
                        </div>
                      )}
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
                          <>
                            <table className="data-table" style={{ margin: 0 }}>
                              <thead>
                                <tr>
                                  <th>Column</th>
                                  <th>PK</th>
                                  <th>SQL Alias</th>
                                  <th>Description</th>
                                  <th>Visible To (Read)</th>
                                  <th>Writable By (R/W)</th>
                                  <th>Masking</th>
                                  <th>Scope</th>
                                </tr>
                              </thead>
                              <tbody>
                                {t.columns.map((c) => (
                                  <Fragment key={c.id}>
                                    <tr>
                                      <td>
                                        <code>{c.columnName}</code>
                                        {c.nativeFilterType && (
                                          <span
                                            style={{
                                              marginLeft: "0.4rem",
                                              fontSize: "0.65rem",
                                              padding: "0.1rem 0.35rem",
                                              borderRadius: "0.25rem",
                                              background:
                                                c.nativeFilterType === "path_param"
                                                  ? "hsl(var(--color-warning) / 0.2)"
                                                  : "hsl(var(--color-info) / 0.2)",
                                              color:
                                                c.nativeFilterType === "path_param"
                                                  ? "hsl(var(--color-warning))"
                                                  : "hsl(var(--color-info))",
                                              fontFamily: "monospace",
                                            }}
                                          >
                                            {c.nativeFilterType === "path_param" ? "path" : "query"}
                                          </span>
                                        )}
                                        {c.isForeignKey && (
                                          <span
                                            style={{
                                              marginLeft: "0.4rem",
                                              fontSize: "0.65rem",
                                              padding: "0.1rem 0.35rem",
                                              borderRadius: "0.25rem",
                                              background: "hsl(var(--color-success) / 0.2)",
                                              color: "hsl(var(--color-success))",
                                              fontFamily: "monospace",
                                            }}
                                          >
                                            FK
                                          </span>
                                        )}
                                        {c.isAlternateKey && (
                                          <span
                                            style={{
                                              marginLeft: "0.4rem",
                                              fontSize: "0.65rem",
                                              padding: "0.1rem 0.35rem",
                                              borderRadius: "0.25rem",
                                              background: "hsl(var(--color-warning) / 0.2)",
                                              color: "hsl(var(--color-warning))",
                                              fontFamily: "monospace",
                                            }}
                                          >
                                            AK
                                          </span>
                                        )}
                                      </td>
                                      <td style={{ textAlign: "center" }}>
                                        {c.isPrimaryKey && (
                                          <span style={{ color: "hsl(var(--color-info))" }}>
                                            &#10003;
                                          </span>
                                        )}
                                      </td>
                                      <td style={{ color: c.alias ? "white" : "var(--text-muted)" }}>{c.computedSqlAlias}</td>
                                      <td className="reasoning-cell">{c.description || ""}</td>
                                      <td>
                                        {c.visibleTo.length > 0 ? c.visibleTo.join(", ") : "all"}
                                      </td>
                                      <td>
                                        {c.writableBy.length > 0 ? c.writableBy.join(", ") : "none"}
                                      </td>
                                      <td>{c.maskType || "none"}</td>
                                      <td>{c.scope || "domain"}</td>
                                    </tr>
                                    {c.maskType && (
                                      <tr>
                                        <td
                                          colSpan={2}
                                          style={{
                                            color: "var(--text-muted)",
                                            fontSize: "0.75rem",
                                            paddingLeft: "1.5rem",
                                          }}
                                        >
                                          ↳{" "}
                                          {c.maskType === "regex"
                                            ? `/${c.maskPattern}/ → ${c.maskReplace}`
                                            : c.maskType === "constant"
                                              ? `= ${c.maskValue ?? "NULL"}`
                                              : `truncate(${c.maskPrecision})`}
                                        </td>
                                        <td
                                          colSpan={4}
                                          style={{
                                            color: "var(--text-muted)",
                                            fontSize: "0.75rem",
                                          }}
                                        >
                                          unmasked:{" "}
                                          {c.unmaskedTo.length > 0
                                            ? c.unmaskedTo.join(", ")
                                            : "none"}
                                        </td>
                                      </tr>
                                    )}
                                  </Fragment>
                                ))}
                              </tbody>
                            </table>
                            {t.apiEndpoint && (
                              <div
                                style={{
                                  padding: "0.5rem 0.75rem",
                                  fontSize: "0.85rem",
                                  color: "var(--text-muted)",
                                }}
                              >
                                API endpoint: <code>{t.apiEndpoint}</code>
                              </div>
                            )}
                            {t.watermarkColumn && (
                              <div
                                style={{
                                  padding: "0.5rem 0.75rem",
                                  fontSize: "0.85rem",
                                  color: "var(--text-muted)",
                                }}
                              >
                                Watermark column: <code>{t.watermarkColumn}</code>
                              </div>
                            )}
                            {t.viewSql && (
                              <div style={{ padding: "0.5rem 0.75rem", fontSize: "0.85rem" }}>
                                <span style={{ color: "var(--text-muted)", marginRight: "0.5rem" }}>
                                  View SQL:
                                </span>
                                <code style={{ fontSize: "0.78rem", wordBreak: "break-all" }}>
                                  {t.viewSql.length > 120
                                    ? t.viewSql.slice(0, 120) + "…"
                                    : t.viewSql}
                                </code>
                              </div>
                            )}
                            <div
                              style={{
                                padding: "0.5rem 0.75rem",
                                fontSize: "0.85rem",
                                display: "flex",
                                alignItems: "center",
                                gap: "0.4rem",
                              }}
                            >
                              <span style={{ color: "var(--text-muted)" }}>Data Product:</span>
                              {t.dataProduct ? (
                                <span
                                  style={{
                                    color: "var(--color-success, #22c55e)",
                                    fontWeight: 600,
                                  }}
                                >
                                  Yes
                                </span>
                              ) : (
                                <span style={{ color: "var(--text-muted)" }}>No</span>
                              )}
                            </div>
                            <div
                              style={{
                                display: "flex",
                                justifyContent: "flex-start",
                                padding: "0.5rem",
                                gap: "0.5rem",
                                flexWrap: "wrap",
                              }}
                            >
                              {t.viewSql && (
                                <button
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    navigate("/sql", { state: { sql: t.viewSql, viewTable: t } });
                                  }}
                                  style={{ padding: "0.25rem 0.6rem", fontSize: "0.78rem" }}
                                  title="Edit this view's SQL in the Explorer"
                                >
                                  {viewsOnly ? "Edit SQL" : "Open in Explorer"}
                                </button>
                              )}
                              {t.canDeployToDb && (
                                <button
                                  onClick={async (e) => {
                                    e.stopPropagation();
                                    setDeploying((prev) => ({ ...prev, [t.id]: true }));
                                    setDeployMsg((prev) => {
                                      const next = { ...prev };
                                      delete next[t.id];
                                      return next;
                                    });
                                    const result = await deployViewToDb(t.id);
                                    setDeploying((prev) => ({ ...prev, [t.id]: false }));
                                    setDeployMsg((prev) => ({ ...prev, [t.id]: result }));
                                    if (result.success) reload();
                                  }}
                                  style={{ padding: "0.25rem 0.6rem", fontSize: "0.78rem" }}
                                  title="Promote this virtual view to a real database view"
                                  disabled={deploying[t.id]}
                                >
                                  {deploying[t.id] ? "Deploying…" : "Deploy to DB"}
                                </button>
                              )}
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleProfile(t.id);
                                }}
                                style={{ padding: "0.25rem 0.6rem", fontSize: "0.78rem" }}
                                title="Sample and profile this table's columns"
                                disabled={tableProfiles[t.id] === "loading"}
                              >
                                {tableProfiles[t.id] === "loading" ? "Profiling…" : "Profile"}
                              </button>
                              <button
                                className="btn-icon"
                                title="View RLS policies for this table"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  navigate("/security/rls", { state: { tableFilter: t.tableName } });
                                }}
                              >
                                Policies
                              </button>
                              <button
                                className="btn-icon"
                                title="Edit"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  startEditing(t);
                                }}
                              >
                                <Pencil size={14} />
                              </button>
                              <button
                                className="btn-icon-danger"
                                title="Delete"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleDelete(t.id);
                                }}
                              >
                                <Trash2 size={14} />
                              </button>
                            </div>
                            {deployMsg[t.id] && (
                              <div
                                style={{
                                  padding: "0.5rem 0.75rem",
                                  fontSize: "0.8rem",
                                  color: deployMsg[t.id].success
                                    ? "var(--color-success, #22c55e)"
                                    : "var(--destructive)",
                                }}
                              >
                                {deployMsg[t.id].message}
                              </div>
                            )}
                            {(() => {
                              const p = tableProfiles[t.id];
                              if (!p || p === "loading") return null;
                              if (typeof p === "string")
                                return (
                                  <div
                                    style={{
                                      padding: "0.5rem 0.75rem",
                                      color: "var(--destructive)",
                                      fontSize: "0.8rem",
                                    }}
                                  >
                                    {p}
                                  </div>
                                );
                              const prof = computeProfile(p.columns, p.rows);
                              return (
                                <div
                                  style={{
                                    borderTop: "1px solid var(--border)",
                                    padding: "0.5rem 0.75rem",
                                  }}
                                >
                                  <div
                                    style={{
                                      fontSize: "0.75rem",
                                      color: "var(--text-muted)",
                                      marginBottom: "0.4rem",
                                    }}
                                  >
                                    Profile — {p.rowCount} sampled rows
                                  </div>
                                  <div style={{ overflowX: "auto" }}>
                                    <table className="data-table" style={{ fontSize: "0.72rem" }}>
                                      <thead>
                                        <tr>
                                          <th>Column</th>
                                          <th title="Null values">Nulls</th>
                                          <th title="Empty strings">Blanks</th>
                                          <th title="Unique values">Distinct</th>
                                          <th>Min</th>
                                          <th>Max</th>
                                          <th>Mean</th>
                                          <th>Top values</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {prof.map((c) => {
                                          const nullPct =
                                            p.rowCount > 0
                                              ? Math.round((c.nullCount / p.rowCount) * 100)
                                              : 0;
                                          const isHighNull = nullPct >= 50;
                                          return (
                                            <tr key={c.col}>
                                              <td
                                                style={{ fontFamily: "monospace", fontWeight: 600 }}
                                              >
                                                {c.col}
                                              </td>
                                              <td
                                                style={{
                                                  color: isHighNull
                                                    ? "var(--destructive)"
                                                    : c.nullCount > 0
                                                      ? "var(--text)"
                                                      : "var(--text-muted)",
                                                }}
                                              >
                                                {c.nullCount > 0
                                                  ? `${c.nullCount} (${nullPct}%)`
                                                  : "—"}
                                              </td>
                                              <td
                                                style={{
                                                  color:
                                                    c.blankCount > 0
                                                      ? "var(--text)"
                                                      : "var(--text-muted)",
                                                }}
                                              >
                                                {c.blankCount > 0 ? c.blankCount : "—"}
                                              </td>
                                              <td>{c.distinctCount}</td>
                                              <td style={{ fontFamily: "monospace" }}>
                                                {c.min !== null ? String(c.min).slice(0, 16) : "—"}
                                              </td>
                                              <td style={{ fontFamily: "monospace" }}>
                                                {c.max !== null ? String(c.max).slice(0, 16) : "—"}
                                              </td>
                                              <td style={{ fontFamily: "monospace" }}>
                                                {c.mean !== null ? c.mean.toFixed(2) : "—"}
                                              </td>
                                              <td>
                                                <div
                                                  style={{
                                                    display: "flex",
                                                    flexWrap: "wrap",
                                                    gap: "0.2rem",
                                                  }}
                                                >
                                                  {c.topValues.map(({ value, count }) => (
                                                    <span
                                                      key={value}
                                                      style={{
                                                        background: "var(--surface)",
                                                        border: "1px solid var(--border)",
                                                        borderRadius: "3px",
                                                        padding: "0 0.3rem",
                                                        fontSize: "0.68rem",
                                                        fontFamily: "monospace",
                                                        whiteSpace: "nowrap",
                                                      }}
                                                    >
                                                      {value.slice(0, 20)}
                                                      <span style={{ color: "var(--text-muted)" }}>
                                                        ×{count}
                                                      </span>
                                                    </span>
                                                  ))}
                                                </div>
                                              </td>
                                            </tr>
                                          );
                                        })}
                                      </tbody>
                                    </table>
                                  </div>
                                </div>
                              );
                            })()}
                          </>
                        ) : (
                          <>
                            <div className="form-card" style={{ marginBottom: "0.75rem" }}>
                              <label>
                                <span
                                  style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
                                >
                                  SQL Alias{" "}
                                  <span
                                    title="The GraphQL/Cypher field name exposed in the API. Defaults to the table name. Changing this renames the entity across all queries and SDL docs."
                                    style={{
                                      cursor: "help",
                                      color: "var(--text-muted)",
                                      fontSize: "0.75rem",
                                      lineHeight: 1,
                                    }}
                                  >
                                    ⓘ
                                  </span>
                                </span>
                                <input
                                  value={editingTable.alias || ""}
                                  onChange={(e) =>
                                    setEditingTable({
                                      ...editingTable,
                                      alias: e.target.value || null,
                                    })
                                  }
                                  placeholder="Semantic name override"
                                />
                              </label>
                              <label>
                                <span
                                  style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
                                >
                                  Naming Convention{" "}
                                  <span
                                    title="Controls how the alias is cased in the API schema. snake_case → my_table, camelCase → myTable, PascalCase → MyTable. 'Inherit' uses the source's convention. Affects GraphQL field names, Cypher labels, and SDL output."
                                    style={{
                                      cursor: "help",
                                      color: "var(--text-muted)",
                                      fontSize: "0.75rem",
                                      lineHeight: 1,
                                    }}
                                  >
                                    ⓘ
                                  </span>
                                </span>
                                <select
                                  value={editingTable.gqlNamingConvention ?? ""}
                                  onChange={(e) =>
                                    setEditingTable({
                                      ...editingTable,
                                      gqlNamingConvention: e.target.value || null,
                                    })
                                  }
                                >
                                  {NAMING_CONVENTIONS.map((nc) => (
                                    <option key={nc.value} value={nc.value}>
                                      {nc.label}
                                    </option>
                                  ))}
                                </select>
                              </label>
                              <label>
                                <span
                                  style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
                                >
                                  Cache TTL (seconds){" "}
                                  <span
                                    title="How long query results for this table are cached in memory. 0 disables caching. Leave blank to inherit the source-level TTL. Reduces load on the source database for frequently-queried tables."
                                    style={{
                                      cursor: "help",
                                      color: "var(--text-muted)",
                                      fontSize: "0.75rem",
                                      lineHeight: 1,
                                    }}
                                  >
                                    ⓘ
                                  </span>
                                </span>
                                <input
                                  type="number"
                                  min={0}
                                  value={
                                    cacheTtlEdits[editingTable.id]?.value ??
                                    (editingTable.cacheTtl != null
                                      ? String(editingTable.cacheTtl)
                                      : "")
                                  }
                                  onChange={(e) =>
                                    setCacheTtlEdits((prev) => ({
                                      ...prev,
                                      [editingTable.id]: {
                                        ...prev[editingTable.id],
                                        value: e.target.value,
                                        dirty: true,
                                      },
                                    }))
                                  }
                                  placeholder="inherit"
                                />
                              </label>
                              <label style={{ gridColumn: "1 / -1" }}>
                                <span
                                  style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
                                >
                                  Description{" "}
                                  <span
                                    title="Human-readable description shown in the API schema (SDL), data catalog, and AI-assisted query generation. Good descriptions improve auto-generated SQL accuracy."
                                    style={{
                                      cursor: "help",
                                      color: "var(--text-muted)",
                                      fontSize: "0.75rem",
                                      lineHeight: 1,
                                    }}
                                  >
                                    ⓘ
                                  </span>
                                </span>
                                <DescriptionField
                                  value={editingTable.description || ""}
                                  onChange={(v) =>
                                    setEditingTable({ ...editingTable, description: v || null })
                                  }
                                  placeholder="Appears in SDL docs"
                                  rows={2}
                                  generating={generatingDesc}
                                  onGenerate={async () => {
                                    setGeneratingDesc(true);
                                    try {
                                      const desc = await generateTableDescription(editingTable.id);
                                      if (desc)
                                        setEditingTable({ ...editingTable, description: desc });
                                    } finally {
                                      setGeneratingDesc(false);
                                    }
                                  }}
                                />
                              </label>
                              {editingTable.viewSql && (
                                <>
                                  <label
                                    style={{
                                      flexDirection: "row",
                                      alignItems: "center",
                                      gap: "0.5rem",
                                      gridColumn: "1 / -1",
                                    }}
                                  >
                                    <input
                                      type="checkbox"
                                      checked={editingTable.materialize}
                                      onChange={(e) =>
                                        setEditingTable({
                                          ...editingTable,
                                          materialize: e.target.checked,
                                        })
                                      }
                                      style={{ width: "auto" }}
                                    />
                                    Materialized View
                                    <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>
                                      (CTAS into mv_cache, refreshed periodically)
                                    </span>
                                  </label>
                                  {editingTable.materialize && (
                                    <label>
                                      Refresh Interval (seconds)
                                      <input
                                        type="number"
                                        min={30}
                                        value={editingTable.mvRefreshInterval}
                                        onChange={(e) =>
                                          setEditingTable({
                                            ...editingTable,
                                            mvRefreshInterval: parseInt(e.target.value, 10) || 300,
                                          })
                                        }
                                      />
                                    </label>
                                  )}
                                </>
                              )}
                              <label
                                style={{
                                  flexDirection: "row",
                                  alignItems: "center",
                                  gap: "0.5rem",
                                  gridColumn: "1 / -1",
                                }}
                              >
                                <input
                                  type="checkbox"
                                  checked={editingTable.dataProduct}
                                  onChange={(e) =>
                                    setEditingTable({
                                      ...editingTable,
                                      dataProduct: e.target.checked,
                                    })
                                  }
                                  style={{ width: "auto" }}
                                />
                                Data Product
                                <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>
                                  (publish to catalog / export to Atlas, Atlan, etc.)
                                </span>
                              </label>
                              {editingTable.apiEndpoint && (
                                <label style={{ gridColumn: "1 / -1" }}>
                                  API Endpoint
                                  <input
                                    readOnly
                                    value={editingTable.apiEndpoint}
                                    style={{ color: "var(--text-muted)", cursor: "default" }}
                                  />
                                </label>
                              )}
                              {
                                <label>
                                  <span
                                    style={{
                                      display: "flex",
                                      alignItems: "center",
                                      gap: "0.25rem",
                                    }}
                                  >
                                    Watermark Column{" "}
                                    <span
                                      title="A timestamp or incrementing integer column used to detect new/changed rows for CDC (Change Data Capture) and real-time subscriptions. Required for GraphQL subscriptions on SQL sources; optional for CDC sources like PostgreSQL that use triggers."
                                      style={{
                                        cursor: "help",
                                        color: "var(--text-muted)",
                                        fontSize: "0.75rem",
                                        lineHeight: 1,
                                      }}
                                    >
                                      ⓘ
                                    </span>
                                  </span>
                                  <span
                                    style={{ fontWeight: "normal", color: "var(--text-muted)" }}
                                  >
                                    {CDC_TYPES.has(
                                      sources.find((s) => s.id === editingTable.sourceId)?.type ??
                                        "",
                                    )
                                      ? "(optional — polling fallback if triggers unavailable)"
                                      : "(required for subscriptions)"}
                                  </span>
                                  <select
                                    value={editingTable.watermarkColumn || ""}
                                    onChange={(e) =>
                                      setEditingTable({
                                        ...editingTable,
                                        watermarkColumn: e.target.value || null,
                                      })
                                    }
                                  >
                                    <option value="">
                                      {CDC_TYPES.has(
                                        sources.find((s) => s.id === editingTable.sourceId)?.type ??
                                          "",
                                      )
                                        ? "None (use triggers)"
                                        : "None (no subscriptions)"}
                                    </option>
                                    {editingTable.columns
                                      .filter((c) => {
                                        const dt = editingColumnTypes[c.columnName];
                                        return !dt || isWatermarkEligible(dt);
                                      })
                                      .map((c) => (
                                        <option key={c.columnName} value={c.columnName}>
                                          {c.columnName}
                                          {editingColumnTypes[c.columnName]
                                            ? ` (${editingColumnTypes[c.columnName]})`
                                            : ""}
                                        </option>
                                      ))}
                                  </select>
                                </label>
                              }
                            </div>
                            {(() => {
                              const NOSQL = new Set(["mongodb", "cassandra"]);
                              const src = sources.find((s) => s.id === editingTable.sourceId);
                              const isMutable = src && !NOSQL.has((src.type ?? "").toLowerCase());
                              return isMutable ? (
                                <ColumnPresetsEditor
                                  presets={editingTable.columnPresets}
                                  columns={editingTable.columns.map((c) => c.columnName)}
                                  columnTypes={editingColumnTypes}
                                  onChange={(presets) =>
                                    setEditingTable({ ...editingTable, columnPresets: presets })
                                  }
                                />
                              ) : null;
                            })()}
                            <table className="data-table" style={{ margin: "0 0 0.5rem" }}>
                              <thead>
                                <tr>
                                  <th>Column</th>
                                  <th>PK</th>
                                  <th>SQL Alias</th>
                                  <th>Description</th>
                                  <th>Visible To (Read)</th>
                                  <th>Writable By (R/W)</th>
                                  <th>Masking</th>
                                  <th>Scope</th>
                                </tr>
                              </thead>
                              <tbody>
                                {editingTable.columns.map((c, i) => (
                                  <Fragment key={c.id}>
                                    <tr>
                                      <td>
                                        <code>{c.columnName}</code>
                                        {c.nativeFilterType && (
                                          <span
                                            style={{
                                              marginLeft: "0.4rem",
                                              fontSize: "0.65rem",
                                              padding: "0.1rem 0.35rem",
                                              borderRadius: "0.25rem",
                                              background:
                                                c.nativeFilterType === "path_param"
                                                  ? "hsl(var(--color-warning) / 0.2)"
                                                  : "hsl(var(--color-info) / 0.2)",
                                              color:
                                                c.nativeFilterType === "path_param"
                                                  ? "hsl(var(--color-warning))"
                                                  : "hsl(var(--color-info))",
                                              fontFamily: "monospace",
                                            }}
                                          >
                                            {c.nativeFilterType === "path_param" ? "path" : "query"}
                                          </span>
                                        )}
                                        {c.isForeignKey && (
                                          <span
                                            style={{
                                              marginLeft: "0.4rem",
                                              fontSize: "0.65rem",
                                              padding: "0.1rem 0.35rem",
                                              borderRadius: "0.25rem",
                                              background: "hsl(var(--color-success) / 0.2)",
                                              color: "hsl(var(--color-success))",
                                              fontFamily: "monospace",
                                            }}
                                          >
                                            FK
                                          </span>
                                        )}
                                        {c.isAlternateKey && (
                                          <span
                                            style={{
                                              marginLeft: "0.4rem",
                                              fontSize: "0.65rem",
                                              padding: "0.1rem 0.35rem",
                                              borderRadius: "0.25rem",
                                              background: "hsl(var(--color-warning) / 0.2)",
                                              color: "hsl(var(--color-warning))",
                                              fontFamily: "monospace",
                                            }}
                                          >
                                            AK
                                          </span>
                                        )}
                                      </td>
                                      <td style={{ textAlign: "center" }}>
                                        <input
                                          type="checkbox"
                                          title="Primary Key"
                                          checked={c.isPrimaryKey || false}
                                          onChange={(e) =>
                                            updateEditCol(i, "isPrimaryKey", e.target.checked)
                                          }
                                        />
                                      </td>
                                      <td>
                                        <input
                                          value={c.alias || c.computedSqlAlias}
                                          onChange={(e) =>
                                            updateEditCol(i, "alias", e.target.value)
                                          }
                                        />
                                      </td>
                                      <td>
                                        <DescriptionField
                                          value={c.description || ""}
                                          onChange={(v) => updateEditCol(i, "description", v)}
                                          placeholder="Column description"
                                          rows={1}
                                          generating={generatingColDesc === c.columnName}
                                          onGenerate={async () => {
                                            setGeneratingColDesc(c.columnName);
                                            try {
                                              const desc = await generateColumnDescription(
                                                editingTable.id,
                                                c.columnName,
                                              );
                                              if (desc) updateEditCol(i, "description", desc);
                                            } catch (err) {
                                              console.error(
                                                "generateColumnDescription failed:",
                                                err,
                                              );
                                            } finally {
                                              setGeneratingColDesc(null);
                                            }
                                          }}
                                        />
                                      </td>
                                      <td>
                                        <MultiSelect
                                          options={roles.map((r) => ({ id: r.id, label: r.id }))}
                                          value={c.visibleTo}
                                          onChange={(selected) =>
                                            updateEditCol(i, "visibleTo", selected)
                                          }
                                        />
                                      </td>
                                      <td>
                                        <MultiSelect
                                          options={roles.map((r) => ({ id: r.id, label: r.id }))}
                                          value={c.writableBy}
                                          onChange={(selected) =>
                                            updateEditCol(i, "writableBy", selected)
                                          }
                                        />
                                      </td>
                                      <td>
                                        <select
                                          value={c.maskType || ""}
                                          onChange={(e) =>
                                            updateEditCol(i, "maskType", e.target.value)
                                          }
                                        >
                                          <option value="">None</option>
                                          <option value="regex">Regex</option>
                                          <option value="constant">Constant</option>
                                          <option value="truncate">Truncate</option>
                                        </select>
                                      </td>
                                      <td>
                                        <select
                                          value={c.scope || "domain"}
                                          onChange={(e) =>
                                            updateEditCol(i, "scope", e.target.value)
                                          }
                                        >
                                          <option value="domain">domain</option>
                                          <option value="public">public</option>
                                          <option value="restricted">restricted</option>
                                        </select>
                                      </td>
                                    </tr>
                                    {c.maskType && (
                                      <tr>
                                        <td
                                          colSpan={2}
                                          style={{
                                            paddingLeft: "1.5rem",
                                            color: "var(--text-muted)",
                                            fontSize: "0.75rem",
                                          }}
                                        >
                                          ↳ masking template
                                        </td>
                                        {c.maskType === "regex" && (
                                          <>
                                            <td>
                                              <input
                                                value={c.maskPattern || ""}
                                                onChange={(e) =>
                                                  updateEditCol(i, "maskPattern", e.target.value)
                                                }
                                                placeholder="regex pattern"
                                              />
                                            </td>
                                            <td>
                                              <input
                                                value={c.maskReplace || ""}
                                                onChange={(e) =>
                                                  updateEditCol(i, "maskReplace", e.target.value)
                                                }
                                                placeholder="replacement"
                                              />
                                            </td>
                                          </>
                                        )}
                                        {c.maskType === "constant" && (
                                          <td colSpan={2}>
                                            <input
                                              value={c.maskValue || ""}
                                              onChange={(e) =>
                                                updateEditCol(i, "maskValue", e.target.value)
                                              }
                                              placeholder="constant value (NULL, 0, ***)"
                                            />
                                          </td>
                                        )}
                                        {c.maskType === "truncate" && (
                                          <td colSpan={2}>
                                            <select
                                              value={c.maskPrecision || ""}
                                              onChange={(e) =>
                                                updateEditCol(i, "maskPrecision", e.target.value)
                                              }
                                            >
                                              <option value="">Select precision...</option>
                                              <option value="year">Year</option>
                                              <option value="month">Month</option>
                                              <option value="day">Day</option>
                                              <option value="hour">Hour</option>
                                            </select>
                                          </td>
                                        )}
                                        <td colSpan={2}>
                                          <MultiSelect
                                            options={roles.map((r) => ({ id: r.id, label: r.id }))}
                                            value={c.unmaskedTo}
                                            onChange={(selected) =>
                                              updateEditCol(i, "unmaskedTo", selected)
                                            }
                                          />
                                        </td>
                                      </tr>
                                    )}
                                  </Fragment>
                                ))}
                              </tbody>
                            </table>
                            <div
                              style={{
                                display: "flex",
                                gap: "0.5rem",
                                justifyContent: "flex-end",
                                padding: "0.75rem 0.5rem",
                              }}
                            >
                              <button
                                className="btn-icon"
                                title="Cancel"
                                onClick={cancelEditing}
                                disabled={saving}
                              >
                                <X size={14} />
                              </button>
                              <button
                                className="btn-icon-primary"
                                title="Save"
                                onClick={handleSaveEdit}
                                disabled={saving}
                              >
                                {saving ? (
                                  <Loader2
                                    size={14}
                                    style={{ animation: "spin 1s linear infinite" }}
                                  />
                                ) : (
                                  <Save size={14} />
                                )}
                              </button>
                            </div>
                          </>
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
