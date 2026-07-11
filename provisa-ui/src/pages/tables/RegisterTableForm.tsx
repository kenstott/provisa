// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8a2d14-7b3c-4e9f-a0d1-6c4e8b2f7a31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, Fragment } from "react";
import { toSnakeCase } from "../../naming";
import { MultiSelect } from "../../components/MultiSelect";
import { useAvailableSchemas, useAvailableTables } from "../../hooks/useAdminQueries";
import type { RegisteredTable, Source } from "../../types/admin";
import type { Role } from "../../types/auth";
import type { ColumnForm } from "./types";
import { CDC_TYPES } from "./constants";
import { isWatermarkEligible, normalizeDomain } from "./helpers";

interface RegisterTableFormProps {
  sources: Source[];
  domainHints: string[];
  domainAccess: string[];
  checkedDomains: Set<string>;
  domainsEnabled: boolean;
  tables: RegisteredTable[];
  roles: Role[];
  getAvailableColumnsMetadata: (
    sourceId: string,
    schemaName: string,
    tableName: string,
  ) => Promise<
    {
      name: string;
      dataType: string;
      comment?: string | null;
      nativeFilterType?: string | null;
      isPrimaryKey?: boolean | null;
    }[]
  >;
  suggestTableAlias: (tableName: string, domainId: string, sourceId: string) => Promise<string>;
  registerTable: (input: Record<string, unknown>) => Promise<{ success: boolean; message: string }>;
  onSuccess: () => void;
  setError: (e: string | null) => void;
}

export function RegisterTableForm({
  sources,
  domainHints,
  domainAccess,
  checkedDomains,
  domainsEnabled,
  tables,
  roles,
  getAvailableColumnsMetadata,
  suggestTableAlias,
  registerTable,
  onSuccess,
  setError,
}: RegisterTableFormProps) {
  const [sourceId, setSourceId] = useState("");
  const [domainId, setDomainId] = useState("");
  const [schemaName, setSchemaName] = useState("");
  const [tableName, setTableName] = useState("");
  const [tableAlias, setTableAlias] = useState("");
  const [tableDescription, setTableDescription] = useState("");
  const [columns, setColumns] = useState<ColumnForm[]>([]);
  const [watermarkColumn, setWatermarkColumn] = useState<string>("");
  const [dataProduct, setDataProduct] = useState(false);
  const [loadingColumns, setLoadingColumns] = useState(false);

  const { schemas: availableSchemas, loading: loadingSchemas } = useAvailableSchemas(
    sourceId || null,
  );
  const isFixedSchema = availableSchemas.length === 1;
  const { tables: availableTables, loading: loadingTables } = useAvailableTables(
    sourceId && schemaName ? sourceId : null,
    schemaName || null,
  );

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- form cascade reset: dependent fields cleared when source selection changes
    setSchemaName("");
    setTableName("");
    setTableDescription("");
    setColumns([]);
  }, [sourceId]);

  useEffect(() => {
    if (availableSchemas.length === 1) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- auto-select the only available schema; not derivable without an effect because schemas load asynchronously
      setSchemaName(availableSchemas[0]);
    }
  }, [availableSchemas, sourceId]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- form cascade reset: dependent fields cleared when schema selection changes
    setTableName("");
    setTableDescription("");
    setColumns([]);
  }, [sourceId, schemaName]);

  // Auto-populate table description from physical database comment
  useEffect(() => {
    if (!tableName) return;
    const meta = availableTables.find((t) => t.name === tableName);
    // eslint-disable-next-line react-hooks/set-state-in-effect -- auto-populate description from physical database comment when table is selected
    if (meta?.comment) setTableDescription(meta.comment);
  }, [tableName, availableTables]);

  // Auto-generate alias from table name using snake_case convention
  useEffect(() => {
    if (!tableName || !domainId || !sourceId) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- cascade reset: alias cleared when table/domain/source deselected
      setTableAlias("");
      return;
    }
    suggestTableAlias(tableName, domainId, sourceId).then(setTableAlias);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- suggestTableAlias is a stable hook callback; re-run only when the table selection changes
  }, [tableName, domainId, sourceId]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- cascade reset: columns cleared before async fetch when table selection changes
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
            nativeFilterType: c.nativeFilterType ?? null,
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

  const updateCol = (i: number, key: keyof ColumnForm, value: string | boolean | string[]) => {
    const next = [...columns];
    next[i] = { ...next[i], [key]: value };
    setColumns(next);
  };

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
      setSourceId("");
      setDomainId("");
      setSchemaName("");
      setTableName("");
      setTableAlias("");
      setTableDescription("");
      setColumns([]);
      setWatermarkColumn("");
      setDataProduct(false);
      onSuccess();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div data-tour="tables-form" className="form-card">
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
                rt.sourceId === sourceId && toSnakeCase(rt.tableName) === toSnakeCase(t.name),
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
  );
}
