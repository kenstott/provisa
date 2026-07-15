// Copyright (c) 2026 Kenneth Stott
// Canary: 4a8c1f59-6e3d-4b2a-9f7c-0d5e8b1a3c72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment } from "react";
import { Check, X, Loader2 } from "lucide-react";
import { MultiSelect } from "../../components/MultiSelect";
import { ColumnPresetsEditor } from "../../components/admin/ColumnPresetsEditor";
import type { RegisteredTable, Source } from "../../types/admin";
import type { Role } from "../../types/auth";
import type { PlatformSettings } from "../../api/admin";
import { sourceProbeTypes } from "../../liveCapability";
import { NAMING_CONVENTIONS } from "./constants";
import { DescriptionField } from "./DescriptionField";
import { LiveDeliveryFieldset } from "./LiveDeliveryFieldset";

interface CacheTtlEdit {
  value: string;
  dirty: boolean;
  saving: boolean;
}

interface TableEditFormProps {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  editingColumnTypes: Record<string, string>;
  cacheTtlEdits: Record<number, CacheTtlEdit>;
  setCacheTtlEdits: React.Dispatch<React.SetStateAction<Record<number, CacheTtlEdit>>>;
  sources: Source[];
  roles: Role[];
  settings: PlatformSettings | null;
  saving: boolean;
  generatingDesc: boolean;
  setGeneratingDesc: React.Dispatch<React.SetStateAction<boolean>>;
  generatingColDesc: string | null;
  setGeneratingColDesc: React.Dispatch<React.SetStateAction<string | null>>;
  generateTableDescription: (id: number) => Promise<string>;
  generateColumnDescription: (id: number, colName: string) => Promise<string>;
  cancelEditing: () => void;
  handleSaveEdit: () => void;
  updateEditCol: (i: number, key: string, value: string | string[] | boolean) => void;
}

export function TableEditForm({
  editingTable,
  setEditingTable,
  editingColumnTypes,
  cacheTtlEdits,
  setCacheTtlEdits,
  sources,
  roles,
  settings,
  saving,
  generatingDesc,
  setGeneratingDesc,
  generatingColDesc,
  setGeneratingColDesc,
  generateTableDescription,
  generateColumnDescription,
  cancelEditing,
  handleSaveEdit,
  updateEditCol,
}: TableEditFormProps) {
  return (
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
              (editingTable.cacheTtl != null ? String(editingTable.cacheTtl) : "")
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
        <label>
          <span
            style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
          >
            Prefer Materialized{" "}
            <span
              title="Force this table to be materialized into the store and federated from there, instead of reached live. Use when the connector is a poor fit for this table's queries. Inherit = follow the source-level default."
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
            value={
              editingTable.preferMaterialized == null
                ? "inherit"
                : editingTable.preferMaterialized
                  ? "on"
                  : "off"
            }
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                preferMaterialized:
                  e.target.value === "inherit" ? null : e.target.value === "on",
              })
            }
          >
            <option value="inherit">Inherit source</option>
            <option value="on">On</option>
            <option value="off">Off</option>
          </select>
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
                if (desc) setEditingTable({ ...editingTable, description: desc });
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
              <span
                style={{ fontWeight: "normal", color: "var(--text-muted)" }}
              >
                (CTAS into mv_cache, refreshed periodically)
              </span>
              <span
                title="Precompute THIS view's SQL into a stored cache table (CTAS into mv_cache) on the Refresh Interval below. Distinct from 'Prefer Materialized' above, which pulls a raw source table into the store — this materializes a Provisa-defined view."
                style={{
                  cursor: "help",
                  color: "var(--text-muted)",
                  fontSize: "0.75rem",
                  lineHeight: 1,
                }}
              >
                ⓘ
              </span>
            </label>
            {editingTable.materialize && (
              <label>
                <span
                  style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
                >
                  Refresh Interval (seconds){" "}
                  <span
                    title="How often the materialized view is rebuilt (CTAS into mv_cache). Governs freshness of the stored table that QUERIES read. Distinct from Cache TTL (which caches query results) and from Poll interval (which paces the live change stream to subscribers)."
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
            {editingTable.materialize && (
              <label
                title="REQ-963 NRT debounce: a burst of upstream changes collapses into one recompute. Quiet = seconds of calm before firing (0 = real-time). Max delay = the hard staleness cap under continuous churn."
              >
                NRT Debounce — quiet / max delay (seconds)
                <div style={{ display: "flex", gap: "0.5rem" }}>
                  <input
                    type="number"
                    min={0}
                    step={0.5}
                    placeholder="quiet"
                    aria-label="NRT debounce quiet seconds"
                    data-testid="mv-debounce-quiet"
                    value={editingTable.mvDebounceQuiet}
                    onChange={(e) =>
                      setEditingTable({
                        ...editingTable,
                        mvDebounceQuiet: parseFloat(e.target.value) || 0,
                      })
                    }
                  />
                  <input
                    type="number"
                    min={0}
                    step={0.5}
                    placeholder="max delay"
                    aria-label="NRT debounce max delay seconds"
                    data-testid="mv-debounce-max-delay"
                    value={editingTable.mvDebounceMaxDelay}
                    onChange={(e) =>
                      setEditingTable({
                        ...editingTable,
                        mvDebounceMaxDelay: parseFloat(e.target.value) || 0,
                      })
                    }
                  />
                </div>
              </label>
            )}
            {editingTable.materialize && (
              <label>
                <span
                  style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
                >
                  Consistency{" "}
                  <span
                    title="REQ-879 cross-instance refresh coordination. shared = fleet-coordinated — one instance refreshes at a time via a CAS lease on the shared catalog, so every instance reads one snapshot-consistent copy. distributed = each instance materializes its own copy independently (eventually consistent; only safe for a deterministic view over a source that quiesces within a refresh cycle)."
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
                  aria-label="MV consistency tier"
                  data-testid="mv-consistency"
                  value={editingTable.mvConsistency}
                  onChange={(e) =>
                    setEditingTable({
                      ...editingTable,
                      mvConsistency: e.target.value,
                    })
                  }
                >
                  <option value="shared">shared (fleet-coordinated)</option>
                  <option value="distributed">distributed (per-instance)</option>
                </select>
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
            checked={editingTable.enableAggregates}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                enableAggregates: e.target.checked,
              })
            }
            style={{ width: "auto" }}
          />
          Enable Aggregates
          <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>
            (expose <code>_aggregate</code> root field in GraphQL)
          </span>
        </label>
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
            checked={editingTable.enableGroupBy}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                enableGroupBy: e.target.checked,
              })
            }
            style={{ width: "auto" }}
          />
          Enable Group By
          <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>
            (expose <code>_group_by</code> root field in GraphQL)
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
        <label>
          <span
            style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}
          >
            Change Signal{" "}
            <span
              title="How Provisa learns rows changed. ttl = refresh on the Cache TTL timer; probe = source-native freshness query, re-pull only on change; ttl_probe = probe after the TTL floor elapses; native/debezium/kafka = source push. Inherit = follow the source-level default."
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
            value={editingTable.changeSignal ?? ""}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                changeSignal: e.target.value || null,
              })
            }
          >
            <option value="">Inherit source</option>
            <option value="ttl">ttl (timer)</option>
            <option value="probe">probe (freshness query)</option>
            <option value="ttl_probe">probe + ttl</option>
            <option value="native">native (source push)</option>
            <option value="debezium">debezium</option>
            <option value="kafka">kafka</option>
          </select>
        </label>
        {(editingTable.changeSignal === "ttl" ||
          editingTable.changeSignal === "ttl_probe") &&
          (() => {
            const cs = sources.find((s) => s.id === editingTable.sourceId);
            // Mirror the Cache TTL input's value resolution: the staged edit
            // (cacheTtlEdits) wins, then the table value, then the source.
            const staged = cacheTtlEdits[editingTable.id]?.value;
            const tableTtl =
              staged != null && staged !== ""
                ? Number(staged)
                : staged === ""
                  ? null
                  : editingTable.cacheTtl;
            const effTtl = tableTtl ?? cs?.cacheTtl ?? null;
            const fromTable = tableTtl != null;
            return effTtl == null ? (
              <p
                style={{
                  gridColumn: "1 / -1",
                  margin: 0,
                  color: "var(--warning, #d19a00)",
                  fontSize: "0.8rem",
                }}
              >
                ttl needs an interval — set <strong>Cache TTL (seconds)</strong>{" "}
                above (neither the table nor the source defines one).
              </p>
            ) : (
              <p
                style={{
                  gridColumn: "1 / -1",
                  margin: 0,
                  color: "var(--text-muted)",
                  fontSize: "0.8rem",
                }}
              >
                Refreshes every {effTtl}s (from {fromTable ? "table" : "source"}{" "}
                Cache TTL).
              </p>
            );
          })()}
        {(editingTable.changeSignal === "debezium" ||
          editingTable.changeSignal === "kafka") &&
          (() => {
            const cs = sources.find((s) => s.id === editingTable.sourceId);
            const hasCdc = !!cs?.cdc?.bootstrapServers;
            const hasPk = editingTable.columns.some((c) => c.isPrimaryKey);
            // Debezium derives {prefix}.{schema}.{table}; a plain Kafka feed
            // consumes the topic named for the table (kafka_provider: topic=table).
            const topic =
              editingTable.changeSignal === "debezium"
                ? `${cs?.cdc?.topicPrefix}.${editingTable.schemaName}.${editingTable.tableName}`
                : editingTable.tableName;
            return (
              <>
                {hasCdc ? (
                  <p
                    style={{
                      gridColumn: "1 / -1",
                      margin: 0,
                      color: "var(--text-muted)",
                      fontSize: "0.8rem",
                    }}
                  >
                    Provisa consumes the source's CDC transport (
                    {cs!.cdc!.bootstrapServers}); topic <code>{topic}</code>. No
                    per-table transport config needed.
                  </p>
                ) : (
                  <p
                    style={{
                      gridColumn: "1 / -1",
                      margin: 0,
                      color: "var(--warning, #d19a00)",
                      fontSize: "0.8rem",
                    }}
                  >
                    {editingTable.changeSignal} needs the source's CDC transport
                    (bootstrap servers, topic prefix) — configure it on the{" "}
                    <strong>source</strong>. It isn't set, so no change events
                    will arrive.
                  </p>
                )}
                {!hasPk && (
                  <p
                    style={{
                      gridColumn: "1 / -1",
                      margin: 0,
                      color: "var(--warning, #d19a00)",
                      fontSize: "0.8rem",
                    }}
                  >
                    No primary key set — the receiver can't apply updates or
                    deletes (tombstones) without one. Mark a <strong>PK</strong>{" "}
                    column below.
                  </p>
                )}
              </>
            );
          })()}
        {(editingTable.changeSignal === "probe" ||
          editingTable.changeSignal === "ttl_probe") &&
          (() => {
            const src = sources.find((s) => s.id === editingTable.sourceId);
            const caps = sourceProbeTypes(src?.type);
            if (caps.length === 0) return null;
            return (
              <label style={{ gridColumn: "1 / -1" }}>
                <span
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "0.25rem",
                  }}
                >
                  Probe type{" "}
                  <span
                    title="How the freshness probe detects change, gated by source type. watermark → append (fetch rows past MAX(watermark)); hash → replace (content token, e.g. ETag/checksum); count → replace (row-count delta, coarse); none → replace on cadence (the output content-hash still suppresses an unchanged ripple). Auto = resolve per source class."
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
                  value={editingTable.probeType ?? ""}
                  onChange={(e) =>
                    setEditingTable({
                      ...editingTable,
                      probeType: e.target.value || null,
                    })
                  }
                >
                  <option value="">Auto (per source type)</option>
                  {caps.map((pt) => (
                    <option key={pt} value={pt}>
                      {pt}
                      {pt === "watermark" ? " → append" : " → replace"}
                    </option>
                  ))}
                </select>
              </label>
            );
          })()}
        {(editingTable.changeSignal === "probe" ||
          editingTable.changeSignal === "ttl_probe") && (
          <label style={{ gridColumn: "1 / -1" }}>
            <span
              style={{
                display: "flex",
                alignItems: "center",
                gap: "0.25rem",
              }}
            >
              Freshness probe{" "}
              <span
                title="Source-native query returning one comparable token; blank → MAX(watermark). Provisa compares stored vs. fresh by equality and re-pulls only on change."
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
              value={editingTable.probeQuery ?? ""}
              onChange={(e) =>
                setEditingTable({
                  ...editingTable,
                  probeQuery: e.target.value || null,
                })
              }
              placeholder="SELECT MAX(id) FROM …"
            />
          </label>
        )}
        <LiveDeliveryFieldset
          editingTable={editingTable}
          setEditingTable={setEditingTable}
          editingColumnTypes={editingColumnTypes}
          sources={sources}
          settings={settings}
        />
      </div>
      {(() => {
        const NOSQL = new Set(["mongodb", "cassandra"]);
        const src = sources.find((s) => s.id === editingTable.sourceId);
        // Views and materialized views are read-only — no INSERT/UPDATE path, so presets never apply.
        const isReadOnlyView = editingTable.viewSql != null;
        const isMutable = src && !NOSQL.has((src.type ?? "").toLowerCase()) && !isReadOnlyView;
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
            <Check size={14} />
          )}
        </button>
      </div>
    </>
  );
}
