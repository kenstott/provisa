import { useState, useEffect, useRef, useLayoutEffect, Fragment } from "react";
import { createPortal } from "react-dom";
import {
  fetchTables, fetchSources, fetchDomains, fetchRoles,
  fetchAvailableSchemas, fetchAvailableTables, fetchAvailableColumnsMetadata,
  registerTable, deleteTable, updateTable,
} from "../api/admin";
import type { TableMetadata } from "../api/admin";
import type { RegisteredTable, Source, Domain } from "../types/admin";
import type { Role } from "../types/auth";

function MultiSelect({ options, value, onChange }: {
  options: { id: string; label: string }[];
  value: string[];
  onChange: (selected: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number; width: number } | null>(null);
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const updatePos = () => {
    if (triggerRef.current) {
      const rect = triggerRef.current.getBoundingClientRect();
      setPos({ top: rect.bottom + 2, left: rect.left, width: rect.width });
    }
  };

  useLayoutEffect(() => {
    if (open) updatePos();
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      if (
        triggerRef.current && !triggerRef.current.contains(target) &&
        dropdownRef.current && !dropdownRef.current.contains(target)
      ) setOpen(false);
    };
    document.addEventListener("mousedown", handleClickOutside);
    window.addEventListener("scroll", updatePos, true);
    window.addEventListener("resize", updatePos);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      window.removeEventListener("scroll", updatePos, true);
      window.removeEventListener("resize", updatePos);
    };
  }, [open]);

  const display = value.length > 0 ? value.join(", ") : "all";

  return (
    <div className="multiselect" ref={triggerRef}>
      <div className="multiselect-trigger" onClick={() => setOpen(!open)}>
        <span className="multiselect-text">{display}</span>
        <span className="multiselect-arrow">{open ? "\u25B4" : "\u25BE"}</span>
      </div>
      {open && pos && createPortal(
        <div
          className="multiselect-dropdown"
          ref={dropdownRef}
          style={{ top: pos.top, left: pos.left, width: pos.width }}
        >
          {options.map((opt) => (
            <label key={opt.id} className="multiselect-option">
              <input
                type="checkbox"
                checked={value.includes(opt.id)}
                onChange={(e) => {
                  const next = e.target.checked
                    ? [...value, opt.id]
                    : value.filter((v) => v !== opt.id);
                  onChange(next);
                }}
              />
              {opt.label}
            </label>
          ))}
        </div>,
        document.body
      )}
    </div>
  );
}

interface ColumnForm {
  name: string;
  visibleTo: string;
  writableBy: string;
  unmaskedTo: string;
  maskType: string;
  maskPattern: string;
  maskReplace: string;
  maskValue: string;
  maskPrecision: string;
  alias: string;
  description: string;
  selected: boolean;
}

export function TablesPage() {
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [domains, setDomains] = useState<Domain[]>([]);
  const [roles, setRoles] = useState<Role[]>([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Form state
  const [sourceId, setSourceId] = useState("");
  const [domainId, setDomainId] = useState("");
  const [schemaName, setSchemaName] = useState("");
  const [tableName, setTableName] = useState("");
  const [tableAlias, setTableAlias] = useState("");
  const [tableDescription, setTableDescription] = useState("");
  const [governance, setGovernance] = useState("open");
  const [columns, setColumns] = useState<ColumnForm[]>([]);

  // Discovery state
  const [availableSchemas, setAvailableSchemas] = useState<string[]>([]);
  const [availableTables, setAvailableTables] = useState<TableMetadata[]>([]);
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);
  const [loadingColumns, setLoadingColumns] = useState(false);

  // Inline edit state for expanded table
  const [editingTable, setEditingTable] = useState<RegisteredTable | null>(null);
  const [saving, setSaving] = useState(false);

  const reload = () => {
    setLoading(true);
    Promise.all([fetchTables(), fetchSources(), fetchDomains(), fetchRoles()])
      .then(([t, s, d, r]) => { setTables(t); setSources(s); setDomains(d); setRoles(r); })
      .finally(() => setLoading(false));
  };

  useEffect(reload, []);

  useEffect(() => {
    setSchemaName(""); setTableName(""); setTableDescription(""); setColumns([]);
    setAvailableSchemas([]); setAvailableTables([]);
    if (!sourceId) return;
    setLoadingSchemas(true);
    fetchAvailableSchemas(sourceId)
      .then(setAvailableSchemas)
      .catch(() => setAvailableSchemas([]))
      .finally(() => setLoadingSchemas(false));
  }, [sourceId]);

  useEffect(() => {
    setTableName(""); setTableDescription(""); setColumns([]); setAvailableTables([]);
    if (!sourceId || !schemaName) return;
    setLoadingTables(true);
    fetchAvailableTables(sourceId, schemaName)
      .then(setAvailableTables)
      .catch(() => setAvailableTables([]))
      .finally(() => setLoadingTables(false));
  }, [sourceId, schemaName]);

  // Auto-populate table description from physical database comment
  useEffect(() => {
    if (!tableName) return;
    const meta = availableTables.find((t) => t.name === tableName);
    if (meta?.comment) setTableDescription(meta.comment);
  }, [tableName, availableTables]);

  useEffect(() => {
    setColumns([]);
    if (!sourceId || !schemaName || !tableName) return;
    setLoadingColumns(true);
    fetchAvailableColumnsMetadata(sourceId, schemaName, tableName)
      .then((cols) =>
        setColumns(cols.map((c) => ({
          name: c.name,
          visibleTo: "",
          writableBy: "",
          unmaskedTo: "",
          maskType: "",
          maskPattern: "",
          maskReplace: "",
          maskValue: "",
          maskPrecision: "",
          alias: "",
          description: c.comment || "",
          selected: true,
        })))
      )
      .catch(() => setColumns([]))
      .finally(() => setLoadingColumns(false));
  }, [sourceId, schemaName, tableName]);

  const handleSubmit = async () => {
    setError(null);
    const selectedCols = columns
      .filter((c) => c.selected)
      .map((c) => ({
        name: c.name,
        visibleTo: c.visibleTo.trim() ? c.visibleTo.split(",").map((s) => s.trim()) : [],
        writableBy: c.writableBy.trim() ? c.writableBy.split(",").map((s) => s.trim()) : [],
        unmaskedTo: c.unmaskedTo.trim() ? c.unmaskedTo.split(",").map((s) => s.trim()) : [],
        maskType: c.maskType || undefined,
        maskPattern: c.maskPattern || undefined,
        maskReplace: c.maskReplace || undefined,
        maskValue: c.maskValue || undefined,
        maskPrecision: c.maskPrecision || undefined,
        alias: c.alias || undefined,
        description: c.description || undefined,
      }));
    if (!sourceId || !domainId || !schemaName || !tableName) {
      setError("Source, domain, schema, and table name are required.");
      return;
    }
    if (selectedCols.length === 0) {
      setError("At least one column must be selected.");
      return;
    }
    try {
      const result = await registerTable({
        sourceId, domainId, schemaName, tableName, governance,
        alias: tableAlias || undefined,
        description: tableDescription || undefined,
        columns: selectedCols,
      });
      if (!result.success) { setError(result.message); return; }
      setShowForm(false);
      setSourceId(""); setDomainId(""); setSchemaName(""); setTableName("");
      setTableAlias(""); setTableDescription("");
      setGovernance("open"); setColumns([]);
      reload();
    } catch (e: any) { setError(e.message); }
  };

  const handleDelete = async (id: number) => {
    if (!confirm("Delete this table registration?")) return;
    try { await deleteTable(id); reload(); } catch (e: any) { setError(e.message); }
  };

  const updateCol = (i: number, key: keyof ColumnForm, value: string | boolean) => {
    const next = [...columns];
    next[i] = { ...next[i], [key]: value };
    setColumns(next);
  };

  const startEditing = (t: RegisteredTable) => {
    setEditingTable(JSON.parse(JSON.stringify(t)));
  };

  const cancelEditing = () => {
    setEditingTable(null);
  };

  const updateEditCol = (i: number, key: string, value: string) => {
    if (!editingTable) return;
    const next = { ...editingTable };
    next.columns = [...next.columns];
    if (key === "visibleTo") {
      next.columns[i] = { ...next.columns[i], visibleTo: value.split(",").map((s) => s.trim()).filter(Boolean) };
    } else if (key === "writableBy") {
      next.columns[i] = { ...next.columns[i], writableBy: value.split(",").map((s) => s.trim()).filter(Boolean) };
    } else if (key === "unmaskedTo") {
      next.columns[i] = { ...next.columns[i], unmaskedTo: value.split(",").map((s) => s.trim()).filter(Boolean) };
    } else {
      next.columns[i] = { ...next.columns[i], [key]: value };
    }
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
        governance: editingTable.governance,
        alias: editingTable.alias || undefined,
        description: editingTable.description || undefined,
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
        })),
      });
      if (!result.success) { setError(result.message); return; }
      setEditingTable(null);
      reload();
    } catch (e: any) { setError(e.message); }
    finally { setSaving(false); }
  };

  if (loading) return <div className="page">Loading tables...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Registered Tables</h2>
        <button onClick={() => setShowForm(!showForm)}>
          {showForm ? "Cancel" : "Register Table"}
        </button>
      </div>

      {error && <div className="error">{error}</div>}

      {showForm && (
        <div className="form-card">
          <label>
            Source
            <select value={sourceId} onChange={(e) => setSourceId(e.target.value)}>
              <option value="">Select source...</option>
              {sources.map((s) => <option key={s.id} value={s.id}>{s.id}</option>)}
            </select>
          </label>
          <label>
            Domain
            <select value={domainId} onChange={(e) => setDomainId(e.target.value)}>
              <option value="">Select domain...</option>
              {domains.map((d) => (
                <option key={d.id} value={d.id}>{d.id}{d.description ? ` — ${d.description}` : ""}</option>
              ))}
            </select>
          </label>
          <label>
            Schema
            <select value={schemaName} onChange={(e) => setSchemaName(e.target.value)} disabled={!sourceId || loadingSchemas}>
              <option value="">{loadingSchemas ? "Loading..." : "Select schema..."}</option>
              {availableSchemas.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
          </label>
          <label>
            Table
            <select value={tableName} onChange={(e) => setTableName(e.target.value)} disabled={!schemaName || loadingTables}>
              <option value="">{loadingTables ? "Loading..." : "Select table..."}</option>
              {availableTables.map((t) => <option key={t.name} value={t.name}>{t.name}</option>)}
            </select>
          </label>
          <label>
            Alias <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>(optional)</span>
            <input value={tableAlias} onChange={(e) => setTableAlias(e.target.value)} placeholder="GraphQL name override" />
          </label>
          <label>
            Description <span style={{ fontWeight: "normal", color: "var(--text-muted)" }}>(optional)</span>
            <input value={tableDescription} onChange={(e) => setTableDescription(e.target.value)} placeholder="Appears in SDL docs" />
          </label>
          <label>
            Governance
            <select value={governance} onChange={(e) => setGovernance(e.target.value)}>
              <option value="open">open</option>
              <option value="restricted">restricted</option>
              <option value="confidential">confidential</option>
            </select>
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Columns {loadingColumns && "(loading...)"}
            {columns.length > 0 && (
              <div className="column-editor">
                <div className="column-editor-header">
                  <span style={{ width: 28 }}></span>
                  <span className="col-name-header">Column</span>
                  <span className="col-flex-header">Visible To (Read)</span>
                  <span className="col-flex-header">Writable By (R/W)</span>
                  <span className="col-flex-header">Masking</span>
                  <span className="col-flex-header">Alias</span>
                  <span className="col-flex-header">Description</span>
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
                        value={col.visibleTo}
                        onChange={(e) => updateCol(i, "visibleTo", e.target.value)}
                        placeholder="roles (csv)"
                        className="col-flex-input"
                      />
                      <input
                        value={col.writableBy}
                        onChange={(e) => updateCol(i, "writableBy", e.target.value)}
                        placeholder="roles (csv)"
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
                        value={col.alias}
                        onChange={(e) => updateCol(i, "alias", e.target.value)}
                        placeholder="alias"
                        className="col-flex-input"
                      />
                      <input
                        value={col.description}
                        onChange={(e) => updateCol(i, "description", e.target.value)}
                        placeholder="description"
                        className="col-flex-input"
                      />
                    </div>
                    {col.maskType && (
                      <div className="column-editor-row column-mask-row">
                        <span style={{ width: 28 }}></span>
                        <span className="col-name" style={{ color: "var(--text-muted)", fontSize: "0.75rem" }}>↳ masking</span>
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
          <button onClick={handleSubmit}>Register Table</button>
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr>
            <th>ID</th><th>Source</th><th>Domain</th><th>Schema</th>
            <th>Table</th><th>Alias</th><th>Description</th>
            <th>Governance</th><th>Cols</th><th></th>
          </tr>
        </thead>
        <tbody>
          {tables.map((t) => {
            const isEditing = editingTable?.id === t.id;
            return (
              <Fragment key={t.id}>
                <tr onClick={() => { setExpanded(expanded === t.id ? null : t.id); if (expanded === t.id) cancelEditing(); }} className="clickable">
                  <td>{t.id}</td>
                  <td>{t.sourceId}</td>
                  <td>{t.domainId}</td>
                  <td>{t.schemaName}</td>
                  <td>{t.tableName}</td>
                  <td>{t.alias || ""}</td>
                  <td className="reasoning-cell">{t.description || ""}</td>
                  <td>{t.governance}</td>
                  <td>{t.columns.length}</td>
                  <td>
                    <button
                      className="destructive"
                      onClick={(e) => { e.stopPropagation(); handleDelete(t.id); }}
                      style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
                {expanded === t.id && (
                  <tr key={`${t.id}-cols`}>
                    <td colSpan={10}>
                      {!isEditing ? (
                        <>
                          <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: "0.5rem" }}>
                            <button
                              onClick={(e) => { e.stopPropagation(); startEditing(t); }}
                              style={{ padding: "0.25rem 0.75rem", fontSize: "0.8rem" }}
                            >
                              Edit
                            </button>
                          </div>
                          <table className="data-table" style={{ margin: "0 0 0.5rem" }}>
                            <thead>
                              <tr>
                                <th>Column</th><th>Alias</th><th>Description</th><th>Visible To (Read)</th><th>Writable By (R/W)</th><th>Masking</th>
                              </tr>
                            </thead>
                            <tbody>
                              {t.columns.map((c) => (
                                <Fragment key={c.id}>
                                  <tr>
                                    <td><code>{c.columnName}</code></td>
                                    <td>{c.alias || ""}</td>
                                    <td className="reasoning-cell">{c.description || ""}</td>
                                    <td>{c.visibleTo.length > 0 ? c.visibleTo.join(", ") : "all"}</td>
                                    <td>{c.writableBy.length > 0 ? c.writableBy.join(", ") : "none"}</td>
                                    <td>{c.maskType || "none"}</td>
                                  </tr>
                                  {c.maskType && (
                                    <tr>
                                      <td colSpan={2} style={{ color: "var(--text-muted)", fontSize: "0.75rem", paddingLeft: "1.5rem" }}>
                                        ↳ {c.maskType === "regex" ? `/${c.maskPattern}/ → ${c.maskReplace}` : c.maskType === "constant" ? `= ${c.maskValue ?? "NULL"}` : `truncate(${c.maskPrecision})`}
                                      </td>
                                      <td colSpan={4} style={{ color: "var(--text-muted)", fontSize: "0.75rem" }}>
                                        unmasked: {c.unmaskedTo.length > 0 ? c.unmaskedTo.join(", ") : "none"}
                                      </td>
                                    </tr>
                                  )}
                                </Fragment>
                              ))}
                            </tbody>
                          </table>
                        </>
                      ) : (
                        <>
                          <div className="form-card" style={{ marginBottom: "0.75rem" }}>
                            <label>
                              Table Alias
                              <input
                                value={editingTable.alias || ""}
                                onChange={(e) => setEditingTable({ ...editingTable, alias: e.target.value || null })}
                                placeholder="GraphQL name override"
                              />
                            </label>
                            <label style={{ gridColumn: "1 / -1" }}>
                              Table Description
                              <textarea
                                value={editingTable.description || ""}
                                onChange={(e) => setEditingTable({ ...editingTable, description: e.target.value || null })}
                                placeholder="Appears in SDL docs"
                                rows={2}
                              />
                            </label>
                          </div>
                          <table className="data-table" style={{ margin: "0 0 0.5rem" }}>
                            <thead>
                              <tr>
                                <th>Column</th><th>Alias</th><th>Description</th><th>Visible To (Read)</th><th>Writable By (R/W)</th><th>Masking</th>
                              </tr>
                            </thead>
                            <tbody>
                              {editingTable.columns.map((c, i) => (
                                <Fragment key={c.id}>
                                  <tr>
                                    <td><code>{c.columnName}</code></td>
                                    <td>
                                      <input
                                        value={c.alias || ""}
                                        onChange={(e) => updateEditCol(i, "alias", e.target.value)}
                                        placeholder="GraphQL alias"
                                      />
                                    </td>
                                    <td>
                                      <textarea
                                        value={c.description || ""}
                                        onChange={(e) => updateEditCol(i, "description", e.target.value)}
                                        placeholder="Column description"
                                        rows={1}
                                      />
                                    </td>
                                    <td>
                                      <MultiSelect
                                        options={roles.map((r) => ({ id: r.id, label: r.id }))}
                                        value={c.visibleTo}
                                        onChange={(selected) => updateEditCol(i, "visibleTo", selected.join(","))}
                                      />
                                    </td>
                                    <td>
                                      <MultiSelect
                                        options={roles.map((r) => ({ id: r.id, label: r.id }))}
                                        value={c.writableBy}
                                        onChange={(selected) => updateEditCol(i, "writableBy", selected.join(","))}
                                      />
                                    </td>
                                    <td>
                                      <select
                                        value={c.maskType || ""}
                                        onChange={(e) => updateEditCol(i, "maskType", e.target.value)}
                                      >
                                        <option value="">None</option>
                                        <option value="regex">Regex</option>
                                        <option value="constant">Constant</option>
                                        <option value="truncate">Truncate</option>
                                      </select>
                                    </td>
                                  </tr>
                                  {c.maskType && (
                                    <tr>
                                      <td colSpan={2} style={{ paddingLeft: "1.5rem", color: "var(--text-muted)", fontSize: "0.75rem" }}>↳ masking template</td>
                                      {c.maskType === "regex" && (
                                        <>
                                          <td>
                                            <input
                                              value={c.maskPattern || ""}
                                              onChange={(e) => updateEditCol(i, "maskPattern", e.target.value)}
                                              placeholder="regex pattern"
                                            />
                                          </td>
                                          <td>
                                            <input
                                              value={c.maskReplace || ""}
                                              onChange={(e) => updateEditCol(i, "maskReplace", e.target.value)}
                                              placeholder="replacement"
                                            />
                                          </td>
                                        </>
                                      )}
                                      {c.maskType === "constant" && (
                                        <td colSpan={2}>
                                          <input
                                            value={c.maskValue || ""}
                                            onChange={(e) => updateEditCol(i, "maskValue", e.target.value)}
                                            placeholder="constant value (NULL, 0, ***)"
                                          />
                                        </td>
                                      )}
                                      {c.maskType === "truncate" && (
                                        <td colSpan={2}>
                                          <select
                                            value={c.maskPrecision || ""}
                                            onChange={(e) => updateEditCol(i, "maskPrecision", e.target.value)}
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
                                          onChange={(selected) => updateEditCol(i, "unmaskedTo", selected.join(","))}
                                        />
                                      </td>
                                    </tr>
                                  )}
                                </Fragment>
                              ))}
                            </tbody>
                          </table>
                          <div style={{ display: "flex", gap: "0.5rem", justifyContent: "flex-end" }}>
                            <button onClick={cancelEditing} style={{ padding: "0.25rem 0.75rem", fontSize: "0.8rem" }}>Cancel</button>
                            <button onClick={handleSaveEdit} disabled={saving} style={{ padding: "0.25rem 0.75rem", fontSize: "0.8rem" }}>
                              {saving ? "Saving..." : "Save"}
                            </button>
                          </div>
                        </>
                      )}
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}