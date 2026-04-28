// Copyright (c) 2026 Kenneth Stott
// Canary: e22ee024-0e37-4e3d-b186-fdc48527a15e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useEffect, useCallback, useMemo } from "react";
import { Trash2, Pencil, Save, X } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import {
  fetchViews,
  fetchDomains,
  fetchTables,
  saveView,
  deleteView,
  sampleView,
} from "../api/admin";
import type { ViewConfig } from "../api/admin";
import type { RegisteredTable } from "../types/admin";

const EMPTY_VIEW: ViewConfig = {
  id: "",
  sql: "",
  description: "",
  domain_id: "",
  governance: "pre-approved",
  materialize: false,
  refresh_interval: 300,
  columns: [],
};

export function ViewsPage() {
  const [views, setViews] = useState<ViewConfig[]>([]);
  const [domainHints, setDomainHints] = useState<string[]>([]);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [editing, setEditing] = useState<ViewConfig | null>(null);
  const [isNew, setIsNew] = useState(false);
  const [sampleData, setSampleData] = useState<{
    viewId: string;
    columns: string[];
    rows: Record<string, unknown>[];
  } | null>(null);
  const [sampling, setSampling] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [msg, setMsg] = useState("");
  const [viewSearch, setViewSearch] = useState("");

  const sqlSchema = useMemo(() => {
    const schema: Record<string, Record<string, string[]> | string[]> = {};
    for (const t of tables) {
      const cols = t.columns.map((c) => c.columnName);
      schema[t.tableName] = cols;
      if (t.alias) schema[t.alias] = cols;
      if (t.schemaName) {
        if (!schema[t.schemaName] || Array.isArray(schema[t.schemaName])) {
          schema[t.schemaName] = {} as Record<string, string[]>;
        }
        (schema[t.schemaName] as Record<string, string[]>)[t.tableName] = cols;
      }
    }
    return schema;
  }, [tables]);

  const sqlExtensions = useMemo(
    () => [
      sql({
        dialect: PostgreSQL,
        schema: sqlSchema,
        keywordCompletion: (kw: string, type: string) => ({
          label: kw,
          type,
          boost: -1,
        }),
      }),
    ],
    [sqlSchema],
  );

  const load = useCallback(async () => {
    setLoading(true);
    const [v, doms, t] = await Promise.all([fetchViews(), fetchDomains(), fetchTables()]);
    setViews(v);
    setDomainHints(doms.map((d) => d.id));
    setTables(t);
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleNew = () => {
    setEditing({ ...EMPTY_VIEW });
    setIsNew(true);
    setExpanded(null);
    setSampleData(null);
    setError("");
    setMsg("");
  };

  const handleEdit = (view: ViewConfig) => {
    setEditing({ ...view, columns: view.columns || [] });
    setIsNew(false);
    setExpanded(view.id);
    setSampleData(null);
    setError("");
    setMsg("");
  };

  const handleCancel = () => {
    setEditing(null);
    setIsNew(false);
    setError("");
    setMsg("");
  };

  const handleSave = useCallback(async () => {
    if (!editing) return;
    if (!editing.id || !editing.sql || !editing.domain_id) {
      setError("ID, SQL, and Domain are required.");
      return;
    }
    setSaving(true);
    setError("");
    setMsg("");
    try {
      const result = await saveView(editing);
      if (result.success) {
        setMsg(result.message);
        setEditing(null);
        setIsNew(false);
        load();
      } else {
        setError(result.message);
      }
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  }, [editing, load]);

  const handleDelete = useCallback(async (id: string) => {
    setError("");
    try {
      await deleteView(id);
      if (expanded === id) setExpanded(null);
      if (editing?.id === id) { setEditing(null); setIsNew(false); }
      load();
    } catch (e: any) {
      setError(e.message);
    }
  }, [load, expanded, editing]);

  const handleSample = useCallback(async (id: string) => {
    setSampling(id);
    setSampleData(null);
    setError("");
    try {
      const result = await sampleView(id);
      setSampleData({ viewId: id, columns: result.columns, rows: result.rows });
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSampling(null);
    }
  }, []);

  const updateEditing = (key: string, value: unknown) => {
    if (!editing) return;
    setEditing({ ...editing, [key]: value });
  };

  const renderEditForm = () => (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <div className="form-row">
        <label>
          ID
          <input
            value={editing!.id}
            onChange={(e) => updateEditing("id", e.target.value)}
            placeholder="monthly-revenue"
            disabled={!isNew}
          />
        </label>
        <label>
          Domain
          <select value={editing!.domain_id} onChange={(e) => updateEditing("domain_id", e.target.value)}>
            <option value="">Select domain...</option>
            {domainHints.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
        </label>
        <label>
          Governance
          <select value={editing!.governance} onChange={(e) => updateEditing("governance", e.target.value)}>
            <option value="pre-approved">pre-approved</option>
            <option value="registry-required">registry-required</option>
          </select>
        </label>
      </div>
      <div className="form-row">
        <label style={{ flex: 2 }}>
          Description
          <input
            value={editing!.description || ""}
            onChange={(e) => updateEditing("description", e.target.value)}
            placeholder="What this view represents"
          />
        </label>
        <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap", flex: "0 0 auto" }}>
          <input
            type="checkbox"
            checked={editing!.materialize}
            onChange={(e) => updateEditing("materialize", e.target.checked)}
            style={{ width: "auto", padding: 0 }}
          />
          Materialize
        </label>
        {editing!.materialize && (
          <label>
            Refresh (s)
            <input
              type="number"
              value={editing!.refresh_interval || 300}
              onChange={(e) => updateEditing("refresh_interval", parseInt(e.target.value) || 300)}
            />
          </label>
        )}
      </div>
      <label className="view-sql-label">
        SQL
        <div className="view-sql-editor">
          <CodeMirror
            value={editing!.sql}
            height="200px"
            theme={oneDark}
            extensions={sqlExtensions}
            onChange={(value) => updateEditing("sql", value)}
          />
        </div>
      </label>
      <div className="view-editor-actions">
        <button className="btn-icon-primary" title="Save" onClick={handleSave} disabled={saving}><Save size={14} /></button>
        <button className="btn-icon" title="Cancel" onClick={handleCancel}><X size={14} /></button>
      </div>
    </div>
  );

  if (loading) return <div className="page">Loading views...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Views</h2>
        <FilterInput value={viewSearch} onChange={setViewSearch} placeholder="Filter by ID or description…" />
        <div className="page-actions">
          {!isNew && (
            <button className="btn-primary" onClick={handleNew}>+ View</button>
          )}
        </div>
      </div>

      {error && <div className="error">{error}</div>}
      {msg && <div style={{ color: "var(--approve)", marginBottom: "1rem", fontSize: "0.875rem" }}>{msg}</div>}

      {isNew && editing && (
        <div className="view-editor">
          {renderEditForm()}
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Description</th>
            <th>Domain</th>
            <th>Governance</th>
            <th>Materialized</th>
            <th>Columns</th>
          </tr>
        </thead>
        <tbody>
          {views.filter((v) => {
            if (!viewSearch.trim()) return true;
            const q = viewSearch.toLowerCase();
            return v.id.toLowerCase().includes(q) || (v.description ?? "").toLowerCase().includes(q);
          }).map((v) => {
            const isExpanded = expanded === v.id;
            const isEditing = editing?.id === v.id && !isNew;
            return (
              <React.Fragment key={v.id}>
                <tr
                  onClick={() => {
                    if (isExpanded) {
                      setExpanded(null);
                      if (isEditing) { setEditing(null); setIsNew(false); }
                    } else {
                      setExpanded(v.id);
                      if (editing && !isNew) { setEditing(null); setIsNew(false); }
                    }
                    setSampleData(null);
                  }}
                  style={{ cursor: "pointer", background: isExpanded ? "var(--surface)" : undefined }}
                >
                  <td><code>{v.id}</code></td>
                  <td className="reasoning-cell">{v.description || ""}</td>
                  <td>{v.domain_id}</td>
                  <td>{v.governance}</td>
                  <td>{v.materialize ? "Yes" : "No"}</td>
                  <td>{v.columns?.length || 0}</td>
                </tr>
                {isExpanded && (
                  <tr>
                    <td colSpan={6} style={{ padding: "0.75rem 1rem", background: "var(--bg)", borderTop: "1px solid var(--border)" }}>
                      {isEditing ? renderEditForm() : (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          <dl style={{ display: "grid", gridTemplateColumns: "max-content 1fr", gap: "0.25rem 1rem", margin: 0, color: "var(--text)" }}>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Domain</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{v.domain_id || "—"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Governance</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{v.governance}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Materialized</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{v.materialize ? `Yes (${v.refresh_interval}s)` : "No"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Columns</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{v.columns?.length || 0}</dd>
                            {v.description && <><dt style={{ color: "var(--text-muted)" }}><strong>Description</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{v.description}</dd></>}
                            <dt style={{ color: "var(--text-muted)" }}><strong>SQL</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}><pre style={{ margin: 0, fontSize: "0.85rem", whiteSpace: "pre-wrap" }}>{v.sql}</pre></dd>
                          </dl>
                          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                            <button className="btn-icon" title="Edit" onClick={(e) => { e.stopPropagation(); handleEdit(v); }}><Pencil size={14} /></button>
                            <button
                              className="btn-secondary"
                              onClick={(e) => { e.stopPropagation(); handleSample(v.id); }}
                              disabled={sampling === v.id}
                            >{sampling === v.id ? "..." : "Sample"}</button>
                            <button className="btn-icon-danger" title="Delete" onClick={(e) => { e.stopPropagation(); handleDelete(v.id); }}><Trash2 size={14} /></button>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
          {views.length === 0 && (
            <tr>
              <td colSpan={6} style={{ textAlign: "center", color: "var(--text-muted)" }}>
                No views defined. Click "New View" to create one.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {sampleData && (
        <div className="sample-panel">
          <div className="sample-header">
            <strong>Sample: {sampleData.viewId}</strong>
            <span className="query-hint">{sampleData.rows.length} rows (limit 20)</span>
            <button className="btn-sm btn-secondary" onClick={() => setSampleData(null)}>Close</button>
          </div>
          <div className="sample-table-wrapper">
            <table className="data-table">
              <thead>
                <tr>{sampleData.columns.map((col) => <th key={col}>{col}</th>)}</tr>
              </thead>
              <tbody>
                {sampleData.rows.map((row, i) => (
                  <tr key={i}>
                    {sampleData.columns.map((col) => (
                      <td key={col}>{row[col] != null ? String(row[col]) : "NULL"}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
