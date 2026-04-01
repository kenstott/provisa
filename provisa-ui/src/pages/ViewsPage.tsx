import { useState, useEffect, useCallback } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import {
  fetchViews,
  fetchDomains,
  saveView,
  deleteView,
  sampleView,
} from "../api/admin";
import type { ViewConfig } from "../api/admin";
import type { Domain } from "../types/admin";

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
  const [domains, setDomains] = useState<Domain[]>([]);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState<ViewConfig | null>(null);
  const [sampleData, setSampleData] = useState<{
    viewId: string;
    columns: string[];
    rows: Record<string, unknown>[];
  } | null>(null);
  const [sampling, setSampling] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [msg, setMsg] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    const [v, d] = await Promise.all([fetchViews(), fetchDomains()]);
    setViews(v);
    setDomains(d);
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleEdit = (view: ViewConfig) => {
    setEditing({ ...view, columns: view.columns || [] });
    setSampleData(null);
    setError("");
    setMsg("");
  };

  const handleNew = () => {
    setEditing({ ...EMPTY_VIEW });
    setSampleData(null);
    setError("");
    setMsg("");
  };

  const handleCancel = () => {
    setEditing(null);
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
      load();
    } catch (e: any) {
      setError(e.message);
    }
  }, [load]);

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

  if (loading) return <div className="page">Loading views...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Views</h2>
        <div className="page-actions">
          {!editing && (
            <button className="btn-primary" onClick={handleNew}>
              New View
            </button>
          )}
        </div>
      </div>

      {error && <div className="error">{error}</div>}
      {msg && <div style={{ color: "var(--approve)", marginBottom: "1rem", fontSize: "0.875rem" }}>{msg}</div>}

      {editing && (
        <div className="view-editor">
          <div className="form-row">
            <label>
              ID
              <input
                value={editing.id}
                onChange={(e) => updateEditing("id", e.target.value)}
                placeholder="monthly-revenue"
                disabled={views.some((v) => v.id === editing.id)}
              />
            </label>
            <label>
              Domain
              <select
                value={editing.domain_id}
                onChange={(e) => updateEditing("domain_id", e.target.value)}
              >
                <option value="">Select...</option>
                {domains.map((d) => (
                  <option key={d.id} value={d.id}>{d.id}</option>
                ))}
              </select>
            </label>
            <label>
              Governance
              <select
                value={editing.governance}
                onChange={(e) => updateEditing("governance", e.target.value)}
              >
                <option value="pre-approved">pre-approved</option>
                <option value="registry-required">registry-required</option>
              </select>
            </label>
          </div>
          <div className="form-row">
            <label style={{ flex: 2 }}>
              Description
              <input
                value={editing.description || ""}
                onChange={(e) => updateEditing("description", e.target.value)}
                placeholder="What this view represents"
              />
            </label>
            <label className="checkbox-label">
              <input
                type="checkbox"
                checked={editing.materialize}
                onChange={(e) => updateEditing("materialize", e.target.checked)}
              />
              Materialize
            </label>
            {editing.materialize && (
              <label>
                Refresh (s)
                <input
                  type="number"
                  value={editing.refresh_interval || 300}
                  onChange={(e) =>
                    updateEditing("refresh_interval", parseInt(e.target.value) || 300)
                  }
                />
              </label>
            )}
          </div>
          <label className="view-sql-label">
            SQL
            <div className="view-sql-editor">
              <CodeMirror
                value={editing.sql}
                height="200px"
                theme={oneDark}
                extensions={[sql({ dialect: PostgreSQL })]}
                onChange={(value) => updateEditing("sql", value)}
              />
            </div>
          </label>
          <div className="view-editor-actions">
            <button className="btn-primary" onClick={handleSave} disabled={saving}>
              {saving ? "Saving..." : "Save"}
            </button>
            <button className="btn-secondary" onClick={handleCancel}>
              Cancel
            </button>
          </div>
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
            <th></th>
          </tr>
        </thead>
        <tbody>
          {views.map((v) => (
            <tr key={v.id}>
              <td><code>{v.id}</code></td>
              <td className="reasoning-cell">{v.description || ""}</td>
              <td>{v.domain_id}</td>
              <td>{v.governance}</td>
              <td>{v.materialize ? "Yes" : "No"}</td>
              <td>{v.columns?.length || 0}</td>
              <td>
                <div style={{ display: "flex", gap: "0.25rem" }}>
                  <button className="btn-sm btn-secondary" onClick={() => handleEdit(v)}>
                    Edit
                  </button>
                  <button
                    className="btn-sm btn-secondary"
                    onClick={() => handleSample(v.id)}
                    disabled={sampling === v.id}
                  >
                    {sampling === v.id ? "..." : "Sample"}
                  </button>
                  <button className="btn-sm btn-danger" onClick={() => handleDelete(v.id)}>
                    Delete
                  </button>
                </div>
              </td>
            </tr>
          ))}
          {views.length === 0 && (
            <tr>
              <td colSpan={7} style={{ textAlign: "center", color: "var(--text-muted)" }}>
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
            <button className="btn-sm btn-secondary" onClick={() => setSampleData(null)}>
              Close
            </button>
          </div>
          <div className="sample-table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  {sampleData.columns.map((col) => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
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
