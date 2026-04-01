import { useState, useEffect, Fragment } from "react";
import {
  fetchTables, fetchSources, fetchDomains,
  fetchAvailableSchemas, fetchAvailableTables, fetchAvailableColumns,
  registerTable, deleteTable,
} from "../api/admin";
import type { RegisteredTable, Source, Domain } from "../types/admin";

interface ColumnForm {
  name: string;
  visibleTo: string;
  alias: string;
  description: string;
  selected: boolean;
}

export function TablesPage() {
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [domains, setDomains] = useState<Domain[]>([]);
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
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);
  const [loadingColumns, setLoadingColumns] = useState(false);

  const reload = () => {
    setLoading(true);
    Promise.all([fetchTables(), fetchSources(), fetchDomains()])
      .then(([t, s, d]) => { setTables(t); setSources(s); setDomains(d); })
      .finally(() => setLoading(false));
  };

  useEffect(reload, []);

  useEffect(() => {
    setSchemaName(""); setTableName(""); setColumns([]);
    setAvailableSchemas([]); setAvailableTables([]);
    if (!sourceId) return;
    setLoadingSchemas(true);
    fetchAvailableSchemas(sourceId)
      .then(setAvailableSchemas)
      .catch(() => setAvailableSchemas([]))
      .finally(() => setLoadingSchemas(false));
  }, [sourceId]);

  useEffect(() => {
    setTableName(""); setColumns([]); setAvailableTables([]);
    if (!sourceId || !schemaName) return;
    setLoadingTables(true);
    fetchAvailableTables(sourceId, schemaName)
      .then(setAvailableTables)
      .catch(() => setAvailableTables([]))
      .finally(() => setLoadingTables(false));
  }, [sourceId, schemaName]);

  useEffect(() => {
    setColumns([]);
    if (!sourceId || !schemaName || !tableName) return;
    setLoadingColumns(true);
    fetchAvailableColumns(sourceId, schemaName, tableName)
      .then((cols) =>
        setColumns(cols.map((c) => ({ name: c, visibleTo: "", alias: "", description: "", selected: true })))
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
              {availableTables.map((t) => <option key={t} value={t}>{t}</option>)}
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
                  <span className="col-flex-header">Visible To</span>
                  <span className="col-flex-header">Alias</span>
                  <span className="col-flex-header">Description</span>
                </div>
                {columns.map((col, i) => (
                  <div key={col.name} className="column-editor-row">
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
          {tables.map((t) => (
            <Fragment key={t.id}>
              <tr onClick={() => setExpanded(expanded === t.id ? null : t.id)} className="clickable">
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
                    <table className="data-table" style={{ margin: "0.5rem 0" }}>
                      <thead>
                        <tr>
                          <th>Column</th><th>Alias</th><th>Description</th><th>Visible To</th>
                        </tr>
                      </thead>
                      <tbody>
                        {t.columns.map((c) => (
                          <tr key={c.id}>
                            <td><code>{c.columnName}</code></td>
                            <td>{c.alias || ""}</td>
                            <td className="reasoning-cell">{c.description || ""}</td>
                            <td>{c.visibleTo.length > 0 ? c.visibleTo.join(", ") : "all"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
        </tbody>
      </table>
    </div>
  );
}
