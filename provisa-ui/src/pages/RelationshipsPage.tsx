// Copyright (c) 2026 Kenneth Stott
// Canary: b2e1f836-e23f-41ef-9df2-88110c6b9f1d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useEffect, useCallback } from "react";
import { Trash2, Pencil, Sparkles, Save, X } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import {
  fetchRelationships,
  fetchTables,
  upsertRelationship,
  deleteRelationship,
  discoverRelationships,
  fetchCandidates,
  fetchRejectedCount,
  acceptCandidate,
  rejectCandidate,
  clearRejectedCandidates,
} from "../api/admin";
import { fetchActions } from "../api/actions";
import type { TrackedFunction } from "../api/actions";
import type { Relationship, RegisteredTable } from "../types/admin";

interface Candidate {
  id: number;
  source_table_id: number;
  target_table_id: number;
  source_column: string;
  target_column: string;
  cardinality: string;
  confidence: number;
  reasoning: string;
  suggested_name?: string;
}

const EMPTY_FORM = {
  id: "",
  originalId: "",
  sourceDomain: "",
  sourceTableId: "",
  sourceColumn: "",
  targetType: "table" as "table" | "function",
  targetDomain: "",
  targetTableId: "",
  targetColumn: "",
  targetFunctionName: "",
  functionArg: "",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: "300",
  alias: "",
  graphqlAlias: "",
};

export function RelationshipsPage() {
  const [rels, setRels] = useState<Relationship[]>([]);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [functions, setFunctions] = useState<TrackedFunction[]>([]);
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState<string | null>(null);
  const [discovering, setDiscovering] = useState(false);
  const [discoverError, setDiscoverError] = useState("");
  const [discoverMsg, setDiscoverMsg] = useState("");
  const [rejectedCount, setRejectedCount] = useState(0);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState(EMPTY_FORM);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [editingRel, setEditingRel] = useState<typeof EMPTY_FORM | null>(null);
  const [relSearch, setRelSearch] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    const [r, t, actions, c, rc] = await Promise.all([
      fetchRelationships(),
      fetchTables(),
      fetchActions().catch(() => ({ functions: [], webhooks: [] })),
      fetchCandidates().catch(() => []),
      fetchRejectedCount().catch(() => 0),
    ]);
    setRels(r);
    setTables(t);
    setFunctions(actions.functions);
    setCandidates(c);
    setRejectedCount(rc);
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const tableNameById = Object.fromEntries(
    tables.map((t) => [t.id, t.tableName]),
  );
  const normalizeDomain = (id: string) => id.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
  const tableDomainById = Object.fromEntries(
    tables.map((t) => [t.id, normalizeDomain(t.domainId)]),
  );

  const handleDelete = useCallback(
    async (id: string) => {
      await deleteRelationship(id);
      setExpanded((prev) => (prev === id ? null : prev));
      setEditingRel(null);
      load();
    },
    [load],
  );

  const handleAdd = useCallback(async () => {
    if (!form.id || !form.sourceTableId) return;
    if (form.targetType === "table" && !form.targetTableId) return;
    if (form.targetType === "function" && !form.targetFunctionName) return;
    setSaving("new");
    await upsertRelationship({
      id: form.id,
      sourceTableId: form.sourceTableId,
      targetTableId: form.targetType === "table" ? form.targetTableId : "",
      sourceColumn: form.sourceColumn,
      targetColumn: form.targetType === "table" ? form.targetColumn : "",
      cardinality: form.targetType === "function" ? "one-to-many" : form.cardinality,
      materialize: form.materialize,
      refreshInterval: parseInt(form.refreshInterval) || 300,
      targetFunctionName: form.targetType === "function" ? form.targetFunctionName : null,
      functionArg: form.targetType === "function" ? form.functionArg : null,
      alias: form.alias || null,
      graphqlAlias: form.graphqlAlias || null,
    });
    setSaving(null);
    setForm(EMPTY_FORM);
    setShowForm(false);
    load();
  }, [form, load]);

  const handleEditSave = useCallback(async () => {
    if (!editingRel?.id) return;
    setSaving(editingRel.originalId || editingRel.id);
    await upsertRelationship({
      id: editingRel.id,
      sourceTableId: editingRel.sourceTableId,
      targetTableId: editingRel.targetType === "table" ? editingRel.targetTableId : "",
      sourceColumn: editingRel.sourceColumn,
      targetColumn: editingRel.targetType === "table" ? editingRel.targetColumn : "",
      cardinality: editingRel.targetType === "function" ? "one-to-many" : editingRel.cardinality,
      materialize: editingRel.materialize,
      refreshInterval: parseInt(editingRel.refreshInterval) || 300,
      targetFunctionName: editingRel.targetType === "function" ? editingRel.targetFunctionName : null,
      functionArg: editingRel.targetType === "function" ? editingRel.functionArg : null,
      alias: editingRel.alias || null,
      graphqlAlias: editingRel.graphqlAlias || null,
    });
    if (editingRel.originalId && editingRel.originalId !== editingRel.id) {
      await deleteRelationship(editingRel.originalId);
    }
    setSaving(null);
    setEditingRel(null);
    load();
  }, [editingRel, load]);

  const handleClearRejections = useCallback(async () => {
    setDiscoverError("");
    setDiscoverMsg("");
    try {
      const result = await clearRejectedCandidates();
      setDiscoverMsg(`Cleared ${result.deleted} rejection${result.deleted !== 1 ? "s" : ""}.`);
      setRejectedCount(0);
    } catch (e: any) {
      setDiscoverError(e.message);
    }
  }, []);

  const handleDiscover = useCallback(async () => {
    setDiscovering(true);
    setDiscoverError("");
    setDiscoverMsg("");
    try {
      const result = await discoverRelationships("cross-domain");
      const c = await fetchCandidates();
      setCandidates(c);
      if (result.candidates_found === 0) {
        setDiscoverError(`AI found 0 candidates (check server logs for details)`);
      }
    } catch (e: any) {
      setDiscoverError(e.message);
    } finally {
      setDiscovering(false);
    }
  }, []);

  const handleAccept = useCallback(async (id: number, name: string) => {
    await acceptCandidate(id, name);
    load();
  }, [load]);

  const handleReject = useCallback(async (id: number) => {
    await rejectCandidate(id, "Rejected by user");
    setCandidates((prev) => prev.filter((c) => c.id !== id));
    setRejectedCount((prev) => prev + 1);
  }, []);

  const startEditing = useCallback((rel: Relationship) => {
    const isComputed = !!rel.targetFunctionName;
    setEditingRel({
      id: String(rel.id),
      originalId: String(rel.id),
      sourceDomain: tableDomainById[rel.sourceTableId] ?? "",
      sourceTableId: rel.sourceTableName,
      sourceColumn: rel.sourceColumn,
      targetType: isComputed ? "function" : "table",
      targetDomain: rel.targetTableId ? (tableDomainById[rel.targetTableId] ?? "") : "",
      targetTableId: rel.targetTableName ?? "",
      targetColumn: rel.targetColumn ?? "",
      targetFunctionName: rel.targetFunctionName ?? "",
      functionArg: rel.functionArg ?? "",
      cardinality: rel.cardinality,
      materialize: rel.materialize,
      refreshInterval: String(rel.refreshInterval ?? 300),
      alias: rel.alias ?? "",
      graphqlAlias: rel.graphqlAlias ?? "",
    });
  }, [tableDomainById]);

  if (loading) return <div className="page">Loading relationships...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Relationships</h2>
        <FilterInput value={relSearch} onChange={setRelSearch} placeholder="Filter by source or target…" />
        <div className="page-actions">
          <button className="btn-primary" onClick={() => setShowForm(!showForm)}>
            {showForm ? "Cancel" : "+ Relationship"}
          </button>
          <button
            className="btn-icon"
            title={discovering ? "Discovering..." : "Suggest with AI"}
            onClick={handleDiscover}
            disabled={discovering}
          ><Sparkles size={14} /></button>
          {rejectedCount > 0 && (
            <button className="btn-secondary" onClick={handleClearRejections}>
              Clear Rejections
            </button>
          )}
        </div>
      </div>

      {discoverError && <div className="error">{discoverError}</div>}
      {discoverMsg && <div style={{ color: "var(--approve)", marginBottom: "1rem", fontSize: "0.875rem" }}>{discoverMsg}</div>}

      {showForm && (
        <div className="form-card">
          <div className="form-row">
            <label>
              ID
              <input value={form.id} onChange={(e) => setForm({ ...form, id: e.target.value })} placeholder="orders-to-customers" />
            </label>
            <label>
              CQL Alias (UPPER_SNAKE)
              <input value={form.alias} onChange={(e) => setForm({ ...form, alias: e.target.value })} placeholder="PLACED_BY" />
            </label>
            <label>
              GQL Alias (camelCase)
              <input value={form.graphqlAlias} onChange={(e) => setForm({ ...form, graphqlAlias: e.target.value })} placeholder="e.g. orders" />
            </label>
            <label>
              Source Table
              <select value={form.sourceTableId} onChange={(e) => setForm({ ...form, sourceTableId: e.target.value })}>
                <option value="">Select...</option>
                {tables.map((t) => <option key={t.id} value={t.tableName}>{t.tableName}</option>)}
              </select>
            </label>
            <label>
              Source Column
              <input value={form.sourceColumn} onChange={(e) => setForm({ ...form, sourceColumn: e.target.value })} placeholder="customer_id" />
            </label>
          </div>
          <div className="form-row">
            <label>
              Target Type
              <select value={form.targetType} onChange={(e) => setForm({ ...form, targetType: e.target.value as "table" | "function", targetTableId: "", targetColumn: "", targetFunctionName: "", functionArg: "" })}>
                <option value="table">Table</option>
                <option value="function">Function (computed)</option>
              </select>
            </label>
            {form.targetType === "table" ? (
              <>
                <label>
                  Target Table
                  <select value={form.targetTableId} onChange={(e) => setForm({ ...form, targetTableId: e.target.value })}>
                    <option value="">Select...</option>
                    {tables.map((t) => <option key={t.id} value={t.tableName}>{t.tableName}</option>)}
                  </select>
                </label>
                <label>
                  Target Column
                  <input value={form.targetColumn} onChange={(e) => setForm({ ...form, targetColumn: e.target.value })} placeholder="id" />
                </label>
              </>
            ) : (
              <>
                <label>
                  Function
                  <select value={form.targetFunctionName} onChange={(e) => setForm({ ...form, targetFunctionName: e.target.value })}>
                    <option value="">Select...</option>
                    {functions.map((f) => <option key={f.name} value={f.name}>{f.name}</option>)}
                  </select>
                </label>
                <label>
                  Function Arg (receives source column)
                  <input value={form.functionArg} onChange={(e) => setForm({ ...form, functionArg: e.target.value })} placeholder="arg name" />
                </label>
              </>
            )}
          </div>
          <div className="form-row">
            {form.targetType === "table" && (
              <label>
                Cardinality
                <select value={form.cardinality} onChange={(e) => setForm({ ...form, cardinality: e.target.value })}>
                  <option value="many-to-one">many-to-one</option>
                  <option value="one-to-many">one-to-many</option>
                </select>
              </label>
            )}
          </div>
          <div className="form-row">
            <label className="checkbox-label">
              <input type="checkbox" checked={form.materialize} onChange={(e) => setForm({ ...form, materialize: e.target.checked })} />
              Materialize (auto-create MV for cross-source joins)
            </label>
            {form.materialize && (
              <label>
                Refresh Interval (s)
                <input type="number" value={form.refreshInterval} onChange={(e) => setForm({ ...form, refreshInterval: e.target.value })} />
              </label>
            )}
            <button className="btn-primary" onClick={handleAdd} disabled={saving === "new"}>
              Save
            </button>
          </div>
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Domain</th>
            <th>Source</th>
            <th>Target</th>
            <th>GQL / CQL Alias</th>
            <th>Cardinality</th>
            <th>Materialize</th>
            <th style={{ whiteSpace: "nowrap" }}>Refresh (s)</th>
          </tr>
        </thead>
        <tbody>
          {rels.filter((r) => {
            if (!relSearch.trim()) return true;
            const q = relSearch.toLowerCase();
            return r.sourceTable.toLowerCase().includes(q) || r.targetTable.toLowerCase().includes(q);
          }).map((r) => {
            const id = String(r.id);
            const isExpanded = expanded === id;
            return (
              <React.Fragment key={r.id}>
                <tr
                  onClick={() => { setExpanded(isExpanded ? null : id); setEditingRel(null); }}
                  style={{ cursor: "pointer", background: isExpanded ? "var(--surface)" : undefined }}
                >
                  <td>{r.id}</td>
                  <td>{tableDomainById[r.sourceTableId] || "—"}</td>
                  <td>{`${r.sourceTableName}.${r.sourceColumn}`}</td>
                  <td>{r.targetFunctionName ? `fn:${r.targetFunctionName}(${r.functionArg ?? ""})` : `${r.targetTableName}.${r.targetColumn}`}</td>
                  <td>
                    <div style={{ fontSize: "0.8rem", lineHeight: 1.4 }}>
                      <div><span style={{ color: "var(--text-muted)" }}>GQL:</span> <code>{r.graphqlAlias ?? "—"}</code></div>
                      <div><span style={{ color: "var(--text-muted)" }}>CQL:</span> <code>{r.alias ?? <em style={{ color: "var(--text-muted)" }}>{r.computedCypherAlias ?? "—"}</em>}</code></div>
                    </div>
                  </td>
                  <td>{r.cardinality}</td>
                  <td>{r.materialize ? "Yes" : "No"}</td>
                  <td>{r.materialize ? r.refreshInterval : "—"}</td>
                </tr>
                {isExpanded && (
                  <tr>
                    <td colSpan={8} style={{ padding: "0.75rem 1rem", background: "var(--bg)", borderTop: "1px solid var(--border)" }}>
                      {!editingRel ? (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          <dl style={{ display: "grid", gridTemplateColumns: "max-content 1fr", gap: "0.25rem 1rem", margin: 0, color: "var(--text)" }}>
                            <dt style={{ color: "var(--text-muted)" }}><strong>ID</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.id}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Source</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{tableDomainById[r.sourceTableId] ? `${tableDomainById[r.sourceTableId]}.${r.sourceTableName}.${r.sourceColumn}` : `${r.sourceTableName}.${r.sourceColumn}`}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Target</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.targetFunctionName ? `fn:${r.targetFunctionName}(${r.functionArg ?? ""})` : (tableDomainById[r.targetTableId!] ? `${tableDomainById[r.targetTableId!]}.${r.targetTableName}.${r.targetColumn}` : `${r.targetTableName}.${r.targetColumn}`)}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>GQL Alias</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}><code>{r.graphqlAlias ?? "—"}</code></dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>CQL Alias</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}><code>{r.alias ?? <em style={{ color: "var(--text-muted)" }}>{r.computedCypherAlias ?? "—"}</em>}</code></dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Cardinality</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.cardinality}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Materialize</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.materialize ? "Yes" : "No"}</dd>
                            {r.materialize && <><dt style={{ color: "var(--text-muted)" }}><strong>Refresh Interval (s)</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.refreshInterval ?? "—"}</dd></>}
                          </dl>
                          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                            <button className="btn-icon" title="Edit" onClick={(e) => { e.stopPropagation(); startEditing(r); }}><Pencil size={14} /></button>
                            <button className="btn-icon-danger" title="Delete" onClick={(e) => { e.stopPropagation(); handleDelete(id); }}><Trash2 size={14} /></button>
                          </div>
                        </div>
                      ) : (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                          <div className="form-row">
                            <label>Name
                              <input value={editingRel.id} onChange={(e) => setEditingRel({ ...editingRel, id: e.target.value })} />
                            </label>
                            <label>CQL Alias (UPPER_SNAKE)
                              <input value={editingRel.alias} onChange={(e) => setEditingRel({ ...editingRel, alias: e.target.value })} placeholder={r.computedCypherAlias ?? "PLACED_BY"} />
                            </label>
                            <label>GQL Alias (camelCase)
                              <input value={editingRel.graphqlAlias} onChange={(e) => setEditingRel({ ...editingRel, graphqlAlias: e.target.value })} placeholder={r.graphqlAlias ?? ""} />
                            </label>
                          </div>
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem" }}>
                            {/* Source panel */}
                            {(() => {
                              const uniqueDomains = [...new Set(tables.map((t) => normalizeDomain(t.domainId)).filter(Boolean))].sort();
                              const filteredSrcTables = editingRel.sourceDomain
                                ? tables.filter((t) => normalizeDomain(t.domainId) === editingRel.sourceDomain)
                                : tables;
                              return (
                                <div style={{ border: "1px solid var(--border)", borderRadius: "4px", padding: "0.75rem", display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                                  <strong style={{ color: "var(--text-muted)", fontSize: "0.75rem", textTransform: "uppercase" as const }}>Source</strong>
                                  <label>Domain
                                    <select value={editingRel.sourceDomain} onChange={(e) => setEditingRel({ ...editingRel, sourceDomain: e.target.value, sourceTableId: "" })}>
                                      <option value="">All</option>
                                      {uniqueDomains.map((d) => <option key={d} value={d}>{d}</option>)}
                                    </select>
                                  </label>
                                  <label>Table
                                    <select value={editingRel.sourceTableId} onChange={(e) => setEditingRel({ ...editingRel, sourceTableId: e.target.value })}>
                                      <option value="">Select...</option>
                                      {filteredSrcTables.map((t) => <option key={t.id} value={t.tableName}>{t.tableName}</option>)}
                                    </select>
                                  </label>
                                  <label>Column
                                    <input value={editingRel.sourceColumn} onChange={(e) => setEditingRel({ ...editingRel, sourceColumn: e.target.value })} />
                                  </label>
                                </div>
                              );
                            })()}
                            {/* Target panel */}
                            <div style={{ border: "1px solid var(--border)", borderRadius: "4px", padding: "0.75rem", display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                              <strong style={{ color: "var(--text-muted)", fontSize: "0.75rem", textTransform: "uppercase" as const }}>Target</strong>
                              <label>Type
                                <select value={editingRel.targetType} onChange={(e) => setEditingRel({ ...editingRel, targetType: e.target.value as "table" | "function", targetTableId: "", targetColumn: "", targetFunctionName: "", functionArg: "" })}>
                                  <option value="table">Table</option>
                                  <option value="function">Function (computed)</option>
                                </select>
                              </label>
                              {editingRel.targetType === "table" ? (
                                (() => {
                                  const uniqueDomains = [...new Set(tables.map((t) => normalizeDomain(t.domainId)).filter(Boolean))].sort();
                                  const filteredTgtTables = editingRel.targetDomain
                                    ? tables.filter((t) => normalizeDomain(t.domainId) === editingRel.targetDomain)
                                    : tables;
                                  return (
                                    <>
                                      <label>Domain
                                        <select value={editingRel.targetDomain} onChange={(e) => setEditingRel({ ...editingRel, targetDomain: e.target.value, targetTableId: "" })}>
                                          <option value="">All</option>
                                          {uniqueDomains.map((d) => <option key={d} value={d}>{d}</option>)}
                                        </select>
                                      </label>
                                      <label>Table
                                        <select value={editingRel.targetTableId} onChange={(e) => setEditingRel({ ...editingRel, targetTableId: e.target.value })}>
                                          <option value="">Select...</option>
                                          {filteredTgtTables.map((t) => <option key={t.id} value={t.tableName}>{t.tableName}</option>)}
                                        </select>
                                      </label>
                                      <label>Column
                                        <input value={editingRel.targetColumn} onChange={(e) => setEditingRel({ ...editingRel, targetColumn: e.target.value })} />
                                      </label>
                                    </>
                                  );
                                })()
                              ) : (
                                <>
                                  <label>Function
                                    <select value={editingRel.targetFunctionName} onChange={(e) => setEditingRel({ ...editingRel, targetFunctionName: e.target.value })}>
                                      <option value="">Select...</option>
                                      {functions.map((f) => <option key={f.name} value={f.name}>{f.name}</option>)}
                                    </select>
                                  </label>
                                  <label>Function Arg (receives source column)
                                    <input value={editingRel.functionArg} onChange={(e) => setEditingRel({ ...editingRel, functionArg: e.target.value })} placeholder="arg name" />
                                  </label>
                                </>
                              )}
                            </div>
                          </div>
                          <div className="form-row">
                            {editingRel.targetType === "table" && (
                            <label>Cardinality
                              <select value={editingRel.cardinality} onChange={(e) => setEditingRel({ ...editingRel, cardinality: e.target.value })}>
                                <option value="many-to-one">many-to-one</option>
                                <option value="one-to-many">one-to-many</option>
                              </select>
                            </label>
                            )}
                            <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap", flex: "0 0 auto" }}>
                              <input type="checkbox" checked={editingRel.materialize} onChange={(e) => setEditingRel({ ...editingRel, materialize: e.target.checked })} style={{ width: "auto", padding: 0 }} />
                              Materialize
                            </label>
                            {editingRel.materialize && (
                              <label>Refresh Interval (s)
                                <input type="number" value={editingRel.refreshInterval} onChange={(e) => setEditingRel({ ...editingRel, refreshInterval: e.target.value })} />
                              </label>
                            )}
                          </div>
                          <div style={{ display: "flex", gap: "0.5rem", justifyContent: "flex-end" }}>
                            <button className="btn-icon" title="Cancel" onClick={() => setEditingRel(null)}><X size={14} /></button>
                            <button className="btn-icon-primary" title="Save" onClick={handleEditSave} disabled={!!saving}><Save size={14} /></button>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>

      {candidates.length > 0 && (
        <>
          <h3 style={{ marginTop: "2rem" }}>AI-Suggested Relationships</h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th><th>Source</th><th>Target</th>
                <th>Cardinality</th><th>Confidence</th><th></th>
              </tr>
            </thead>
            <tbody>
              {candidates.map((c) => {
                const srcDomain = tableDomainById[c.source_table_id];
                const tgtDomain = tableDomainById[c.target_table_id];
                const srcTable = tableNameById[c.source_table_id] ?? String(c.source_table_id);
                const tgtTable = tableNameById[c.target_table_id] ?? String(c.target_table_id);
                const srcLabel = srcDomain ? `${srcDomain}.${srcTable}.${c.source_column}` : `${srcTable}.${c.source_column}`;
                const tgtLabel = tgtDomain ? `${tgtDomain}.${tgtTable}.${c.target_column}` : `${tgtTable}.${c.target_column}`;
                const suggestedName = c.suggested_name || `${srcTable}-${c.source_column}-to-${tgtTable}`;
                return (
                  <React.Fragment key={c.id}>
                    <tr>
                      <td><code>{suggestedName}</code></td>
                      <td>{srcLabel}</td>
                      <td>{tgtLabel}</td>
                      <td>{c.cardinality}</td>
                      <td>{(c.confidence * 100).toFixed(0)}%</td>
                      <td>
                        <div style={{ display: "flex", gap: "0.5rem" }}>
                          <button className="btn-primary" onClick={() => handleAccept(c.id, suggestedName)}>Accept</button>
                          <button className="btn-danger" onClick={() => handleReject(c.id)}>Reject</button>
                        </div>
                      </td>
                    </tr>
                    <tr>
                      <td colSpan={8} style={{ padding: "0.25rem 1rem 0.75rem", color: "var(--text-muted)", fontSize: "0.85rem", fontStyle: "italic", borderTop: "none" }}>
                        {c.reasoning}
                      </td>
                    </tr>
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
