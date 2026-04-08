// Copyright (c) 2026 Kenneth Stott
// Canary: b2e1f836-e23f-41ef-9df2-88110c6b9f1d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useCallback } from "react";
import {
  fetchRelationships,
  fetchTables,
  upsertRelationship,
  deleteRelationship,
  discoverRelationships,
  fetchCandidates,
  acceptCandidate,
  rejectCandidate,
} from "../api/admin";
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
}

const EMPTY_FORM = {
  id: "",
  sourceTableId: "",
  targetTableId: "",
  sourceColumn: "",
  targetColumn: "",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: "300",
};

export function RelationshipsPage() {
  const [rels, setRels] = useState<Relationship[]>([]);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState<string | null>(null);
  const [discovering, setDiscovering] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState(EMPTY_FORM);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [editingRel, setEditingRel] = useState<typeof EMPTY_FORM | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    const [r, t, c] = await Promise.all([
      fetchRelationships(),
      fetchTables(),
      fetchCandidates().catch(() => []),
    ]);
    setRels(r);
    setTables(t);
    setCandidates(c);
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const tableNameById = Object.fromEntries(
    tables.map((t) => [t.id, t.tableName]),
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
    if (!form.id || !form.sourceTableId || !form.targetTableId) return;
    setSaving("new");
    await upsertRelationship({
      id: form.id,
      sourceTableId: form.sourceTableId,
      targetTableId: form.targetTableId,
      sourceColumn: form.sourceColumn,
      targetColumn: form.targetColumn,
      cardinality: form.cardinality,
      materialize: form.materialize,
      refreshInterval: parseInt(form.refreshInterval) || 300,
    });
    setSaving(null);
    setForm(EMPTY_FORM);
    setShowForm(false);
    load();
  }, [form, load]);

  const handleEditSave = useCallback(async () => {
    if (!editingRel?.id) return;
    setSaving(editingRel.id);
    await upsertRelationship({
      id: editingRel.id,
      sourceTableId: editingRel.sourceTableId,
      targetTableId: editingRel.targetTableId,
      sourceColumn: editingRel.sourceColumn,
      targetColumn: editingRel.targetColumn,
      cardinality: editingRel.cardinality,
      materialize: editingRel.materialize,
      refreshInterval: parseInt(editingRel.refreshInterval) || 300,
    });
    setSaving(null);
    setEditingRel(null);
    load();
  }, [editingRel, load]);

  const handleDiscover = useCallback(async () => {
    setDiscovering(true);
    await discoverRelationships("cross-domain");
    const c = await fetchCandidates().catch(() => []);
    setCandidates(c);
    setDiscovering(false);
  }, []);

  const handleAccept = useCallback(async (id: number) => {
    await acceptCandidate(id);
    load();
  }, [load]);

  const handleReject = useCallback(async (id: number) => {
    await rejectCandidate(id, "Rejected by user");
    setCandidates((prev) => prev.filter((c) => c.id !== id));
  }, []);

  const startEditing = useCallback((rel: Relationship) => {
    setEditingRel({
      id: String(rel.id),
      sourceTableId: rel.sourceTableName,
      targetTableId: rel.targetTableName,
      sourceColumn: rel.sourceColumn,
      targetColumn: rel.targetColumn,
      cardinality: rel.cardinality,
      materialize: rel.materialize,
      refreshInterval: String(rel.refreshInterval ?? 300),
    });
  }, []);

  if (loading) return <div className="page">Loading relationships...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Relationships</h2>
        <div className="page-actions">
          <button className="btn-primary" onClick={() => setShowForm(!showForm)}>
            {showForm ? "Cancel" : "Add Relationship"}
          </button>
          <button
            className="btn-secondary"
            onClick={handleDiscover}
            disabled={discovering}
          >
            {discovering ? "Discovering..." : "Suggest with AI"}
          </button>
        </div>
      </div>

      {showForm && (
        <div className="form-card">
          <div className="form-row">
            <label>
              ID
              <input value={form.id} onChange={(e) => setForm({ ...form, id: e.target.value })} placeholder="orders-to-customers" />
            </label>
            <label>
              Source Table
              <select value={form.sourceTableId} onChange={(e) => setForm({ ...form, sourceTableId: e.target.value })}>
                <option value="">Select...</option>
                {tables.map((t) => <option key={t.id} value={t.tableName}>{t.tableName}</option>)}
              </select>
            </label>
            <label>
              Target Table
              <select value={form.targetTableId} onChange={(e) => setForm({ ...form, targetTableId: e.target.value })}>
                <option value="">Select...</option>
                {tables.map((t) => <option key={t.id} value={t.tableName}>{t.tableName}</option>)}
              </select>
            </label>
          </div>
          <div className="form-row">
            <label>
              Source Column
              <input value={form.sourceColumn} onChange={(e) => setForm({ ...form, sourceColumn: e.target.value })} placeholder="customer_id" />
            </label>
            <label>
              Target Column
              <input value={form.targetColumn} onChange={(e) => setForm({ ...form, targetColumn: e.target.value })} placeholder="id" />
            </label>
            <label>
              Cardinality
              <select value={form.cardinality} onChange={(e) => setForm({ ...form, cardinality: e.target.value })}>
                <option value="many-to-one">many-to-one</option>
                <option value="one-to-many">one-to-many</option>
              </select>
            </label>
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
            <th>Source Table</th>
            <th>Target Table</th>
            <th>Source Column</th>
            <th>Target Column</th>
            <th>Cardinality</th>
            <th>Materialize</th>
            <th>Refresh (s)</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {rels.map((r) => {
            const id = String(r.id);
            const isExpanded = expanded === id;
            return (
              <>
                <tr
                  key={r.id}
                  onClick={() => { setExpanded(isExpanded ? null : id); setEditingRel(null); }}
                  style={{ cursor: "pointer", background: isExpanded ? "var(--color-row-selected, #e8f0fe)" : undefined }}
                >
                  <td>{r.id}</td>
                  <td>{r.sourceTableName}</td>
                  <td>{r.targetTableName}</td>
                  <td>{r.sourceColumn}</td>
                  <td>{r.targetColumn}</td>
                  <td>{r.cardinality}</td>
                  <td>{r.materialize ? "Yes" : "No"}</td>
                  <td>{r.refreshInterval}</td>
                  <td>
                    <button
                      className="btn-danger btn-sm"
                      onClick={(e) => { e.stopPropagation(); handleDelete(id); }}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
                {isExpanded && (
                  <tr key={`${r.id}-detail`}>
                    <td colSpan={9} style={{ padding: "0.75rem 1rem", background: "var(--surface-secondary, #f8f9fa)" }}>
                      {!editingRel ? (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          <dl style={{ display: "grid", gridTemplateColumns: "max-content 1fr", gap: "0.25rem 1rem", margin: 0 }}>
                            <dt><strong>ID</strong></dt><dd>{r.id}</dd>
                            <dt><strong>Source Table</strong></dt><dd>{r.sourceTableName}</dd>
                            <dt><strong>Target Table</strong></dt><dd>{r.targetTableName}</dd>
                            <dt><strong>Source Column</strong></dt><dd>{r.sourceColumn}</dd>
                            <dt><strong>Target Column</strong></dt><dd>{r.targetColumn}</dd>
                            <dt><strong>Cardinality</strong></dt><dd>{r.cardinality}</dd>
                            <dt><strong>Materialize</strong></dt><dd>{r.materialize ? "Yes" : "No"}</dd>
                            <dt><strong>Refresh Interval (s)</strong></dt><dd>{r.refreshInterval ?? "—"}</dd>
                          </dl>
                          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                            <button className="btn-secondary btn-sm" onClick={(e) => { e.stopPropagation(); startEditing(r); }}>Edit</button>
                          </div>
                        </div>
                      ) : (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                          <div className="form-row">
                            <label>
                              Source Table
                              <select value={editingRel.sourceTableId} onChange={(e) => setEditingRel({ ...editingRel, sourceTableId: e.target.value })}>
                                <option value="">Select...</option>
                                {tables.map((t) => <option key={t.id} value={t.tableName}>{t.tableName}</option>)}
                              </select>
                            </label>
                            <label>
                              Target Table
                              <select value={editingRel.targetTableId} onChange={(e) => setEditingRel({ ...editingRel, targetTableId: e.target.value })}>
                                <option value="">Select...</option>
                                {tables.map((t) => <option key={t.id} value={t.tableName}>{t.tableName}</option>)}
                              </select>
                            </label>
                          </div>
                          <div className="form-row">
                            <label>
                              Source Column
                              <input value={editingRel.sourceColumn} onChange={(e) => setEditingRel({ ...editingRel, sourceColumn: e.target.value })} />
                            </label>
                            <label>
                              Target Column
                              <input value={editingRel.targetColumn} onChange={(e) => setEditingRel({ ...editingRel, targetColumn: e.target.value })} />
                            </label>
                            <label>
                              Cardinality
                              <select value={editingRel.cardinality} onChange={(e) => setEditingRel({ ...editingRel, cardinality: e.target.value })}>
                                <option value="many-to-one">many-to-one</option>
                                <option value="one-to-many">one-to-many</option>
                              </select>
                            </label>
                          </div>
                          <div className="form-row">
                            <label className="checkbox-label">
                              <input
                                type="checkbox"
                                checked={editingRel.materialize}
                                onChange={(e) => setEditingRel({ ...editingRel, materialize: e.target.checked })}
                              />
                              Materialize
                            </label>
                            {editingRel.materialize && (
                              <label>
                                Refresh Interval (s)
                                <input
                                  type="number"
                                  value={editingRel.refreshInterval}
                                  onChange={(e) => setEditingRel({ ...editingRel, refreshInterval: e.target.value })}
                                />
                              </label>
                            )}
                            <button className="btn-primary btn-sm" onClick={handleEditSave} disabled={saving === editingRel.id}>
                              {saving === editingRel.id ? "Saving..." : "Save"}
                            </button>
                            <button className="btn-secondary btn-sm" onClick={() => setEditingRel(null)}>
                              Cancel
                            </button>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </>
            );
          })}
        </tbody>
      </table>

      {candidates.length > 0 && (
        <>
          <h3>AI-Suggested Relationships</h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>Source</th><th>Target</th>
                <th>Columns</th><th>Cardinality</th>
                <th>Confidence</th><th>Reasoning</th><th></th>
              </tr>
            </thead>
            <tbody>
              {candidates.map((c) => (
                <tr key={c.id}>
                  <td>{tableNameById[c.source_table_id] ?? c.source_table_id}</td>
                  <td>{tableNameById[c.target_table_id] ?? c.target_table_id}</td>
                  <td>{c.source_column} → {c.target_column}</td>
                  <td>{c.cardinality}</td>
                  <td>{(c.confidence * 100).toFixed(0)}%</td>
                  <td className="reasoning-cell">{c.reasoning}</td>
                  <td>
                    <button className="btn-primary btn-sm" onClick={() => handleAccept(c.id)}>Accept</button>
                    <button className="btn-danger btn-sm" onClick={() => handleReject(c.id)}>Reject</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
