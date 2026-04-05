// Copyright (c) 2025 Kenneth Stott
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

  const toggleMaterialize = useCallback(
    async (rel: Relationship) => {
      setSaving(String(rel.id));
      await upsertRelationship({
        id: String(rel.id),
        sourceTableId: rel.sourceTableName,
        targetTableId: rel.targetTableName,
        sourceColumn: rel.sourceColumn,
        targetColumn: rel.targetColumn,
        cardinality: rel.cardinality,
        materialize: !rel.materialize,
        refreshInterval: rel.refreshInterval,
      });
      setSaving(null);
      load();
    },
    [load],
  );

  const handleDelete = useCallback(
    async (id: string) => {
      await deleteRelationship(id);
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
          {rels.map((r) => (
            <tr key={r.id}>
              <td>{r.id}</td>
              <td>{r.sourceTableName}</td>
              <td>{r.targetTableName}</td>
              <td>{r.sourceColumn}</td>
              <td>{r.targetColumn}</td>
              <td>{r.cardinality}</td>
              <td>
                <input
                  type="checkbox"
                  checked={r.materialize}
                  disabled={saving === String(r.id)}
                  onChange={() => toggleMaterialize(r)}
                />
              </td>
              <td>{r.refreshInterval}</td>
              <td>
                <button className="btn-danger btn-sm" onClick={() => handleDelete(String(r.id))}>
                  Delete
                </button>
              </td>
            </tr>
          ))}
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
