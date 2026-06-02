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
import { useSearchParams } from "react-router-dom";
import { Trash2, Pencil, Sparkles, Save, X, ArrowLeftRight, Code2 } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useAuth } from "../context/AuthContext";
import { SqlModelingModal } from "../components/SqlModelingModal";
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
  disableCypher: false,
};

export function RelationshipsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
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
  const [reverseForm, setReverseForm] = useState<typeof EMPTY_FORM | null>(null);
  const [relSearch, setRelSearch] = useState(() => searchParams.get("search") ?? "");
  const [relPage, setRelPage] = useState(0);
  const PAGE_SIZE = 50;
  const [showModelingModal, setShowModelingModal] = useState(false);
  const [conflictRel, setConflictRel] = useState<Relationship | null>(null);

  const { selectedDomain } = useDomainFilter();
  const { capabilities } = useAuth();
  const canManage = capabilities.includes("create_relationship");

  const updateSearch = (v: string) => {
    setRelSearch(v);
    setRelPage(0);
    setSearchParams((p) => { const n = new URLSearchParams(p); if (v) n.set("search", v); else n.delete("search"); return n; }, { replace: true });
  };

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
    setCandidates(c as Candidate[]);
    setRejectedCount(rc);
    setLoading(false);
  }, []);

  /* eslint-disable-next-line react-hooks/set-state-in-effect --
     mount data-fetch: load() sets loading state synchronously by design */
  useEffect(() => { load(); }, [load]);

  const tableNameById = Object.fromEntries(
    tables.map((t) => [t.id, t.tableName]),
  );
  const normalizeDomain = (id: string) => id.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
  const tableDomainById = Object.fromEntries(
    tables.map((t) => [t.id, normalizeDomain(t.domainId)]),
  );
  const tableSourceById = Object.fromEntries(tables.map((t) => [t.id, t.sourceId]));
  const remoteTableIds = new Set(
    tables
      .filter((t) => t.schemaName === "graphql_remote" || t.schemaName === "grpc_remote")
      .map((t) => t.id),
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
      disableCypher: form.disableCypher,
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
      disableCypher: editingRel.disableCypher,
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
    } catch (e) {
      setDiscoverError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const handleDiscover = useCallback(async () => {
    setDiscovering(true);
    setDiscoverError("");
    setDiscoverMsg("");
    try {
      const result = await discoverRelationships("cross-domain");
      const c = await fetchCandidates();
      setCandidates(c as Candidate[]);
      if (result.candidates_found === 0) {
        setDiscoverError(`AI found 0 candidates (check server logs for details)`);
      }
    } catch (e) {
      setDiscoverError(e instanceof Error ? e.message : String(e));
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
      disableCypher: rel.disableCypher ?? false,
    });
  }, [tableDomainById]);

  const buildReverse = useCallback((r: Relationship): typeof EMPTY_FORM => {
    const flipCardinality = (c: string) =>
      c === "many-to-one" ? "one-to-many" : "many-to-one";
    const suggestCqlAlias = (alias: string | null) => {
      if (!alias) return "";
      if (alias.endsWith("_BY")) return alias.slice(0, -3);
      if (alias.endsWith("_OF")) return alias.slice(0, -3);
      return `${alias}_BY`;
    };
    const cqlToGql = (cql: string) => {
      const parts = cql.toLowerCase().split("_").filter(Boolean);
      return parts[0] + parts.slice(1).map((p) => p[0].toUpperCase() + p.slice(1)).join("");
    };
    const suggestGqlAlias = (gql: string | null, cqlSuggestion: string) => {
      if (gql) {
        if (gql.endsWith("By")) return gql.slice(0, -2);
        if (gql.endsWith("Of")) return gql.slice(0, -2);
        return `${gql}By`;
      }
      return cqlSuggestion ? cqlToGql(cqlSuggestion) : "";
    };
    const cqlSuggestion = suggestCqlAlias(r.alias ?? r.computedCypherAlias ?? null);
    return {
      ...EMPTY_FORM,
      id: `${r.targetTableName}-to-${r.sourceTableName}`,
      sourceTableId: r.targetTableName ?? "",
      sourceDomain: r.targetTableId ? (tableDomainById[r.targetTableId] ?? "") : "",
      sourceColumn: r.targetColumn ?? "",
      targetType: "table",
      targetTableId: r.sourceTableName,
      targetDomain: tableDomainById[r.sourceTableId] ?? "",
      targetColumn: r.sourceColumn,
      cardinality: flipCardinality(r.cardinality),
      alias: cqlSuggestion,
      graphqlAlias: suggestGqlAlias(r.graphqlAlias, cqlSuggestion),
      materialize: r.materialize,
      refreshInterval: String(r.refreshInterval ?? 300),
    };
  }, [tableDomainById]);

  const handleReverseAdd = useCallback(async () => {
    if (!reverseForm || !reverseForm.id || !reverseForm.sourceTableId || !reverseForm.targetTableId) return;
    setSaving("reverse");
    await upsertRelationship({
      id: reverseForm.id,
      sourceTableId: reverseForm.sourceTableId,
      targetTableId: reverseForm.targetTableId,
      sourceColumn: reverseForm.sourceColumn,
      targetColumn: reverseForm.targetColumn,
      cardinality: reverseForm.cardinality,
      materialize: reverseForm.materialize,
      refreshInterval: parseInt(reverseForm.refreshInterval) || 300,
      targetFunctionName: null,
      functionArg: null,
      alias: reverseForm.alias || null,
      graphqlAlias: reverseForm.graphqlAlias || null,
    });
    setSaving(null);
    setReverseForm(null);
    load();
  }, [reverseForm, load]);

  if (loading) return <div className="page">Loading relationships...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Relationships</h2>
        <FilterInput value={relSearch} onChange={updateSearch} placeholder="Filter by source or target…" />
        <div className="page-actions">
          {canManage && (
            <button className="btn-primary" onClick={() => setShowForm(!showForm)}>
              {showForm ? "Cancel" : "+ Relationship"}
            </button>
          )}
          <button
            className="btn-icon"
            title="SQL Modeling tool"
            onClick={() => setShowModelingModal(true)}
          ><Code2 size={14} /></button>
          {canManage && (
            <button
              className="btn-icon"
              title={discovering ? "Discovering..." : "Suggest with AI"}
              onClick={handleDiscover}
              disabled={discovering}
            ><Sparkles size={14} /></button>
          )}
          {canManage && rejectedCount > 0 && (
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
                {form.cardinality === "many-to-one" && (
                  <span style={{ color: "var(--warning, #b45309)", fontSize: "0.78rem", marginTop: "0.25rem", display: "block" }}>
                    Warning: if this join returns more than one row per parent, only the first value will be used.
                  </span>
                )}
              </label>
            )}
          </div>
          <div className="form-row">
            <label className="checkbox-label">
              <input type="checkbox" checked={form.materialize} onChange={(e) => setForm({ ...form, materialize: e.target.checked })} />
              Materialize (auto-create MV for cross-source joins)
            </label>
            <label className="checkbox-label">
              <input type="checkbox" checked={form.disableCypher} onChange={(e) => setForm({ ...form, disableCypher: e.target.checked })} />
              Exclude from Cypher graph
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
          {rels.length > 75 && !relSearch.trim() ? (
            <tr><td colSpan={8} style={{ textAlign: "center", padding: "2rem", color: "var(--text-muted)" }}>
              {rels.length} relationships — use the filter above to browse
            </td></tr>
          ) : (() => {
            const filtered = rels.filter((r) => {
              if (tableSourceById[r.sourceTableId] === "provisa-admin") return false;
              if (remoteTableIds.has(r.sourceTableId)) return false;
              if (selectedDomain !== "all") {
                const srcDomain = tableDomainById[r.sourceTableId];
                const tgtDomain = r.targetTableId != null ? tableDomainById[r.targetTableId] : null;
                const ownerDomain = r.ownerDomainId ? normalizeDomain(r.ownerDomainId) : null;
                if (srcDomain !== selectedDomain && tgtDomain !== selectedDomain && ownerDomain !== selectedDomain) return false;
              }
              if (!relSearch.trim()) return true;
              const q = relSearch.toLowerCase();
              return r.sourceTableName.toLowerCase().includes(q) || r.targetTableName.toLowerCase().includes(q);
            });
            const paged = filtered.slice(relPage * PAGE_SIZE, (relPage + 1) * PAGE_SIZE);
            return paged.map((r) => {
            const id = String(r.id);
            const isExpanded = expanded === id;
            return (
              <React.Fragment key={r.id}>
                <tr
                  onClick={() => { setExpanded(isExpanded ? null : id); setEditingRel(null); }}
                  style={{ cursor: "pointer", background: isExpanded ? "var(--surface)" : undefined }}
                >
                  <td>
                    <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
                      {r.autoSuggested && (
                        <span title="Auto-tracked from FK constraint" style={{ fontSize: "0.65rem", fontWeight: 600, padding: "1px 5px", borderRadius: 3, background: "var(--text-muted)", color: "var(--bg)", letterSpacing: "0.03em" }}>FK</span>
                      )}
                      {r.id}
                    </div>
                  </td>
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
                            <dt style={{ color: "var(--text-muted)" }}><strong>ID</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.id}{r.autoSuggested && <span title="Auto-tracked from FK constraint" style={{ marginLeft: 6, fontSize: "0.65rem", fontWeight: 600, padding: "1px 5px", borderRadius: 3, background: "var(--text-muted)", color: "var(--bg)" }}>FK</span>}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Source</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{tableDomainById[r.sourceTableId] ? `${tableDomainById[r.sourceTableId]}.${r.sourceTableName}.${r.sourceColumn}` : `${r.sourceTableName}.${r.sourceColumn}`}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Target</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.targetFunctionName ? `fn:${r.targetFunctionName}(${r.functionArg ?? ""})` : (tableDomainById[r.targetTableId!] ? `${tableDomainById[r.targetTableId!]}.${r.targetTableName}.${r.targetColumn}` : `${r.targetTableName}.${r.targetColumn}`)}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>GQL Alias</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}><code>{r.graphqlAlias ?? "—"}</code></dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>CQL Alias</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}><code>{r.alias ?? <em style={{ color: "var(--text-muted)" }}>{r.computedCypherAlias ?? "—"}</em>}</code></dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Cardinality</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.cardinality}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Materialize</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.materialize ? "Yes" : "No"}</dd>
                            {r.materialize && <><dt style={{ color: "var(--text-muted)" }}><strong>Refresh Interval (s)</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.refreshInterval ?? "—"}</dd></>}
                            <dt style={{ color: "var(--text-muted)" }}><strong>Cypher Graph</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{r.disableCypher ? <em style={{ color: "var(--text-muted)" }}>excluded</em> : "included"}</dd>
                          </dl>
                          {canManage && (
                            <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                              <button className="btn-icon" title="Edit" onClick={(e) => { e.stopPropagation(); startEditing(r); }}><Pencil size={14} /></button>
                              <button className="btn-icon" title="Generate reverse relationship" onClick={(e) => { e.stopPropagation(); setReverseForm(buildReverse(r)); }}><ArrowLeftRight size={14} /></button>
                              <button className="btn-icon-danger" title="Delete" onClick={(e) => { e.stopPropagation(); handleDelete(id); }}><Trash2 size={14} /></button>
                            </div>
                          )}
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
                              {editingRel.cardinality === "many-to-one" && (
                                <span style={{ color: "var(--warning, #b45309)", fontSize: "0.78rem", marginTop: "0.25rem", display: "block" }}>
                                  Warning: if this join returns more than one row per parent, only the first value will be used.
                                </span>
                              )}
                            </label>
                            )}
                            <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap", flex: "0 0 auto" }}>
                              <input type="checkbox" checked={editingRel.materialize} onChange={(e) => setEditingRel({ ...editingRel, materialize: e.target.checked })} style={{ width: "auto", padding: 0 }} />
                              Materialize
                            </label>
                            <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap", flex: "0 0 auto" }}>
                              <input type="checkbox" checked={editingRel.disableCypher} onChange={(e) => setEditingRel({ ...editingRel, disableCypher: e.target.checked })} style={{ width: "auto", padding: 0 }} />
                              Exclude from Cypher
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
            });
          })()}
        </tbody>
      </table>
      {(() => {
        const filtered = rels.filter((r) => {
          if (remoteTableIds.has(r.sourceTableId)) return false;
          if (selectedDomain !== "all") {
            const srcDomain = tableDomainById[r.sourceTableId];
            const tgtDomain = r.targetTableId != null ? tableDomainById[r.targetTableId] : null;
            const ownerDomain = r.ownerDomainId ? normalizeDomain(r.ownerDomainId) : null;
            if (srcDomain !== selectedDomain && tgtDomain !== selectedDomain && ownerDomain !== selectedDomain) return false;
          }
          if (!relSearch.trim()) return true;
          const q = relSearch.toLowerCase();
          return r.sourceTableName.toLowerCase().includes(q) || r.targetTableName.toLowerCase().includes(q);
        });
        const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
        if (totalPages === 1) return null;
        return (
          <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0" }}>
            <button onClick={() => setRelPage(0)} disabled={relPage === 0}>«</button>
            <button onClick={() => setRelPage(p => p - 1)} disabled={relPage === 0}>‹</button>
            <span>Page {relPage + 1} / {totalPages}</span>
            <button onClick={() => setRelPage(p => p + 1)} disabled={relPage >= totalPages - 1}>›</button>
            <button onClick={() => setRelPage(totalPages - 1)} disabled={relPage >= totalPages - 1}>»</button>
          </div>
        );
      })()}

      {showModelingModal && (
        <SqlModelingModal
          tables={tables}
          existingRels={rels}
          onClose={() => setShowModelingModal(false)}
          onPromote={canManage ? async (c) => {
            const existing = rels.find(
              (r) =>
                r.sourceTableName === c.sourceTable &&
                r.sourceColumn === c.sourceCol &&
                r.targetTableName === c.targetTable &&
                r.targetColumn === c.targetCol,
            );
            if (existing) {
              setConflictRel(existing);
              return;
            }
            await upsertRelationship({
              id: c.id,
              sourceTableId: c.sourceTable,
              targetTableId: c.targetTable,
              sourceColumn: c.sourceCol,
              targetColumn: c.targetCol,
              cardinality: c.cardinality,
              materialize: false,
              refreshInterval: 300,
              targetFunctionName: null,
              functionArg: null,
              alias: null,
              graphqlAlias: null,
              recordCandidate: true,
            });
            load();
          } : undefined}
        />
      )}

      {conflictRel && (
        <div className="modal-overlay" onClick={() => setConflictRel(null)}>
          <div className="modal" style={{ width: "500px", maxWidth: "500px" }} onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Relationship Already Exists</h3>
              <button className="modal-close" onClick={() => setConflictRel(null)}><X size={14} /></button>
            </div>
            <div className="form-card" style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
              <p style={{ margin: 0, fontSize: "0.85rem", color: "var(--text-muted)" }}>
                A relationship with this source and target already exists:
              </p>
              <dl style={{ margin: 0, display: "grid", gridTemplateColumns: "auto 1fr", gap: "0.35rem 1rem", fontSize: "0.82rem" }}>
                <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>ID</dt>
                <dd style={{ margin: 0, fontFamily: "monospace" }}>{conflictRel.id}</dd>
                <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>Source</dt>
                <dd style={{ margin: 0, fontFamily: "monospace" }}>{conflictRel.sourceTableName}.{conflictRel.sourceColumn}</dd>
                <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>Target</dt>
                <dd style={{ margin: 0, fontFamily: "monospace" }}>{conflictRel.targetTableName}.{conflictRel.targetColumn}</dd>
                <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>Cardinality</dt>
                <dd style={{ margin: 0 }}>{conflictRel.cardinality}</dd>
                {conflictRel.alias && (
                  <>
                    <dt style={{ color: "var(--text-muted)", fontWeight: 600 }}>Alias</dt>
                    <dd style={{ margin: 0, fontFamily: "monospace" }}>{conflictRel.alias}</dd>
                  </>
                )}
              </dl>
              <div style={{ display: "flex", justifyContent: "flex-end" }}>
                <button className="btn-secondary" onClick={() => setConflictRel(null)}>Close</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {reverseForm && (
        <div className="modal-overlay" onClick={() => setReverseForm(null)}>
          <div className="modal" style={{ width: "730px", maxWidth: "730px" }} onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Generate Reverse Relationship</h3>
              <button className="modal-close" onClick={() => setReverseForm(null)}><X size={14} /></button>
            </div>
            <div className="form-card">
              <div className="form-row">
                <label>ID
                  <input value={reverseForm.id} onChange={(e) => setReverseForm({ ...reverseForm, id: e.target.value })} />
                </label>
                <label>CQL Alias (UPPER_SNAKE)
                  <input value={reverseForm.alias} onChange={(e) => setReverseForm({ ...reverseForm, alias: e.target.value })} placeholder="PLACED_BY" />
                </label>
                <label>GQL Alias (camelCase)
                  <input value={reverseForm.graphqlAlias} onChange={(e) => setReverseForm({ ...reverseForm, graphqlAlias: e.target.value })} />
                </label>
                <label className="checkbox-label">
                  <input type="checkbox" checked={reverseForm.materialize} onChange={(e) => setReverseForm({ ...reverseForm, materialize: e.target.checked })} />
                  Materialize
                </label>
                {reverseForm.materialize && (
                  <label>Refresh Interval (s)
                    <input type="number" value={reverseForm.refreshInterval} onChange={(e) => setReverseForm({ ...reverseForm, refreshInterval: e.target.value })} />
                  </label>
                )}
              </div>
            </div>
            <div className="modal-actions">
              <button className="btn-secondary" onClick={() => setReverseForm(null)}>Cancel</button>
              <button className="btn-primary" onClick={handleReverseAdd} disabled={saving === "reverse"}>Save</button>
            </div>
          </div>
        </div>
      )}

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
