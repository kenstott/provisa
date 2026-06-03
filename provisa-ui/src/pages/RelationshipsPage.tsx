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
import { useSearchParams } from "react-router-dom";
import { Sparkles, Code2 } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useAuth } from "../context/AuthContext";
import { SqlModelingModal } from "../components/SqlModelingModal";
import {
  discoverRelationships,
  fetchCandidates,
  fetchRejectedCount,
  acceptCandidate,
  rejectCandidate,
  clearRejectedCandidates,
} from "../api/admin";
import {
  useRelationships,
  useTables,
  useUpsertRelationship,
  useDeleteRelationship,
} from "../hooks/useAdminQueries";
import { fetchActions } from "../api/actions";
import type { TrackedFunction } from "../api/actions";
import type { Relationship } from "../types/admin";
import { EMPTY_FORM, type Candidate, type RelForm } from "../components/relationships/relationship-types";
import { AddRelationshipForm } from "../components/relationships/AddRelationshipForm";
import { RelationshipRow } from "../components/relationships/RelationshipRow";
import {
  ConflictModal,
  ReverseRelationshipModal,
} from "../components/relationships/RelationshipModals";
import { CandidatesTable } from "../components/relationships/CandidatesTable";

export function RelationshipsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { relationships: rels, loading: relsLoading, refetch: refetchRels } = useRelationships();
  const { tables, loading: tablesLoading } = useTables();
  const { upsertRelationship } = useUpsertRelationship();
  const { deleteRelationship } = useDeleteRelationship();
  const [functions, setFunctions] = useState<TrackedFunction[]>([]);
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [restLoading, setRestLoading] = useState(true);
  const [saving, setSaving] = useState<string | null>(null);
  const [discovering, setDiscovering] = useState(false);
  const [discoverError, setDiscoverError] = useState("");
  const [discoverMsg, setDiscoverMsg] = useState("");
  const [rejectedCount, setRejectedCount] = useState(0);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<RelForm>(EMPTY_FORM);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [editingRel, setEditingRel] = useState<RelForm | null>(null);
  const [reverseForm, setReverseForm] = useState<RelForm | null>(null);
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
    setSearchParams(
      (p) => {
        const n = new URLSearchParams(p);
        if (v) n.set("search", v);
        else n.delete("search");
        return n;
      },
      { replace: true },
    );
  };

  const load = useCallback(async () => {
    setRestLoading(true);
    const [actions, c, rc] = await Promise.all([
      fetchActions().catch(() => ({ functions: [], webhooks: [] })),
      fetchCandidates().catch(() => []),
      fetchRejectedCount().catch(() => 0),
    ]);
    setFunctions(actions.functions);
    setCandidates(c as Candidate[]);
    setRejectedCount(rc);
    setRestLoading(false);
  }, []);

  /* eslint-disable react-hooks/set-state-in-effect -- mount data-fetch: load() sets loading state synchronously by design */
  useEffect(() => {
    load();
  }, [load]);
  /* eslint-enable react-hooks/set-state-in-effect */

  const loading = relsLoading || tablesLoading || restLoading;

  const tableNameById = Object.fromEntries(tables.map((t) => [t.id, t.tableName]));
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
    },
    [deleteRelationship],
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
  }, [form, upsertRelationship]);

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
      targetFunctionName:
        editingRel.targetType === "function" ? editingRel.targetFunctionName : null,
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
  }, [editingRel, upsertRelationship, deleteRelationship]);

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

  const handleAccept = useCallback(
    async (id: number, name: string) => {
      await acceptCandidate(id, name);
      await refetchRels();
      load();
    },
    [load, refetchRels],
  );

  const handleReject = useCallback(async (id: number) => {
    await rejectCandidate(id, "Rejected by user");
    setCandidates((prev) => prev.filter((c) => c.id !== id));
    setRejectedCount((prev) => prev + 1);
  }, []);

  const startEditing = useCallback(
    (rel: Relationship) => {
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
    },
    [tableDomainById],
  );

  const buildReverse = useCallback(
    (r: Relationship): RelForm => {
      const flipCardinality = (c: string) => (c === "many-to-one" ? "one-to-many" : "many-to-one");
      const suggestCqlAlias = (alias: string | null) => {
        if (!alias) return "";
        if (alias.endsWith("_BY")) return alias.slice(0, -3);
        if (alias.endsWith("_OF")) return alias.slice(0, -3);
        return `${alias}_BY`;
      };
      const cqlToGql = (cql: string) => {
        const parts = cql.toLowerCase().split("_").filter(Boolean);
        return (
          parts[0] +
          parts
            .slice(1)
            .map((p) => p[0].toUpperCase() + p.slice(1))
            .join("")
        );
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
    },
    [tableDomainById],
  );

  const handleReverseAdd = useCallback(async () => {
    if (!reverseForm || !reverseForm.id || !reverseForm.sourceTableId || !reverseForm.targetTableId)
      return;
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
  }, [reverseForm, upsertRelationship]);

  if (loading) return <div className="page">Loading relationships...</div>;

  const matchesFilter = (r: Relationship) => {
    if (remoteTableIds.has(r.sourceTableId)) return false;
    if (selectedDomain !== "all") {
      const srcDomain = tableDomainById[r.sourceTableId];
      const tgtDomain = r.targetTableId != null ? tableDomainById[r.targetTableId] : null;
      const ownerDomain = r.ownerDomainId ? normalizeDomain(r.ownerDomainId) : null;
      if (srcDomain !== selectedDomain && tgtDomain !== selectedDomain && ownerDomain !== selectedDomain)
        return false;
    }
    if (!relSearch.trim()) return true;
    const q = relSearch.toLowerCase();
    return (
      r.sourceTableName.toLowerCase().includes(q) || r.targetTableName.toLowerCase().includes(q)
    );
  };

  const filteredForPaging = rels.filter(matchesFilter);
  const totalPages = Math.max(1, Math.ceil(filteredForPaging.length / PAGE_SIZE));

  return (
    <div className="page">
      <div className="page-header">
        <h2>Relationships</h2>
        <FilterInput
          value={relSearch}
          onChange={updateSearch}
          placeholder="Filter by source or target…"
        />
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
          >
            <Code2 size={14} />
          </button>
          {canManage && (
            <button
              className="btn-icon"
              title={discovering ? "Discovering..." : "Suggest with AI"}
              onClick={handleDiscover}
              disabled={discovering}
            >
              <Sparkles size={14} />
            </button>
          )}
          {canManage && rejectedCount > 0 && (
            <button className="btn-secondary" onClick={handleClearRejections}>
              Clear Rejections
            </button>
          )}
        </div>
      </div>

      {discoverError && <div className="error">{discoverError}</div>}
      {discoverMsg && (
        <div style={{ color: "var(--approve)", marginBottom: "1rem", fontSize: "0.875rem" }}>
          {discoverMsg}
        </div>
      )}

      {showForm && (
        <AddRelationshipForm
          form={form}
          setForm={setForm}
          tables={tables}
          functions={functions}
          saving={saving}
          onSave={handleAdd}
        />
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
            <tr>
              <td
                colSpan={8}
                style={{ textAlign: "center", padding: "2rem", color: "var(--text-muted)" }}
              >
                {rels.length} relationships — use the filter above to browse
              </td>
            </tr>
          ) : (
            rels
              .filter((r) => {
                if (tableSourceById[r.sourceTableId] === "provisa-admin") return false;
                return matchesFilter(r);
              })
              .slice(relPage * PAGE_SIZE, (relPage + 1) * PAGE_SIZE)
              .map((r) => {
                const id = String(r.id);
                return (
                  <RelationshipRow
                    key={r.id}
                    rel={r}
                    isExpanded={expanded === id}
                    onToggle={() => {
                      setExpanded(expanded === id ? null : id);
                      setEditingRel(null);
                    }}
                    editingRel={editingRel}
                    setEditingRel={setEditingRel}
                    canManage={canManage}
                    onStartEdit={() => startEditing(r)}
                    onReverse={() => setReverseForm(buildReverse(r))}
                    onDelete={() => handleDelete(id)}
                    onEditSave={handleEditSave}
                    saving={saving}
                    tables={tables}
                    functions={functions}
                    tableDomainById={tableDomainById}
                    normalizeDomain={normalizeDomain}
                  />
                );
              })
          )}
        </tbody>
      </table>
      {totalPages > 1 && (
        <div
          style={{
            display: "flex",
            gap: "0.5rem",
            alignItems: "center",
            justifyContent: "flex-end",
            padding: "0.5rem 0",
          }}
        >
          <button onClick={() => setRelPage(0)} disabled={relPage === 0}>
            «
          </button>
          <button onClick={() => setRelPage((p) => p - 1)} disabled={relPage === 0}>
            ‹
          </button>
          <span>
            Page {relPage + 1} / {totalPages}
          </span>
          <button onClick={() => setRelPage((p) => p + 1)} disabled={relPage >= totalPages - 1}>
            ›
          </button>
          <button onClick={() => setRelPage(totalPages - 1)} disabled={relPage >= totalPages - 1}>
            »
          </button>
        </div>
      )}

      {showModelingModal && (
        <SqlModelingModal
          tables={tables}
          existingRels={rels}
          onClose={() => setShowModelingModal(false)}
          onPromote={
            canManage
              ? async (c) => {
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
                }
              : undefined
          }
        />
      )}

      {conflictRel && <ConflictModal rel={conflictRel} onClose={() => setConflictRel(null)} />}

      {reverseForm && (
        <ReverseRelationshipModal
          reverseForm={reverseForm}
          setReverseForm={setReverseForm}
          saving={saving}
          onSave={handleReverseAdd}
        />
      )}

      {candidates.length > 0 && (
        <CandidatesTable
          candidates={candidates}
          tableDomainById={tableDomainById}
          tableNameById={tableNameById}
          onAccept={handleAccept}
          onReject={handleReject}
        />
      )}
    </div>
  );
}
