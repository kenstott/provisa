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
import { Sparkles, Code2, Network } from "lucide-react";
import { ErdModal } from "../components/erd/ErdModal";
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
  useAllRelationships,
  useTables,
  useDomains,
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
  const { relationships: allRels } = useAllRelationships();
  const { tables, loading: tablesLoading } = useTables();
  const { domains } = useDomains();
  const [showErd, setShowErd] = useState(false);
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
  const [sortCol, setSortCol] = useState<"id" | "domain" | "source" | "target" | "cardinality">("id");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [groupBy, setGroupBy] = useState<Array<"domain" | "cardinality" | "materialize">>([]);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const toggleGroupBy = (col: "domain" | "cardinality" | "materialize") =>
    setGroupBy((prev) => (prev.includes(col) ? prev.filter((g) => g !== col) : [...prev, col]));
  const [showModelingModal, setShowModelingModal] = useState(false);
  const [conflictRel, setConflictRel] = useState<Relationship | null>(null);

  const { selectedDomain, domainsEnabled } = useDomainFilter();
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
        sourceDomain: rel.sourceDomainId ? normalizeDomain(rel.sourceDomainId) : "",
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
        targetDomain: r.sourceDomainId ?? "",
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
      const srcDomain = r.sourceDomainId ? normalizeDomain(r.sourceDomainId) : undefined;
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
          <button className="btn-icon" title="View ERD" onClick={() => setShowErd(true)}>
            <Network size={14} />
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

      <div style={{ overflowX: "auto" }}>
      <table className="data-table" style={{ width: "100%", tableLayout: "fixed" }}>
        <thead>
          <tr>
            {(
              [
                ["id", "ID", "14%", false],
                ["domain", "Domain", "7%", true],
                ["source", "Source", "15%", false],
                ["target", "Target", "15%", false],
              ] as const
            )
              .filter(([col]) => domainsEnabled || col !== "domain")
              .map(([col, label, width, isGroupable]) => {
              const groupLevel = isGroupable ? groupBy.indexOf(col as "domain" | "cardinality" | "materialize") : -1;
              const isGrouped = groupLevel !== -1;
              return (
                <th key={col} style={{ width, whiteSpace: "nowrap" }}>
                  <span
                    onClick={() => {
                      if (sortCol === col) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                      else { setSortCol(col); setSortDir("asc"); }
                    }}
                    style={{ cursor: "pointer", userSelect: "none" }}
                  >
                    {label}{" "}
                    <span style={{ color: "var(--text-muted)", fontSize: "0.7rem" }}>
                      {sortCol === col ? (sortDir === "asc" ? "▲" : "▼") : "⇅"}
                    </span>
                  </span>
                  {isGroupable && (
                    <span
                      title={isGrouped ? `Ungroup (level ${groupLevel + 1})` : `Group by ${label}`}
                      onClick={() => toggleGroupBy(col as "domain" | "cardinality" | "materialize")}
                      style={{
                        marginLeft: "0.3rem",
                        fontSize: "0.65rem",
                        cursor: "pointer",
                        userSelect: "none",
                        opacity: isGrouped ? 1 : 0.35,
                        color: isGrouped ? "var(--primary, #6366f1)" : undefined,
                      }}
                    >
                      {isGrouped ? `⊞${groupLevel + 1}` : "⊞"}
                    </span>
                  )}
                </th>
              );
            })}
            <th style={{ width: "20%" }}>GQL / CQL Alias</th>
            {(
              [["cardinality", "Cardinality", "11%"], ["materialize", "Materialize", "10%"]] as const
            ).map(([col, label, width]) => {
              const groupLevel = groupBy.indexOf(col as "cardinality" | "materialize");
              const isGrouped = groupLevel !== -1;
              return (
                <th key={col} style={{ width, whiteSpace: "nowrap" }}>
                  <span
                    onClick={() => {
                      if (sortCol === col) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                      else { setSortCol(col); setSortDir("asc"); }
                    }}
                    style={{ cursor: "pointer", userSelect: "none" }}
                  >
                    {label}{" "}
                    <span style={{ color: "var(--text-muted)", fontSize: "0.7rem" }}>
                      {sortCol === col ? (sortDir === "asc" ? "▲" : "▼") : "⇅"}
                    </span>
                  </span>
                  <span
                    title={isGrouped ? `Ungroup (level ${groupLevel + 1})` : `Group by ${label}`}
                    onClick={() => toggleGroupBy(col as "cardinality" | "materialize")}
                    style={{
                      marginLeft: "0.3rem",
                      fontSize: "0.65rem",
                      cursor: "pointer",
                      userSelect: "none",
                      opacity: isGrouped ? 1 : 0.35,
                      color: isGrouped ? "var(--primary, #6366f1)" : undefined,
                    }}
                  >
                    {isGrouped ? `⊞${groupLevel + 1}` : "⊞"}
                  </span>
                </th>
              );
            })}
            <th style={{ width: "8%", whiteSpace: "nowrap" }}>Refresh (s)</th>
          </tr>
        </thead>
        <tbody>
          {(() => {
            const filtered = rels.filter((r) => {
              if (tableSourceById[r.sourceTableId] === "provisa-admin") return false;
              return matchesFilter(r);
            });

            if (filtered.length > 75 && !relSearch.trim() && groupBy.length === 0) {
              return (
                <tr>
                  <td colSpan={domainsEnabled ? 8 : 7} style={{ textAlign: "center", padding: "2rem", color: "var(--text-muted)" }}>
                    {filtered.length} relationships — use the filter above to browse
                  </td>
                </tr>
              );
            }

            filtered.sort((a, b) => {
              let cmp = 0;
              if (sortCol === "id") cmp = String(a.id).localeCompare(String(b.id));
              else if (sortCol === "domain") cmp = (a.sourceDomainId ?? "").localeCompare(b.sourceDomainId ?? "");
              else if (sortCol === "source") cmp = a.sourceTableName.localeCompare(b.sourceTableName);
              else if (sortCol === "target") cmp = (a.targetTableName ?? "").localeCompare(b.targetTableName ?? "");
              else if (sortCol === "cardinality") cmp = a.cardinality.localeCompare(b.cardinality);
              return sortDir === "asc" ? cmp : -cmp;
            });

            const getGroupKey = (r: Relationship, col: "domain" | "cardinality" | "materialize") =>
              col === "domain" ? (r.sourceDomainId ? normalizeDomain(r.sourceDomainId) : "(none)") : col === "materialize" ? (r.materialize ? "Materialized" : "Not Materialized") : r.cardinality;
            const colLabel = (col: "domain" | "cardinality" | "materialize") =>
              col === "domain" ? "Domain" : col === "materialize" ? "Materialize" : "Cardinality";

            type GroupItem =
              | { type: "header"; level: 1 | 2 | 3; key: string; label: string; count: number }
              | { type: "row"; r: Relationship };

            let items: GroupItem[];

            if (groupBy.length === 0) {
              items = filtered
                .slice(relPage * PAGE_SIZE, (relPage + 1) * PAGE_SIZE)
                .map((r) => ({ type: "row" as const, r }));
            } else {
              items = [];
              const l1Col = groupBy[0];
              const l2Col = groupBy[1];
              const l3Col = groupBy[2];
              const l1Map = new Map<string, Relationship[]>();
              for (const r of filtered) {
                const k = getGroupKey(r, l1Col);
                if (!l1Map.has(k)) l1Map.set(k, []);
                l1Map.get(k)!.push(r);
              }
              for (const [l1Key, l1Rels] of [...l1Map.entries()].sort(([a], [b]) => a.localeCompare(b))) {
                items.push({ type: "header", level: 1, key: l1Key, label: `${colLabel(l1Col)}: ${l1Key}`, count: l1Rels.length });
                if (collapsedGroups.has(l1Key)) continue;
                if (!l2Col) {
                  for (const r of l1Rels) items.push({ type: "row", r });
                } else {
                  const l2Map = new Map<string, Relationship[]>();
                  for (const r of l1Rels) {
                    const k = getGroupKey(r, l2Col);
                    if (!l2Map.has(k)) l2Map.set(k, []);
                    l2Map.get(k)!.push(r);
                  }
                  for (const [l2Key, l2Rels] of [...l2Map.entries()].sort(([a], [b]) => a.localeCompare(b))) {
                    const compositeKey = `${l1Key}|${l2Key}`;
                    items.push({ type: "header", level: 2, key: compositeKey, label: `${colLabel(l2Col)}: ${l2Key}`, count: l2Rels.length });
                    if (collapsedGroups.has(compositeKey)) continue;
                    if (!l3Col) {
                      for (const r of l2Rels) items.push({ type: "row", r });
                    } else {
                      const l3Map = new Map<string, Relationship[]>();
                      for (const r of l2Rels) {
                        const k = getGroupKey(r, l3Col);
                        if (!l3Map.has(k)) l3Map.set(k, []);
                        l3Map.get(k)!.push(r);
                      }
                      for (const [l3Key, l3Rels] of [...l3Map.entries()].sort(([a], [b]) => a.localeCompare(b))) {
                        const l3CompositeKey = `${compositeKey}|${l3Key}`;
                        items.push({ type: "header", level: 3, key: l3CompositeKey, label: `${colLabel(l3Col)}: ${l3Key}`, count: l3Rels.length });
                        if (collapsedGroups.has(l3CompositeKey)) continue;
                        for (const r of l3Rels) items.push({ type: "row", r });
                      }
                    }
                  }
                }
              }
            }

            return items.map((item) => {
              if (item.type === "header") {
                const lvl = item.level;
                return (
                  <tr key={`grp-${item.key}`}>
                    <td
                      colSpan={domainsEnabled ? 8 : 7}
                      onClick={() =>
                        setCollapsedGroups((prev) => {
                          const next = new Set(prev);
                          if (next.has(item.key)) next.delete(item.key);
                          else next.add(item.key);
                          return next;
                        })
                      }
                      style={{
                        fontWeight: lvl === 1 ? 600 : lvl === 2 ? 500 : 400,
                        fontSize: lvl === 1 ? "0.8rem" : "0.75rem",
                        padding: lvl === 1 ? "0.35rem 0.75rem" : lvl === 2 ? "0.25rem 1.5rem" : "0.2rem 2.25rem",
                        color: "var(--text-muted)",
                        background: lvl === 1 ? "var(--surface)" : lvl === 2 ? "var(--surface-raised, var(--surface))" : "var(--bg)",
                        borderTop: lvl === 1 ? "2px solid var(--border)" : "1px solid var(--border)",
                        cursor: "pointer",
                        userSelect: "none",
                      }}
                    >
                      {collapsedGroups.has(item.key) ? "▶" : "▼"} {item.label}{" "}
                      <span style={{ fontWeight: "normal", opacity: 0.7 }}>({item.count})</span>
                    </td>
                  </tr>
                );
              }
              const r = item.r;
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
                  domainsEnabled={domainsEnabled}
                />
              );
            });
          })()}
        </tbody>
      </table>
      </div>
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
      {showErd && (
        <ErdModal
          tables={tables}
          relationships={allRels}
          domains={domains}
          activeDomain={selectedDomain !== "all" ? selectedDomain : null}
          onClose={() => setShowErd(false)}
        />
      )}
    </div>
  );
}
