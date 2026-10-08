// Copyright (c) 2026 Kenneth Stott
// Canary: b2e1f836-e23f-41ef-9df2-88110c6b9f1d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { relationshipInCheckedDomains } from "./relationshipDomainFilter";
import { useState, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Sparkles, Code2, Network, X } from "lucide-react";
import { ActionIcon, Alert, Button, Group, Pagination, Table, Text, Title } from "@mantine/core";
import { ErdModal } from "../components/erd/ErdModal";
import { FilterInput } from "../components/admin/FilterInput";
import { HelpBubble } from "../components/HelpBubble";
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
import {
  EMPTY_FORM,
  type Candidate,
  type RelForm,
} from "../components/relationships/relationship-types";
import { AddRelationshipForm } from "../components/relationships/AddRelationshipForm";
import { RelationshipRow } from "../components/relationships/RelationshipRow";
import { ListTable, ListHead, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, pageItems, type ListColumn } from "../components/list/useListSortGroup";
import {
  ConflictModal,
  ReverseRelationshipModal,
} from "../components/relationships/RelationshipModals";
import { CandidatesTable } from "../components/relationships/CandidatesTable";
import { PageLoading } from "../components/PageLoading";
import { useDependentsDialog } from "../hooks/useDependentsDialog";

export function RelationshipsPage() {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { t } = useTranslation();
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

  const {
    domainsEnabled,
    domains: filterDomains,
    checkedDomains,
  } = useDomainFilter();
  const erdCheckedDomains =
    checkedDomains.size > 0 && checkedDomains.size < filterDomains.length ? checkedDomains : null;
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
    const [actions, c, rc] = await Promise.all([
      fetchActions().catch(() => ({ functions: [], webhooks: [] })),
      fetchCandidates().catch(() => []),
      fetchRejectedCount().catch(() => 0),
    ]);
    setFunctions(actions.functions);
    setCandidates(c as Candidate[]);
    setRejectedCount(rc);
  }, []);

  /* eslint-disable react-hooks/set-state-in-effect -- mount data-fetch: load() stores its results in state by design */
  useEffect(() => {
    load();
  }, [load]);
  /* eslint-enable react-hooks/set-state-in-effect */

  // The page's own content is the relationships table, so only its two queries can withhold it.
  // The three REST reads back secondary regions that render themselves once they arrive — the
  // discovered-candidates table, the action list behind the add-form's function picker, and the
  // rejected-suggestion count — and blanking the whole page for them meant the guided tour
  // (REQ-1362) landed here on a "Loading…" screen with none of its anchors present.
  const loading = relsLoading || tablesLoading;

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
      const result = await deleteRelationship(id);
      if (refusal.refused(result, id)) return;
      setExpanded((prev) => (prev === id ? null : prev));
      setEditingRel(null);
    },
    [deleteRelationship, refusal],
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
      // REQ-1586: the junction declaration saves with the edge.
      viaTable: form.viaTable,
      viaSourceColumn: form.viaSourceColumn,
      viaTargetColumn: form.viaTargetColumn,
      viaTypeColumn: form.viaTypeColumn || null,
      viaTypeValue: form.viaTypeValue || null,
      viaLabelSource: form.viaLabelSource,
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
      // REQ-1586: the junction declaration saves with the edge.
      viaTable: editingRel.viaTable,
      viaSourceColumn: editingRel.viaSourceColumn,
      viaTargetColumn: editingRel.viaTargetColumn,
      viaTypeColumn: editingRel.viaTypeColumn || null,
      viaTypeValue: editingRel.viaTypeValue || null,
      viaLabelSource: editingRel.viaLabelSource,
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
      setDiscoverMsg(t("relationshipsPage.clearedRejections", { count: result.deleted }));
      setRejectedCount(0);
    } catch (e) {
      setDiscoverError(e instanceof Error ? e.message : String(e));
    }
  }, [t]);

  const handleDiscover = useCallback(async () => {
    setDiscovering(true);
    setDiscoverError("");
    setDiscoverMsg("");
    try {
      const result = await discoverRelationships("cross-domain");
      const c = await fetchCandidates();
      setCandidates(c as Candidate[]);
      if (result.candidates_found === 0) {
        setDiscoverError(t("relationshipsPage.discoverZeroCandidates"));
      }
    } catch (e) {
      setDiscoverError(e instanceof Error ? e.message : String(e));
    } finally {
      setDiscovering(false);
    }
  }, [t]);

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
        viaTable: rel.viaTableName ?? "",
        viaSourceColumn: rel.viaSourceColumn ?? "",
        viaTargetColumn: rel.viaTargetColumn ?? "",
        viaTypeColumn: rel.viaTypeColumn ?? "",
        viaTypeValue: rel.viaTypeValue ?? "",
        viaLabelSource: rel.viaLabelSource ?? "",
        // The panel opens for a row that already declares a junction; the checkbox only has to
        // hold it open for a row that does not yet name a table.
        junctionDeclared: Boolean(rel.viaTableName),
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

  const narrowTo = erdCheckedDomains ? new Set([...erdCheckedDomains].map(normalizeDomain)) : null;
  const matchesFilter = (r: Relationship) => {
    if (remoteTableIds.has(r.sourceTableId)) return false;
    const srcDomain = r.sourceDomainId ? normalizeDomain(r.sourceDomainId) : undefined;
    const tgtDomain = r.targetTableId != null ? tableDomainById[r.targetTableId] : null;
    const ownerDomain = r.ownerDomainId ? normalizeDomain(r.ownerDomainId) : null;
    if (!relationshipInCheckedDomains(narrowTo, [srcDomain, tgtDomain, ownerDomain])) return false;
    if (!relSearch.trim()) return true;
    const q = relSearch.toLowerCase();
    return (
      r.sourceTableName.toLowerCase().includes(q) || r.targetTableName.toLowerCase().includes(q)
    );
  };

  // REQ-1940: sort and group are the shared list mechanism.
  const filteredRels = rels.filter(
    (r) => tableSourceById[r.sourceTableId] !== "provisa-admin" && matchesFilter(r),
  );
  const relDomain = (r: Relationship) =>
    r.sourceDomainId ? normalizeDomain(r.sourceDomainId) : t("relationshipsPage.none");
  const relColumns: ListColumn<Relationship>[] = [
    {
      key: "domain",
      label: t("relationshipsPage.domain"),
      sortValue: (r) => r.sourceDomainId ?? "",
      groupValue: relDomain,
    },
    { key: "source", label: t("relationshipsPage.source"), sortValue: (r) => r.sourceTableName },
    {
      key: "target",
      label: t("relationshipsPage.target"),
      sortValue: (r) => r.targetTableName ?? "",
    },
    {
      key: "cardinality",
      label: t("relationshipsPage.cardinality"),
      sortValue: (r) => r.cardinality,
      groupValue: (r) => r.cardinality,
    },
    {
      key: "materialize",
      label: t("relationshipsPage.materialize"),
      sortValue: (r) => (r.materialize ? 1 : 0),
      groupValue: (r) =>
        r.materialize ? t("relationshipsPage.materialized") : t("relationshipsPage.notMaterialized"),
    },
  ];
  const sortGroup = useListSortGroup(filteredRels, relColumns, "relationships", "source");
  const groupBy = sortGroup.groupBy;

  if (loading) return <PageLoading message={t("relationshipsPage.loading")} />;

  const totalPages = Math.max(1, Math.ceil(filteredRels.length / PAGE_SIZE));

  return (
    <div className="page page-sticky-head">
      <div className="page-header">
        <Title order={2}>{t("relationshipsPage.title")}</Title>
        <FilterInput
          value={relSearch}
          onChange={updateSearch}
          placeholder={t("relationshipsPage.filterPlaceholder")}
        />
        <div className="page-actions">
          {canManage && (
            <Button
              data-tour="rels-add"
              data-testid="rels-add-toggle"
              variant={showForm ? "outline" : "filled"}
              aria-label={showForm ? t("relationshipsPage.closeForm") : undefined}
              onClick={() => setShowForm(!showForm)}
            >
              {showForm ? <X size={14} /> : t("relationshipsPage.addRelationship")}
            </Button>
          )}
          <HelpBubble
            title={t("relationshipsPage.purposeTitle")}
            paragraphs={[t("relationshipsPage.purposeBody"), t("relationshipsPage.purposeAdd")]}
            ariaLabel={t("relationshipsPage.purposeAria")}
            testId="rels-purpose-help"
          />
          {/* Each icon is its own hover target. A row of "?" glyphs beside a row of icons
              would double the thing a reader is trying to decode. */}
          <HelpBubble
            title={t("relationshipsPage.modelingHelpTitle")}
            paragraphs={[t("relationshipsPage.modelingHelpBody")]}
            ariaLabel={t("relationshipsPage.sqlModelingTool")}
            testId="rels-modeling-help"
            target={
              <ActionIcon
                variant="subtle"
                aria-label={t("relationshipsPage.sqlModelingTool")}
                onClick={() => setShowModelingModal(true)}
              >
                <Code2 size={14} />
              </ActionIcon>
            }
          />
          <HelpBubble
            title={t("relationshipsPage.erdHelpTitle")}
            paragraphs={[t("relationshipsPage.erdHelpBody")]}
            ariaLabel={t("relationshipsPage.viewErd")}
            testId="rels-erd-help"
            target={
              <ActionIcon
                data-tour="rels-erd"
                variant="subtle"
                aria-label={t("relationshipsPage.viewErd")}
                onClick={() => setShowErd(true)}
              >
                <Network size={14} />
              </ActionIcon>
            }
          />
          {canManage && (
            <HelpBubble
              title={t("relationshipsPage.suggestHelpTitle")}
              paragraphs={[t("relationshipsPage.suggestHelpBody")]}
              ariaLabel={t("relationshipsPage.suggestWithAi")}
              testId="rels-suggest-help"
              target={
                <ActionIcon
                  data-tour="rels-suggest"
                  variant="subtle"
                  aria-label={
                    discovering
                      ? t("relationshipsPage.discovering")
                      : t("relationshipsPage.suggestWithAi")
                  }
                  onClick={handleDiscover}
                  disabled={discovering}
                >
                  <Sparkles size={14} />
                </ActionIcon>
              }
            />
          )}
          {canManage && rejectedCount > 0 && (
            <Button variant="default" onClick={handleClearRejections}>
              {t("relationshipsPage.clearRejections")}
            </Button>
          )}
        </div>
      </div>

      {discoverError && (
        <Alert color="red" mb="md" data-testid="rels-discover-error">
          {discoverError}
        </Alert>
      )}
      {discoverMsg && (
        <Text c="var(--approve)" mb="md" fz="0.875rem" data-testid="rels-discover-msg">
          {discoverMsg}
        </Text>
      )}

      {/* page-aux: `.page-sticky-head`'s overflow:hidden otherwise clips a tall form with no
          scrollport a mouse wheel can act on — see TablesPage.tsx's RegisterTableForm wrapper
          for the full explanation; same treatment this page's own candidates block already
          gets below. */}
      {showForm && (
        <div data-tour="rels-form" className="page-aux">
          <AddRelationshipForm
            form={form}
            setForm={setForm}
            tables={tables}
            functions={functions}
            saving={saving}
            onSave={handleAdd}
          />
        </div>
      )}

      <ListTable testId="relationships-list" style={{ tableLayout: "fixed" }}>
          <ListHead
            sortGroup={sortGroup}
            columns={[
              ...(domainsEnabled ? [{ col: "domain", width: "7%" }] : []),
              { col: "source", width: "22%" },
              { col: "target", width: "22%" },
              { label: t("relationshipsPage.gqlCqlAlias"), width: "20%" },
              { col: "cardinality", width: "11%" },
              { col: "materialize", width: "10%" },
              { label: t("relationshipsPage.refreshSeconds"), width: "8%" },
            ]}
          />
          <Table.Tbody>
            {(() => {
              const filtered = filteredRels;

              if (filtered.length > 75 && !relSearch.trim() && groupBy.length === 0) {
                return (
                  <ListEmpty colSpan={domainsEnabled ? 7 : 6}>
                    {t("relationshipsPage.tooManyRelationships", { count: filtered.length })}
                  </ListEmpty>
                );
              }
              if (filtered.length === 0) {
                return (
                  <ListEmpty colSpan={domainsEnabled ? 7 : 6} testId="relationships-empty">
                    {t("relationshipsPage.empty")}
                  </ListEmpty>
                );
              }

              return (
                <ListItems
                  state={sortGroup}
                  items={pageItems(sortGroup, relPage, PAGE_SIZE)}
                  colSpan={domainsEnabled ? 7 : 6}
                  rowKey={(r) => r.id}
                  render={(r) => {
                    const id = String(r.id);
                return (
                  <RelationshipRow
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
                  }}
                />
              );
            })()}
          </Table.Tbody>
      </ListTable>
      {totalPages > 1 && groupBy.length === 0 && (
        <Group gap="sm" align="center" justify="flex-end" py="sm">
          <ActionIcon
            variant="subtle"
            aria-label={t("relationshipsPage.firstPage")}
            title={t("relationshipsPage.firstPage")}
            onClick={() => setRelPage(0)}
            disabled={relPage === 0}
          >
            «
          </ActionIcon>
          <Pagination
            total={totalPages}
            value={relPage + 1}
            onChange={(p) => setRelPage(p - 1)}
            size="sm"
            data-testid="rels-pagination"
          />
          <Text fz="sm" data-testid="rels-page-label">
            {t("relationshipsPage.pageOf", { page: relPage + 1, totalPages })}
          </Text>
          <ActionIcon
            variant="subtle"
            aria-label={t("relationshipsPage.lastPage")}
            title={t("relationshipsPage.lastPage")}
            onClick={() => setRelPage(totalPages - 1)}
            disabled={relPage >= totalPages - 1}
          >
            »
          </ActionIcon>
        </Group>
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
        <div className="page-aux">
          <CandidatesTable
            candidates={candidates}
            tableDomainById={tableDomainById}
            tableNameById={tableNameById}
            onAccept={handleAccept}
            onReject={handleReject}
          />
        </div>
      )}
      {showErd && (
        <ErdModal
          tables={tables}
          relationships={allRels}
          domains={domains}
          checkedDomains={erdCheckedDomains}
          onClose={() => setShowErd(false)}
        />
      )}
      {refusal.dialog}
    </div>
  );
}
