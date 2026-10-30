// Copyright (c) 2026 Kenneth Stott
// Canary: b2e1f836-e23f-41ef-9df2-88110c6b9f1d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { PageTitle } from "../components/PageTitle";
import { relationshipInCheckedDomains } from "./relationshipDomainFilter";
import { useState, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { ArrowLeft, Sparkles, Code2, X, Link2 } from "lucide-react";
import { ActionIcon, Alert, Button, Group, Modal, Pagination, Table, Text } from "@mantine/core";
import type { ColumnTagChange, TagLossResolution } from "../api/glossary";
import { TagLossSummary } from "../components/admin/TagLossSummary";
import { tagLossTitle } from "../components/admin/tagLoss";
import { ErdToggle } from "../components/erd/ErdToggle";
import { ErdView } from "../components/erd/ErdView";
import { useErdView } from "../components/erd/useErdView";
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
  useUpsertRelationship,
  useDeleteRelationship,
} from "../hooks/useAdminQueries";
import { fetchActions } from "../api/actions";
import type { TrackedFunction } from "../api/actions";
import type { MutationResult, Relationship } from "../types/admin";
import {
  EMPTY_FORM,
  tableRef,
  type Candidate,
  type RelForm,
} from "../components/relationships/relationship-types";
import { AddRelationshipForm } from "../components/relationships/AddRelationshipForm";
import { RelationshipRow } from "../components/relationships/RelationshipRow";
import { RelationshipDetail } from "../components/relationships/RelationshipDetail";
import { SplitPane } from "../components/list/SplitPane";
import { ListTable, ListHead, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, pageItems, type ListColumn } from "../components/list/useListSortGroup";
import {
  ConflictModal,
  ReverseRelationshipForm,
} from "../components/relationships/RelationshipModals";
import { CandidatesTable } from "../components/relationships/CandidatesTable";
import { PageLoading } from "../components/PageLoading";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import { domainToSqlName } from "../naming";
import { Tip } from "../components/Tip";
import { serverMessage } from "../i18n/serverMessage";
import { matchesQuery } from "../lib/searchItems";

/** A refusal over the tags a re-homed key ref would lose (REQ-2145): each column's change. */
function tagLossOf(result: MutationResult): ColumnTagChange[] | null {
  return result.code === "tag.move_loses"
    ? (result.params as { columns: ColumnTagChange[] }).columns
    : null;
}

/** How the save goes ahead over those tags, as RelationshipInput takes it. */
function tagLossInput(resolve?: TagLossResolution) {
  return resolve
    ? { confirmTagLoss: resolve.confirm_tag_loss ?? [], carryTags: resolve.carry_tags === true }
    : {};
}

export function RelationshipsPage() {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const { relationships: rels, loading: relsLoading, refetch: refetchRels } = useRelationships();
  // What the ERD draws, the system's own relationships too (REQ-2187: a line opens its panel here).
  const { relationships: allRels } = useAllRelationships();
  const { tables, loading: tablesLoading } = useTables();
  // REQ-2187: the ERD in the list's place, its state in the URL.
  const erd = useErdView();
  const { upsertRelationship } = useUpsertRelationship();
  const { deleteRelationship } = useDeleteRelationship();
  const [functions, setFunctions] = useState<TrackedFunction[]>([]);
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [saving, setSaving] = useState<string | null>(null);
  // What the open form has to say of a save that did not go through.
  const [saveProblem, setSaveProblem] = useState("");
  const [discovering, setDiscovering] = useState(false);
  const [discoverError, setDiscoverError] = useState("");
  const [discoverMsg, setDiscoverMsg] = useState("");
  const [rejectedCount, setRejectedCount] = useState(0);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<RelForm>(EMPTY_FORM);
  // `?relationship=<id>` (a graph's link, lib/entityHref) opens that relationship's panel.
  const [expanded, setExpanded] = useState<string | null>(() => searchParams.get("relationship"));
  const linkedRelationship = searchParams.get("relationship");
  const [seededRelationship, setSeededRelationship] = useState(linkedRelationship);
  if (linkedRelationship !== seededRelationship) {
    setSeededRelationship(linkedRelationship);
    if (linkedRelationship !== null) setExpanded(linkedRelationship);
  }
  const [editingRel, setEditingRel] = useState<RelForm | null>(null);
  // REQ-2145: a save or a delete that re-homes key refs off a tagged term, refused over the tags
  // those columns would lose: what each loses and gains, and what to do again once resolved.
  const [tagLoss, setTagLoss] = useState<{
    columns: ColumnTagChange[];
    retry: { kind: "add" } | { kind: "save" } | { kind: "delete"; id: string };
  } | null>(null);
  const [reverseForm, setReverseForm] = useState<RelForm | null>(null);
  const [relSearch, setRelSearch] = useState(() => searchParams.get("search") ?? "");
  const [relPage, setRelPage] = useState(0);
  const PAGE_SIZE = 50;
  const [showModelingModal, setShowModelingModal] = useState(false);
  const [conflictRel, setConflictRel] = useState<Relationship | null>(null);

  const { domainsEnabled, domains: filterDomains, checkedDomains } = useDomainFilter();
  const erdCheckedDomains =
    checkedDomains.size > 0 && checkedDomains.size < filterDomains.length ? checkedDomains : null;
  const { capabilities } = useAuth();
  const canManage = capabilities.includes("create_relationship");

  const updateSearch = (v: string) => {
    setRelSearch(v);
    setRelPage(0);
    // REQ-2187: on the ERD the filter box is not part of the link (a focus is what is shared).
    if (erd.on) return;
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
  // The name a relationship's save takes for each table (relationship-types.tableRef).
  const tableRefById: Record<number, string> = Object.fromEntries(
    tables.map((t) => [t.id, tableRef(t)]),
  );
  const tableDomainById = Object.fromEntries(
    tables.map((t) => [t.id, domainToSqlName(t.domainId)]),
  );
  const tableSourceById = Object.fromEntries(tables.map((t) => [t.id, t.sourceId]));
  const remoteTableIds = new Set(
    tables
      .filter((t) => t.schemaName === "graphql_remote" || t.schemaName === "grpc_remote")
      .map((t) => t.id),
  );

  const handleDelete = useCallback(
    async (id: string, resolve?: TagLossResolution) => {
      const result = await deleteRelationship(id, resolve);
      const lost = tagLossOf(result);
      if (lost) {
        setTagLoss({ columns: lost, retry: { kind: "delete", id } });
        return;
      }
      if (refusal.refused(result, id)) return;
      setExpanded((prev) => (prev === id ? null : prev));
      setEditingRel(null);
    },
    [deleteRelationship, refusal],
  );

  const handleAdd = useCallback(
    async (resolve?: TagLossResolution) => {
      if (!form.id || !form.sourceTableId) return;
      if (form.targetType === "table" && !form.targetTableId) return;
      if (form.targetType === "function" && !form.targetFunctionName) return;
      setSaving("new");
      setSaveProblem("");
      let result: MutationResult;
      try {
        result = await upsertRelationship(
          {
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
            sameEntity: form.sameEntity,
            // REQ-1586: the junction declaration saves with the edge.
            viaTable: form.viaTable,
            viaSourceColumn: form.viaSourceColumn,
            viaTargetColumn: form.viaTargetColumn,
            viaTypeColumn: form.viaTypeColumn || null,
            viaTypeValue: form.viaTypeValue || null,
            viaLabelSource: form.viaLabelSource,
            parameters: form.parameters,
            ...tagLossInput(resolve),
          },
          true,
        );
      } catch (err) {
        // A save that errors rather than answering a refusal still ends: the form says why.
        setSaveProblem(err instanceof Error ? err.message : String(err));
        return;
      } finally {
        setSaving(null);
      }
      const lost = tagLossOf(result);
      if (lost) {
        setTagLoss({ columns: lost, retry: { kind: "add" } });
        return;
      }
      if (!result.success) {
        // Refused: the form stays open and says why.
        setSaveProblem(serverMessage(result, result.message));
        return;
      }
      // The relationship exists now, with everything else at its defaults: it is selected in the
      // list and its detail opens in edit, where the rest is filled in.
      setExpanded(form.id);
      setEditingRel({ ...form, originalId: form.id });
      setForm(EMPTY_FORM);
      setShowForm(false);
    },
    [form, upsertRelationship],
  );

  const handleEditSave = useCallback(
    async (resolve?: TagLossResolution) => {
      if (!editingRel?.id) return;
      setSaving(editingRel.originalId || editingRel.id);
      setSaveProblem("");
      try {
        const result = await upsertRelationship(
          {
            id: editingRel.id,
            sourceTableId: editingRel.sourceTableId,
            targetTableId: editingRel.targetType === "table" ? editingRel.targetTableId : "",
            sourceColumn: editingRel.sourceColumn,
            targetColumn: editingRel.targetType === "table" ? editingRel.targetColumn : "",
            cardinality:
              editingRel.targetType === "function" ? "one-to-many" : editingRel.cardinality,
            materialize: editingRel.materialize,
            refreshInterval: parseInt(editingRel.refreshInterval) || 300,
            targetFunctionName:
              editingRel.targetType === "function" ? editingRel.targetFunctionName : null,
            functionArg: editingRel.targetType === "function" ? editingRel.functionArg : null,
            alias: editingRel.alias || null,
            graphqlAlias: editingRel.graphqlAlias || null,
            disableCypher: editingRel.disableCypher,
            sameEntity: editingRel.sameEntity,
            // REQ-1586: the junction declaration saves with the edge.
            viaTable: editingRel.viaTable,
            viaSourceColumn: editingRel.viaSourceColumn,
            viaTargetColumn: editingRel.viaTargetColumn,
            viaTypeColumn: editingRel.viaTypeColumn || null,
            viaTypeValue: editingRel.viaTypeValue || null,
            viaLabelSource: editingRel.viaLabelSource,
            // REQ-2099: the parameters the edge feeds save with it.
            parameters: editingRel.parameters,
            ...tagLossInput(resolve),
          },
          editingRel.id !== editingRel.originalId, // renamed: made anew, the old one deleted
        );
        const lost = tagLossOf(result);
        if (lost) {
          setTagLoss({ columns: lost, retry: { kind: "save" } });
          return;
        }
        if (!result.success) {
          setSaveProblem(serverMessage(result, result.message));
          return;
        }
        if (editingRel.originalId && editingRel.originalId !== editingRel.id) {
          await deleteRelationship(editingRel.originalId);
        }
        setEditingRel(null);
      } catch (err) {
        // A save that errors rather than answering a refusal still ends: the form says why.
        setSaveProblem(err instanceof Error ? err.message : String(err));
      } finally {
        setSaving(null);
      }
    },
    [editingRel, upsertRelationship, deleteRelationship],
  );

  // The refused save or delete again, as the reader resolved its tags.
  const retryTagLoss = (resolve: TagLossResolution) => {
    const retry = (tagLoss as NonNullable<typeof tagLoss>).retry;
    setTagLoss(null);
    if (retry.kind === "delete") void handleDelete(retry.id, resolve);
    else if (retry.kind === "save") void handleEditSave(resolve);
    else void handleAdd(resolve);
  };

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
        sourceDomain: rel.sourceDomainId ? domainToSqlName(rel.sourceDomainId) : "",
        sourceTableId: tableRefById[rel.sourceTableId],
        sourceColumn: rel.sourceColumn,
        targetType: isComputed ? "function" : "table",
        targetDomain: rel.targetTableId ? (tableDomainById[rel.targetTableId] ?? "") : "",
        targetTableId: rel.targetTableId != null ? tableRefById[rel.targetTableId] : "",
        targetColumn: rel.targetColumn ?? "",
        targetFunctionName: rel.targetFunctionName ?? "",
        functionArg: rel.functionArg ?? "",
        cardinality: rel.cardinality,
        materialize: rel.materialize,
        refreshInterval: String(rel.refreshInterval ?? 300),
        alias: rel.alias ?? "",
        graphqlAlias: rel.graphqlAlias ?? "",
        disableCypher: rel.disableCypher ?? false,
        sameEntity: rel.sameEntity,
        viaTable: rel.viaTableId != null ? tableRefById[rel.viaTableId] : "",
        viaSourceColumn: rel.viaSourceColumn ?? "",
        viaTargetColumn: rel.viaTargetColumn ?? "",
        viaTypeColumn: rel.viaTypeColumn ?? "",
        viaTypeValue: rel.viaTypeValue ?? "",
        viaLabelSource: rel.viaLabelSource ?? "",
        parameters: rel.parameters.map(({ parameter, sourceColumn }) => ({
          parameter,
          sourceColumn,
        })),
        // The panel opens for a row that already declares a junction; the checkbox only has to
        // hold it open for a row that does not yet name a table.
        junctionDeclared: Boolean(rel.viaTableName),
      });
    },
    [tableDomainById, tableRefById],
  );

  const buildReverse = useCallback(
    (r: Relationship): RelForm => {
      // One-to-one reads the same both ways.
      const flipCardinality = (c: string) =>
        c === "one-to-one" ? c : c === "many-to-one" ? "one-to-many" : "many-to-one";
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
        sourceTableId: r.targetTableId != null ? tableRefById[r.targetTableId] : "",
        sourceDomain: r.targetTableId ? (tableDomainById[r.targetTableId] ?? "") : "",
        sourceColumn: r.targetColumn ?? "",
        targetType: "table",
        targetTableId: tableRefById[r.sourceTableId],
        targetDomain: r.sourceDomainId ?? "",
        targetColumn: r.sourceColumn,
        cardinality: flipCardinality(r.cardinality),
        alias: cqlSuggestion,
        graphqlAlias: suggestGqlAlias(r.graphqlAlias, cqlSuggestion),
        materialize: r.materialize,
        refreshInterval: String(r.refreshInterval ?? 300),
      };
    },
    [tableDomainById, tableRefById],
  );

  const handleReverseAdd = useCallback(async () => {
    if (!reverseForm || !reverseForm.id || !reverseForm.sourceTableId || !reverseForm.targetTableId)
      return;
    setSaving("reverse");
    setSaveProblem("");
    let result: MutationResult;
    try {
      result = await upsertRelationship(
        {
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
        },
        true, // the reverse is a new relationship
      );
    } catch (err) {
      // A save that errors rather than answering a refusal still ends: the form says why.
      setSaveProblem(err instanceof Error ? err.message : String(err));
      return;
    } finally {
      setSaving(null);
    }
    if (!result.success) {
      setSaveProblem(serverMessage(result, result.message));
      return;
    }
    setReverseForm(null);
  }, [reverseForm, upsertRelationship]);

  const narrowTo = erdCheckedDomains ? new Set([...erdCheckedDomains].map(domainToSqlName)) : null;
  const matchesFilter = (r: Relationship) => {
    if (remoteTableIds.has(r.sourceTableId)) return false;
    const srcDomain = r.sourceDomainId ? domainToSqlName(r.sourceDomainId) : undefined;
    const tgtDomain = r.targetTableId != null ? tableDomainById[r.targetTableId] : null;
    if (!relationshipInCheckedDomains(narrowTo, srcDomain, tgtDomain)) return false;
    // A relationship is also found by its alias: a tour step names it so (demoTour.ts).
    return matchesQuery([r.sourceTableName, r.targetTableName, r.alias], relSearch);
  };

  // REQ-1940: sort and group are the shared list mechanism.
  const filteredRels = rels.filter(
    (r) => tableSourceById[r.sourceTableId] !== "provisa-admin" && matchesFilter(r),
  );
  const relDomain = (r: Relationship) =>
    r.sourceDomainId ? domainToSqlName(r.sourceDomainId) : t("relationshipsPage.none");
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
        r.materialize
          ? t("relationshipsPage.materialized")
          : t("relationshipsPage.notMaterialized"),
    },
  ];
  const sortGroup = useListSortGroup(filteredRels, relColumns, "relationships", "source");
  const groupBy = sortGroup.groupBy;

  if (loading) return <PageLoading message={t("relationshipsPage.loading")} />;

  // The right-hand panel: the form that adds a relationship while it is open, otherwise the
  // selected relationship -- its detail, the form that edits it, or the form that creates its
  // reverse. Nothing selected: no panel, and the list has the page.
  const closePanel = () => {
    setSaveProblem("");
    setShowForm(false);
    setReverseForm(null);
    setEditingRel(null);
    setExpanded(null);
    if (erd.on) erd.setPanel("relationship", null);
  };
  // REQ-2187: a line clicked in the ERD: its panel, in place of whatever the panel held.
  const openFromErd = (id: string) => {
    setSaveProblem("");
    setShowForm(false);
    setReverseForm(null);
    setEditingRel(null);
    setExpanded(id);
    erd.setPanel("relationship", id);
  };
  const selectedRel =
    expanded === null
      ? null
      : ((erd.on ? allRels : rels).find((r) => String(r.id) === expanded) ?? null);
  const panelBody = showForm ? (
    <div data-tour="rels-form">
      <AddRelationshipForm
        form={form}
        setForm={setForm}
        tables={tables}
        functions={functions}
        saving={saving}
        problem={saveProblem}
        onSave={() => handleAdd()}
        onCancel={() => setShowForm(false)}
      />
    </div>
  ) : selectedRel && reverseForm ? (
    <>
      <div className="split-pane__bar">
        <Button
          variant="subtle"
          size="compact-sm"
          leftSection={<ArrowLeft size={14} />}
          onClick={() => setReverseForm(null)}
          data-testid="reverse-rel-back"
        >
          {t("relationshipsPage.backToRelationship", { id: selectedRel.id })}
        </Button>
      </div>
      <ReverseRelationshipForm
        reverseForm={reverseForm}
        setReverseForm={setReverseForm}
        saving={saving}
        problem={saveProblem}
        onSave={handleReverseAdd}
      />
    </>
  ) : selectedRel ? (
    <RelationshipDetail
      rel={selectedRel}
      editingRel={editingRel}
      setEditingRel={setEditingRel}
      // A line of the system's own (the ERD draws them) is read here, never edited.
      canManage={canManage && rels.some((r) => r.id === selectedRel.id)}
      onStartEdit={() => {
        setSaveProblem("");
        startEditing(selectedRel);
      }}
      onReverse={() => {
        setSaveProblem("");
        setReverseForm(buildReverse(selectedRel));
      }}
      onDelete={() => handleDelete(String(selectedRel.id))}
      onEditSave={() => handleEditSave()}
      saving={saving}
      problem={saveProblem}
      tables={tables}
      functions={functions}
      tableDomainById={tableDomainById}
      domainsEnabled={domainsEnabled}
    />
  ) : null;
  const panelIsReadOnly = !showForm && reverseForm === null && editingRel === null;
  const panelTitle = showForm
    ? t("relationshipsPage.panelNewRelationship")
    : selectedRel && reverseForm
      ? t("relationshipModals.reverseTitle")
      : selectedRel
        ? String(selectedRel.id)
        : "";

  const totalPages = Math.max(1, Math.ceil(filteredRels.length / PAGE_SIZE));

  return (
    <div className="page page-sticky-head">
      <div className="page-header">
        <PageTitle icon={Link2}>{t("relationshipsPage.title")}</PageTitle>
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
              onClick={() => {
                // One thing in the panel at a time: the form replaces whatever row was open.
                setSaveProblem("");
                setExpanded(null);
                setEditingRel(null);
                setReverseForm(null);
                setShowForm(!showForm);
              }}
            >
              {showForm ? <X size={14} /> : t("relationshipsPage.addRelationship")}
            </Button>
          )}
          {canManage && rejectedCount > 0 && (
            <Button variant="default" onClick={handleClearRejections}>
              {t("relationshipsPage.clearRejections")}
            </Button>
          )}
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
          <ErdToggle
            page="relationships"
            title={t("relationshipsPage.erdHelpTitle")}
            body={t("relationshipsPage.erdHelpBody")}
            label={t("relationshipsPage.viewErd")}
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
          <HelpBubble
            title={t("relationshipsPage.purposeTitle")}
            paragraphs={[t("relationshipsPage.purposeBody"), t("relationshipsPage.purposeAdd")]}
            ariaLabel={t("relationshipsPage.purposeAria")}
            testId="rels-purpose-help"
          />
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

      <SplitPane
        testId="relationships-split"
        storageKey="provisa.relationships.detailWidth"
        title={panelTitle}
        onClose={closePanel}
        // Escape closes a relationship that is only being looked at; it never drops what is typed.
        closeOnEscape={panelIsReadOnly}
        labels={{
          panel: t("relationshipsPage.detailPanel"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          erd.on ? (
            <ErdView page="relationships" search={relSearch} onOpen={openFromErd} />
          ) : (
            <>
              {/* As on Tables: every column keeps its width and a name stays on one line; what the
                panel leaves too little room for scrolls sideways. */}
              <ListTable testId="relationships-list">
                <ListHead
                  sortGroup={sortGroup}
                  columns={[
                    ...(domainsEnabled ? [{ col: "domain" }] : []),
                    { col: "source" },
                    { col: "target" },
                    { label: t("relationshipsPage.gqlCqlAlias") },
                    { col: "cardinality" },
                    { col: "materialize" },
                    { label: t("relationshipsPage.refreshSeconds") },
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
                              selected={expanded === id}
                              onToggle={() => {
                                // The row opens its detail in the panel, swaps it, or closes it.
                                setSaveProblem("");
                                setExpanded(expanded === id ? null : id);
                                setEditingRel(null);
                                setReverseForm(null);
                                setShowForm(false);
                              }}
                              tableDomainById={tableDomainById}
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
                  <Tip text={t("relationshipsPage.firstPage")} named>
                    <ActionIcon
                      variant="subtle"
                      aria-label={t("relationshipsPage.firstPage")}
                      onClick={() => setRelPage(0)}
                      disabled={relPage === 0}
                    >
                      «
                    </ActionIcon>
                  </Tip>
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
                  <Tip text={t("relationshipsPage.lastPage")} named>
                    <ActionIcon
                      variant="subtle"
                      aria-label={t("relationshipsPage.lastPage")}
                      onClick={() => setRelPage(totalPages - 1)}
                      disabled={relPage >= totalPages - 1}
                    >
                      »
                    </ActionIcon>
                  </Tip>
                </Group>
              )}
            </>
          )
        }
      >
        {panelBody}
      </SplitPane>

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
                  await upsertRelationship(
                    {
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
                    },
                    true, // a candidate accepted is a new relationship
                  );
                }
              : undefined
          }
        />
      )}

      {conflictRel && <ConflictModal rel={conflictRel} onClose={() => setConflictRel(null)} />}

      {!erd.on && candidates.length > 0 && (
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
      {refusal.dialog}
      {tagLoss && (
        <Modal
          opened
          onClose={() => setTagLoss(null)}
          title={tagLossTitle(t, tagLoss.columns, null)}
        >
          <TagLossSummary
            columns={tagLoss.columns}
            into={null}
            saving={saving !== null}
            onMove={(confirm_tag_loss) => retryTagLoss({ confirm_tag_loss })}
            onCarry={() => retryTagLoss({ carry_tags: true })}
            onCancel={() => setTagLoss(null)}
          />
        </Modal>
      )}
    </div>
  );
}
