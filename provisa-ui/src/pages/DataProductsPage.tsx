// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  Modal,
  Paper,
  Table,
  Text,
  Title,
} from "@mantine/core";
import { Plus } from "lucide-react";
import {
  useDataProducts,
  useCreateDataProduct,
  useDeleteDataProduct,
  useDomains,
  useTables,
  useUpdateTable,
  useRelationships,
  useRoles,
  useSources,
} from "../hooks/useAdminQueries";
import { buildTableUpdateInput } from "./tables/helpers";
import type { DataProduct, RegisteredTable, Relationship } from "../types/admin";
import { fetchActions, saveFunction, type TrackedFunction } from "../api/actions";
import { HelpBubble } from "../components/HelpBubble";
import { FilterInput } from "../components/admin/FilterInput";
import { DataProductDetailPanel, type InputPortRow } from "./data-products/DataProductDetailPanel";
import { OwnerResolutionIcon } from "../components/OwnerResolution";
import { useCapability } from "../hooks/useCapability";
import { fetchFederationGraph, type LineageGraphData } from "../api/lineage";
import { fetchRelatedGlossaryTerms, type RelatedGlossaryTerm } from "../api/glossary";
import { domainToSqlName } from "../naming";
import { columnDescriber } from "../components/lineage/column-descriptions";
import { DataProductFormCard } from "./data-products/DataProductFormCard";
import { EMPTY_FORM, type DataProductForm } from "./data-products/types";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import {
  ListTable,
  ListHead,
  ListRow,
  ListExpandRow,
  ListEmpty,
  ListDetail,
  ListItems,
} from "../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../components/list/useListSortGroup";
import { PageLoading } from "../components/PageLoading";

// Exported for the tour, which clears it so the page mounts collapsed and its click on the first
// row deterministically EXPANDS rather than toggling whatever a prior visit left open.
export const EXPANDED_STORAGE_KEY = "provisa.data_products.expanded";

// REQ-1634: top-level data-products management page (list / create / edit / delete).
// Follows the MetricsPage detail-then-edit pattern (REQ-1323) — row click expands the
// detail panel; Edit/Delete live inside it and edit swaps the panel for the inline form.
export function DataProductsPage() {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [search, setSearch] = useState(() => searchParams.get("search") ?? "");
  const updateSearch = (v: string) => {
    setSearch(v);
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
  const { dataProducts, loading, error } = useDataProducts();
  const { domains } = useDomains();
  const { tables } = useTables(); // REQ-1634: member-table list + assignment
  const { sources } = useSources(); // REQ-1443 clause 10: checker type for a scan's rules modal
  const { relationships } = useRelationships(); // REQ-1634: related-tables panel
  const [functions, setFunctions] = useState<TrackedFunction[]>([]); // REQ-1634: member-command list + assignment
  const { roles } = useRoles();
  const roleOptions = roles.map((r) => r.id);
  const { createDataProduct, loading: saving } = useCreateDataProduct();
  const { deleteDataProduct, loading: deleting } = useDeleteDataProduct();
  const { updateTable } = useUpdateTable();
  // REQ-1634: New/Edit/Delete and the table picker require the write right; a
  // data_product_read-only caller sees the list and detail view only.
  const canEdit = useCapability("data_product_rw");
  // REQ-1640: lineage panel needs its own gate — view_governance is independent of the
  // data_product_read/rw pair, so a data-product editor may still lack lineage visibility.
  const canSeeLineage = useCapability("view_governance");
  const [creating, setCreating] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [form, setForm] = useState<DataProductForm>({ ...EMPTY_FORM });
  const [selectedTableIds, setSelectedTableIds] = useState<string[]>([]);
  const [selectedFunctionNames, setSelectedFunctionNames] = useState<string[]>([]);
  // REQ-1659: `?product=<id>` is the deep link a catalog listing (Snowflake Horizon's
  // documentation box, Analytics Hub) carries back to this page; it names the product to open and
  // wins over the last-expanded row remembered in localStorage.
  const [expanded, setExpandedState] = useState<string | null>(() => {
    const linked = searchParams.get("product");
    if (linked) return linked;
    try {
      return localStorage.getItem(EXPANDED_STORAGE_KEY);
    } catch {
      return null;
    }
  });
  const setExpanded = (id: string | null) => {
    setExpandedState(id);
    setSearchParams(
      (p) => {
        const n = new URLSearchParams(p);
        if (id) n.set("product", id);
        else n.delete("product");
        return n;
      },
      { replace: true },
    );
    try {
      if (id) localStorage.setItem(EXPANDED_STORAGE_KEY, id);
      else localStorage.removeItem(EXPANDED_STORAGE_KEY);
    } catch {
      // localStorage unavailable (private mode, quota) — expanded state just won't persist
    }
  };
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [msg, setMsg] = useState("");
  // REQ-1640: whole-federation provenance graph, fetched once (no domains filter — scoping
  // to the product's own domain would sever legitimate cross-domain ancestor chains).
  const [lineageGraph, setLineageGraph] = useState<LineageGraphData | null>(null);
  const [lineageLoading, setLineageLoading] = useState(false);
  const [lineageError, setLineageError] = useState("");
  // REQ-1634: Related Terms panel — fetched lazily for the expanded product's member tables,
  // since the transitive-closure BFS is a server-side call, not a client-side derivation like
  // relatedTables()/lineageFor() above.
  const [relatedTerms, setRelatedTerms] = useState<RelatedGlossaryTerm[]>([]);
  const [relatedTermsLoading, setRelatedTermsLoading] = useState(false);

  useEffect(() => {
    // REQ-1634: command <-> data product membership lives on the (REST-served) Function row.
    fetchActions()
      .then((actions) => setFunctions(actions.functions))
      .catch(() => setFunctions([]));
  }, []);

  useEffect(() => {
    if (!canSeeLineage) return;
    let cancelled = false;
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       flips the loading flag synchronously before the async fetch starts */
    setLineageLoading(true);
    fetchFederationGraph()
      .then((g) => {
        if (!cancelled) setLineageGraph(g);
      })
      .catch((e: Error) => {
        if (!cancelled) setLineageError(e.message);
      })
      .finally(() => {
        if (!cancelled) setLineageLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [canSeeLineage]);

  const domainOptions = domains.map((d) => d.id);
  const memberTables = (id: string) => tables.filter((tb) => tb.productId === id);
  // Descriptions for the lineage hover come from EVERY registered table: a contributor's or a
  // consumer's column is as hoverable as a member's.
  const describeColumn = useMemo(() => columnDescriber(tables), [tables]);
  const memberFunctions = (id: string) => functions.filter((fn) => fn.productId === id);

  useEffect(() => {
    if (!expanded) {
      /* eslint-disable-next-line react-hooks/set-state-in-effect --
         clears stale related terms synchronously when the expanded product collapses */
      setRelatedTerms([]);
      return;
    }
    let cancelled = false;
    setRelatedTermsLoading(true);
    fetchRelatedGlossaryTerms(memberTables(expanded).map((tb) => tb.id))
      .then((terms) => {
        // proposed terms (not yet live) aren't ready for consumers to rely on
        if (!cancelled) setRelatedTerms(terms.filter((term) => term.live));
      })
      .catch(() => {
        if (!cancelled) setRelatedTerms([]);
      })
      .finally(() => {
        if (!cancelled) setRelatedTermsLoading(false);
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- memberTables is derived from tables each render, including it would just restate the tables dep
  }, [expanded, tables]);

  // REQ-1640: subgraph of the whole federation lineage graph scoped to this product's member
  // tables — the published endpoint — plus their full upstream ancestry (walked backward,
  // source -> target = derives-into, transitively) and the tables ONE hop downstream, so the
  // reader sees what the product is built from and who consumes it directly, without the graph
  // running on into everything those consumers feed in turn.
  // Same LineageDag component the Lineage page renders (REQ-1161/1627), not a table-list view.
  // Informational only — never stored, mirrors the Related Tables computation.
  //
  // A data-quality results table is a member by inheritance (REQ-1443 clause 10) but not part of
  // the product's data flow: its rows are scan outcomes ABOUT a member table, and its lineage
  // would pull the checker's own plumbing into the graph. Left out here, as in the member picker.
  const lineageFor = (id: string): LineageGraphData | null => {
    if (!lineageGraph) return null;
    const relationOf = (tb: { domainId: string; tableName: string }) =>
      `${domainToSqlName(tb.domainId)}.${tb.tableName}`;
    const members = memberTables(id).filter((tb) => tb.dqContract == null);
    const checkerRelations = new Set(
      tables.filter((tb) => tb.dqContract != null).map((tb) => relationOf(tb)),
    );
    const memberRelations = new Set(members.map(relationOf));
    const nodesById = new Map(lineageGraph.nodes.map((n) => [n.id, n]));
    const relationEdges = lineageGraph.edges
      .map((e) => ({
        from: nodesById.get(e.source)?.relation,
        to: nodesById.get(e.target)?.relation,
      }))
      .filter((e): e is { from: string; to: string } => !!e.from && !!e.to && e.from !== e.to);
    const keepRelations = new Set(memberRelations);
    let frontier = memberRelations;
    while (frontier.size > 0) {
      const next = new Set<string>();
      for (const e of relationEdges) {
        if (frontier.has(e.to) && !keepRelations.has(e.from)) {
          keepRelations.add(e.from);
          next.add(e.from);
        }
      }
      frontier = next;
    }
    for (const e of relationEdges) {
      if (memberRelations.has(e.from) && !checkerRelations.has(e.to)) keepRelations.add(e.to);
    }
    if (keepRelations.size === 0) return { nodes: [], edges: [], outputs: [] };
    const nodes = lineageGraph.nodes.filter((n) => n.relation && keepRelations.has(n.relation));
    const keepIds = new Set(nodes.map((n) => n.id));
    // The product's tables are what it publishes, so EVERY column of a member table is an output
    // of this graph — including the ones no registered view derives from, which the federation
    // graph never mentions. Those are added as source nodes of their table (the graph's own id
    // shape, relation.column) so the table draws complete, and all of them ring as outputs.
    for (const tb of members) {
      const relation = relationOf(tb);
      for (const c of tb.columns) {
        const nodeId = `${relation}.${c.columnName}`;
        if (keepIds.has(nodeId)) continue;
        keepIds.add(nodeId);
        nodes.push({
          id: nodeId,
          column: c.columnName,
          relation,
          kind: "source",
          materialized: false,
        });
      }
    }
    const edges = lineageGraph.edges.filter((e) => keepIds.has(e.source) && keepIds.has(e.target));
    const outputs = nodes.filter((n) => memberRelations.has(n.relation as string)).map((n) => n.id);
    return { nodes, edges, outputs };
  };

  // REQ-1667: the relations the detail's swimlanes are built around — the same members
  // lineageFor() publishes as outputs.
  const lineageMembersFor = (id: string): ReadonlySet<string> =>
    new Set(
      memberTables(id)
        .filter((tb) => tb.dqContract == null)
        .map((tb) => `${domainToSqlName(tb.domainId)}.${tb.tableName}`),
    );

  // REQ-1660 (ODPS inputPorts): the 1-hop input(s) -> transform -> output edges landing on this
  // product's member-table columns, grouped by target column. Unlike lineageFor() this does not
  // walk transitively upstream — inputPorts is the product's direct contract surface, not its
  // full provenance.
  const inputPortsFor = (id: string): InputPortRow[] => {
    if (!lineageGraph) return [];
    const memberRelations = new Set(
      memberTables(id).map((tb) => `${domainToSqlName(tb.domainId)}.${tb.tableName}`),
    );
    const nodesById = new Map(lineageGraph.nodes.map((n) => [n.id, n]));
    const byTarget = new Map<string, { transform: string; inputs: Set<string> }>();
    for (const edge of lineageGraph.edges) {
      const tgt = nodesById.get(edge.target);
      const src = nodesById.get(edge.source);
      if (!tgt?.relation || !src || !memberRelations.has(tgt.relation)) continue;
      const outputLabel = `${tgt.relation}.${tgt.column}`;
      const inputLabel = src.relation ? `${src.relation}.${src.column}` : src.column;
      const entry = byTarget.get(outputLabel) ?? {
        transform: edge.transform,
        inputs: new Set<string>(),
      };
      entry.inputs.add(inputLabel);
      byTarget.set(outputLabel, entry);
    }
    return Array.from(byTarget.entries()).map(([output, { transform, inputs }]) => ({
      output,
      transform,
      inputs: Array.from(inputs),
    }));
  };

  // REQ-1634: tables one approved relationship away from this product's member tables, so a
  // curated view can pull in the related data or the product's owner can see how to extend it.
  // Informational only — computed from `relationships` + `registered_tables`, never stored as
  // membership.
  const relatedTables = (id: string) => {
    const memberIds = new Set(memberTables(id).map((tb) => tb.id));
    // One row per relationship, not per table — two distinct named relationships can lead to the
    // same related table, and collapsing on the table id silently dropped the second one.
    const related: { table: RegisteredTable; relationship: Relationship }[] = [];
    for (const r of relationships) {
      if (r.targetTableId == null) continue; // computed (function-target) relationship, no table
      const srcIn = memberIds.has(r.sourceTableId);
      const tgtIn = memberIds.has(r.targetTableId);
      const otherId = srcIn && !tgtIn ? r.targetTableId : tgtIn && !srcIn ? r.sourceTableId : null;
      if (otherId === null) continue;
      const table = tables.find((tb) => tb.id === otherId);
      if (table) related.push({ table, relationship: r });
    }
    return related;
  };

  // Relationships where both endpoints are member tables of this product — the connections that
  // exist entirely within the published surface, as opposed to relatedTables()'s one-hop-outside view.
  const internalRelationships = (id: string) => {
    const memberIds = new Set(memberTables(id).map((tb) => tb.id));
    return relationships.filter(
      (r) =>
        memberIds.has(r.sourceTableId) && r.targetTableId != null && memberIds.has(r.targetTableId),
    );
  };

  const openCreate = () => {
    setEditingId(null);
    setForm({ ...EMPTY_FORM });
    setSelectedTableIds([]);
    setSelectedFunctionNames([]);
    setMsg("");
    setCreating(true);
  };

  const openEdit = (p: DataProduct) => {
    setCreating(false);
    setEditingId(p.id);
    setExpanded(p.id);
    setForm({
      id: p.id,
      domainId: p.domainId,
      name: p.name,
      ownerRole: p.ownerRole ?? "",
      teamRole: p.teamRole ?? "",
      purpose: p.purpose,
      limitations: p.limitations,
      usage: p.usage,
      version: p.version ?? "",
      status: p.status ?? "",
      sla: p.sla ?? "",
      support: p.support ?? "",
      // The editor works on Record<string, string>; ODPS customProperties values are
      // arbitrary JSON, so a non-string existing value is rendered as its string form.
      customProperties: Object.fromEntries(
        Object.entries(p.customProperties ?? {}).map(([k, v]) => [k, String(v)]),
      ),
    });
    setSelectedTableIds(memberTables(p.id).map((tb) => String(tb.id)));
    setSelectedFunctionNames(memberFunctions(p.id).map((fn) => fn.name));
    setMsg("");
  };

  const closeForm = () => {
    setCreating(false);
    setEditingId(null);
    setMsg("");
  };

  const handleSave = async () => {
    setMsg("");
    const result = await createDataProduct({
      id: form.id.trim(),
      domainId: form.domainId.trim(),
      name: form.name.trim(),
      ownerRole: form.ownerRole.trim() || null,
      teamRole: form.teamRole.trim() || null,
      purpose: form.purpose.trim(),
      limitations: form.limitations.trim(),
      usage: form.usage.trim(),
      version: form.version.trim() || null,
      status: form.status.trim() || null,
      sla: form.sla.trim() || null,
      support: form.support.trim() || null,
      customProperties: form.customProperties,
    });
    if (!result.success) {
      setMsg(result.message || t("dataProductsTab.saveFailed"));
      return;
    }
    // REQ-1634: table -> product membership is owned by the table row; reconcile the
    // multi-select against current membership by upserting only the tables that changed.
    // form.id is known before save (user-typed on create, fixed on edit), so this applies
    // in both modes once createDataProduct has confirmed the row exists.
    const productId = form.id.trim();
    const selected = new Set(selectedTableIds);
    const changed = tables.filter(
      // REQ-1443 clause 10: a checker table's membership is derived; it is never written.
      (tb) => tb.dqContract == null && selected.has(String(tb.id)) !== (tb.productId === productId),
    );
    for (const tb of changed) {
      await updateTable(
        buildTableUpdateInput({ ...tb, productId: selected.has(String(tb.id)) ? productId : null }),
      );
    }
    // REQ-1634: command -> product membership is owned by the function row; reconcile the
    // multi-select against current membership. saveFunction upserts by name (function_repo
    // .upsert_function), so resending the full existing field set with only productId toggled
    // is safe.
    const selectedFns = new Set(selectedFunctionNames);
    const changedFns = functions.filter(
      (fn) => selectedFns.has(fn.name) !== (fn.productId === productId),
    );
    for (const fn of changedFns) {
      await saveFunction({
        name: fn.name,
        sourceId: fn.sourceId,
        schemaName: fn.schemaName,
        functionName: fn.functionName,
        returns: fn.returns,
        arguments: fn.arguments,
        visibleTo: fn.visibleTo,
        domainId: fn.domainId,
        description: fn.description,
        kind: fn.kind,
        returnSchema: fn.returnSchema,
        outputColumns: fn.outputColumns,
        implKind: fn.implKind,
        binding: fn.binding,
        materialize: fn.materialize,
        productId: selectedFns.has(fn.name) ? productId : null,
      });
    }
    if (changedFns.length > 0) {
      const refreshed = await fetchActions();
      setFunctions(refreshed.functions);
    }
    closeForm();
  };

  const handleDelete = async () => {
    if (!deleteTarget) return;
    const result = await deleteDataProduct(deleteTarget);
    setDeleteTarget(null);
    if (refusal.refused(result, deleteTarget)) return;
    if (expanded === deleteTarget) setExpanded(null);
    if (!result.success) setMsg(result.message || t("dataProductsTab.deleteFailed"));
  };

  const formCard = (
    <DataProductFormCard
      editingId={editingId}
      form={form}
      setForm={setForm}
      domainOptions={domainOptions}
      roleOptions={roleOptions}
      tables={tables}
      selectedTableIds={selectedTableIds}
      setSelectedTableIds={setSelectedTableIds}
      functions={functions}
      selectedFunctionNames={selectedFunctionNames}
      setSelectedFunctionNames={setSelectedFunctionNames}
      saving={saving}
      msg={msg}
      onSave={handleSave}
      onCancel={closeForm}
    />
  );


  // REQ-1940: sort and group are the shared list mechanism.
  const filteredProducts = useMemo(() => {
    const q = search.trim().toLowerCase();
    return q
      ? dataProducts.filter(
          (p) =>
            p.id.toLowerCase().includes(q) ||
            p.name.toLowerCase().includes(q) ||
            p.domainId.toLowerCase().includes(q) ||
            (p.ownerRole ?? "").toLowerCase().includes(q) ||
            p.purpose.toLowerCase().includes(q),
        )
      : dataProducts;
  }, [dataProducts, search]);
  const productColumns = useMemo<ListColumn<DataProduct>[]>(
    () => [
      { key: "id", label: t("dataProductsTab.colId"), sortValue: (p) => p.id },
      {
        key: "domain",
        label: t("dataProductsTab.colDomain"),
        sortValue: (p) => p.domainId,
        groupValue: (p) => p.domainId,
      },
      { key: "name", label: t("dataProductsTab.colName"), sortValue: (p) => p.name },
      {
        key: "owner",
        label: t("dataProductsTab.colOwner"),
        sortValue: (p) => p.ownerRole ?? "",
        groupValue: (p) => p.ownerRole ?? "",
      },
      {
        key: "status",
        label: t("dataProductsTab.colStatus"),
        sortValue: (p) => p.status ?? "",
        groupValue: (p) => p.status ?? "",
      },
      { key: "purpose", label: t("dataProductsTab.colPurpose"), sortValue: (p) => p.purpose },
    ],
    [t],
  );
  const sortGroup = useListSortGroup(filteredProducts, productColumns, "data-products");

  return (
    <div
      style={{ flex: 1, overflow: "auto", padding: "1rem 1.25rem" }}
      data-tour="data-products-content"
    >
      <Group justify="space-between" mb="md">
        <Title order={3}>{t("dataProductsTab.title")}</Title>
        <FilterInput
          value={search}
          onChange={updateSearch}
          placeholder={t("dataProductsTab.filterPlaceholder")}
        />
        <Group gap="xs">
          {canEdit && (
            <Button
              size="xs"
              leftSection={<Plus size={13} />}
              onClick={openCreate}
              data-testid="data-products-new-button"
            >
              {t("dataProductsTab.newDataProduct")}
            </Button>
          )}
          <HelpBubble
            title={t("dataProductsTab.purposeTitle")}
            paragraphs={[t("dataProductsTab.purposeBody")]}
            ariaLabel={t("dataProductsTab.purposeAria")}
            testId="data-products-purpose-help"
          />
        </Group>
      </Group>

      {error && (
        <Alert color="red" mb="sm">
          {error.message}
        </Alert>
      )}
      {msg && !creating && editingId === null && (
        <Alert color="red" mb="sm" withCloseButton onClose={() => setMsg("")}>
          {msg}
        </Alert>
      )}

      {creating && (
        <Paper withBorder p="md" mb="md" data-testid="data-product-create-card">
          <Title order={5} mb="sm">
            {t("dataProductsTab.createTitle")}
          </Title>
          {formCard}
        </Paper>
      )}

      {(() => {
        if (loading && dataProducts.length === 0) {
          return <PageLoading message={t("dataProductsTab.loading")} />;
        }
        return (
          <ListTable testId="data-products-table">
            <ListHead
              sortGroup={sortGroup}
              columns={[
                { col: "id" },
                { col: "domain" },
                { col: "name" },
                { col: "owner" },
                { col: "status" },
                { col: "purpose" },
              ]}
            />
            <Table.Tbody>
              {filteredProducts.length === 0 && (
                <ListEmpty colSpan={6} testId="data-products-empty">
                  {t("dataProductsTab.empty")}
                </ListEmpty>
              )}
              <ListItems
                state={sortGroup}
                colSpan={6}
                rowKey={(p) => p.id}
                render={(p) => {
                const isExpanded = expanded === p.id;
                const isEditing = editingId === p.id;
                return (
                  <React.Fragment key={p.id}>
                    <ListRow
                      testId={`data-products-row-${p.id}`}
                      aria-expanded={isExpanded}
                      onClick={() => {
                        setExpanded(isExpanded ? null : p.id);
                        if (isEditing && isExpanded) closeForm();
                      }}
                    >
                      <Table.Td>
                        <Text size="sm" fw={600} ff="monospace">
                          {p.id}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{p.domainId}</Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{p.name}</Text>
                      </Table.Td>
                      <Table.Td>
                        <Group gap={4} wrap="nowrap">
                          <Text size="xs">{p.ownerRole ?? ""}</Text>
                          {p.ownerRole && (
                            <OwnerResolutionIcon
                              refs={[p.ownerRole]}
                              ariaLabel={t("dataProductsTab.resolveOwner", { id: p.id })}
                            />
                          )}
                        </Group>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{p.status ?? ""}</Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs" c="var(--text-muted)">
                          {p.purpose}
                        </Text>
                      </Table.Td>
                    </ListRow>
                    {isExpanded && (
                      <ListExpandRow colSpan={6} testId="data-product-detail" contained>
                        <ListDetail>
                          {isEditing ? (
                            formCard
                          ) : (
                            <DataProductDetailPanel
                              p={p}
                              tables={memberTables(p.id)}
                              sources={sources}
                              functions={memberFunctions(p.id)}
                              relatedTerms={relatedTerms}
                              relatedTermsLoading={relatedTermsLoading}
                              relatedTables={relatedTables(p.id)}
                              internalRelationships={internalRelationships(p.id)}
                              canEdit={canEdit}
                              canSeeLineage={canSeeLineage}
                              lineageLoading={lineageLoading}
                              lineageError={lineageError}
                              lineageGraph={lineageFor(p.id)}
                              lineageMembers={lineageMembersFor(p.id)}
                              describeColumn={describeColumn}
                              inputPorts={inputPortsFor(p.id)}
                              onEdit={() => openEdit(p)}
                              onDelete={() => setDeleteTarget(p.id)}
                            />
                          )}
                        </ListDetail>
                      </ListExpandRow>
                    )}
                  </React.Fragment>
                );
                }}
              />
            </Table.Tbody>
          </ListTable>
        );
      })()}

      {/* Delete confirm */}
      <Modal
        opened={deleteTarget !== null}
        onClose={() => setDeleteTarget(null)}
        title={t("dataProductsTab.deleteTitle")}
        centered
        data-testid="data-product-delete-modal"
      >
        <Text mb="lg" size="sm">
          {t("dataProductsTab.deleteConfirm", { id: deleteTarget ?? "" })}
        </Text>
        <Group justify="flex-end" gap="sm">
          <Button variant="default" onClick={() => setDeleteTarget(null)}>
            {t("dataProductsTab.cancel")}
          </Button>
          <Button
            color="red"
            onClick={handleDelete}
            loading={deleting}
            data-testid="data-product-delete-confirm"
          >
            {t("dataProductsTab.delete")}
          </Button>
        </Group>
      </Modal>
      {refusal.dialog}
    </div>
  );
}
