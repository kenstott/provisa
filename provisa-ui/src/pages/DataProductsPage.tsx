// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  Modal,
  MultiSelect,
  Paper,
  Select,
  Stack,
  Table,
  Text,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, Plus, X } from "lucide-react";
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
import { CustomPropertiesEditor } from "./data-products/CustomPropertiesEditor";

interface DataProductForm {
  id: string;
  domainId: string;
  name: string;
  ownerRole: string;
  teamRole: string;
  purpose: string;
  limitations: string;
  usage: string;
  version: string;
  status: string;
  sla: string;
  support: string;
  customProperties: Record<string, string>;
}

// Exported for the tour, which clears it so the page mounts collapsed and its click on the first
// row deterministically EXPANDS rather than toggling whatever a prior visit left open.
export const EXPANDED_STORAGE_KEY = "provisa.data_products.expanded";

const EMPTY_FORM: DataProductForm = {
  id: "",
  domainId: "",
  name: "",
  ownerRole: "",
  teamRole: "",
  purpose: "",
  limitations: "",
  usage: "",
  version: "",
  status: "",
  sla: "",
  support: "",
  customProperties: {},
};

interface DataProductFormCardProps {
  editingId: string | null;
  form: DataProductForm;
  setForm: React.Dispatch<React.SetStateAction<DataProductForm>>;
  domainOptions: string[];
  roleOptions: string[];
  tables: RegisteredTable[];
  selectedTableIds: string[];
  setSelectedTableIds: React.Dispatch<React.SetStateAction<string[]>>;
  functions: TrackedFunction[];
  selectedFunctionNames: string[];
  setSelectedFunctionNames: React.Dispatch<React.SetStateAction<string[]>>;
  saving: boolean;
  msg: string;
  onSave: () => void;
  onCancel: () => void;
}

// Inline create/edit form — the app-wide pattern: no modal, the form renders in
// place (creation card above the table, edit inside the expanded detail row).
function DataProductFormCard({
  editingId,
  form,
  setForm,
  domainOptions,
  roleOptions,
  tables,
  selectedTableIds,
  setSelectedTableIds,
  functions,
  selectedFunctionNames,
  setSelectedFunctionNames,
  saving,
  msg,
  onSave,
  onCancel,
}: DataProductFormCardProps) {
  const { t } = useTranslation();
  return (
    <Stack gap="sm" data-testid="data-product-form">
      <TextInput
        label={t("dataProductsTab.idLabel")}
        required
        value={form.id}
        disabled={editingId !== null}
        onChange={(e) => setForm((f) => ({ ...f, id: e.target.value }))}
        placeholder={t("dataProductsTab.idPlaceholder")}
        data-testid="data-product-id-input"
      />
      <Select
        label={t("dataProductsTab.domainLabel")}
        required
        value={form.domainId}
        onChange={(v) => setForm((f) => ({ ...f, domainId: v ?? "" }))}
        data={domainOptions}
        searchable
        data-testid="data-product-domain-input"
      />
      <TextInput
        label={t("dataProductsTab.nameLabel")}
        required
        value={form.name}
        onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
        placeholder={t("dataProductsTab.namePlaceholder")}
        data-testid="data-product-name-input"
      />
      <Select
        label={t("dataProductsTab.ownerLabel")}
        value={form.ownerRole || null}
        onChange={(v) => setForm((f) => ({ ...f, ownerRole: v ?? "" }))}
        data={roleOptions}
        placeholder={t("dataProductsTab.ownerPlaceholder")}
        searchable
        clearable
        data-testid="data-product-owner-input"
      />
      <Select
        label={t("dataProductsTab.teamLabel")}
        value={form.teamRole || null}
        onChange={(v) => setForm((f) => ({ ...f, teamRole: v ?? "" }))}
        data={roleOptions}
        placeholder={t("dataProductsTab.teamPlaceholder")}
        searchable
        clearable
        data-testid="data-product-team-input"
      />
      <Textarea
        label={t("dataProductsTab.purposeLabel")}
        value={form.purpose}
        onChange={(e) => setForm((f) => ({ ...f, purpose: e.target.value }))}
        placeholder={t("dataProductsTab.purposePlaceholder")}
        rows={2}
        data-testid="data-product-purpose-input"
      />
      <Textarea
        label={t("dataProductsTab.limitationsLabel")}
        value={form.limitations}
        onChange={(e) => setForm((f) => ({ ...f, limitations: e.target.value }))}
        placeholder={t("dataProductsTab.limitationsPlaceholder")}
        rows={2}
        data-testid="data-product-limitations-input"
      />
      <Textarea
        label={t("dataProductsTab.usageLabel")}
        value={form.usage}
        onChange={(e) => setForm((f) => ({ ...f, usage: e.target.value }))}
        placeholder={t("dataProductsTab.usagePlaceholder")}
        rows={2}
        data-testid="data-product-usage-input"
      />
      <TextInput
        label={t("dataProductsTab.versionLabel")}
        value={form.version}
        onChange={(e) => setForm((f) => ({ ...f, version: e.target.value }))}
        placeholder={t("dataProductsTab.versionPlaceholder")}
        data-testid="data-product-version-input"
      />
      <TextInput
        label={t("dataProductsTab.statusLabel")}
        value={form.status}
        onChange={(e) => setForm((f) => ({ ...f, status: e.target.value }))}
        placeholder={t("dataProductsTab.statusPlaceholder")}
        data-testid="data-product-status-input"
      />
      <TextInput
        label={t("dataProductsTab.slaLabel")}
        value={form.sla}
        onChange={(e) => setForm((f) => ({ ...f, sla: e.target.value }))}
        placeholder={t("dataProductsTab.slaPlaceholder")}
        data-testid="data-product-sla-input"
      />
      <TextInput
        label={t("dataProductsTab.supportLabel")}
        value={form.support}
        onChange={(e) => setForm((f) => ({ ...f, support: e.target.value }))}
        placeholder={t("dataProductsTab.supportPlaceholder")}
        data-testid="data-product-support-input"
      />
      <CustomPropertiesEditor
        value={form.customProperties}
        onChange={(v) => setForm((f) => ({ ...f, customProperties: v }))}
      />
      <MultiSelect
        label={t("dataProductsTab.tablesLabel")}
        description={t("dataProductsTab.tablesDesc")}
        placeholder={
          form.domainId ? t("dataProductsTab.tablesPlaceholder") : t("dataProductsTab.tablesNoDomain")
        }
        disabled={!form.domainId}
        value={selectedTableIds}
        onChange={setSelectedTableIds}
        // REQ-1634: a table may only join a data product owned by its own domain.
        // REQ-1443 clause 10: a checker table (one carrying a contract) is a member through the
        // table it scans, so it is listed but not toggled.
        data={tables
          .filter((tb) => tb.domainId === form.domainId)
          .map((tb) => ({
            value: String(tb.id),
            label: `${tb.domainId}.${tb.tableName}`,
            disabled: tb.dqContract != null,
          }))}
        searchable
        data-testid="data-product-tables-input"
      />
      <MultiSelect
        label={t("dataProductsTab.commandsLabel")}
        description={t("dataProductsTab.commandsDesc")}
        placeholder={
          form.domainId
            ? t("dataProductsTab.commandsPlaceholder")
            : t("dataProductsTab.commandsNoDomain")
        }
        disabled={!form.domainId}
        value={selectedFunctionNames}
        onChange={setSelectedFunctionNames}
        // REQ-1634: a command may only join a data product owned by its own domain.
        data={functions
          .filter((fn) => fn.domainId === form.domainId)
          .map((fn) => ({ value: fn.name, label: fn.name }))}
        searchable
        data-testid="data-product-commands-input"
      />
      {msg && (
        <Alert color="red" data-testid="data-product-form-error">
          {msg}
        </Alert>
      )}
      <Group justify="flex-end" gap="sm">
        <Button
          variant="default"
          leftSection={<X size={14} />}
          onClick={onCancel}
          data-testid="data-product-cancel-button"
        >
          {t("dataProductsTab.cancel")}
        </Button>
        <Button
          variant="filled"
          leftSection={<Check size={14} />}
          onClick={onSave}
          loading={saving}
          disabled={!form.id.trim() || !form.domainId.trim() || !form.name.trim()}
          data-testid="data-product-save-button"
        >
          {t("dataProductsTab.save")}
        </Button>
      </Group>
    </Stack>
  );
}

// REQ-1634: top-level data-products management page (list / create / edit / delete).
// Follows the MetricsPage detail-then-edit pattern (REQ-1323) — row click expands the
// detail panel; Edit/Delete live inside it and edit swaps the panel for the inline form.
export function DataProductsPage() {
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
  const [expanded, setExpandedState] = useState<string | null>(() => {
    try {
      return localStorage.getItem(EXPANDED_STORAGE_KEY);
    } catch {
      return null;
    }
  });
  const setExpanded = (id: string | null) => {
    setExpandedState(id);
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
  // tables plus their full upstream ancestry — the product's tables are the published endpoint,
  // so we walk backward (source -> target = derives-into) transitively to show everything that
  // was used to derive them, and never walk forward into whatever consumes them downstream.
  // Same LineageDag component the Lineage page renders (REQ-1161/1627), not a table-list view.
  // Informational only — never stored, mirrors the Related Tables computation.
  const lineageFor = (id: string): LineageGraphData | null => {
    if (!lineageGraph) return null;
    const memberRelations = new Set(
      memberTables(id).map((tb) => `${domainToSqlName(tb.domainId)}.${tb.tableName}`),
    );
    const nodesById = new Map(lineageGraph.nodes.map((n) => [n.id, n]));
    const keepRelations = new Set(memberRelations);
    let frontier = memberRelations;
    while (frontier.size > 0) {
      const next = new Set<string>();
      for (const edge of lineageGraph.edges) {
        const src = nodesById.get(edge.source);
        const tgt = nodesById.get(edge.target);
        if (!src?.relation || !tgt?.relation || src.relation === tgt.relation) continue;
        if (frontier.has(tgt.relation) && !keepRelations.has(src.relation)) {
          keepRelations.add(src.relation);
          next.add(src.relation);
        }
      }
      frontier = next;
    }
    if (keepRelations.size === 0) return { nodes: [], edges: [], outputs: [] };
    const nodes = lineageGraph.nodes.filter((n) => n.relation && keepRelations.has(n.relation));
    const keepIds = new Set(nodes.map((n) => n.id));
    const edges = lineageGraph.edges.filter(
      (e) => keepIds.has(e.source) && keepIds.has(e.target),
    );
    return { nodes, edges, outputs: [] };
  };

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
      const entry = byTarget.get(outputLabel) ?? { transform: edge.transform, inputs: new Set<string>() };
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
        memberIds.has(r.sourceTableId) &&
        r.targetTableId != null &&
        memberIds.has(r.targetTableId),
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
        writableBy: fn.writableBy,
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
        const q = search.trim().toLowerCase();
        const filtered = q
          ? dataProducts.filter(
              (p) =>
                p.id.toLowerCase().includes(q) ||
                p.name.toLowerCase().includes(q) ||
                p.domainId.toLowerCase().includes(q) ||
                (p.ownerRole ?? "").toLowerCase().includes(q) ||
                p.purpose.toLowerCase().includes(q),
            )
          : dataProducts;
        return loading && dataProducts.length === 0 ? (
        <Text size="sm" c="var(--text-muted)">
          {t("dataProductsTab.loading")}
        </Text>
      ) : filtered.length === 0 ? (
        <Text size="sm" c="var(--text-muted)" data-testid="data-products-empty">
          {t("dataProductsTab.empty")}
        </Text>
      ) : (
        <Table striped highlightOnHover data-testid="data-products-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("dataProductsTab.colId")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colDomain")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colName")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colOwner")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colStatus")}</Table.Th>
              <Table.Th>{t("dataProductsTab.colPurpose")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {filtered.map((p) => {
              const isExpanded = expanded === p.id;
              const isEditing = editingId === p.id;
              return (
                <React.Fragment key={p.id}>
                  <Table.Tr
                    data-testid={`data-products-row-${p.id}`}
                    aria-expanded={isExpanded}
                    onClick={() => {
                      setExpanded(isExpanded ? null : p.id);
                      if (isEditing && isExpanded) closeForm();
                    }}
                    style={{
                      cursor: "pointer",
                      background: isExpanded ? "var(--surface)" : undefined,
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
                  </Table.Tr>
                  {isExpanded && (
                    <Table.Tr key={`${p.id}-detail`} data-testid="data-product-detail">
                      <Table.Td
                        colSpan={6}
                        style={{
                          padding: "0.75rem 1rem",
                          background: "var(--bg)",
                          borderTop: "1px solid var(--border)",
                          // maxWidth: 0 stops this cell's flex-wrap content from ballooning the
                          // table's auto-layout column widths to fit everything on one row — the
                          // cell still renders at the table's actual (viewport-bound) width, which
                          // lets the flex-wrap panels inside actually reflow on browser resize.
                          maxWidth: 0,
                        }}
                        onClick={(e) => e.stopPropagation()}
                      >
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
                            inputPorts={inputPortsFor(p.id)}
                            onEdit={() => openEdit(p)}
                            onDelete={() => setDeleteTarget(p.id)}
                          />
                        )}
                      </Table.Td>
                    </Table.Tr>
                  )}
                </React.Fragment>
              );
            })}
          </Table.Tbody>
        </Table>
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
    </div>
  );
}
