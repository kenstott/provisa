// Copyright (c) 2026 Kenneth Stott
// Canary: e1499f85-6ff4-44b7-aad6-327499acea72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MaskEntry } from "../lib/masks";
import { NEVER_EXPIRES, ttlText } from "../components/form/cacheTtl";
import { resultRedirectText } from "../components/resultRedirectText";
import { Tip } from "../components/Tip";
import { useSetTableDraft, useRegionChoices } from "../hooks/useRegionQueries";
import { useRegionSelection } from "../hooks/useRegionSelection";
import { filterByRegion } from "../hooks/regionFilter";
import { RegionSelector } from "../components/RegionSelector";
import { useState, useEffect, Fragment, useCallback, useRef, useMemo } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { ArrowLeft, FileSpreadsheet, Network, X } from "lucide-react";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Modal,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { ErdModal } from "../components/erd/ErdModal";
import { fetchSettings, profileTable, runSql } from "../api/admin";
import { downloadModelReport } from "../api/modelReport";
import type { PlatformSettings } from "../api/admin";
import {
  useTables,
  useSources,
  useDomains,
  useRoles,
  useAvailableColumnsMetadataLazy,
  useGenerateTableDescription,
  useGenerateColumnDescription,
  useColumnDependents,
  useUpdateTable,
  useDeleteTable,
  useDeployViewToDb,
  useSuggestTableAlias,
  useForceRegen,
  useAllRelationships,
  useDataProducts, // REQ-1634
} from "../hooks/useAdminQueries";
import { usePurgeCacheByTable, useReplicaBuilds } from "../hooks/useAdminOpsQueries";
import type { RegisteredTable, ColumnDependentsResult } from "../types/admin";
import { DERIVED_SOURCE_ID } from "../types/admin";
import { FilterInput } from "../components/admin/FilterInput";
import { HelpBubble } from "../components/HelpBubble";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useAuth } from "../context/AuthContext";
import { diffEditedColumns } from "./tables/columnDiff";
import { namingConventionLabel } from "../hooks/useNamingConventions";
import { buildTableUpdateInput } from "./tables/helpers";
import { RegisterTableForm } from "./tables/RegisterTableForm";
import { ViewDefinitionForm } from "./tables/ViewDefinitionForm";
import { ModelingForm } from "./tables/ModelingForm";
import { TableReadView } from "./tables/TableReadView";
import { TABLE_SECTIONS, type TableSection } from "./tables/tableSections";
import { COLUMN_TABS, type ColumnTab } from "./tables/columnTopics";
import { ReplicatingMark } from "./tables/ReplicatingMark";
import { buildOf } from "./tables/replicaBuild";
import { ReplicaBuildsContext } from "./tables/replicaBuildsContext";
import { TablePreviewPane } from "./tables/TablePreviewPane";
import { usePanelLeave } from "../components/list/panelLeave";
import { FormField, FormFooter, FormLayout } from "../components/form/FormLayout";
import { NativeParamsModal } from "../components/NativeParamsModal";
import { previewSql, requiredParamColumns } from "../components/nativeParams";
import { TagControl } from "../components/TagControl";
import { TableEditForm } from "./tables/TableEditForm";
import { draftFromRegistration, type Registration } from "./tables/draftTable";
import { useCreateTable } from "./tables/useCreateTable";
import { useNavPayload } from "../hooks/useNavPayload";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import { roleTtlValid, tableTtlSignalError } from "./tables/roleTtl";
import { declaredPaging, pagingChanged, pagingInput, pagingProblem } from "./tables/paging";
import { refusalError, serverMessage } from "../i18n/serverMessage";
import {
  replicateContradictsLoadProtection,
  resolvedReplicate,
} from "../components/admin/replicate";
import {
  ListTable,
  ListHead,
  ListGroupRow,
  ListRow,
  ListEmpty,
} from "../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../components/list/useListSortGroup";
import { PageLoading } from "../components/PageLoading";
import { useCapability } from "../hooks/useCapability";
import { hidingDomains } from "../lib/capabilities";
import { domainToSqlName } from "../naming";
import { SplitPane } from "../components/list/SplitPane";

export function TablesPage({ viewsOnly = false }: { viewsOnly?: boolean } = {}) {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { t: translate, i18n } = useTranslation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const {
    tables,
    loading: tablesLoading,
    error: tablesError,
    refetch: refetchTables,
  } = useTables();
  const { sources, refetch: refetchSources } = useSources();
  const { domains, refetch: refetchDomains } = useDomains();
  const { dataProducts } = useDataProducts(); // REQ-1634
  const { relationships } = useAllRelationships();
  // The detail's section in view: the same in the read view and the edit form, so opening the
  // editor keeps the tab that was being read.
  const [section, setSection] = useState<TableSection>("identity");
  const [showErd, setShowErd] = useState(false);
  const { roles, refetch: refetchRoles } = useRoles();
  const domainHints = domains.map((d) => d.id);
  const getAvailableColumnsMetadata = useAvailableColumnsMetadataLazy();
  const { generateTableDescription } = useGenerateTableDescription();
  const { generateColumnDescription } = useGenerateColumnDescription();
  const getColumnDependents = useColumnDependents();
  const { updateTable } = useUpdateTable();
  const { deleteTable } = useDeleteTable();
  const setTableDraft = useSetTableDraft();
  const { purgeCacheByTable } = usePurgeCacheByTable();
  const { forceRegen } = useForceRegen();
  const { deployViewToDb } = useDeployViewToDb();
  const { suggestTableAlias } = useSuggestTableAlias();
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<number | null>(null);
  // One request for every row; asked for often only while a build runs or a detail is open.
  const { replicaBuilds } = useReplicaBuilds({ watching: expanded !== null });
  // REQ-1386: table whose governed SELECT * preview modal is open.
  const [previewTable, setPreviewTable] = useState<RegisteredTable | null>(null);
  // Table awaiting native-param entry before its profile sample can run.
  const [profileParamsTable, setProfileParamsTable] = useState<RegisteredTable | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [showModeling, setShowModeling] = useState(false); // REQ-1164: entity/fact modeling modal
  // REQ-1318: Views-page definition-mode flow (SQL editor vs metric/dimension picker).
  const [showViewForm, setShowViewForm] = useState(false);
  const [editingViewDef, setEditingViewDef] = useState<RegisteredTable | null>(null);
  const [error, setError] = useState<string | null>(null);
  // `?source=` seeds the filter box, and seeds it again whenever it changes while the page stays
  // mounted (the tour moves /tables?source=dq-checker -> /tables?source=pet-store-sqlite in place).
  const sourceParam = searchParams.get("source");
  const [tableSearch, setTableSearch] = useState(sourceParam ?? "");
  const [seededSource, setSeededSource] = useState(sourceParam);
  if (sourceParam !== seededSource) {
    setSeededSource(sourceParam);
    setTableSearch(sourceParam ?? "");
  }
  // `?table=<id>` opens that table's detail panel: a link from elsewhere (its checkers, its
  // source) lands on the table itself, again whenever the link changes while the page stays.
  const tableParam = searchParams.get("table");
  const [seededTable, setSeededTable] = useState<string | null>(null);
  if (tableParam !== seededTable) {
    setSeededTable(tableParam);
    if (tableParam !== null) setExpanded(Number(tableParam));
  }
  // `?open=<source>.<table>` opens that table's detail panel by its name -- a demo tour's step
  // names a table so, whatever id the org gave it -- on `section` and, in Columns, `tab`.
  const openParam = searchParams.get("open");
  const sectionParam = searchParams.get("section");
  const tabParam = searchParams.get("tab");
  const opened = openParam
    ? tables.find(
        (t) =>
          `${t.sourceId}.${t.tableName}` === openParam || `${t.sourceId}.${t.alias}` === openParam,
      )
    : undefined;
  const [seededOpen, setSeededOpen] = useState<string | null>(null);
  const openKey = opened ? `${opened.id}:${sectionParam}:${tabParam}` : null;
  if (openKey !== null && openKey !== seededOpen) {
    setSeededOpen(openKey);
    setExpanded(opened!.id);
    if (sectionParam && (TABLE_SECTIONS as string[]).includes(sectionParam)) {
      setSection(sectionParam as TableSection);
    }
  }
  const linkedColumnTab =
    tabParam && (COLUMN_TABS as string[]).includes(tabParam) ? (tabParam as ColumnTab) : undefined;
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 50;
  const { checkedDomains, domainsEnabled, ensureDomainChecked } = useDomainFilter();
  // REQ-1944: without table_registration the page is a governance surface -- the tables of the
  // domains the caller's hiding rights reach, editable only in how their columns are hidden.
  const tableEditor = useCapability("table_registration");
  const hidingOnly = !tableEditor;
  const { selectedRoles, actingRoles } = useAuth();
  const governedDomains = useMemo(
    () => (hidingOnly ? hidingDomains(selectedRoles) : null),
    [hidingOnly, selectedRoles],
  );
  // REQ-1940: sort and group are the shared list mechanism.
  const filteredTables = useMemo(
    () =>
      tables.filter((t) => {
        if (t.sourceId === "provisa-admin" || t.sourceId === "provisa-otel") return false;
        if (viewsOnly && !t.viewSql) return false;
        if (governedDomains !== null && !governedDomains.has(t.domainId ?? "")) return false;
        if (t.domainId && checkedDomains.size > 0 && !checkedDomains.has(t.domainId)) return false;
        const terms = tableSearch.trim().toLowerCase().split(/\s+/).filter(Boolean);
        if (terms.length === 0) return true;
        const haystack = [t.sourceId, t.tableName, t.domainId ?? ""].join(" ").toLowerCase();
        return terms.every((term) => haystack.includes(term));
      }),
    [tables, viewsOnly, checkedDomains, tableSearch, governedDomains],
  );
  const listColumns = useMemo<ListColumn<RegisteredTable>[]>(
    () => [
      {
        key: "source",
        label: translate("tablesPage.colSource"),
        sortValue: (t) => t.sourceId,
        groupValue: (t) => t.sourceId,
      },
      {
        key: "domain",
        label: translate("tablesPage.colDomain"),
        sortValue: (t) => t.domainId ?? "",
        groupValue: (t) => (t.domainId ? domainToSqlName(t.domainId) : "(none)"),
      },
      {
        key: "table",
        label: translate("tablesPage.colTable"),
        sortValue: (t) => t.alias || t.tableName,
      },
      { key: "cols", label: translate("tablesPage.colCols"), sortValue: (t) => t.columns.length },
    ],
    [translate],
  );
  // REQ-1922: the region selector filters the list; a name search crosses regions (filter off).
  const { regions, connected, error: regionError } = useRegionChoices();
  const [regionSel, setRegionSel] = useRegionSelection(regions, connected);
  const hasRegions = regions.length > 0;
  const nameSearchActive = tableSearch.trim().length > 0;
  const { visible: regionTables, hidden: regionHidden } = nameSearchActive
    ? { visible: filteredTables, hidden: 0 }
    : filterByRegion(filteredTables, regionSel, connected, (t) => t.region ?? null);
  const sortGroup = useListSortGroup(regionTables, listColumns, "tables");
  const groupBy = sortGroup.groupBy;
  const { domainAccess } = useAuth();
  // REQ-1620: the rows are read as the roles the user acts as -- one role, or under "Role: All"
  // the set, which the server serves as its meta-role -- as every other page reads them.
  const actingRole = actingRoles;
  // REQ-1592: the model-report download. Busy while the server builds the workbook.
  const [reportBusy, setReportBusy] = useState(false);

  // Per-table profile state: tableId → profile result or "loading"
  const [tableProfiles, setTableProfiles] = useState<
    Record<
      number,
      { columns: string[]; rows: Record<string, unknown>[]; rowCount: number } | "loading" | string
    >
  >({});

  // Inline edit state for expanded table
  const [editingTable, setEditingTable] = useState<RegisteredTable | null>(null);
  // #98: the row copied into the edit form can come from a stale cache snapshot (the
  // localStorage-restored Apollo cache, before the post-navigation refetch lands). The
  // JSON of the row as copied; while the form still matches it (user hasn't typed),
  // a fresher tables result replaces the form state. First keystroke ends the follow.
  const pristineEdit = useRef<string | null>(null);
  // The same, where rendering needs it: what the open edit is compared with to say "changed".
  const [pristine, setPristine] = useState<string | null>(null);
  // Leaving a form in the panel with unsaved changes asks first; the page's own openers of
  // other forms go through the same guard as the panel's close and the list's rows.
  const leave = usePanelLeave();
  const [editingColumnTypes, setEditingColumnTypes] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  // REQ-2109 (settled): a table or view being created -- what its create step chose. Its settings
  // are edited in the same sections as an editor's (editingTable holds them) until Save registers it.
  const [newRegistration, setNewRegistration] = useState<Registration | null>(null);
  const createTable = useCreateTable();

  // Cache state
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [cacheTtlEdits, setCacheTtlEdits] = useState<
    Record<number, { value: string; dirty: boolean; saving: boolean }>
  >({});
  const [purging, setPurging] = useState<Record<number, boolean>>({});
  // REQ-968 forced run: the table the operator is rebuilding, and the reason the server records on
  // the event. The reason is asked for rather than defaulted — a forced rebuild with no why is the
  // one the estate's history cannot explain later.
  const [regenTable, setRegenTable] = useState<RegisteredTable | null>(null);
  const [regenReason, setRegenReason] = useState("");
  const [regenning, setRegenning] = useState(false);
  const [regenQueued, setRegenQueued] = useState<string | null>(null);
  const [deploying, setDeploying] = useState<Record<number, boolean>>({});
  const [deployMsg, setDeployMsg] = useState<Record<number, { success: boolean; message: string }>>(
    {},
  );

  const reload = useCallback(() => {
    setLoading(true);
    // tables/sources/domains/roles come from useQuery subscribers; refetch revalidates
    // their caches. Settings is still an imperative REST read (fetch(), not GraphQL).
    // These refetches outlive this callback (a subsequent navigation can abort them in
    // flight), so an unhandled rejection here becomes an uncaught error in the page —
    // Apollo's own error state already reflects the failure, so just swallow it.
    // Returned so a caller that must not act on pre-reload data (handleSaveEdit) can await it.
    // Callers that only want a background revalidation ignore the promise, as before.
    return Promise.all([
      refetchTables().catch(() => {}),
      refetchSources().catch(() => {}),
      refetchDomains().catch(() => {}),
      refetchRoles().catch(() => {}),
      fetchSettings()
        .then((st) => setSettings(st))
        .finally(() => setLoading(false)),
    ]);
  }, [refetchTables, refetchSources, refetchDomains, refetchRoles]);

  // Seed/refresh per-table TTL edits whenever the tables list changes. Preserve any
  // in-progress dirty edit so a background cache-and-network refetch can't clobber it.
  useEffect(() => {
    setCacheTtlEdits((prev) => {
      const next: Record<number, { value: string; dirty: boolean; saving: boolean }> = {};
      // A table being created is not in the list yet: what was typed for it is kept.
      for (const [id, edit] of Object.entries(prev)) {
        if (edit.dirty && !tables.some((tbl) => tbl.id === Number(id))) next[Number(id)] = edit;
      }
      for (const tbl of tables) {
        const existing = prev[tbl.id];
        next[tbl.id] =
          existing && existing.dirty
            ? existing
            : {
                value: tbl.cacheTtl != null ? String(tbl.cacheTtl) : "",
                dirty: false,
                saving: false,
              };
      }
      return next;
    });
  }, [tables]);

  // #98: while the edit form is pristine (form state still equals the row as copied),
  // follow the tables query — a fresher result for the same table replaces the copy.
  // The first user edit diverges the form from the pristine JSON and ends the follow,
  // so an in-progress edit is never clobbered (same contract as cacheTtlEdits above).
  useEffect(() => {
    setEditingTable((current) => {
      if (current === null || pristineEdit.current === null) return current;
      if (JSON.stringify(current) !== pristineEdit.current) return current;
      const fresh = tables.find((tbl) => tbl.id === current.id);
      if (!fresh) return current;
      const freshJson = JSON.stringify(fresh);
      if (freshJson === pristineEdit.current) return current;
      pristineEdit.current = freshJson;
      setPristine(freshJson);
      return JSON.parse(freshJson);
    });
  }, [tables]);

  const getEffectiveTableTtl = (t: RegisteredTable): string => {
    const source = sources.find((s) => s.id === t.sourceId);
    const ttl = t.cacheTtl ?? source?.cacheTtl;
    if (ttl === NEVER_EXPIRES) {
      // REQ-2031: a replica that never goes stale says when it landed, where its TTL would.
      const landed = buildOf(replicaBuilds?.builds, t)?.completedAt;
      const said = ttlText(ttl, translate);
      return landed
        ? translate("cacheTtl.asOf", {
            ttl: said,
            when: new Date(landed).toLocaleString(i18n.language),
          })
        : said;
    }
    if (t.cacheTtl != null) return `${t.cacheTtl}s (custom)`;
    if (source?.cacheTtl != null) return `${source.cacheTtl}s (from source)`;
    if (settings) return `${settings.cache.default_ttl}s (global)`;
    return "default";
  };

  // A table whose source caches its results (a remote API's) has a cache to drop; the panel's
  // head offers that for it alone.
  const hasCacheable = (table: RegisteredTable) => {
    const srcType = sources.find((s) => s.id === table.sourceId)?.type;
    return srcType === "graphql_remote" || srcType === "openapi" || srcType === "grpc_remote";
  };

  const handlePurgeTableCache = async (tableId: number) => {
    setPurging((prev) => ({ ...prev, [tableId]: true }));
    setError(null);
    try {
      const result = await purgeCacheByTable(tableId);
      if (!result.success) throw refusalError(result);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setPurging((prev) => ({ ...prev, [tableId]: false }));
    }
  };

  const handleForceRegen = async () => {
    if (regenTable === null) return;
    const table = regenTable;
    setRegenning(true);
    setError(null);
    try {
      const result = await forceRegen(table.id, regenReason);
      if (!result.success) throw refusalError(result);
      setRegenQueued(
        translate("tablesPage.runNowQueued", {
          table: `${table.schemaName}.${table.tableName}`,
        }),
      );
      setRegenTable(null);
      setRegenReason("");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRegenning(false);
    }
  };

  useEffect(() => {
    // useQuery hooks (tables/sources/domains/roles) fetch on mount automatically.
    // Only the imperative REST call (fetchSettings) needs explicit initialization.
    fetchSettings()
      .then((st) => setSettings(st))
      .finally(() => setLoading(false));
  }, []);

  // Reset pagination when filters change
  useEffect(() => {
    setPage(0);
  }, [tableSearch, checkedDomains, groupBy]);

  // REQ-1592: send the page's domain filter along, so the workbook covers what the page shows. A
  // selection of every domain is no selection at all — omitting the parameter then leaves the
  // caller's own authority to decide, which is what the server does with an absent `domains`.
  const downloadReport = async () => {
    const narrowed = checkedDomains.size > 0 && checkedDomains.size < domains.length;
    setReportBusy(true);
    setError(null);
    try {
      await downloadModelReport(narrowed ? [...checkedDomains] : undefined);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setReportBusy(false);
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm("Delete this table registration?")) return;
    try {
      const result = await deleteTable(id);
      const name = tables.find((tbl) => tbl.id === id)?.tableName ?? String(id);
      if (refusal.refused(result, name)) return;
      if (!result.success) {
        setError(serverMessage(result, result.message));
        return;
      }
      reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  // REQ-1921: the detail's Draft box. The same mutation the edit form's draft control saves
  // through; the box shows what is stored, so the tables are reloaded once it answers.
  const handleSetDraft = async (table: RegisteredTable, draft: boolean) => {
    setError(null);
    const result = await setTableDraft(table.id, draft);
    if (!result.success) setError(serverMessage(result, result.message));
    await reload();
  };

  const handleProfile = async (tableId: number, paramValues?: Record<string, string>) => {
    const existing = tableProfiles[tableId];
    if (existing !== undefined && existing !== "loading") {
      setTableProfiles((prev) => {
        const next = { ...prev };
        delete next[tableId];
        return next;
      });
      return;
    }
    if (!actingRole) {
      setTableProfiles((prev) => ({ ...prev, [tableId]: "No active role" }));
      return;
    }
    // A table with required native params (isRequiredParam) cannot be sampled bare:
    // collect the params first, then profile through the governed SQL pipeline
    // with the params as WHERE predicates.
    const table = tables.find((tbl) => tbl.id === tableId);
    if (table && requiredParamColumns(table).length > 0 && paramValues === undefined) {
      setProfileParamsTable(table);
      return;
    }
    setTableProfiles((prev) => ({ ...prev, [tableId]: "loading" }));
    try {
      if (table && paramValues !== undefined) {
        const result = await runSql(previewSql(table, paramValues), actingRole);
        if (result.error) throw new Error(result.error);
        // A sample landed in the results store is there, not here: said, not profiled as empty.
        if (result.redirect)
          throw new Error(resultRedirectText(translate, i18n.language, result.redirect));
        setTableProfiles((prev) => ({
          ...prev,
          [tableId]: { columns: result.columns, rows: result.rows, rowCount: result.rows.length },
        }));
      } else {
        const result = await profileTable(tableId, actingRole);
        setTableProfiles((prev) => ({ ...prev, [tableId]: result }));
      }
    } catch (e) {
      setTableProfiles((prev) => ({
        ...prev,
        [tableId]: e instanceof Error ? e.message : String(e),
      }));
    }
  };

  const startEditing = (t: RegisteredTable) => {
    setEditingTable(JSON.parse(JSON.stringify(t)));
    pristineEdit.current = JSON.stringify(t);
    setPristine(pristineEdit.current);
    setEditingColumnTypes({});
    getAvailableColumnsMetadata(t.sourceId, t.schemaName, t.tableName)
      .then((meta) => {
        const map: Record<string, string> = {};
        for (const c of meta) map[c.name] = c.dataType;
        setEditingColumnTypes(map);
      })
      .catch(() => {});
  };

  const cancelEditing = () => {
    setEditingTable(null);
    setNewRegistration(null);
  };

  // REQ-2109 (settled): the create step chose what is registered; its settings follow in the same
  // sections as the editor's, on the table as it will be registered.
  const startCreating = (registration: Registration) => {
    setShowForm(false);
    setShowViewForm(false);
    setExpanded(null);
    setSection("identity");
    startEditing(draftFromRegistration(registration));
    setNewRegistration(registration);
  };
  // A view handed over from the SQL editor's + View is created here, with every setting.
  useNavPayload<{ createView?: Registration }>((payload) => {
    if (payload.createView) startCreating(payload.createView);
  });

  // One Save registers the table with every setting its form holds, then shows it.
  const handleCreate = async () => {
    if (!editingTable || !newRegistration) return;
    setError(null);
    const problem = saveProblem(editingTable);
    if (problem !== null) {
      setError(problem);
      return;
    }
    const staged = cacheTtlEdits[editingTable.id]?.value;
    const cacheTtl =
      staged === undefined ? editingTable.cacheTtl : staged === "" ? null : Number(staged);
    setSaving(true);
    try {
      const outcome = await createTable(editingTable, newRegistration, cacheTtl);
      if (outcome.refused) {
        setError(serverMessage(outcome.refused, outcome.refused.message));
        return;
      }
      // A table registered into a domain the filter has not seen (one with no tables until now)
      // would otherwise stay hidden from the list until a reload.
      ensureDomainChecked(editingTable.domainId);
      setEditingTable(null);
      setNewRegistration(null);
      await reload();
      // The table just registered is selected and its detail shown.
      if (outcome.registered) setExpanded(outcome.registered.id);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const updateEditCol = (
    i: number,
    key: string,
    value: string | string[] | boolean | MaskEntry[],
  ) => {
    if (!editingTable) return;
    const next = { ...editingTable };
    next.columns = [...next.columns];
    next.columns[i] = { ...next.columns[i], [key]: value };
    setEditingTable(next);
  };

  // REQ-1484: the pending edit's dependent artifacts, held while the administrator decides. Null =
  // nothing to confirm.
  const [pendingDependents, setPendingDependents] = useState<ColumnDependentsResult[] | null>(null);

  // REQ-1484: warn before a save that renames a column's SQL alias or drops a column, because both
  // break artifacts authored against it. The check runs BEFORE the save — a renamed column is
  // located by the exposed name it still carries in the registry.
  const handleSaveEdit = async () => {
    if (!editingTable) return;
    const stored = tables.find((t) => t.id === editingTable.id);
    if (!stored) {
      await performSaveEdit();
      return;
    }
    const { renamed, removed } = diffEditedColumns(stored.columns, editingTable.columns);
    if (renamed.length === 0 && removed.length === 0) {
      await performSaveEdit();
      return;
    }
    setError(null);
    setSaving(true);
    try {
      const found = await getColumnDependents(editingTable.id, renamed, removed);
      if (found.length > 0) {
        setPendingDependents(found);
        return;
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return;
    } finally {
      setSaving(false);
    }
    await performSaveEdit();
  };

  // What Save refuses of the table in the form before any mutation runs, the same when it is
  // created and when it is edited; null when nothing.
  const saveProblem = (editingTable: RegisteredTable): string | null => {
    // REQ-1907: an invalid role TTL row blocks the whole save before any mutation runs.
    if (!roleTtlValid(editingTable.roleTtl)) {
      return translate("tableEditForm.roleTtlFixErrors");
    }
    // REQ-318: paging the table cannot take blocks the save too (the server refuses it by name).
    if (
      editingTable.pagingKind !== null &&
      pagingProblem(
        editingTable.pagingKind,
        editingTable.pagination,
        editingTable.pagingCeilingRows,
      ) !== null
    ) {
      return translate("tableEditForm.pagingFixErrors");
    }
    const source = sources.find((s) => s.id === editingTable.sourceId);
    const effectiveLoadProtected = editingTable.loadProtected ?? source?.loadProtected ?? false;
    // REQ-930: a ttl/ttl_probe signal needs a landing Cache TTL (table, else source) as its clock.
    if (editingTable.viewSql == null) {
      const staged = cacheTtlEdits[editingTable.id]?.value;
      const tableTtl =
        staged === undefined ? editingTable.cacheTtl : staged === "" ? null : Number(staged);
      if (tableTtlSignalError(editingTable, source, tableTtl ?? source?.cacheTtl ?? null)) {
        return translate("tableEditForm.loadManagementFixErrors");
      }
      // REQ-826: load protection with Replicate = Never is contradictory.
      if (
        replicateContradictsLoadProtection(
          resolvedReplicate(editingTable.replicate, source?.replicate),
          effectiveLoadProtected,
        )
      ) {
        return translate("replicateSelect.neverWithLoadProtection");
      }
    }
    return null;
  };

  const performSaveEdit = async () => {
    if (!editingTable) return;
    setError(null);
    const problem = saveProblem(editingTable);
    if (problem !== null) {
      setError(problem);
      return;
    }
    const saved = tables.find((tbl) => tbl.id === editingTable.id);
    // The Cache TTL as typed (staged apart from the table), carried only when it was changed.
    const ttlEdit = cacheTtlEdits[editingTable.id];
    const typedTtl =
      ttlEdit?.dirty !== true
        ? undefined
        : ttlEdit.value.trim() === ""
          ? null
          : parseInt(ttlEdit.value, 10);
    if (typedTtl !== undefined && typedTtl !== null && isNaN(typedTtl)) {
      setError(translate("tableEditForm.loadManagementFixErrors"));
      return;
    }
    const paging = declaredPaging(editingTable.pagination);
    setSaving(true);
    try {
      // REQ-2109 (settled): the table and every setting of it -- naming convention, role TTLs,
      // replicate, load protection, draft, region, Cache TTL and paging -- in one save, applied
      // in one transaction: a refusal of any one saves nothing. A caller who may change only how
      // columns are hidden (REQ-1944) sends the table's other settings as they are stored.
      const result = await updateTable({
        ...buildTableUpdateInput(editingTable),
        ...(hidingOnly
          ? {}
          : {
              region: editingTable.region,
              ...(typedTtl === undefined ? {} : { cacheTtl: typedTtl }),
              ...(pagingChanged(saved?.pagination ?? null, editingTable.pagination)
                ? { pagination: paging === null ? null : pagingInput(paging) }
                : {}),
            }),
      });
      if (!result.success) {
        setError(serverMessage(result, result.message));
        return;
      }
      if (typedTtl !== undefined) {
        setCacheTtlEdits((prev) => ({
          ...prev,
          [editingTable.id]: { ...prev[editingTable.id], dirty: false, saving: false },
        }));
      }
      // Await the reload before clearing the form. The refetches are otherwise fire-and-forget, so
      // the read view (and any editor re-opened from it) would render off the pre-save cache entry
      // and show the values as they were before this save until the refetch happens to land.
      await reload();
      setEditingTable(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  // Only full-page block on the INITIAL load (no cached tables yet). Background refetches
  // (refetchQueries after mutations, reload()) serve stale-then-fresh from Apollo's cache —
  // blocking the whole page on those hides cached rows unnecessarily and causes test timeouts
  // under concurrent load where backend schema rebuilds extend refetch latency to >10s.
  if (loading || (tablesLoading && tables.length === 0))
    return <PageLoading message={translate("tablesPage.loading")} />;

  // The right-hand panel: a create form while one is open, otherwise the selected table -- its
  // read view, or its edit form once editing has begun. Nothing selected: no panel, and the list
  // has the page.
  const closePanel = () => {
    setShowForm(false);
    setShowViewForm(false);
    setShowModeling(false);
    setExpanded(null);
    setPreviewTable(null);
    setEditingViewDef(null);
    cancelEditing();
  };
  const selectedTable =
    expanded === null ? null : (tables.find((tb) => tb.id === expanded) ?? null);
  // The table's form: its editor, or -- with no saved table -- the same sections as its create form.
  const editForm = (editing: RegisteredTable, saved: RegisteredTable | null) => (
    <ReplicaBuildsContext.Provider value={replicaBuilds}>
      <TableEditForm
        hidingOnly={hidingOnly}
        editingTable={editing}
        setEditingTable={setEditingTable}
        savedTable={saved}
        savedProfilerId={saved?.profilerSourceId ?? null}
        editingColumnTypes={editingColumnTypes}
        cacheTtlEdits={cacheTtlEdits}
        setCacheTtlEdits={setCacheTtlEdits}
        sources={sources}
        roles={roles}
        dataProducts={dataProducts}
        settings={settings}
        saving={saving}
        generateTableDescription={generateTableDescription}
        generateColumnDescription={generateColumnDescription}
        cancelEditing={cancelEditing}
        handleSaveEdit={saved === null ? handleCreate : handleSaveEdit}
        problem={error}
        // What the create step chose is lost on leaving, so a form creating a table is changed.
        dirty={
          saved === null ||
          JSON.stringify(editing) !== pristine ||
          cacheTtlEdits[editing.id]?.dirty === true
        }
        updateEditCol={updateEditCol}
        relationships={relationships}
        section={section}
        onSectionChange={setSection}
      />
    </ReplicaBuildsContext.Provider>
  );
  const panelBody =
    newRegistration !== null && editingTable !== null ? (
      editForm(editingTable, null)
    ) : editingViewDef ? (
      // REQ-1318: a view's definition is edited in the panel, with a way back to its detail: a
      // working surface is never a dialog over the page. A metric view opens in Metrics mode
      // prefilled; a free-hand view opens in SQL mode.
      <div className="split-pane__fill" data-testid="view-definition-modal">
        <div className="split-pane__bar">
          <Button
            variant="subtle"
            size="compact-sm"
            leftSection={<ArrowLeft size={14} />}
            onClick={() => setEditingViewDef(null)}
            data-testid="view-definition-back"
          >
            {translate("tablesPage.backToDetail")}
          </Button>
          <Text fw={600}>{translate("tablesPage.editViewDefinitionTitle")}</Text>
        </div>
        <ViewDefinitionForm
          editing={editingViewDef}
          tables={tables}
          relationships={relationships}
          domainHints={domainHints}
          updateTable={updateTable}
          onContinue={startCreating}
          onSuccess={() => {
            setEditingViewDef(null);
            reload();
          }}
          onCancel={() => setEditingViewDef(null)}
        />
      </div>
    ) : showForm && !viewsOnly ? (
      <RegisterTableForm
        sources={sources}
        domainHints={domainHints}
        domainAccess={domainAccess}
        checkedDomains={checkedDomains}
        domainsEnabled={domainsEnabled}
        tables={tables}
        roles={roles}
        getAvailableColumnsMetadata={getAvailableColumnsMetadata}
        suggestTableAlias={suggestTableAlias}
        onContinue={startCreating}
        onCancel={() => setShowForm(false)}
        setError={setError}
        problem={error}
      />
    ) : showViewForm && viewsOnly ? (
      <ViewDefinitionForm
        editing={null}
        tables={tables}
        relationships={relationships}
        domainHints={domainHints}
        updateTable={updateTable}
        onContinue={startCreating}
        onSuccess={() => setShowViewForm(false)}
        onCancel={() => setShowViewForm(false)}
      />
    ) : showModeling ? (
      <ModelingForm
        domains={domains}
        onSuccess={() => {
          setShowModeling(false);
          reload();
        }}
        onCancel={() => setShowModeling(false)}
      />
    ) : selectedTable ? (
      ((t: RegisteredTable) => {
        const readView = (extra: { actionsOnly?: boolean }) => (
          <TableReadView
            t={t}
            build={buildOf(replicaBuilds?.builds, t)}
            hidingOnly={hidingOnly}
            dataProducts={dataProducts}
            navigate={navigate}
            viewsOnly={viewsOnly}
            onEditDefinition={viewsOnly ? setEditingViewDef : undefined}
            deploying={deploying}
            setDeploying={setDeploying}
            deployMsg={deployMsg}
            setDeployMsg={setDeployMsg}
            tableProfiles={tableProfiles}
            deployViewToDb={deployViewToDb}
            reload={reload}
            startEditing={startEditing}
            handleDelete={handleDelete}
            handleProfile={handleProfile}
            onPreview={setPreviewTable}
            onSetDraft={handleSetDraft}
            source={sources.find((s) => s.id === t.sourceId)}
            relationships={relationships}
            onInvalidateCache={!hidingOnly && hasCacheable(t) ? handlePurgeTableCache : undefined}
            invalidating={purging[t.id] === true}
            section={section}
            onSectionChange={setSection}
            columnTab={linkedColumnTab}
            profileColumnsTable={
              // REQ-1934: this table's profile, if its columns table is registered.
              tables.find(
                (tb) =>
                  tb.tableName === `${t.tableName}_profile_columns` &&
                  sources.find((s) => s.id === tb.sourceId)?.type === "data_profiler",
              ) ?? null
            }
            {...extra}
          />
        );
        // Preview is a view of the table inside the panel, with a way back to its detail: a
        // working surface is never a dialog over the page.
        if (previewTable?.id === t.id) {
          return (
            <div className="split-pane__fill" data-testid="table-preview-modal">
              {/* The table's actions stay in the panel's head while its rows are looked at. */}
              {readView({ actionsOnly: true })}
              <TablePreviewPane table={t} role={actingRole} onBack={() => setPreviewTable(null)} />
            </div>
          );
        }
        if (editingTable?.id !== t.id) {
          return (
            <ReplicaBuildsContext.Provider value={replicaBuilds}>
              {readView({})}
            </ReplicaBuildsContext.Provider>
          );
        }
        return (
          <>
            {/* While it is edited the panel's head holds the form's Save and Cancel in place
                of the table's actions: none of those can be used from inside a form. */}
            {editForm(editingTable, t)}
          </>
        );
      })(selectedTable)
    ) : null;
  // A form whose Save reports through the page's error: it shows that with its own actions.
  const panelFormOpen = (showForm && !viewsOnly) || editingTable !== null;
  const panelIsReadOnly =
    !showForm &&
    !showViewForm &&
    !showModeling &&
    editingTable === null &&
    previewTable === null &&
    editingViewDef === null;
  const panelTitle =
    newRegistration !== null
      ? translate(viewsOnly ? "tablesPage.panelNewView" : "tablesPage.panelRegisterTable")
      : showForm && !viewsOnly
        ? translate("tablesPage.panelRegisterTable")
        : showViewForm && viewsOnly
          ? translate("tablesPage.panelNewView")
          : showModeling
            ? translate("tablesPage.panelNewModel")
            : selectedTable
              ? (selectedTable.alias ?? selectedTable.tableName)
              : "";

  return (
    <div className="page page-sticky-head">
      {/* Anchor the tour waits on before highlighting a step whose element lives in the shell —
          tables-add is absent in viewsOnly, so the header is the one marker both modes render. */}
      <div className="page-header" data-tour="tables-header">
        <Title order={2}>
          {viewsOnly ? translate("tablesPage.titleViews") : translate("tablesPage.titleTables")}
        </Title>
        <FilterInput
          value={tableSearch}
          onChange={setTableSearch}
          placeholder={
            viewsOnly
              ? translate("tablesPage.filterPlaceholderViews")
              : translate("tablesPage.filterPlaceholderTables")
          }
        />
        <RegionSelector
          regions={regions}
          connected={connected}
          value={regionSel}
          onChange={setRegionSel}
          hidden={regionHidden}
          error={regionError}
        />
        <div className="page-actions">
          {!viewsOnly && !hidingOnly && (
            <Button
              data-tour="tables-add"
              data-testid="tables-add-toggle"
              variant={showForm ? "outline" : "filled"}
              onClick={() => leave.guard(() => setShowForm(!showForm))}
              aria-label={showForm ? translate("tablesPage.closeForm") : undefined}
            >
              {showForm ? <X size={14} /> : translate("tablesPage.addTable")}
            </Button>
          )}
          {!hidingOnly && !(viewsOnly && showViewForm) && (
            <Tip text={translate("tablesPage.addViewTitle")}>
              <Button
                variant="default"
                data-testid="views-add-toggle"
                // REQ-1318: on the Views page, Add View opens the definition-mode form
                // (SQL | Metrics toggle); elsewhere it keeps routing to the SQL editor.
                onClick={() =>
                  leave.guard(() => (viewsOnly ? setShowViewForm(true) : navigate("/sql")))
                }
              >
                {translate("tablesPage.addView")}
              </Button>
            </Tip>
          )}
          {!viewsOnly && !hidingOnly && (
            <Tip text={translate("tablesPage.modelTitle")}>
              <Button
                variant="default"
                data-testid="tables-model-toggle"
                onClick={() => leave.guard(() => setShowModeling(true))}
              >
                {translate("tablesPage.model")}
              </Button>
            </Tip>
          )}
          {!viewsOnly && (
            // An ancillary action, reached rarely: a borderless icon keeps it out of the
            // decision the bordered buttons beside it present.
            <Tip text={translate("tablesPage.reportXlsxTitle")}>
              <ActionIcon
                variant="subtle"
                size="lg"
                data-testid="tables-report-xlsx"
                loading={reportBusy}
                // REQ-1592: the whole model as one reviewable workbook, narrowed by the same domain
                // filter the table below is narrowed by. Download only — never an edit path back in.
                onClick={downloadReport}
                aria-label={translate("tablesPage.reportXlsx")}
              >
                <FileSpreadsheet size={18} />
              </ActionIcon>
            </Tip>
          )}
          <HelpBubble
            title={translate("tablesPage.purposeTitle")}
            paragraphs={[
              translate("tablesPage.purposeBody"),
              translate("tablesPage.purposeAdd"),
              translate("tablesPage.purposeModel"),
            ]}
            ariaLabel={translate("tablesPage.purposeAria")}
            testId="tables-purpose-help"
          />
          {/* The icon is the opaque thing here, so it is its own hover target: a "?" next to
              it would add one more glyph to the row a reader is already trying to decode. */}
          <HelpBubble
            title={translate("tablesPage.erdHelpTitle")}
            paragraphs={[translate("tablesPage.erdHelpBody")]}
            ariaLabel={translate("tablesPage.viewErd")}
            testId="tables-erd-help"
            target={
              <ActionIcon
                data-tour="tables-erd"
                variant="subtle"
                aria-label={translate("tablesPage.viewErd")}
                onClick={() => setShowErd(true)}
              >
                <Network size={14} />
              </ActionIcon>
            }
          />
        </div>
      </div>

      {/* A form open in the panel says its own problem under the panel's head. */}
      {error && !panelFormOpen && (
        <Alert color="red" mb="md" data-testid="tables-error">
          {error}
        </Alert>
      )}
      {regenQueued && (
        <Alert
          color="blue"
          mb="md"
          withCloseButton
          onClose={() => setRegenQueued(null)}
          data-testid="tables-regen-queued"
        >
          {regenQueued}
        </Alert>
      )}

      {/* REQ-1293: a failed `tables` query previously rendered as an empty grid, which reads as
          "this org has no registered tables" — a statement about the org's data that the failed
          request never established. Report the failure instead of impersonating an answer. */}
      {tablesError && (
        <Alert color="red" mb="md" data-testid="tables-load-error">
          {translate("tablesPage.loadFailed", { message: tablesError.message })}
        </Alert>
      )}

      <SplitPane
        testId="tables-split"
        storageKey="provisa.tables.detailWidth"
        title={panelTitle}
        onClose={closePanel}
        leave={leave}
        // Discard: the open edit or create form is dropped before the reader goes where they
        // were going.
        onDiscard={() => {
          cancelEditing();
          setShowForm(false);
          setShowViewForm(false);
          setShowModeling(false);
          setEditingViewDef(null);
        }}
        // Escape closes a table that is only being looked at; it never drops what is being typed.
        closeOnEscape={panelIsReadOnly}
        labels={{
          panel: translate("tablesPage.detailPanel"),
          resize: translate("tablesPage.resizeDetail"),
          expand: translate("tablesPage.expandDetail"),
          restore: translate("tablesPage.restoreDetail"),
          close: translate("tablesPage.closeDetail"),
          backToList: translate("tablesPage.backToList"),
        }}
        list={
          <>
            <ListTable testId="tables-list">
              <ListHead
                sortGroup={sortGroup}
                columns={[
                  { col: "source" },
                  ...(domainsEnabled ? [{ col: "domain" }] : []),
                  { col: "table" },
                  translate("tablesPage.colNaming"),
                  translate("tablesPage.colCacheTtl"),
                  translate("tablesPage.colEffectiveTtl"),
                  ...(hasRegions ? [translate("regionSelector.columnHeader")] : []),
                  { col: "cols" },
                  "",
                ]}
              />
              <Table.Tbody>
                {(() => {
                  const items =
                    groupBy.length === 0
                      ? sortGroup.items.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
                      : sortGroup.items;

                  if (items.length === 0) {
                    return (
                      <ListEmpty
                        colSpan={(domainsEnabled ? 12 : 11) + (hasRegions ? 1 : 0)}
                        testId="tables-empty"
                      >
                        {translate("tablesPage.empty")}
                      </ListEmpty>
                    );
                  }

                  return items.map((item) => {
                    if (item.type === "header") {
                      return (
                        <ListGroupRow
                          key={`grp-${item.key}`}
                          colSpan={(domainsEnabled ? 9 : 8) + (hasRegions ? 1 : 0)}
                          level={item.level}
                          label={item.label}
                          count={item.count}
                          collapsed={sortGroup.collapsed.has(item.key)}
                          onToggle={() => sortGroup.toggleCollapsed(item.key)}
                        />
                      );
                    }
                    const t = item.row;
                    const row = (
                      <Fragment key={t.id}>
                        <ListRow
                          // A stable per-table anchor (source.table) for tour steps that open one row.
                          data-table-row={`${t.sourceId}.${t.tableName}`}
                          // The row whose detail the right-hand panel is showing.
                          selected={expanded === t.id}
                          aria-selected={expanded === t.id}
                          onClick={() => {
                            setExpanded(expanded === t.id ? null : t.id);
                            // Another table, or none: the panel shows that table's detail.
                            setPreviewTable(null);
                            if (expanded === t.id) cancelEditing();
                          }}
                        >
                          <Table.Td>
                            {t.sourceId === DERIVED_SOURCE_ID ? (
                              // A derived relation has no external source — its provenance is the
                              // lineage of its definition, so never print the storage sentinel.
                              <Badge
                                size="xs"
                                variant="light"
                                color="gray"
                                data-testid={`tables-derived-${t.tableName}`}
                              >
                                {translate("tablesPage.derived")}
                              </Badge>
                            ) : (
                              t.sourceId
                            )}
                          </Table.Td>
                          {domainsEnabled && (
                            <Table.Td>{t.domainId ? domainToSqlName(t.domainId) : ""}</Table.Td>
                          )}
                          <Table.Td
                            style={{ fontFamily: "monospace", fontSize: "0.9rem" }}
                            title={t.description || undefined}
                          >
                            <Group gap="0.35rem">
                              {t.alias || t.tableName}
                              <ReplicatingMark build={buildOf(replicaBuilds?.builds, t)} />
                              {/* REQ-1320: star-schema role is metadata on the registration itself —
                                the badge derives live from it, so it can never drift from the def. */}
                              {t.modelingRole ? (
                                <Badge
                                  size="xs"
                                  variant="light"
                                  color={t.modelingRole === "fact" ? "grape" : "teal"}
                                  data-testid={`tables-modeling-role-${t.tableName}`}
                                >
                                  {translate(`tablesPage.modelingRole.${t.modelingRole}`)}
                                </Badge>
                              ) : (
                                <>
                                  {/* REQ-1361: no explicit star-schema role — Enable Aggregates / Enable
                                    Group By directly imply fact/dimension usage, so badge from those. */}
                                  {t.enableAggregates && (
                                    <Badge
                                      size="xs"
                                      variant="outline"
                                      color="grape"
                                      title={translate("tablesPage.modelingRole.impliedFactTitle")}
                                      data-testid={`tables-implied-fact-${t.tableName}`}
                                    >
                                      {translate("tablesPage.modelingRole.fact")}
                                    </Badge>
                                  )}
                                  {t.enableGroupBy && (
                                    <Badge
                                      size="xs"
                                      variant="outline"
                                      color="teal"
                                      title={translate(
                                        "tablesPage.modelingRole.impliedDimensionTitle",
                                      )}
                                      data-testid={`tables-implied-dimension-${t.tableName}`}
                                    >
                                      {translate("tablesPage.modelingRole.dimension")}
                                    </Badge>
                                  )}
                                </>
                              )}
                              {/* REQ-2109: a table or view that keeps its versions. */}
                              {t.businessKey.length > 0 && (
                                <Badge
                                  size="xs"
                                  variant="light"
                                  color="orange"
                                  title={translate("tablesPage.history.title")}
                                  data-testid={`tables-history-${t.tableName}`}
                                >
                                  {translate("tablesPage.history.badge")}
                                </Badge>
                              )}
                              {/* REQ-1443: a checker table names the checker its contract runs under —
                                the source's type is the one authority for that, never a table field. */}
                              {(() => {
                                const checker = sources.find((s) => s.id === t.sourceId)?.type;
                                return checker === "soda" || checker === "great_expectations" ? (
                                  <Badge
                                    size="xs"
                                    variant="light"
                                    color="cyan"
                                    title={translate("tablesPage.checker.title")}
                                    data-testid={`tables-checker-${t.tableName}`}
                                  >
                                    {translate(`tablesPage.checker.${checker}`)}
                                  </Badge>
                                ) : null;
                              })()}
                              {t.productId != null && (
                                <Badge
                                  size="xs"
                                  variant="light"
                                  color="indigo"
                                  title={translate("tablesPage.dataProductTitle")}
                                  data-testid={`tables-data-product-${t.tableName}`}
                                >
                                  {dataProducts.find((p) => p.id === t.productId)?.name ??
                                    t.productId}
                                </Badge>
                              )}
                              <TagControl objectType="table" tableId={t.id} />
                            </Group>
                          </Table.Td>
                          <Table.Td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                            {t.gqlNamingConvention
                              ? namingConventionLabel(translate, t.gqlNamingConvention)
                              : translate("tablesPage.inheritSource")}
                          </Table.Td>
                          <Table.Td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                            {t.cacheTtl != null
                              ? ttlText(t.cacheTtl, translate)
                              : translate("tablesPage.inherit")}
                          </Table.Td>
                          <Table.Td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                            {getEffectiveTableTtl(t)}
                          </Table.Td>
                          {hasRegions && (
                            <Table.Td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                              {t.region ?? translate("regionSelector.noRegion")}
                            </Table.Td>
                          )}
                          <Table.Td>{t.columns.length}</Table.Td>
                          <Table.Td onClick={(e) => e.stopPropagation()}>
                            <Group gap="xs" wrap="nowrap">
                              {!hidingOnly &&
                                (() => {
                                  // REQ-968: only a table whose rows are LANDED can be rebuilt on demand.
                                  // The server derives that from the same policy resolution (REQ-1143) this
                                  // reads, so the button appears exactly where the mutation would accept it.
                                  const serving = t.refreshPolicySummary?.serving;
                                  const isLanded = serving === "scheduled" || serving === "frozen";
                                  return (
                                    <>
                                      {isLanded && (
                                        <Tip text={translate("tablesPage.runNowTitle")}>
                                          <Button
                                            size="compact-xs"
                                            variant="default"
                                            onClick={() => {
                                              setRegenReason("");
                                              setRegenTable(t);
                                            }}
                                            data-testid={`tables-run-now-${t.tableName}`}
                                          >
                                            {translate("tablesPage.runNow")}
                                          </Button>
                                        </Tip>
                                      )}
                                    </>
                                  );
                                })()}
                            </Group>
                          </Table.Td>
                        </ListRow>
                      </Fragment>
                    );
                    return row;
                  });
                })()}
              </Table.Tbody>
            </ListTable>

            {(() => {
              const filtered = regionTables;
              if (groupBy.length > 0) return null;
              const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
              if (totalPages === 1) return null;
              return (
                <Group gap="sm" justify="flex-end" py="sm">
                  <ActionIcon
                    variant="default"
                    aria-label={translate("tablesPage.firstPage")}
                    onClick={() => setPage(0)}
                    disabled={page === 0}
                  >
                    «
                  </ActionIcon>
                  <ActionIcon
                    variant="default"
                    aria-label={translate("tablesPage.prevPage")}
                    onClick={() => setPage((p) => p - 1)}
                    disabled={page === 0}
                  >
                    ‹
                  </ActionIcon>
                  <Text fz="sm">
                    {translate("tablesPage.pageOf", { page: page + 1, totalPages })}
                  </Text>
                  <ActionIcon
                    variant="default"
                    aria-label={translate("tablesPage.nextPage")}
                    onClick={() => setPage((p) => p + 1)}
                    disabled={page >= totalPages - 1}
                  >
                    ›
                  </ActionIcon>
                  <ActionIcon
                    variant="default"
                    aria-label={translate("tablesPage.lastPage")}
                    onClick={() => setPage(totalPages - 1)}
                    disabled={page >= totalPages - 1}
                  >
                    »
                  </ActionIcon>
                </Group>
              );
            })()}
          </>
        }
      >
        {panelBody}
      </SplitPane>
      <Modal
        opened={regenTable !== null}
        onClose={() => setRegenTable(null)}
        title={translate("tablesPage.runNowModalTitle", {
          table: regenTable ? `${regenTable.schemaName}.${regenTable.tableName}` : "",
        })}
        data-testid="run-now-modal"
      >
        <FormLayout>
          <FormField size="long" help={translate("tablesPage.runNowReasonHelp")}>
            <TextInput
              label={translate("tablesPage.runNowReasonLabel")}
              placeholder={translate("tablesPage.runNowReasonPlaceholder")}
              value={regenReason}
              onChange={(e) => setRegenReason(e.currentTarget.value)}
              data-testid="run-now-reason"
            />
          </FormField>
          {/* Queuing the run is not a save: the button keeps its word, first; Cancel follows. */}
          <FormFooter
            onCancel={() => setRegenTable(null)}
            action={
              <Button
                onClick={handleForceRegen}
                disabled={regenReason.trim() === "" || regenning}
                data-testid="run-now-submit"
              >
                {regenning
                  ? translate("tablesPage.runNowQueuing")
                  : translate("tablesPage.runNowSubmit")}
              </Button>
            }
          />
        </FormLayout>
      </Modal>
      {/* REQ-1484: advisory — the estate's dependents are listed, the administrator decides. */}
      <Modal
        opened={pendingDependents !== null}
        onClose={() => setPendingDependents(null)}
        title={translate("tablesPage.dependentsTitle")}
        size="lg"
        data-testid="column-dependents-modal"
      >
        <Text size="sm" mb="md">
          {translate("tablesPage.dependentsIntro")}
        </Text>
        {(pendingDependents ?? []).map((entry) => (
          <div key={entry.columnName} style={{ marginBottom: "1rem" }}>
            <Text fw={600} style={{ fontFamily: "monospace" }}>
              {entry.columnName}
            </Text>
            <Table>
              <Table.Tbody>
                {entry.dependents.map((d) => (
                  <Table.Tr key={`${d.kind}:${d.name}:${d.detail}`}>
                    <Table.Td>
                      <Badge variant="light">{d.kind}</Badge>
                    </Table.Td>
                    <Table.Td style={{ fontFamily: "monospace" }}>{d.name}</Table.Td>
                    <Table.Td>{d.detail}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </div>
        ))}
        {/* The action first, at the start of the row; the way back to the form follows. */}
        <Group justify="flex-start" mt="md">
          <Button
            color="red"
            data-testid="column-dependents-confirm"
            onClick={() => {
              setPendingDependents(null);
              void performSaveEdit();
            }}
          >
            {translate("tablesPage.dependentsConfirm")}
          </Button>
          <Button
            variant="default"
            onClick={() => setPendingDependents(null)}
            data-testid="column-dependents-back"
          >
            {translate("tablesPage.dependentsCancel")}
          </Button>
        </Group>
      </Modal>
      {showErd && (
        <ErdModal
          tables={tables}
          relationships={relationships}
          domains={domains}
          checkedDomains={
            checkedDomains.size > 0 && checkedDomains.size < domains.length ? checkedDomains : null
          }
          onClose={() => setShowErd(false)}
        />
      )}
      <NativeParamsModal
        key={profileParamsTable?.id ?? "none"}
        table={profileParamsTable}
        onClose={() => setProfileParamsTable(null)}
        onSubmit={(values) => {
          const tbl = profileParamsTable;
          setProfileParamsTable(null);
          if (tbl) void handleProfile(tbl.id, values);
        }}
      />
      {refusal.dialog}
    </div>
  );
}
