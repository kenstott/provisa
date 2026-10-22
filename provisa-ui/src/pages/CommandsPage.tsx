// Copyright (c) 2026 Kenneth Stott
// Canary: b12f5409-a0a1-4db6-b8de-245a25bd1768
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { FunctionRead, WebhookRead } from "./commands/CommandRead";
import React, { useState, useEffect, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Card,
  Group,
  Pagination,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";
import { Trash2, Pencil, Plus } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import {
  fetchActions,
  saveFunction,
  saveWebhook,
  deleteFunction,
  deleteWebhook,
  testAction,
} from "../api/actions";
import type { TrackedFunction, TrackedWebhook } from "../api/actions";
import {
  useSources,
  useTables,
  useDomains,
  useAvailableFunctionsLazy,
  useDataProducts,
} from "../hooks/useAdminQueries";
import type { TableMetadata } from "../api/admin";
import { fetchOrgRoles } from "../api/admin";
import type { Role } from "../types/auth";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { PanelActions, SplitPane } from "../components/list/SplitPane";
import { PanelWorking } from "./sources/PanelWorking";
import { TagControl } from "../components/TagControl";
import type { ActionType, FormState } from "./commands/types";
import {
  EMPTY_FORM,
  OPERATION_SCHEMA,
  deriveReturnSchema,
  columnsFromReturnSchema,
} from "./commands/types";
import { CommandFormFields } from "./commands/CommandFormFields";
import { PageLoading } from "../components/PageLoading";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, pageItems, type ListColumn } from "../components/list/useListSortGroup";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../components/form/FormLayout";
import { matchesQuery } from "../lib/searchItems";

export function CommandsPage() {
  const { t } = useTranslation();
  const { sources } = useSources();
  const { tables } = useTables();
  const { domains } = useDomains();
  const { dataProducts } = useDataProducts(); // REQ-1634: command <-> data product picker
  const { checkedDomains } = useDomainFilter();
  const getAvailableFunctions = useAvailableFunctionsLazy();
  const { activeOrgId } = useAuth();
  const orgId = activeOrgId ?? "root";
  const domainHints = domains.map((d) => d.id);
  const [functions, setFunctions] = useState<TrackedFunction[]>([]);
  const [webhooks, setWebhooks] = useState<TrackedWebhook[]>([]);
  const [roles, setRoles] = useState<Role[]>([]);
  const [testRoleId, setTestRoleId] = useState<string>("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [msg, setMsg] = useState("");
  const [cmdSearch, setCmdSearch] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<FormState>({ ...EMPTY_FORM });
  const [editingName, setEditingName] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{ name: string; data: unknown } | null>(null);
  const [testing, setTesting] = useState<string | null>(null);
  // `?command=<name>` opens that command's detail -- a lineage edge links to the command it names.
  const [searchParams] = useSearchParams();
  const [expandedFn, setExpandedFn] = useState<string | null>(() => searchParams.get("command"));
  const [expandedWh, setExpandedWh] = useState<string | null>(null);
  const [fnPage, setFnPage] = useState(1);
  const [whPage, setWhPage] = useState(1);
  const PAGE_SIZE = 50;
  const [availableFunctions, setAvailableFunctions] = useState<TableMetadata[]>([]);
  const [loadingFunctions, setLoadingFunctions] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const actions = await fetchActions();
      setFunctions(actions.functions);
      setWebhooks(actions.webhooks);
      return actions;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  /* eslint-disable react-hooks/set-state-in-effect -- mount data-fetch: load() sets loading state synchronously by design */
  useEffect(() => {
    load();
  }, [load]);
  /* eslint-enable react-hooks/set-state-in-effect */

  useEffect(() => {
    fetchOrgRoles(orgId)
      .then(setRoles)
      .catch(() => {});
  }, [orgId]);

  useEffect(() => {
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       resets the available-functions list synchronously when the selected source changes, before refetching */
    setAvailableFunctions([]);
    const src = sources.find((s) => s.id === form.sourceId);
    if (!src) return;
    // A remote source offers its write operations; a database source offers the routines of
    // the schema named (REQ-887). Either list only proposes: nothing is registered until saved.
    const schema = OPERATION_SCHEMA[src.type] ?? form.schemaName;
    if (!schema) return;
    setLoadingFunctions(true);
    getAvailableFunctions(form.sourceId, schema)
      .then(setAvailableFunctions)
      .catch(() => setAvailableFunctions([]))
      .finally(() => setLoadingFunctions(false));
  }, [form.sourceId, form.schemaName, sources, getAvailableFunctions]);

  // `held`: the lists as just read, where the page's own have not caught up yet (a command
  // opened in edit straight after it was created).
  const handleEdit = (
    actionType: ActionType,
    name: string,
    held: { functions: TrackedFunction[]; webhooks: TrackedWebhook[] } = { functions, webhooks },
  ) => {
    if (actionType === "function") {
      const fn = held.functions.find((f) => f.name === name);
      if (!fn) return;
      // REQ-1159: output is a dataset. A registered table → "table" mode; anything else is an
      // IR-typed column list. Legacy commands carry only return_schema → reverse-project it.
      const outputColumns = fn.outputColumns ?? columnsFromReturnSchema(fn.returnSchema);
      setForm({
        actionType: "function",
        name: fn.name,
        sourceId: fn.sourceId,
        schemaName: fn.schemaName,
        functionName: fn.functionName,
        returns: fn.returns,
        visibleTo: fn.visibleTo.join(", "),
        domainId: fn.domainId,
        description: fn.description ?? "",
        arguments: fn.arguments.length > 0 ? fn.arguments : [],
        url: "",
        method: "POST",
        timeoutMs: 5000,
        inlineReturnType: [],
        kind: fn.kind ?? "mutation",
        returnSchemaMode: fn.returns ? "table" : "dataset",
        implKind: fn.implKind ?? "source_procedure",
        binding: fn.binding ?? {},
        materialize: fn.materialize ?? false,
        requiresApproval: fn.requiresApproval ?? false, // REQ-1924
        writesTable: fn.writesTable ?? "", // REQ-1924, REQ-871
        outputColumns, // REQ-1159
        productId: fn.productId ?? "", // REQ-1634
      });
      setExpandedFn(name);
    } else {
      const wh = held.webhooks.find((w) => w.name === name);
      if (!wh) return;
      setForm({
        actionType: "webhook",
        name: wh.name,
        sourceId: "",
        schemaName: "public",
        functionName: "",
        returns: wh.returns ?? "",
        visibleTo: wh.visibleTo.join(", "),
        domainId: wh.domainId,
        description: wh.description ?? "",
        arguments: wh.arguments.length > 0 ? wh.arguments : [],
        url: wh.url,
        method: wh.method,
        timeoutMs: wh.timeoutMs,
        inlineReturnType: wh.inlineReturnType.length > 0 ? wh.inlineReturnType : [],
        kind: wh.kind ?? "mutation",
        returnSchemaMode: "table",
        implKind: "source_procedure",
        binding: {},
        materialize: false,
        requiresApproval: wh.requiresApproval, // REQ-209
        writesTable: "",
        outputColumns: [],
        productId: "",
      });
      setExpandedWh(name);
    }
    setEditingName(name);
    setShowForm(false);
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setError("");
    setMsg("");
    try {
      const visibleTo = form.visibleTo
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (form.actionType === "function") {
        // REQ-1159: output is a dataset. In "dataset" mode the IR-typed columns are the sole
        // contract; return_schema (the GraphQL projection) is DERIVED, never authored.
        const outputColumns = form.outputColumns.filter((c) => c.name.trim() !== "");
        const isDataset = form.returnSchemaMode === "dataset";
        // REQ-2001: a statement command's statement declares its kind, its written table and its
        // output; the server derives them, and one sent beside it would have to agree.
        const isStatement = form.implKind === "statement";
        await saveFunction(
          {
            name: form.name,
            sourceId: form.sourceId,
            schemaName: form.schemaName,
            functionName: form.functionName,
            returns: isDataset ? "" : form.returns,
            arguments: form.arguments,
            visibleTo,
            domainId: form.domainId,
            description: form.description || undefined,
            kind: isStatement ? undefined : form.kind,
            returnSchema: isDataset && !isStatement ? deriveReturnSchema(outputColumns) : null,
            outputColumns: isStatement ? null : outputColumns, // REQ-1159
            implKind: form.implKind,
            binding: form.binding,
            materialize: form.materialize,
            requiresApproval: form.requiresApproval, // REQ-1924
            writesTable: isStatement ? undefined : form.writesTable || null, // REQ-1924, REQ-871
            productId: form.productId || null, // REQ-1634
          },
          editingName, // the open command is updated; a new one is created
        );
      } else {
        await saveWebhook(
          {
            name: form.name,
            url: form.url,
            method: form.method,
            timeoutMs: form.timeoutMs,
            returns: form.returns || undefined,
            inlineReturnType: form.inlineReturnType,
            arguments: form.arguments,
            visibleTo,
            domainId: form.domainId,
            description: form.description || undefined,
            kind: form.kind,
            requiresApproval: form.requiresApproval, // REQ-209
          },
          editingName,
        );
      }
      setMsg(t("commandsPage.savedMessage", { type: form.actionType, name: form.name }));
      setShowForm(false);
      setForm({ ...EMPTY_FORM });
      // A command the short step just made is selected and its detail opens in edit, with
      // everything the step did not ask for at its default.
      const created = editingName ? null : { type: form.actionType, name: form.name };
      setEditingName(null);
      const held = await load();
      if (created && held) {
        setExpandedFn(created.type === "function" ? created.name : null);
        setExpandedWh(created.type === "webhook" ? created.name : null);
        handleEdit(created.type, created.name, held);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async (actionType: ActionType, name: string) => {
    setTesting(name);
    setTestResult(null);
    setError("");
    try {
      const result = await testAction(actionType, name, testRoleId);
      setTestResult({ name, data: result });
    } catch (e) {
      setError(`Test failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setTesting(null);
    }
  };

  const handleCancel = () => {
    setShowForm(false);
    setForm({ ...EMPTY_FORM });
    setEditingName(null);
  };

  const formFieldsProps = {
    form,
    setForm,
    sources,
    tables,
    domainHints,
    availableFunctions,
    loadingFunctions,
    dataProducts,
    roleOptions: roles.map((r) => ({ id: r.id, label: r.id })),
  };

  // Hide rows whose domain is unchecked in the NavBar domain filter (an empty set = show all),
  // matching the TablesPage/SqlPage convention. Rows with no domain are always shown.
  const inCheckedDomain = (domainId: string | null | undefined) =>
    !domainId || checkedDomains.size === 0 || checkedDomains.has(domainId);

  const filteredFunctions = functions.filter(
    (fn) => inCheckedDomain(fn.domainId) && matchesQuery([fn.name], cmdSearch),
  );
  const fnTotalPages = Math.max(1, Math.ceil(filteredFunctions.length / PAGE_SIZE));

  const filteredWebhooks = webhooks.filter(
    (wh) => inCheckedDomain(wh.domainId) && matchesQuery([wh.name], cmdSearch),
  );
  const whTotalPages = Math.max(1, Math.ceil(filteredWebhooks.length / PAGE_SIZE));

  // REQ-1940: sort and group are the shared list mechanism.
  const fnColumns: ListColumn<TrackedFunction>[] = [
    { key: "name", label: t("commandsPage.colName"), sortValue: (f) => f.name },
    {
      key: "source",
      label: t("commandsPage.colSource"),
      sortValue: (f) => f.sourceId,
      groupValue: (f) => f.sourceId,
    },
    {
      key: "domain",
      label: t("commandsPage.colDomain"),
      sortValue: (f) => f.domainId || t("commandsPage.dash"),
      groupValue: (f) => f.domainId || t("commandsPage.dash"),
    },
    {
      key: "function",
      label: t("commandsPage.colFunction"),
      sortValue: (f) => `${f.schemaName}.${f.functionName}`,
    },
    {
      key: "returns",
      label: t("commandsPage.colReturns"),
      sortValue: (f) =>
        f.returns || (f.returnSchema ? t("commandsPage.customSchema") : t("commandsPage.dash")),
      groupValue: (f) =>
        f.returns || (f.returnSchema ? t("commandsPage.customSchema") : t("commandsPage.dash")),
    },
    { key: "args", label: t("commandsPage.colArgs"), sortValue: (f) => f.arguments.length },
    {
      key: "visibleTo",
      label: t("commandsPage.colVisibleTo"),
      sortValue: (f) => f.visibleTo.join(", ") || t("commandsPage.all"),
    },
  ];
  const whColumns: ListColumn<TrackedWebhook>[] = [
    { key: "name", label: t("commandsPage.colName"), sortValue: (w) => w.name },
    {
      key: "status",
      label: t("commandsPage.colStatus"),
      sortValue: (w) =>
        w.approved === false ? t("commandsPage.statusPending") : t("commandsPage.statusApproved"),
      groupValue: (w) =>
        w.approved === false ? t("commandsPage.statusPending") : t("commandsPage.statusApproved"),
    },
    {
      key: "domain",
      label: t("commandsPage.colDomain"),
      sortValue: (w) => w.domainId || t("commandsPage.dash"),
      groupValue: (w) => w.domainId || t("commandsPage.dash"),
    },
    { key: "url", label: t("commandsPage.colUrl"), sortValue: (w) => w.url },
    {
      key: "method",
      label: t("commandsPage.colMethod"),
      sortValue: (w) => w.method,
      groupValue: (w) => w.method,
    },
    { key: "timeout", label: t("commandsPage.colTimeout"), sortValue: (w) => w.timeoutMs },
    {
      key: "returns",
      label: t("commandsPage.colReturns"),
      sortValue: (w) =>
        w.returns || t("commandsPage.inlineReturns", { count: w.inlineReturnType.length }),
      groupValue: (w) =>
        w.returns || t("commandsPage.inlineReturns", { count: w.inlineReturnType.length }),
    },
    { key: "args", label: t("commandsPage.colArgs"), sortValue: (w) => w.arguments.length },
    {
      key: "visibleTo",
      label: t("commandsPage.colVisibleTo"),
      sortValue: (w) => w.visibleTo.join(", ") || t("commandsPage.all"),
    },
  ];
  const fnSortGroup = useListSortGroup(filteredFunctions, fnColumns, "commands-fn");
  const whSortGroup = useListSortGroup(filteredWebhooks, whColumns, "commands-wh");

  // What the right-hand panel shows: the result of a test run (with a way back to the command),
  // the short step that creates a command, or the selected command -- being edited, or read.
  const selectedFn = expandedFn ? (functions.find((f) => f.name === expandedFn) ?? null) : null;
  const selectedWh = expandedWh ? (webhooks.find((w) => w.name === expandedWh) ?? null) : null;
  // Whom a test run is made as: chosen where the run is started, in the command's own panel.
  const testAs = (
    <Group gap="sm" align="center" mt="sm">
      <Text size="sm" c="dimmed">
        {t("commandsPage.testAsRole")}
      </Text>
      <FormLayout>
        <FormField size="medium">
          <Select
            aria-label={t("commandsPage.testAsRole")}
            size="xs"
            data={roles.map((r) => ({ value: r.id, label: r.id }))}
            value={testRoleId || null}
            onChange={(v) => setTestRoleId(v ?? "")}
            allowDeselect={false}
          />
        </FormField>
      </FormLayout>
    </Group>
  );
  const panelBody: React.ReactNode = testResult ? (
    <PanelWorking
      back={t("commandsPage.backToCommand")}
      onBack={() => setTestResult(null)}
      testId="commands-test-back"
    >
      <Title order={5} data-testid="commands-test-result">
        {t("commandsPage.testResultHeading", { name: testResult.name })}
      </Title>
      <>
        {!!(
          testResult.data &&
          typeof testResult.data === "object" &&
          "enforcement" in testResult.data
        ) && (
          <Alert color="blue" mb="sm" title={t("commandsPage.governanceApplied")}>
            <pre style={{ margin: "0.25rem 0 0", fontSize: "0.78rem" }}>
              {JSON.stringify((testResult.data as Record<string, unknown>).enforcement, null, 2)}
            </pre>
          </Alert>
        )}
        <pre style={{ fontSize: "0.85rem", overflow: "auto", maxHeight: "60vh" }}>
          {JSON.stringify(
            testResult.data && typeof testResult.data === "object" && "rows" in testResult.data
              ? (testResult.data as Record<string, unknown>).rows
              : testResult.data,
            null,
            2,
          )}
        </pre>
      </>
    </PanelWorking>
  ) : showForm && !editingName ? (
    <Card component="form" id="commands-form" withBorder onSubmit={handleSave}>
      <Stack gap="sm">
        <FormLayout>
          <FormIdentifiers>
            <FormField size="medium">
              <TextInput
                label={t("commandsPage.nameLabel")}
                required
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                placeholder={t("commandsPage.namePlaceholder")}
              />
            </FormField>
          </FormIdentifiers>
          <FormField size="medium">
            <Select
              label={t("commandsPage.typeLabel")}
              data={[
                { value: "function", label: t("commandsPage.typeFunction") },
                { value: "webhook", label: t("commandsPage.typeWebhook") },
              ]}
              value={form.actionType}
              onChange={(v) =>
                setForm({ ...EMPTY_FORM, actionType: (v ?? "function") as ActionType })
              }
              allowDeselect={false}
            />
          </FormField>
        </FormLayout>
        {/* Only what a command needs in order to exist; the rest is set in the edit form
            the new command opens in. */}
        <CommandFormFields {...formFieldsProps} essential />
        <FormFooter
          panel
          submit
          form="commands-form"
          saving={saving}
          onCancel={handleCancel}
          saveProps={{ "data-testid": "commands-save" }}
        />
      </Stack>
    </Card>
  ) : selectedFn ? (
    ((fn: TrackedFunction) =>
      editingName === fn.name ? (
        <Stack component="form" id="commands-form" gap="sm" onSubmit={handleSave}>
          <CommandFormFields {...formFieldsProps} />
          <FormFooter panel submit form="commands-form" saving={saving} onCancel={handleCancel} />
        </Stack>
      ) : (
        <>
          <Stack gap="sm">
            <FunctionRead
              fn={fn}
              productName={
                fn.productId
                  ? (dataProducts.find((dp) => dp.id === fn.productId)?.name ?? fn.productId)
                  : null
              }
            />
            <PanelActions>
              <Group gap="sm" wrap="nowrap">
                <ActionIcon
                  aria-label={t("commandsPage.editAria", { name: fn.name })}
                  onClick={(e) => {
                    e.stopPropagation();
                    handleEdit("function", fn.name);
                  }}
                >
                  <Pencil size={14} />
                </ActionIcon>
                <Button
                  onClick={(e) => {
                    e.stopPropagation();
                    handleTest("function", fn.name);
                  }}
                  disabled={testing === fn.name}
                  loading={testing === fn.name}
                >
                  {testing === fn.name
                    ? t("commandsPage.testingButton")
                    : t("commandsPage.testButton")}
                </Button>
                <ConfirmDialog
                  title={t("commandsPage.deleteFunctionTitle", { name: fn.name })}
                  consequence={t("commandsPage.deleteFunctionConsequence")}
                  onConfirm={async () => {
                    await deleteFunction(fn.name);
                    setExpandedFn(null);
                    load();
                  }}
                >
                  {(open) => (
                    <ActionIcon
                      color="red"
                      aria-label={t("commandsPage.deleteFunctionAria", {
                        name: fn.name,
                      })}
                      onClick={(e) => {
                        e.stopPropagation();
                        open();
                      }}
                    >
                      <Trash2 size={14} />
                    </ActionIcon>
                  )}
                </ConfirmDialog>
              </Group>
            </PanelActions>
          </Stack>
          {testAs}
        </>
      ))(selectedFn)
  ) : selectedWh ? (
    ((wh: TrackedWebhook) =>
      editingName === wh.name ? (
        <Stack component="form" id="commands-form" gap="sm" onSubmit={handleSave}>
          <CommandFormFields {...formFieldsProps} />
          <FormFooter panel submit form="commands-form" saving={saving} onCancel={handleCancel} />
        </Stack>
      ) : (
        <>
          <Stack gap="sm">
            {wh.approved === false && (
              <Alert color="yellow" data-testid={`webhook-pending-${wh.name}`}>
                {t("commandsPage.webhookPendingHint")}
              </Alert>
            )}
            <WebhookRead wh={wh} />
            <PanelActions>
              <Group gap="sm" wrap="nowrap">
                <ActionIcon
                  aria-label={t("commandsPage.editAria", { name: wh.name })}
                  onClick={(e) => {
                    e.stopPropagation();
                    handleEdit("webhook", wh.name);
                  }}
                >
                  <Pencil size={14} />
                </ActionIcon>
                <Button
                  onClick={(e) => {
                    e.stopPropagation();
                    handleTest("webhook", wh.name);
                  }}
                  disabled={testing === wh.name}
                  loading={testing === wh.name}
                >
                  {testing === wh.name
                    ? t("commandsPage.testingButton")
                    : t("commandsPage.testButton")}
                </Button>
                <ConfirmDialog
                  title={t("commandsPage.deleteWebhookTitle", { name: wh.name })}
                  consequence={t("commandsPage.deleteWebhookConsequence")}
                  onConfirm={async () => {
                    await deleteWebhook(wh.name);
                    setExpandedWh(null);
                    load();
                  }}
                >
                  {(open) => (
                    <ActionIcon
                      color="red"
                      aria-label={t("commandsPage.deleteWebhookAria", {
                        name: wh.name,
                      })}
                      onClick={(e) => {
                        e.stopPropagation();
                        open();
                      }}
                    >
                      <Trash2 size={14} />
                    </ActionIcon>
                  )}
                </ConfirmDialog>
              </Group>
            </PanelActions>
          </Stack>
          {testAs}
        </>
      ))(selectedWh)
  ) : null;
  const panelIsReadOnly = !showForm && editingName === null;
  const panelTitle =
    showForm && !editingName
      ? t("commandsPage.panelNewCommand")
      : (selectedFn?.name ?? selectedWh?.name ?? testResult?.name ?? "");
  const closePanel = () => {
    setTestResult(null);
    if (showForm || editingName) handleCancel();
    setExpandedFn(null);
    setExpandedWh(null);
  };

  if (loading) return <PageLoading message={t("commandsPage.loading")} />;

  return (
    <div className="page page-sticky-head">
      <div className="page-header">
        <Title order={2}>{t("commandsPage.title")}</Title>
        <FilterInput
          value={cmdSearch}
          onChange={(v) => {
            setCmdSearch(v);
            setFnPage(1);
            setWhPage(1);
          }}
          placeholder={t("commandsPage.filterPlaceholder")}
        />
        <div className="page-actions">
          {!editingName && !showForm && (
            <Button
              leftSection={<Plus size={14} />}
              data-testid="commands-toggle-form"
              onClick={() => setShowForm(true)}
            >
              {t("commandsPage.addCommand")}
            </Button>
          )}
        </div>
      </div>

      {error && (
        <Alert color="red" mb="md" data-testid="commands-error">
          {error}
        </Alert>
      )}
      {msg && (
        <Alert color="green" mb="md" data-testid="commands-success">
          {msg}
        </Alert>
      )}

      <SplitPane
        testId="commands-split"
        storageKey="provisa.commands.detailWidth"
        title={panelTitle}
        onClose={closePanel}
        // Escape closes a command that is only being looked at; it never drops what is being typed.
        closeOnEscape={panelIsReadOnly}
        labels={{
          panel: t("commandsPage.detailPanel"),
          resize: t("commandsPage.resizeDetail"),
          expand: t("commandsPage.expandDetail"),
          restore: t("commandsPage.restoreDetail"),
          close: t("commandsPage.closeDetail"),
          backToList: t("commandsPage.backToList"),
        }}
        list={
          // Each list is the pane's own child: it scrolls sideways under the panel's edge,
          // where a wrapper would let it hang out from under it.
          <>
            <Title order={3} mb="sm">
              {t("commandsPage.dbFunctionsHeading")}
            </Title>
            <ListTable minWidth={720}>
              <ListHead
                sortGroup={fnSortGroup}
                columns={[
                  { col: "name" },
                  { col: "source" },
                  { col: "domain" },
                  { col: "function" },
                  { col: "returns" },
                  { col: "args" },
                  { col: "visibleTo" },
                ]}
              />
              <Table.Tbody>
                {filteredFunctions.length === 0 && (
                  <ListEmpty colSpan={7}>{t("commandsPage.noFunctions")}</ListEmpty>
                )}
                <ListItems
                  state={fnSortGroup}
                  items={pageItems(fnSortGroup, fnPage - 1, PAGE_SIZE)}
                  colSpan={7}
                  rowKey={(fn) => fn.name}
                  render={(fn) => {
                    const isExpanded = expandedFn === fn.name;
                    const isEditing = editingName === fn.name;
                    return (
                      <React.Fragment key={fn.name}>
                        <ListRow
                          testId={`commands-fn-row-${fn.name}`}
                          selected={isExpanded}
                          onClick={() => {
                            setTestResult(null);
                            setExpandedWh(null);
                            setExpandedFn(isExpanded ? null : fn.name);
                            if (isEditing) setEditingName(null);
                          }}
                        >
                          <Table.Td>
                            <Group gap="xs" wrap="wrap">
                              {fn.name}
                              <TagControl objectType="command" commandName={fn.name} />
                            </Group>
                          </Table.Td>
                          <Table.Td>{fn.sourceId}</Table.Td>
                          <Table.Td>{fn.domainId || t("commandsPage.dash")}</Table.Td>
                          <Table.Td>
                            {fn.schemaName}.{fn.functionName}
                          </Table.Td>
                          <Table.Td>
                            {fn.returns ||
                              (fn.returnSchema
                                ? t("commandsPage.customSchema")
                                : t("commandsPage.dash"))}
                          </Table.Td>
                          <Table.Td>{fn.arguments.length}</Table.Td>
                          <Table.Td>{fn.visibleTo.join(", ") || t("commandsPage.all")}</Table.Td>
                        </ListRow>
                      </React.Fragment>
                    );
                  }}
                />
              </Table.Tbody>
            </ListTable>
            {fnTotalPages > 1 && fnSortGroup.groupBy.length === 0 && (
              <Group justify="flex-end">
                <Pagination total={fnTotalPages} value={fnPage} onChange={setFnPage} size="sm" />
              </Group>
            )}

            <Title order={3} mt="md">
              {t("commandsPage.webhooksHeading")}
            </Title>
            <ListTable minWidth={720}>
              <ListHead
                sortGroup={whSortGroup}
                columns={[
                  { col: "name" },
                  { col: "status" },
                  { col: "domain" },
                  { col: "url" },
                  { col: "method" },
                  { col: "timeout" },
                  { col: "returns" },
                  { col: "args" },
                  { col: "visibleTo" },
                ]}
              />
              <Table.Tbody>
                {filteredWebhooks.length === 0 && (
                  <ListEmpty colSpan={9}>{t("commandsPage.noWebhooks")}</ListEmpty>
                )}
                <ListItems
                  state={whSortGroup}
                  items={pageItems(whSortGroup, whPage - 1, PAGE_SIZE)}
                  colSpan={9}
                  rowKey={(wh) => wh.name}
                  render={(wh) => {
                    const isExpanded = expandedWh === wh.name;
                    const isEditing = editingName === wh.name;
                    return (
                      <React.Fragment key={wh.name}>
                        <ListRow
                          testId={`commands-wh-row-${wh.name}`}
                          selected={isExpanded}
                          onClick={() => {
                            setTestResult(null);
                            setExpandedFn(null);
                            setExpandedWh(isExpanded ? null : wh.name);
                            if (isEditing) setEditingName(null);
                          }}
                        >
                          <Table.Td>
                            <Group gap="xs" wrap="wrap">
                              {wh.name}
                              <TagControl objectType="command" commandName={wh.name} />
                            </Group>
                          </Table.Td>
                          <Table.Td>
                            <Badge
                              size="sm"
                              variant="light"
                              color={wh.approved === false ? "yellow" : "green"}
                              data-testid={`webhook-status-${wh.name}`}
                            >
                              {wh.approved === false
                                ? t("commandsPage.statusPending")
                                : t("commandsPage.statusApproved")}
                            </Badge>
                          </Table.Td>
                          <Table.Td>{wh.domainId || t("commandsPage.dash")}</Table.Td>
                          <Table.Td>{wh.url}</Table.Td>
                          <Table.Td>{wh.method}</Table.Td>
                          <Table.Td>{wh.timeoutMs}ms</Table.Td>
                          <Table.Td>
                            {wh.returns ||
                              t("commandsPage.inlineReturns", {
                                count: wh.inlineReturnType.length,
                              })}
                          </Table.Td>
                          <Table.Td>{wh.arguments.length}</Table.Td>
                          <Table.Td>{wh.visibleTo.join(", ") || t("commandsPage.all")}</Table.Td>
                        </ListRow>
                      </React.Fragment>
                    );
                  }}
                />
              </Table.Tbody>
            </ListTable>
            {whTotalPages > 1 && whSortGroup.groupBy.length === 0 && (
              <Group justify="flex-end">
                <Pagination total={whTotalPages} value={whPage} onChange={setWhPage} size="sm" />
              </Group>
            )}
          </>
        }
      >
        {panelBody}
      </SplitPane>
    </div>
  );
}
