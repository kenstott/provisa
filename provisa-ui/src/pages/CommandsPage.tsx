// Copyright (c) 2026 Kenneth Stott
// Canary: b12f5409-a0a1-4db6-b8de-245a25bd1768
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useEffect, useCallback } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Card,
  Group,
  Modal,
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
import { Trash2, Pencil, Check, X, Plus } from "lucide-react";
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
} from "../hooks/useAdminQueries";
import type { TableMetadata } from "../api/admin";
import { fetchOrgRoles } from "../api/admin";
import type { Role } from "../types/auth";
import { ConfirmDialog } from "../components/ConfirmDialog";
import type { ActionType, FormState } from "./commands/types";
import { EMPTY_FORM } from "./commands/types";
import { CommandFormFields } from "./commands/CommandFormFields";

export function CommandsPage() {
  const { t } = useTranslation();
  const { sources } = useSources();
  const { tables } = useTables();
  const { domains } = useDomains();
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
  const [expandedFn, setExpandedFn] = useState<string | null>(null);
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
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
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
    fetchOrgRoles(orgId).then(setRoles).catch(() => {});
  }, [orgId]);

  useEffect(() => {
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       resets the available-functions list synchronously when the selected source changes, before refetching */
    setAvailableFunctions([]);
    const src = sources.find((s) => s.id === form.sourceId);
    if (!src || src.type !== "openapi") return;
    setLoadingFunctions(true);
    getAvailableFunctions(form.sourceId)
      .then(setAvailableFunctions)
      .catch(() => setAvailableFunctions([]))
      .finally(() => setLoadingFunctions(false));
  }, [form.sourceId, sources, getAvailableFunctions]);

  const handleEdit = (actionType: ActionType, name: string) => {
    if (actionType === "function") {
      const fn = functions.find((f) => f.name === name);
      if (!fn) return;
      const hasCustomSchema = !!fn.returnSchema && !fn.returns;
      setForm({
        actionType: "function",
        name: fn.name,
        sourceId: fn.sourceId,
        schemaName: fn.schemaName,
        functionName: fn.functionName,
        returns: fn.returns,
        visibleTo: fn.visibleTo.join(", "),
        writablBy: fn.writableBy.join(", "),
        domainId: fn.domainId,
        description: fn.description ?? "",
        arguments: fn.arguments.length > 0 ? fn.arguments : [],
        url: "",
        method: "POST",
        timeoutMs: 5000,
        inlineReturnType: [],
        kind: fn.kind ?? "mutation",
        returnSchemaMode: hasCustomSchema ? "custom" : "table",
        sampleJson: "",
        returnSchemaStr: hasCustomSchema ? JSON.stringify(fn.returnSchema, null, 2) : "",
        implKind: fn.implKind ?? "source_procedure",
        binding: fn.binding ?? {},
        materialize: fn.materialize ?? false,
      });
      setExpandedFn(name);
    } else {
      const wh = webhooks.find((w) => w.name === name);
      if (!wh) return;
      setForm({
        actionType: "webhook",
        name: wh.name,
        sourceId: "",
        schemaName: "public",
        functionName: "",
        returns: wh.returns ?? "",
        visibleTo: wh.visibleTo.join(", "),
        writablBy: "",
        domainId: wh.domainId,
        description: wh.description ?? "",
        arguments: wh.arguments.length > 0 ? wh.arguments : [],
        url: wh.url,
        method: wh.method,
        timeoutMs: wh.timeoutMs,
        inlineReturnType: wh.inlineReturnType.length > 0 ? wh.inlineReturnType : [],
        kind: wh.kind ?? "mutation",
        returnSchemaMode: "table",
        sampleJson: "",
        returnSchemaStr: "",
        implKind: "source_procedure",
        binding: {},
        materialize: false,
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
        const writableBy = form.writablBy
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
        let returnSchema: Record<string, unknown> | null = null;
        if (form.returnSchemaMode === "custom" && form.returnSchemaStr) {
          try {
            returnSchema = JSON.parse(form.returnSchemaStr);
          } catch {
            /* leave null */
          }
        }
        await saveFunction({
          name: form.name,
          sourceId: form.sourceId,
          schemaName: form.schemaName,
          functionName: form.functionName,
          returns: form.returnSchemaMode === "custom" ? "" : form.returns,
          arguments: form.arguments,
          visibleTo,
          writableBy,
          domainId: form.domainId,
          description: form.description || undefined,
          kind: form.kind,
          returnSchema,
          implKind: form.implKind,
          binding: form.binding,
          materialize: form.materialize,
        });
      } else {
        await saveWebhook({
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
        });
      }
      setMsg(t("commandsPage.savedMessage", { type: form.actionType, name: form.name }));
      setShowForm(false);
      setForm({ ...EMPTY_FORM });
      setEditingName(null);
      load();
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
      const result = await testAction(actionType, name, testRoleId || undefined);
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
  };

  if (loading) return <Text p="md">{t("commandsPage.loading")}</Text>;

  // Hide rows whose domain is unchecked in the NavBar domain filter (an empty set = show all),
  // matching the TablesPage/SqlPage convention. Rows with no domain are always shown.
  const inCheckedDomain = (domainId: string | null | undefined) =>
    !domainId || checkedDomains.size === 0 || checkedDomains.has(domainId);

  const filteredFunctions = functions.filter(
    (fn) =>
      inCheckedDomain(fn.domainId) &&
      (!cmdSearch.trim() || fn.name.toLowerCase().includes(cmdSearch.toLowerCase())),
  );
  const fnTotalPages = Math.max(1, Math.ceil(filteredFunctions.length / PAGE_SIZE));
  const pagedFunctions = filteredFunctions.slice((fnPage - 1) * PAGE_SIZE, fnPage * PAGE_SIZE);

  const filteredWebhooks = webhooks.filter(
    (wh) =>
      inCheckedDomain(wh.domainId) &&
      (!cmdSearch.trim() || wh.name.toLowerCase().includes(cmdSearch.toLowerCase())),
  );
  const whTotalPages = Math.max(1, Math.ceil(filteredWebhooks.length / PAGE_SIZE));
  const pagedWebhooks = filteredWebhooks.slice((whPage - 1) * PAGE_SIZE, whPage * PAGE_SIZE);

  return (
    <Stack gap="md" p="md">
      <Group justify="space-between" wrap="wrap">
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
        <Group>
          {!editingName && (
            <Button
              leftSection={showForm ? <X size={14} /> : <Plus size={14} />}
              data-testid="commands-toggle-form"
              onClick={() => {
                setShowForm(!showForm);
                if (showForm) handleCancel();
              }}
            >
              {showForm ? t("commandsPage.closeForm") : t("commandsPage.addCommand")}
            </Button>
          )}
        </Group>
      </Group>

      {error && (
        <Alert color="red" data-testid="commands-error">
          {error}
        </Alert>
      )}
      {msg && (
        <Alert color="green" data-testid="commands-success">
          {msg}
        </Alert>
      )}

      {showForm && !editingName && (
        <Card component="form" withBorder onSubmit={handleSave}>
          <Stack gap="sm">
            <Group grow align="flex-end">
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
              <TextInput
                label={t("commandsPage.nameLabel")}
                required
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                placeholder={t("commandsPage.namePlaceholder")}
              />
            </Group>
            <CommandFormFields {...formFieldsProps} />
            <Group justify="flex-end">
              <Button type="submit" disabled={saving} loading={saving}>
                {saving ? t("commandsPage.savingButton") : t("commandsPage.createButton")}
              </Button>
            </Group>
          </Stack>
        </Card>
      )}

      <Title order={3}>{t("commandsPage.dbFunctionsHeading")}</Title>
      <Table.ScrollContainer minWidth={720}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("commandsPage.colName")}</Table.Th>
              <Table.Th>{t("commandsPage.colSource")}</Table.Th>
              <Table.Th>{t("commandsPage.colDomain")}</Table.Th>
              <Table.Th>{t("commandsPage.colFunction")}</Table.Th>
              <Table.Th>{t("commandsPage.colReturns")}</Table.Th>
              <Table.Th>{t("commandsPage.colArgs")}</Table.Th>
              <Table.Th>{t("commandsPage.colVisibleTo")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {filteredFunctions.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={7} ta="center" c="dimmed">
                  {t("commandsPage.noFunctions")}
                </Table.Td>
              </Table.Tr>
            )}
            {pagedFunctions.map((fn) => {
              const isExpanded = expandedFn === fn.name;
              const isEditing = editingName === fn.name;
              return (
                <React.Fragment key={fn.name}>
                  <Table.Tr
                    onClick={() => {
                      setExpandedFn(isExpanded ? null : fn.name);
                      if (isEditing) setEditingName(null);
                    }}
                    style={{
                      cursor: "pointer",
                      background: isExpanded ? "var(--surface)" : undefined,
                    }}
                  >
                    <Table.Td>{fn.name}</Table.Td>
                    <Table.Td>{fn.sourceId}</Table.Td>
                    <Table.Td>{fn.domainId || t("commandsPage.dash")}</Table.Td>
                    <Table.Td>
                      {fn.schemaName}.{fn.functionName}
                    </Table.Td>
                    <Table.Td>
                      {fn.returns || (fn.returnSchema ? t("commandsPage.customSchema") : t("commandsPage.dash"))}
                    </Table.Td>
                    <Table.Td>{fn.arguments.length}</Table.Td>
                    <Table.Td>{fn.visibleTo.join(", ") || t("commandsPage.all")}</Table.Td>
                  </Table.Tr>
                  {isExpanded && (
                    <Table.Tr>
                      <Table.Td
                        colSpan={7}
                        style={{
                          padding: "0.75rem 1rem",
                          background: "var(--bg)",
                          borderTop: "1px solid var(--border)",
                        }}
                      >
                        {isEditing ? (
                          <Stack component="form" gap="sm" onSubmit={handleSave}>
                            <CommandFormFields {...formFieldsProps} />
                            <Group justify="flex-end">
                              <ActionIcon
                                type="button"
                                variant="default"
                                aria-label={t("commandsPage.cancelAria")}
                                onClick={handleCancel}
                              >
                                <X size={14} />
                              </ActionIcon>
                              <ActionIcon
                                type="submit"
                                variant="filled"
                                aria-label={t("commandsPage.saveAria")}
                                disabled={saving}
                              >
                                <Check size={14} />
                              </ActionIcon>
                            </Group>
                          </Stack>
                        ) : (
                          <Stack gap="sm">
                            <Table withRowBorders={false} verticalSpacing={2}>
                              <Table.Tbody>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailName")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.name}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailKind")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.kind ?? "mutation"}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailSource")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.sourceId}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailSchema")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.schemaName}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailFunction")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.functionName}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailReturns")}</strong>
                                  </Table.Td>
                                  <Table.Td>
                                    {fn.returns ||
                                      (fn.returnSchema
                                        ? t("commandsPage.customSchema")
                                        : t("commandsPage.dash"))}
                                  </Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailVisibleTo")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.visibleTo.join(", ") || t("commandsPage.all")}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailWritableBy")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.writableBy.join(", ") || t("commandsPage.all")}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailDomain")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.domainId || t("commandsPage.dash")}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailDescription")}</strong>
                                  </Table.Td>
                                  <Table.Td>{fn.description || t("commandsPage.dash")}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailArguments")}</strong>
                                  </Table.Td>
                                  <Table.Td>
                                    {fn.arguments.length === 0
                                      ? t("commandsPage.none")
                                      : fn.arguments.map((a) => `${a.name}: ${a.type}`).join(", ")}
                                  </Table.Td>
                                </Table.Tr>
                              </Table.Tbody>
                            </Table>
                            <Group gap="sm">
                              <ActionIcon
                                variant="default"
                                aria-label={t("commandsPage.editAria", { name: fn.name })}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleEdit("function", fn.name);
                                }}
                              >
                                <Pencil size={14} />
                              </ActionIcon>
                              <Button
                                variant="light"
                                size="xs"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleTest("function", fn.name);
                                }}
                                disabled={testing === fn.name}
                                loading={testing === fn.name}
                              >
                                {testing === fn.name ? t("commandsPage.testingButton") : t("commandsPage.testButton")}
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
                                    variant="subtle"
                                    color="red"
                                    aria-label={t("commandsPage.deleteFunctionAria", { name: fn.name })}
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
                          </Stack>
                        )}
                      </Table.Td>
                    </Table.Tr>
                  )}
                </React.Fragment>
              );
            })}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {fnTotalPages > 1 && (
        <Group justify="flex-end">
          <Pagination total={fnTotalPages} value={fnPage} onChange={setFnPage} size="sm" />
        </Group>
      )}

      <Title order={3}>{t("commandsPage.webhooksHeading")}</Title>
      <Table.ScrollContainer minWidth={720}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("commandsPage.colName")}</Table.Th>
              <Table.Th>{t("commandsPage.colStatus")}</Table.Th>
              <Table.Th>{t("commandsPage.colDomain")}</Table.Th>
              <Table.Th>{t("commandsPage.colUrl")}</Table.Th>
              <Table.Th>{t("commandsPage.colMethod")}</Table.Th>
              <Table.Th>{t("commandsPage.colTimeout")}</Table.Th>
              <Table.Th>{t("commandsPage.colReturns")}</Table.Th>
              <Table.Th>{t("commandsPage.colArgs")}</Table.Th>
              <Table.Th>{t("commandsPage.colVisibleTo")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {filteredWebhooks.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={9} ta="center" c="dimmed">
                  {t("commandsPage.noWebhooks")}
                </Table.Td>
              </Table.Tr>
            )}
            {pagedWebhooks.map((wh) => {
              const isExpanded = expandedWh === wh.name;
              const isEditing = editingName === wh.name;
              return (
                <React.Fragment key={wh.name}>
                  <Table.Tr
                    onClick={() => {
                      setExpandedWh(isExpanded ? null : wh.name);
                      if (isEditing) setEditingName(null);
                    }}
                    style={{
                      cursor: "pointer",
                      background: isExpanded ? "var(--surface)" : undefined,
                    }}
                  >
                    <Table.Td>{wh.name}</Table.Td>
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
                    <Table.Td style={{ maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis" }}>
                      {wh.url}
                    </Table.Td>
                    <Table.Td>{wh.method}</Table.Td>
                    <Table.Td>{wh.timeoutMs}ms</Table.Td>
                    <Table.Td>
                      {wh.returns || t("commandsPage.inlineReturns", { count: wh.inlineReturnType.length })}
                    </Table.Td>
                    <Table.Td>{wh.arguments.length}</Table.Td>
                    <Table.Td>{wh.visibleTo.join(", ") || t("commandsPage.all")}</Table.Td>
                  </Table.Tr>
                  {isExpanded && (
                    <Table.Tr>
                      <Table.Td
                        colSpan={9}
                        style={{
                          padding: "0.75rem 1rem",
                          background: "var(--bg)",
                          borderTop: "1px solid var(--border)",
                        }}
                      >
                        {isEditing ? (
                          <Stack component="form" gap="sm" onSubmit={handleSave}>
                            <CommandFormFields {...formFieldsProps} />
                            <Group justify="flex-end">
                              <ActionIcon
                                type="button"
                                variant="default"
                                aria-label={t("commandsPage.cancelAria")}
                                onClick={handleCancel}
                              >
                                <X size={14} />
                              </ActionIcon>
                              <ActionIcon
                                type="submit"
                                variant="filled"
                                aria-label={t("commandsPage.saveAria")}
                                disabled={saving}
                              >
                                <Check size={14} />
                              </ActionIcon>
                            </Group>
                          </Stack>
                        ) : (
                          <Stack gap="sm">
                            {wh.approved === false && (
                              <Alert color="yellow" data-testid={`webhook-pending-${wh.name}`}>
                                {t("commandsPage.webhookPendingHint")}
                              </Alert>
                            )}
                            <Table withRowBorders={false} verticalSpacing={2}>
                              <Table.Tbody>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailName")}</strong>
                                  </Table.Td>
                                  <Table.Td>{wh.name}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailStatus")}</strong>
                                  </Table.Td>
                                  <Table.Td>
                                    {wh.approved === false
                                      ? t("commandsPage.statusPending")
                                      : t("commandsPage.statusApproved")}
                                  </Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailKind")}</strong>
                                  </Table.Td>
                                  <Table.Td>{wh.kind ?? "mutation"}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailUrl")}</strong>
                                  </Table.Td>
                                  <Table.Td>{wh.url}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailMethod")}</strong>
                                  </Table.Td>
                                  <Table.Td>{wh.method}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailTimeout")}</strong>
                                  </Table.Td>
                                  <Table.Td>{wh.timeoutMs}ms</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailReturns")}</strong>
                                  </Table.Td>
                                  <Table.Td>
                                    {wh.returns ||
                                      t("commandsPage.inlineReturns", {
                                        count: wh.inlineReturnType.length,
                                      })}
                                  </Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailVisibleTo")}</strong>
                                  </Table.Td>
                                  <Table.Td>{wh.visibleTo.join(", ") || t("commandsPage.all")}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailDomain")}</strong>
                                  </Table.Td>
                                  <Table.Td>{wh.domainId || t("commandsPage.dash")}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailDescription")}</strong>
                                  </Table.Td>
                                  <Table.Td>{wh.description || t("commandsPage.dash")}</Table.Td>
                                </Table.Tr>
                                <Table.Tr>
                                  <Table.Td c="dimmed">
                                    <strong>{t("commandsPage.detailArguments")}</strong>
                                  </Table.Td>
                                  <Table.Td>
                                    {wh.arguments.length === 0
                                      ? t("commandsPage.none")
                                      : wh.arguments.map((a) => `${a.name}: ${a.type}`).join(", ")}
                                  </Table.Td>
                                </Table.Tr>
                                {wh.inlineReturnType.length > 0 && (
                                  <Table.Tr>
                                    <Table.Td c="dimmed">
                                      <strong>{t("commandsPage.detailInlineFields")}</strong>
                                    </Table.Td>
                                    <Table.Td>
                                      {wh.inlineReturnType.map((f) => `${f.name}: ${f.type}`).join(", ")}
                                    </Table.Td>
                                  </Table.Tr>
                                )}
                              </Table.Tbody>
                            </Table>
                            <Group gap="sm">
                              <ActionIcon
                                variant="default"
                                aria-label={t("commandsPage.editAria", { name: wh.name })}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleEdit("webhook", wh.name);
                                }}
                              >
                                <Pencil size={14} />
                              </ActionIcon>
                              <Button
                                variant="light"
                                size="xs"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleTest("webhook", wh.name);
                                }}
                                disabled={testing === wh.name}
                                loading={testing === wh.name}
                              >
                                {testing === wh.name ? t("commandsPage.testingButton") : t("commandsPage.testButton")}
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
                                    variant="subtle"
                                    color="red"
                                    aria-label={t("commandsPage.deleteWebhookAria", { name: wh.name })}
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
                          </Stack>
                        )}
                      </Table.Td>
                    </Table.Tr>
                  )}
                </React.Fragment>
              );
            })}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {whTotalPages > 1 && (
        <Group justify="flex-end">
          <Pagination total={whTotalPages} value={whPage} onChange={setWhPage} size="sm" />
        </Group>
      )}

      <Group gap="sm" align="center">
        <Text size="sm" c="dimmed">
          {t("commandsPage.testAsRole")}
        </Text>
        <Select
          aria-label={t("commandsPage.testAsRole")}
          size="xs"
          data={[
            { value: "", label: t("commandsPage.noGovernance") },
            ...roles.map((r) => ({ value: r.id, label: r.id })),
          ]}
          value={testRoleId}
          onChange={(v) => setTestRoleId(v ?? "")}
          allowDeselect={false}
        />
      </Group>

      <Modal
        opened={testResult !== null}
        onClose={() => setTestResult(null)}
        size="lg"
        title={testResult ? t("commandsPage.testResultHeading", { name: testResult.name }) : ""}
      >
        {testResult && (
          <>
            {!!(testResult.data && typeof testResult.data === "object" && "enforcement" in testResult.data) && (
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
        )}
      </Modal>
    </Stack>
  );
}
