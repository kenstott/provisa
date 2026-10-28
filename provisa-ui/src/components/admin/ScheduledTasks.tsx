// Copyright (c) 2026 Kenneth Stott
// Canary: a132b62f-ffb6-41f5-bd78-07c2ad185615
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2054: the admin Triggers page -- every automatic run in one place. A trigger fires on a cron
// schedule, when a registered table changes, or on a table's error or quarantine event; it runs a
// webhook, a SQL statement or a command, as a role. The list shows each with how its last run
// ended; a row's detail opens in the right-hand panel, where it is read, edited, enabled or
// disabled and deleted. What the page shows and edits is what the org's model declares.

import { useEffect, useMemo, useState, type FormEvent } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Pagination,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Textarea,
} from "@mantine/core";
import { Pencil, Plus, Power, PowerOff, Trash2 } from "lucide-react";
import { ConfirmDialog } from "../ConfirmDialog";
import { Tip } from "../Tip";
import {
  useScheduledTasks,
  useToggleScheduledTask,
  useCreateScheduledTask,
  useUpdateScheduledTask,
  useDeleteScheduledTask,
  type CreateScheduledTaskInput,
} from "../../hooks/useAdminOpsQueries";
import { useRoles, useTables } from "../../hooks/useAdminQueries";
import { fetchActions, type TrackedFunction, type TrackedWebhook } from "../../api/actions";
import type { ScheduledTask } from "../../api/admin";
import { DetailFields, type DetailField } from "../detail/DetailFields";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../list/ListTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { pageItems, useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";

const PAGE_SIZE = 50;

type Kind = ScheduledTask["kind"];
type FiresOn = ScheduledTask["firesOn"];

/** A trigger's definition as the form holds it. */
interface TriggerForm {
  id: string;
  name: string;
  firesOn: FiresOn;
  cron: string;
  onTable: string | null;
  /** For a trigger on a table's event: error | quarantine. */
  onEvent: string;
  kind: Kind;
  webhookName: string;
  args: Record<string, string>;
  sql: string;
  command: string | null;
  role: string | null;
}

const EMPTY: TriggerForm = {
  id: "",
  name: "",
  firesOn: "schedule",
  cron: "",
  onTable: null,
  onEvent: "error",
  kind: "webhook",
  webhookName: "",
  args: {},
  sql: "",
  command: null,
  role: null,
};

/** The form for a trigger that exists: its definition, as the list read it. */
function formOf(task: ScheduledTask, webhooks: TrackedWebhook[]): TriggerForm {
  return {
    ...EMPTY,
    id: task.id,
    name: task.name,
    firesOn: task.firesOn,
    cron: task.cronExpression ?? "",
    onTable: task.onTable,
    onEvent: task.firesOn === "event" ? (task.onEvent ?? "error") : "error",
    kind: task.kind,
    // The list carries a webhook trigger's URL; its webhook is the one registered at that URL.
    webhookName: webhooks.find((w) => w.url === task.webhookUrl)?.name ?? "",
    sql: task.sql ?? "",
    command: task.command,
    role: task.role,
  };
}

/** What the form says, as the create and update mutations take it. */
function inputOf(form: TriggerForm): CreateScheduledTaskInput {
  const when =
    form.firesOn === "schedule"
      ? { cron: form.cron.trim() }
      : {
          onTable: form.onTable ?? undefined,
          onEvent: form.firesOn === "change" ? "change" : form.onEvent,
        };
  const base = { id: form.id.trim(), name: form.name.trim(), ...when, kind: form.kind };
  if (form.kind === "webhook")
    return {
      ...base,
      webhookName: form.webhookName,
      argsJson: Object.keys(form.args).length ? JSON.stringify(form.args) : undefined,
    };
  if (form.kind === "sql") return { ...base, sql: form.sql.trim(), role: form.role ?? undefined };
  return { ...base, command: form.command ?? undefined, role: form.role ?? undefined };
}

export function ScheduledTasks() {
  const { t } = useTranslation();
  const { scheduledTasks: tasks, loading } = useScheduledTasks();
  const { toggleScheduledTask } = useToggleScheduledTask();
  const { createScheduledTask } = useCreateScheduledTask();
  const { updateScheduledTask } = useUpdateScheduledTask();
  const { deleteScheduledTask } = useDeleteScheduledTask();
  const { roles } = useRoles();
  const { tables } = useTables({ inPage: true, columns: false });

  const [webhooks, setWebhooks] = useState<TrackedWebhook[]>([]);
  const [commands, setCommands] = useState<TrackedFunction[]>([]);
  const [actionsError, setActionsError] = useState("");
  useEffect(() => {
    fetchActions()
      .then((a) => {
        setWebhooks(a.webhooks);
        setCommands(a.functions);
      })
      .catch((e: unknown) =>
        setActionsError(
          t("scheduledTasks.actionsLoadFailed", {
            error: e instanceof Error ? e.message : String(e),
          }),
        ),
      );
  }, [t]);

  const [selectedId, setSelectedId] = useState<string | null>(null);
  // What the panel shows: the selected trigger, read or being edited, or a new one being made.
  const [mode, setMode] = useState<"read" | "edit" | "create">("read");
  const [form, setForm] = useState<TriggerForm>(EMPTY);
  const [formMsg, setFormMsg] = useState("");
  const [saving, setSaving] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [actionMsg, setActionMsg] = useState("");
  const [page, setPage] = useState(1);

  const selected = tasks.find((task) => task.id === selectedId) ?? null;
  const tableNames = useMemo(
    () => tables.map((tb) => `${tb.sourceId}.${tb.schemaName}.${tb.tableName}`).sort(),
    [tables],
  );

  const kindLabel = (kind: Kind) =>
    kind === "webhook"
      ? t("scheduledTasks.kindWebhook")
      : kind === "sql"
        ? t("scheduledTasks.kindSql")
        : t("scheduledTasks.kindCommand");
  const eventLabel = (event: string | null) =>
    event === "quarantine"
      ? t("scheduledTasks.eventQuarantine")
      : event === "error"
        ? t("scheduledTasks.eventError")
        : t("scheduledTasks.eventChange");
  const firesLabel = (task: ScheduledTask) =>
    task.firesOn === "schedule"
      ? (task.cronExpression ?? "")
      : t("scheduledTasks.firesOnTable", { table: task.onTable, event: eventLabel(task.onEvent) });
  const targetOf = (task: ScheduledTask) =>
    task.kind === "sql" ? task.sql : task.kind === "command" ? task.command : task.webhookUrl;
  const when = (iso: string | null) => (iso ? new Date(iso).toLocaleString() : null);
  const outcomeBadge = (outcome: string | null) =>
    outcome === null ? null : (
      <Badge
        variant="light"
        color={outcome === "succeeded" ? "green" : outcome === "refused" ? "yellow" : "red"}
      >
        {outcome === "succeeded"
          ? t("scheduledTasks.outcomeSucceeded")
          : outcome === "refused"
            ? t("scheduledTasks.outcomeRefused")
            : t("scheduledTasks.outcomeFailed")}
      </Badge>
    );

  const columns: ListColumn<ScheduledTask>[] = [
    { key: "name", label: t("scheduledTasks.colName"), sortValue: (k) => k.name },
    {
      key: "fires",
      label: t("scheduledTasks.colFires"),
      sortValue: (k) => firesLabel(k),
      groupValue: (k) =>
        k.firesOn === "schedule"
          ? t("scheduledTasks.firesSchedule")
          : k.firesOn === "change"
            ? t("scheduledTasks.firesChange")
            : t("scheduledTasks.firesEvent"),
    },
    {
      key: "runs",
      label: t("scheduledTasks.colRuns"),
      sortValue: (k) => kindLabel(k.kind),
      groupValue: (k) => kindLabel(k.kind),
    },
    {
      key: "role",
      label: t("scheduledTasks.colRole"),
      sortValue: (k) => k.role ?? "",
      groupValue: (k) => k.role ?? "",
    },
    {
      key: "enabled",
      label: t("scheduledTasks.colEnabled"),
      sortValue: (k) => (k.enabled ? 1 : 0),
      groupValue: (k) => (k.enabled ? t("scheduledTasks.enabled") : t("scheduledTasks.disabled")),
    },
    { key: "lastRun", label: t("scheduledTasks.colLastRun"), sortValue: (k) => k.lastRunAt ?? "" },
    { key: "nextRun", label: t("scheduledTasks.colNextRun"), sortValue: (k) => k.nextRunAt ?? "" },
  ];
  const sortGroup = useListSortGroup(tasks, columns, "scheduled-tasks");
  const totalPages = Math.max(1, Math.ceil(tasks.length / PAGE_SIZE));

  // A trigger's toggle or delete: the server's refusal, or the error it raised, is said above the list.
  const listAction = async (
    id: string,
    run: () => Promise<{ success: boolean; message: string }>,
  ): Promise<boolean> => {
    setBusy(id);
    setActionMsg("");
    try {
      const result = await run();
      if (!result.success) setActionMsg(result.message);
      return result.success;
    } catch (e) {
      setActionMsg(e instanceof Error ? e.message : String(e));
      return false;
    } finally {
      setBusy(null);
    }
  };

  const openCreate = () => {
    setSelectedId(null);
    setForm(EMPTY);
    setFormMsg("");
    setMode("create");
  };
  const openEdit = (task: ScheduledTask) => {
    setForm(formOf(task, webhooks));
    setFormMsg("");
    setMode("edit");
  };
  const cancel = () => {
    setFormMsg("");
    setMode("read");
  };
  const closePanel = () => {
    setSelectedId(null);
    cancel();
  };

  const refusal = (f: TriggerForm): string | null => {
    if (!f.id.trim() || !f.name.trim()) return t("scheduledTasks.validationRequired");
    if (f.firesOn === "schedule" && !f.cron.trim()) return t("scheduledTasks.validationCronRequired");
    if (f.firesOn !== "schedule" && !f.onTable) return t("scheduledTasks.validationTableRequired");
    if (f.kind === "webhook" && !f.webhookName) return t("scheduledTasks.validationWebhookRequired");
    if (f.kind === "sql" && !f.sql.trim()) return t("scheduledTasks.validationSqlRequired");
    if (f.kind === "command" && !f.command) return t("scheduledTasks.validationCommandRequired");
    if (f.kind !== "webhook" && !f.role) return t("scheduledTasks.validationRoleRequired");
    return null;
  };

  const handleSave = async (e: FormEvent) => {
    e.preventDefault();
    const refused = refusal(form);
    if (refused) {
      setFormMsg(refused);
      return;
    }
    setSaving(true);
    setFormMsg("");
    try {
      const input = inputOf(form);
      const result =
        mode === "create" ? await createScheduledTask(input) : await updateScheduledTask(input);
      if (!result.success) {
        setFormMsg(result.message);
        return;
      }
      setSelectedId(input.id);
      setMode("read");
    } catch (err) {
      setFormMsg(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const selectedWebhook = webhooks.find((w) => w.name === form.webhookName) ?? null;
  const formFields = (
    <FormLayout>
      <FormIdentifiers>
        <FormField size="medium">
          <TextInput
            label={t("scheduledTasks.idLabel")}
            value={form.id}
            onChange={(e) => setForm({ ...form, id: e.currentTarget.value })}
            placeholder={t("scheduledTasks.idPlaceholder")}
            // A trigger is known by its id: it is set when the trigger is made.
            disabled={mode === "edit"}
          />
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("scheduledTasks.nameLabel")}
            value={form.name}
            onChange={(e) => setForm({ ...form, name: e.currentTarget.value })}
            placeholder={t("scheduledTasks.namePlaceholder")}
          />
        </FormField>
      </FormIdentifiers>
      <FormField size="medium">
        <Select
          label={t("scheduledTasks.firesOnLabel")}
          aria-label={t("scheduledTasks.firesOnLabel")}
          data={[
            { value: "schedule", label: t("scheduledTasks.firesSchedule") },
            { value: "change", label: t("scheduledTasks.firesChange") },
            { value: "event", label: t("scheduledTasks.firesEvent") },
          ]}
          value={form.firesOn}
          onChange={(v) => setForm({ ...form, firesOn: (v as FiresOn) ?? "schedule" })}
          allowDeselect={false}
          data-testid="scheduled-task-fires-on"
        />
      </FormField>
      {form.firesOn === "schedule" ? (
        <FormField size="medium">
          <TextInput
            label={t("scheduledTasks.cronLabel")}
            value={form.cron}
            onChange={(e) => setForm({ ...form, cron: e.currentTarget.value })}
            placeholder={t("scheduledTasks.cronPlaceholder")}
          />
        </FormField>
      ) : (
        <>
          <FormField size="long">
            <Select
              label={t("scheduledTasks.tableLabel")}
              aria-label={t("scheduledTasks.tableLabel")}
              data={tableNames}
              value={form.onTable}
              onChange={(v) => setForm({ ...form, onTable: v })}
              searchable
              data-testid="scheduled-task-table"
            />
          </FormField>
          {form.firesOn === "event" && (
            <FormField size="medium">
              <Select
                label={t("scheduledTasks.eventLabel")}
                aria-label={t("scheduledTasks.eventLabel")}
                data={[
                  { value: "error", label: t("scheduledTasks.eventError") },
                  { value: "quarantine", label: t("scheduledTasks.eventQuarantine") },
                ]}
                value={form.onEvent}
                onChange={(v) => setForm({ ...form, onEvent: v ?? "error" })}
                allowDeselect={false}
              />
            </FormField>
          )}
        </>
      )}
      <FormField size="medium">
        <Select
          label={t("scheduledTasks.kindLabel")}
          aria-label={t("scheduledTasks.kindLabel")}
          data={[
            { value: "webhook", label: t("scheduledTasks.kindWebhook") },
            { value: "sql", label: t("scheduledTasks.kindSql") },
            { value: "command", label: t("scheduledTasks.kindCommand") },
          ]}
          value={form.kind}
          onChange={(v) => {
            setForm({ ...form, kind: (v as Kind) ?? "webhook" });
            setFormMsg("");
          }}
          allowDeselect={false}
        />
      </FormField>
      {form.kind === "webhook" && (
        <>
          <FormField size="medium">
            <Select
              label={t("scheduledTasks.webhookLabel")}
              placeholder={t("scheduledTasks.webhookPlaceholder")}
              data={webhooks.map((w) => ({ value: w.name, label: w.name }))}
              value={form.webhookName || null}
              onChange={(v) => setForm({ ...form, webhookName: v ?? "", args: {} })}
            />
          </FormField>
          {selectedWebhook?.arguments.map((arg) => (
            <FormField size="medium" key={arg.name}>
              <TextInput
                label={
                  <>
                    {arg.name}{" "}
                    <Text span c="dimmed" fz="xs">
                      ({arg.type})
                    </Text>
                  </>
                }
                value={form.args[arg.name] ?? ""}
                onChange={(e) =>
                  setForm({ ...form, args: { ...form.args, [arg.name]: e.currentTarget.value } })
                }
                placeholder={arg.type}
              />
            </FormField>
          ))}
        </>
      )}
      {form.kind === "sql" && (
        <FormField size="full" hint={t("scheduledTasks.sqlTokensHint")}>
          <Textarea
            label={t("scheduledTasks.sqlLabel")}
            aria-label={t("scheduledTasks.sqlLabel")}
            value={form.sql}
            onChange={(e) => setForm({ ...form, sql: e.currentTarget.value })}
            placeholder={t("scheduledTasks.sqlPlaceholder")}
            rows={4}
            styles={{ input: { fontFamily: "monospace" } }}
          />
        </FormField>
      )}
      {form.kind === "command" && (
        <FormField size="medium" hint={t("scheduledTasks.commandHint")}>
          <Select
            label={t("scheduledTasks.commandLabel")}
            aria-label={t("scheduledTasks.commandLabel")}
            placeholder={t("scheduledTasks.commandPlaceholder")}
            data={commands.map((c) => c.name)}
            value={form.command}
            onChange={(v) => setForm({ ...form, command: v })}
            searchable
            data-testid="scheduled-task-command"
          />
        </FormField>
      )}
      {form.kind !== "webhook" && (
        <FormField size="medium">
          <Select
            label={t("scheduledTasks.roleLabel")}
            aria-label={t("scheduledTasks.roleLabel")}
            data={roles.map((r) => r.id)}
            value={form.role}
            onChange={(v) => setForm({ ...form, role: v })}
            data-testid="scheduled-task-role"
          />
        </FormField>
      )}
      {formMsg && (
        <FormField size="full">
          <Text c="red" fz="sm" data-testid="scheduled-tasks-form-error">
            {formMsg}
          </Text>
        </FormField>
      )}
    </FormLayout>
  );

  const readFields = (task: ScheduledTask): DetailField[] => [
    { label: t("scheduledTasks.idLabel"), value: task.id },
    { label: t("scheduledTasks.nameLabel"), value: task.name },
    {
      label: t("scheduledTasks.firesOnLabel"),
      value:
        task.firesOn === "schedule"
          ? t("scheduledTasks.firesSchedule")
          : task.firesOn === "change"
            ? t("scheduledTasks.firesChange")
            : t("scheduledTasks.firesEvent"),
    },
    ...(task.firesOn === "schedule"
      ? [{ label: t("scheduledTasks.cronLabel"), value: task.cronExpression }]
      : [
          { label: t("scheduledTasks.tableLabel"), value: task.onTable },
          ...(task.firesOn === "event"
            ? [{ label: t("scheduledTasks.eventLabel"), value: eventLabel(task.onEvent) }]
            : []),
        ]),
    { label: t("scheduledTasks.kindLabel"), value: kindLabel(task.kind) },
    {
      label:
        task.kind === "sql"
          ? t("scheduledTasks.sqlLabel")
          : task.kind === "command"
            ? t("scheduledTasks.commandLabel")
            : t("scheduledTasks.webhookLabel"),
      value: targetOf(task) ? (
        <Text ff="monospace" fz="sm" component="span">
          {targetOf(task)}
        </Text>
      ) : null,
    },
    ...(task.kind !== "webhook" ? [{ label: t("scheduledTasks.roleLabel"), value: task.role }] : []),
    {
      label: t("scheduledTasks.colEnabled"),
      value: task.enabled ? t("scheduledTasks.enabled") : t("scheduledTasks.disabled"),
    },
    { label: t("scheduledTasks.colLastRun"), value: when(task.lastRunAt) },
    { label: t("scheduledTasks.lastOutcomeLabel"), value: outcomeBadge(task.lastOutcome) },
    {
      label: t("scheduledTasks.lastErrorLabel"),
      value: task.lastError ? (
        <Text c="red" fz="sm" component="span">
          {task.lastError}
        </Text>
      ) : null,
    },
    { label: t("scheduledTasks.lastFiredByLabel"), value: task.lastFiredBy },
    ...(task.firesOn === "schedule"
      ? [{ label: t("scheduledTasks.colNextRun"), value: when(task.nextRunAt) }]
      : []),
  ];

  const panel =
    mode === "create" || (mode === "edit" && selected) ? (
      <Stack component="form" id="scheduled-tasks-form" gap="sm" onSubmit={handleSave}>
        {formFields}
        <FormFooter
          panel
          submit
          form="scheduled-tasks-form"
          saving={saving}
          onCancel={mode === "create" ? closePanel : cancel}
          saveProps={{ "data-testid": "scheduled-tasks-submit" }}
        />
      </Stack>
    ) : selected ? (
      <>
        <DetailFields fields={readFields(selected)} testId="scheduled-task-read" />
        <PanelActions>
          <Group gap="sm" wrap="nowrap">
            <Tip
              text={
                selected.enabled
                  ? t("scheduledTasks.disableAria", { name: selected.name })
                  : t("scheduledTasks.enableAria", { name: selected.name })
              }
            >
              <ActionIcon
                aria-label={
                  selected.enabled
                    ? t("scheduledTasks.disableAria", { name: selected.name })
                    : t("scheduledTasks.enableAria", { name: selected.name })
                }
                disabled={busy === selected.id}
                onClick={() =>
                  listAction(selected.id, () =>
                    toggleScheduledTask(selected.id, !selected.enabled),
                  )
                }
                data-testid="scheduled-task-toggle"
              >
                {selected.enabled ? <PowerOff size={14} /> : <Power size={14} />}
              </ActionIcon>
            </Tip>
            <Tip text={t("scheduledTasks.editAria", { name: selected.name })}>
              <ActionIcon
                aria-label={t("scheduledTasks.editAria", { name: selected.name })}
                onClick={() => openEdit(selected)}
                data-testid="scheduled-task-edit"
              >
                <Pencil size={14} />
              </ActionIcon>
            </Tip>
            <ConfirmDialog
              title={t("scheduledTasks.deleteTitle", { name: selected.name })}
              consequence={t("scheduledTasks.deleteConsequence")}
              onConfirm={async () => {
                if (await listAction(selected.id, () => deleteScheduledTask(selected.id)))
                  setSelectedId(null);
              }}
            >
              {(open) => (
                <ActionIcon
                  color="red"
                  aria-label={t("scheduledTasks.deleteAria", { name: selected.name })}
                  disabled={busy === selected.id}
                  onClick={open}
                  data-testid="scheduled-task-delete"
                >
                  <Trash2 size={14} />
                </ActionIcon>
              )}
            </ConfirmDialog>
          </Group>
        </PanelActions>
      </>
    ) : null;

  if (loading) return <Text>{t("scheduledTasks.loading")}</Text>;

  return (
    <Stack gap="md">
      {actionsError && (
        <Alert color="red" data-testid="scheduled-tasks-actions-error">
          {actionsError}
        </Alert>
      )}
      {actionMsg && (
        <Alert color="red" data-testid="scheduled-tasks-action-error">
          {actionMsg}
        </Alert>
      )}
      {mode !== "create" && mode !== "edit" && (
        <Group>
          <Button
            data-testid="scheduled-tasks-toggle-form"
            onClick={openCreate}
            leftSection={<Plus size={14} />}
          >
            {t("scheduledTasks.addButton")}
          </Button>
        </Group>
      )}
      <SplitPane
        testId="scheduled-tasks-split"
        storageKey="provisa.triggers.detailWidth"
        title={mode === "create" ? t("scheduledTasks.panelNew") : (selected?.name ?? "")}
        onClose={closePanel}
        // Escape closes a trigger that is only being looked at; it never drops what is being typed.
        closeOnEscape={mode === "read"}
        labels={{
          panel: t("scheduledTasks.detailPanel"),
          resize: t("scheduledTasks.resizeDetail"),
          expand: t("scheduledTasks.expandDetail"),
          restore: t("scheduledTasks.restoreDetail"),
          close: t("scheduledTasks.closeDetail"),
          backToList: t("scheduledTasks.backToList"),
        }}
        list={
          <>
            <ListTable minWidth={720}>
              <ListHead
                sortGroup={sortGroup}
                columns={[
                  { col: "name" },
                  { col: "fires" },
                  { col: "runs" },
                  { col: "role" },
                  { col: "enabled" },
                  { col: "lastRun" },
                  { col: "nextRun" },
                ]}
              />
              <Table.Tbody>
                {tasks.length === 0 && <ListEmpty colSpan={7}>{t("scheduledTasks.empty")}</ListEmpty>}
                <ListItems
                  state={sortGroup}
                  items={pageItems(sortGroup, page - 1, PAGE_SIZE)}
                  colSpan={7}
                  rowKey={(task) => task.id}
                  render={(task) => (
                    <ListRow
                      key={task.id}
                      testId={`scheduled-task-row-${task.id}`}
                      selected={task.id === selectedId}
                      onClick={() => {
                        if (mode !== "read") return; // a form being filled keeps the panel
                        setSelectedId(task.id === selectedId ? null : task.id);
                      }}
                    >
                      <Table.Td>{task.name}</Table.Td>
                      <Table.Td>
                        <Text ff={task.firesOn === "schedule" ? "monospace" : undefined} fz="sm">
                          {firesLabel(task)}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Stack gap={2}>
                          <Badge variant="light">{kindLabel(task.kind)}</Badge>
                          <Text ff="monospace" fz="xs" c="dimmed" lineClamp={2}>
                            {targetOf(task) || t("scheduledTasks.noTarget")}
                          </Text>
                        </Stack>
                      </Table.Td>
                      <Table.Td>{task.role ?? t("scheduledTasks.noTarget")}</Table.Td>
                      <Table.Td>
                        <Badge color={task.enabled ? "green" : "gray"} variant="light">
                          {task.enabled ? t("scheduledTasks.enabled") : t("scheduledTasks.disabled")}
                        </Badge>
                      </Table.Td>
                      <Table.Td>
                        <Stack gap={2}>
                          <Text fz="sm">{when(task.lastRunAt) ?? t("scheduledTasks.never")}</Text>
                          {outcomeBadge(task.lastOutcome)}
                        </Stack>
                      </Table.Td>
                      <Table.Td>{when(task.nextRunAt) ?? t("scheduledTasks.noNextRun")}</Table.Td>
                    </ListRow>
                  )}
                />
              </Table.Tbody>
            </ListTable>
            {totalPages > 1 && sortGroup.groupBy.length === 0 && (
              <Group justify="flex-end" mt="sm">
                <Pagination total={totalPages} value={page} onChange={setPage} size="sm" />
              </Group>
            )}
          </>
        }
      >
        {panel}
      </SplitPane>
    </Stack>
  );
}
