// Copyright (c) 2026 Kenneth Stott
// Canary: ce243a88-d4cf-41a3-99e0-741e6acd3802
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
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
  Textarea,
} from "@mantine/core";
import { X } from "lucide-react";
import {
  useScheduledTasks,
  useToggleScheduledTask,
  useCreateScheduledTask,
  useDeleteScheduledTask,
} from "../../hooks/useAdminQueries";
import { fetchActions, type TrackedWebhook } from "../../api/actions";

const PAGE_SIZE = 50;

type TriggerKind = "webhook" | "sql";

export function ScheduledTasks() {
  const { t } = useTranslation();
  const { scheduledTasks: tasks, loading } = useScheduledTasks();
  const { toggleScheduledTask } = useToggleScheduledTask();
  const { createScheduledTask } = useCreateScheduledTask();
  const { deleteScheduledTask } = useDeleteScheduledTask();
  const [toggling, setToggling] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [taskPage, setTaskPage] = useState(1);

  const [webhooks, setWebhooks] = useState<TrackedWebhook[]>([]);
  useEffect(() => {
    fetchActions().then((a) => setWebhooks(a.webhooks)).catch(() => {});
  }, []);

  const [showForm, setShowForm] = useState(false);
  const [newKind, setNewKind] = useState<TriggerKind>("webhook");
  const [newId, setNewId] = useState("");
  const [newName, setNewName] = useState("");
  const [newCron, setNewCron] = useState("");
  const [newWebhookName, setNewWebhookName] = useState("");
  const [newSql, setNewSql] = useState("");
  const [argValues, setArgValues] = useState<Record<string, string>>({});
  const [formMsg, setFormMsg] = useState("");
  const [creating, setCreating] = useState(false);

  const selectedWebhook = webhooks.find((w) => w.name === newWebhookName) ?? null;

  const resetForm = () => {
    setNewId("");
    setNewName("");
    setNewCron("");
    setNewWebhookName("");
    setNewSql("");
    setArgValues({});
  };

  const handleToggle = async (id: string, enabled: boolean) => {
    setToggling(id);
    await toggleScheduledTask(id, enabled);
    setToggling(null);
  };

  const handleDelete = async (id: string) => {
    setDeleting(id);
    await deleteScheduledTask(id);
    setDeleting(null);
  };

  const handleCreate = async () => {
    if (!newId.trim() || !newName.trim() || !newCron.trim()) {
      setFormMsg(t("scheduledTasks.validationRequired"));
      return;
    }
    if (newKind === "webhook" && !newWebhookName) {
      setFormMsg(t("scheduledTasks.validationWebhookRequired"));
      return;
    }
    if (newKind === "sql" && !newSql.trim()) {
      setFormMsg(t("scheduledTasks.validationSqlRequired"));
      return;
    }
    setCreating(true);
    setFormMsg("");
    const result = await createScheduledTask(
      newKind === "webhook"
        ? {
            id: newId.trim(),
            name: newName.trim(),
            cron: newCron.trim(),
            kind: "webhook",
            webhookName: newWebhookName,
            argsJson: Object.keys(argValues).length
              ? JSON.stringify(argValues)
              : undefined,
          }
        : {
            id: newId.trim(),
            name: newName.trim(),
            cron: newCron.trim(),
            kind: "sql",
            sql: newSql.trim(),
          },
    );
    setCreating(false);
    if (result.success) {
      setShowForm(false);
      resetForm();
    } else {
      setFormMsg(result.message);
    }
  };

  if (loading) return <Text>{t("scheduledTasks.loading")}</Text>;

  const totalPages = Math.max(1, Math.ceil(tasks.length / PAGE_SIZE));
  const paged = tasks.slice((taskPage - 1) * PAGE_SIZE, taskPage * PAGE_SIZE);

  return (
    <Stack gap="md">
      <Group justify="flex-end">
        <Button
          data-testid="scheduled-tasks-toggle-form"
          onClick={() => {
            setShowForm((v) => !v);
            setFormMsg("");
          }}
          leftSection={showForm ? <X size={14} /> : undefined}
          aria-expanded={showForm}
        >
          {showForm ? t("scheduledTasks.close") : t("scheduledTasks.addButton")}
        </Button>
      </Group>

      {showForm && (
        <Card withBorder padding="md">
          <Stack gap="sm" maw={480}>
            <Select
              label={t("scheduledTasks.kindLabel")}
              aria-label={t("scheduledTasks.kindLabel")}
              data={[
                { value: "webhook", label: t("scheduledTasks.kindWebhook") },
                { value: "sql", label: t("scheduledTasks.kindSql") },
              ]}
              value={newKind}
              onChange={(v) => {
                setNewKind((v as TriggerKind) ?? "webhook");
                setFormMsg("");
              }}
              allowDeselect={false}
            />
            <TextInput
              label={t("scheduledTasks.idLabel")}
              value={newId}
              onChange={(e) => setNewId(e.currentTarget.value)}
              placeholder={t("scheduledTasks.idPlaceholder")}
            />
            <TextInput
              label={t("scheduledTasks.nameLabel")}
              value={newName}
              onChange={(e) => setNewName(e.currentTarget.value)}
              placeholder={t("scheduledTasks.namePlaceholder")}
            />
            <TextInput
              label={t("scheduledTasks.cronLabel")}
              value={newCron}
              onChange={(e) => setNewCron(e.currentTarget.value)}
              placeholder={t("scheduledTasks.cronPlaceholder")}
            />

            {newKind === "webhook" ? (
              <>
                <Select
                  label={t("scheduledTasks.webhookLabel")}
                  placeholder={t("scheduledTasks.webhookPlaceholder")}
                  data={webhooks.map((w) => ({ value: w.name, label: w.name }))}
                  value={newWebhookName || null}
                  onChange={(v) => {
                    setNewWebhookName(v ?? "");
                    setArgValues({});
                  }}
                />
                {selectedWebhook?.arguments.map((arg) => (
                  <TextInput
                    key={arg.name}
                    label={
                      <>
                        {arg.name}{" "}
                        <Text span c="dimmed" fz="xs">
                          ({arg.type})
                        </Text>
                      </>
                    }
                    value={argValues[arg.name] ?? ""}
                    onChange={(e) =>
                      setArgValues((prev) => ({ ...prev, [arg.name]: e.currentTarget.value }))
                    }
                    placeholder={arg.type}
                  />
                ))}
              </>
            ) : (
              <Stack gap={4}>
                <Textarea
                  label={t("scheduledTasks.sqlLabel")}
                  aria-label={t("scheduledTasks.sqlLabel")}
                  value={newSql}
                  onChange={(e) => setNewSql(e.currentTarget.value)}
                  placeholder={t("scheduledTasks.sqlPlaceholder")}
                  rows={4}
                  styles={{ input: { fontFamily: "monospace" } }}
                />
                <Text c="dimmed" fz="xs">
                  {t("scheduledTasks.sqlTokensHint")}
                </Text>
              </Stack>
            )}

            {formMsg && (
              <Text c="red" fz="sm">
                {formMsg}
              </Text>
            )}
            <Group>
              <Button
                data-testid="scheduled-tasks-submit"
                onClick={handleCreate}
                disabled={creating}
              >
                {creating ? t("scheduledTasks.creating") : t("scheduledTasks.addButton")}
              </Button>
            </Group>
          </Stack>
        </Card>
      )}

      {tasks.length === 0 ? (
        <Text c="dimmed">{t("scheduledTasks.empty")}</Text>
      ) : (
        <>
          <Table.ScrollContainer minWidth={800}>
            <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("scheduledTasks.colId")}</Table.Th>
                  <Table.Th>{t("scheduledTasks.colName")}</Table.Th>
                  <Table.Th>{t("scheduledTasks.colCron")}</Table.Th>
                  <Table.Th>{t("scheduledTasks.colKind")}</Table.Th>
                  <Table.Th>{t("scheduledTasks.colTarget")}</Table.Th>
                  <Table.Th>{t("scheduledTasks.colEnabled")}</Table.Th>
                  <Table.Th>{t("scheduledTasks.colLastRun")}</Table.Th>
                  <Table.Th>{t("scheduledTasks.colNextRun")}</Table.Th>
                  <Table.Th>
                    <Text span visibleFrom="xs" fz="sm" fw={600}>
                      {t("scheduledTasks.colActions")}
                    </Text>
                  </Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {paged.map((task) => (
                  <Table.Tr key={task.id}>
                    <Table.Td>
                      <Text ff="monospace" fz="sm">
                        {task.id}
                      </Text>
                    </Table.Td>
                    <Table.Td>{task.name}</Table.Td>
                    <Table.Td>
                      <Text ff="monospace" fz="sm">
                        {task.cronExpression}
                      </Text>
                    </Table.Td>
                    <Table.Td>
                      <Badge variant="light">{task.kind}</Badge>
                    </Table.Td>
                    <Table.Td maw={300}>
                      {task.kind === "sql" ? (
                        <Text ff="monospace" fz="sm">
                          {task.sql}
                        </Text>
                      ) : (
                        task.webhookUrl || t("scheduledTasks.noTarget")
                      )}
                    </Table.Td>
                    <Table.Td>
                      <Badge color={task.enabled ? "green" : "gray"} variant="light">
                        {task.enabled ? t("scheduledTasks.enabled") : t("scheduledTasks.disabled")}
                      </Badge>
                    </Table.Td>
                    <Table.Td>
                      {task.lastRunAt
                        ? new Date(task.lastRunAt).toLocaleString()
                        : t("scheduledTasks.never")}
                    </Table.Td>
                    <Table.Td>
                      {task.nextRunAt
                        ? new Date(task.nextRunAt).toLocaleString()
                        : t("scheduledTasks.noNextRun")}
                    </Table.Td>
                    <Table.Td>
                      <Group gap="xs" wrap="nowrap">
                        <Button
                          size="compact-xs"
                          onClick={() => handleToggle(task.id, !task.enabled)}
                          disabled={toggling === task.id}
                        >
                          {toggling === task.id
                            ? t("scheduledTasks.working")
                            : task.enabled
                              ? t("scheduledTasks.disable")
                              : t("scheduledTasks.enable")}
                        </Button>
                        <Button
                          size="compact-xs"
                          color="red"
                          variant="light"
                          onClick={() => handleDelete(task.id)}
                          disabled={deleting === task.id}
                        >
                          {deleting === task.id ? t("scheduledTasks.working") : t("scheduledTasks.delete")}
                        </Button>
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
          {totalPages > 1 && (
            <Group justify="flex-end">
              <Pagination total={totalPages} value={taskPage} onChange={setTaskPage} size="sm" />
            </Group>
          )}
        </>
      )}
    </Stack>
  );
}
