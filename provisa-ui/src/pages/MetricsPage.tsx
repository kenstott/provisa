// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Modal,
  Stack,
  Table,
  Text,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import { Pencil, Plus, Trash2 } from "lucide-react";
import { useMetrics, useUpsertMetric, useDeleteMetric } from "../hooks/useAdminQueries";
import type { Metric } from "../types/admin";
import { OssieInterchangePanel } from "./metrics/OssieInterchangePanel";

interface MetricForm {
  name: string;
  expression: string;
  datatype: string;
  description: string;
  aiContext: string;
}

const EMPTY_FORM: MetricForm = {
  name: "",
  expression: "",
  datatype: "",
  description: "",
  aiContext: "",
};

// REQ-1317: registered-metrics management page (list / create / edit / delete).
export function MetricsPage() {
  const { t } = useTranslation();
  const { metrics, loading, error } = useMetrics();
  const { upsertMetric, loading: saving } = useUpsertMetric();
  const { deleteMetric, loading: deleting } = useDeleteMetric();
  const [modalOpen, setModalOpen] = useState(false);
  const [editingName, setEditingName] = useState<string | null>(null);
  const [form, setForm] = useState<MetricForm>({ ...EMPTY_FORM });
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [msg, setMsg] = useState("");

  const openCreate = () => {
    setEditingName(null);
    setForm({ ...EMPTY_FORM });
    setMsg("");
    setModalOpen(true);
  };

  const openEdit = (m: Metric) => {
    setEditingName(m.name);
    setForm({
      name: m.name,
      expression: m.expression,
      datatype: m.datatype ?? "",
      description: m.description ?? "",
      aiContext: m.aiContext ?? "",
    });
    setMsg("");
    setModalOpen(true);
  };

  const handleSave = async () => {
    setMsg("");
    const result = await upsertMetric({
      name: form.name.trim(),
      expression: form.expression.trim(),
      datatype: form.datatype.trim() || null,
      description: form.description.trim() || null,
      aiContext: form.aiContext.trim() || null,
    });
    if (result.success) {
      setModalOpen(false);
    } else {
      setMsg(result.message || t("metricsPage.saveFailed"));
    }
  };

  const handleDelete = async () => {
    if (!deleteTarget) return;
    const result = await deleteMetric(deleteTarget);
    setDeleteTarget(null);
    if (!result.success) setMsg(result.message || t("metricsPage.deleteFailed"));
  };

  return (
    <div style={{ flex: 1, overflow: "auto", padding: "1rem 1.25rem" }}>
      <Group justify="space-between" mb="md">
        <Title order={3}>{t("metricsPage.title")}</Title>
        <Button
          size="xs"
          leftSection={<Plus size={13} />}
          onClick={openCreate}
          data-testid="metrics-new-button"
        >
          {t("metricsPage.newMetric")}
        </Button>
      </Group>

      {/* REQ-1316: semantic interchange (Ossie) — export endpoint/download + import review. */}
      <OssieInterchangePanel />

      {error && (
        <Alert color="red" mb="sm">
          {error.message}
        </Alert>
      )}
      {msg && (
        <Alert color="red" mb="sm" withCloseButton onClose={() => setMsg("")}>
          {msg}
        </Alert>
      )}

      {loading && metrics.length === 0 ? (
        <Text size="sm" c="var(--text-muted)">
          {t("metricsPage.loading")}
        </Text>
      ) : metrics.length === 0 ? (
        <Text size="sm" c="var(--text-muted)" data-testid="metrics-empty">
          {t("metricsPage.empty")}
        </Text>
      ) : (
        <Table striped highlightOnHover data-testid="metrics-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("metricsPage.colName")}</Table.Th>
              <Table.Th>{t("metricsPage.colExpression")}</Table.Th>
              <Table.Th>{t("metricsPage.colDatatype")}</Table.Th>
              <Table.Th>{t("metricsPage.colDescription")}</Table.Th>
              <Table.Th>{t("metricsPage.colAiContext")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {metrics.map((m) => (
              <Table.Tr key={m.name} data-testid={`metrics-row-${m.name}`}>
                <Table.Td>
                  <Group gap="0.35rem" wrap="nowrap">
                    <Text size="sm" fw={600} ff="monospace">
                      {m.name}
                    </Text>
                    {m.fromFact && (
                      <Badge size="xs" variant="light" data-testid={`metrics-from-fact-${m.name}`}>
                        {t("metricsPage.fromFact")}
                      </Badge>
                    )}
                  </Group>
                </Table.Td>
                <Table.Td>
                  <Text size="xs" ff="monospace" style={{ whiteSpace: "pre-wrap" }}>
                    {m.expression}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Text size="xs" ff="monospace">
                    {m.datatype ?? ""}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Text size="xs">{m.description ?? ""}</Text>
                </Table.Td>
                <Table.Td>
                  <Text size="xs" c="var(--text-muted)">
                    {m.aiContext ?? ""}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Group gap="0.25rem" wrap="nowrap" justify="flex-end">
                    <ActionIcon
                      variant="subtle"
                      size="sm"
                      aria-label={t("metricsPage.editMetric", { name: m.name })}
                      onClick={() => openEdit(m)}
                      data-testid={`metrics-edit-${m.name}`}
                    >
                      <Pencil size={13} />
                    </ActionIcon>
                    <ActionIcon
                      variant="subtle"
                      color="red"
                      size="sm"
                      aria-label={t("metricsPage.deleteMetric", { name: m.name })}
                      onClick={() => setDeleteTarget(m.name)}
                      data-testid={`metrics-delete-${m.name}`}
                    >
                      <Trash2 size={13} />
                    </ActionIcon>
                  </Group>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}

      {/* Create / edit modal */}
      <Modal
        opened={modalOpen}
        onClose={() => setModalOpen(false)}
        title={
          <Title order={4}>
            {editingName ? t("metricsPage.editTitle") : t("metricsPage.createTitle")}
          </Title>
        }
        size="lg"
        data-testid="metric-modal"
      >
        <Stack gap="sm">
          <TextInput
            label={t("metricsPage.fieldName")}
            required
            value={form.name}
            disabled={editingName !== null}
            onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
            placeholder={t("metricsPage.fieldNamePlaceholder")}
            data-testid="metric-name-input"
          />
          <Textarea
            label={t("metricsPage.fieldExpression")}
            required
            value={form.expression}
            onChange={(e) => setForm((f) => ({ ...f, expression: e.target.value }))}
            placeholder={t("metricsPage.fieldExpressionPlaceholder")}
            rows={3}
            styles={{ input: { fontFamily: "monospace" } }}
            data-testid="metric-expression-input"
          />
          <TextInput
            label={t("metricsPage.fieldDatatype")}
            value={form.datatype}
            onChange={(e) => setForm((f) => ({ ...f, datatype: e.target.value }))}
            placeholder={t("metricsPage.fieldDatatypePlaceholder")}
            data-testid="metric-datatype-input"
          />
          <Textarea
            label={t("metricsPage.fieldDescription")}
            value={form.description}
            onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
            rows={2}
            data-testid="metric-description-input"
          />
          <Textarea
            label={t("metricsPage.fieldAiContext")}
            value={form.aiContext}
            onChange={(e) => setForm((f) => ({ ...f, aiContext: e.target.value }))}
            placeholder={t("metricsPage.fieldAiContextPlaceholder")}
            rows={2}
            data-testid="metric-ai-context-input"
          />
          {msg && (
            <Alert color="red" data-testid="metric-modal-error">
              {msg}
            </Alert>
          )}
          <Group justify="flex-end" gap="sm">
            <Button variant="default" onClick={() => setModalOpen(false)}>
              {t("metricsPage.cancel")}
            </Button>
            <Button
              onClick={handleSave}
              loading={saving}
              disabled={!form.name.trim() || !form.expression.trim()}
              data-testid="metric-save-button"
            >
              {t("metricsPage.save")}
            </Button>
          </Group>
        </Stack>
      </Modal>

      {/* Delete confirm */}
      <Modal
        opened={deleteTarget !== null}
        onClose={() => setDeleteTarget(null)}
        title={<Title order={4}>{t("metricsPage.deleteTitle")}</Title>}
        centered
        data-testid="metric-delete-modal"
      >
        <Text mb="lg" size="sm">
          {t("metricsPage.deleteConfirm", { name: deleteTarget ?? "" })}
        </Text>
        <Group justify="flex-end" gap="sm">
          <Button variant="default" onClick={() => setDeleteTarget(null)}>
            {t("metricsPage.cancel")}
          </Button>
          <Button
            color="red"
            onClick={handleDelete}
            loading={deleting}
            data-testid="metric-delete-confirm"
          >
            {t("metricsPage.delete")}
          </Button>
        </Group>
      </Modal>
    </div>
  );
}
