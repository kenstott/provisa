// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Modal,
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
import { useMetrics, useTables, useUpsertMetric, useDeleteMetric } from "../hooks/useAdminQueries";
import type { Metric, RegisteredTable } from "../types/admin";
import { OssieInterchangePanel } from "./metrics/OssieInterchangePanel";
import { MetricDetailPanel } from "./metrics/MetricDetailPanel";

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

// The expression-builder shape: AGG(fact.column). A hand-edited expression that
// matches prefills the pickers on edit; anything else leaves them blank.
const AGGREGATES = ["SUM", "AVG", "COUNT", "MIN", "MAX"] as const;
const BUILDER_EXPR_RE = /^\s*(SUM|AVG|COUNT|MIN|MAX)\s*\(\s*([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s*\)\s*$/i;

interface BuilderState {
  fact: string | null;
  column: string | null;
  agg: string | null;
}

const EMPTY_BUILDER: BuilderState = { fact: null, column: null, agg: null };

// The aggregate determines the metric datatype: COUNT is always a row count,
// AVG always fractional; SUM/MIN/MAX carry the measure column's own type.
function deriveDatatype(agg: string, columnDataType: string | null): string {
  if (agg === "COUNT") return "bigint";
  if (agg === "AVG") return "numeric";
  return columnDataType ?? "numeric";
}

interface MetricFormCardProps {
  editingName: string | null;
  form: MetricForm;
  setForm: React.Dispatch<React.SetStateAction<MetricForm>>;
  builder: BuilderState;
  applyBuilder: (next: BuilderState) => void;
  factTables: RegisteredTable[];
  saving: boolean;
  msg: string;
  onSave: () => void;
  onCancel: () => void;
}

// Inline create/edit form — the app-wide pattern: no modal, the form renders in
// place (creation card above the table, edit inside the expanded detail row).
function MetricFormCard({
  editingName,
  form,
  setForm,
  builder,
  applyBuilder,
  factTables,
  saving,
  msg,
  onSave,
  onCancel,
}: MetricFormCardProps) {
  const { t } = useTranslation();
  return (
    <Stack gap="sm" data-testid="metric-form">
      <TextInput
        label={t("metricsPage.fieldName")}
        required
        value={form.name}
        disabled={editingName !== null}
        onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
        placeholder={t("metricsPage.fieldNamePlaceholder")}
        data-testid="metric-name-input"
      />
      {/* REQ-1320/1324: facts are the metric sources — pick fact + measure + aggregate
          to compose the expression; the textarea below stays the escape hatch. Always
          visible so the fact-sourced path is discoverable even before any fact exists. */}
      <Group grow align="flex-end">
        <Select
          label={t("metricsPage.builderFact")}
          description={factTables.length === 0 ? t("metricsPage.builderNoFacts") : undefined}
          value={builder.fact}
          onChange={(v) => applyBuilder({ ...builder, fact: v, column: null })}
          data={factTables.map((tbl) => tbl.tableName)}
          disabled={factTables.length === 0}
          clearable
          searchable
          data-testid="metric-builder-fact"
        />
        <Select
          label={t("metricsPage.builderMeasure")}
          value={builder.column}
          onChange={(v) => applyBuilder({ ...builder, column: v })}
          data={
            factTables
              .find((tbl) => tbl.tableName === builder.fact)
              ?.columns.map((c) => c.columnName) ?? []
          }
          disabled={!builder.fact}
          clearable
          searchable
          data-testid="metric-builder-measure"
        />
        <Select
          label={t("metricsPage.builderAgg")}
          value={builder.agg}
          onChange={(v) => applyBuilder({ ...builder, agg: v })}
          data={[...AGGREGATES]}
          disabled={factTables.length === 0}
          clearable
          data-testid="metric-builder-agg"
        />
      </Group>
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
        description={t("metricsPage.fieldDatatypeDerived")}
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
        <Alert color="red" data-testid="metric-form-error">
          {msg}
        </Alert>
      )}
      <Group justify="flex-end" gap="sm">
        <ActionIcon
          variant="subtle"
          aria-label={t("metricsPage.cancel")}
          title={t("metricsPage.cancel")}
          onClick={onCancel}
          data-testid="metric-cancel-button"
        >
          <X size={14} />
        </ActionIcon>
        <ActionIcon
          variant="filled"
          aria-label={t("metricsPage.save")}
          title={t("metricsPage.save")}
          onClick={onSave}
          loading={saving}
          disabled={!form.name.trim() || !form.expression.trim()}
          data-testid="metric-save-button"
        >
          <Check size={14} />
        </ActionIcon>
      </Group>
    </Stack>
  );
}

// REQ-1317: registered-metrics management page (list / create / edit / delete).
// REQ-1323: detail-then-edit — row click expands the detail panel; Edit/Delete
// live inside it and edit swaps the panel for the inline form (no modals).
export function MetricsPage() {
  const { t } = useTranslation();
  const { metrics, loading, error } = useMetrics();
  const { tables } = useTables();
  const { upsertMetric, loading: saving } = useUpsertMetric();
  const { deleteMetric, loading: deleting } = useDeleteMetric();
  const [creating, setCreating] = useState(false);
  const [editingName, setEditingName] = useState<string | null>(null);
  const [form, setForm] = useState<MetricForm>({ ...EMPTY_FORM });
  const [builder, setBuilder] = useState<BuilderState>({ ...EMPTY_BUILDER });
  const [expanded, setExpanded] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [msg, setMsg] = useState("");

  // REQ-1320: facts are the metric sources — the builder's source picker.
  const factTables = tables.filter((tbl) => tbl.modelingRole === "fact");

  const dependentViews = (name: string) =>
    tables
      .filter((tbl) => tbl.viewMetrics?.metrics.includes(name))
      .map((tbl) => tbl.tableName);

  const openCreate = () => {
    setEditingName(null);
    setForm({ ...EMPTY_FORM });
    setBuilder({ ...EMPTY_BUILDER });
    setMsg("");
    setCreating(true);
  };

  const openEdit = (m: Metric) => {
    setCreating(false);
    setEditingName(m.name);
    setExpanded(m.name);
    setForm({
      name: m.name,
      expression: m.expression,
      datatype: m.datatype ?? "",
      description: m.description ?? "",
      aiContext: m.aiContext ?? "",
    });
    const match = BUILDER_EXPR_RE.exec(m.expression);
    setBuilder(
      match
        ? { agg: match[1].toUpperCase(), fact: match[2], column: match[3] }
        : { ...EMPTY_BUILDER },
    );
    setMsg("");
  };

  const closeForm = () => {
    setCreating(false);
    setEditingName(null);
    setMsg("");
  };

  const applyBuilder = (next: BuilderState) => {
    setBuilder(next);
    if (next.fact && next.column && next.agg) {
      const columnType =
        factTables
          .find((tbl) => tbl.tableName === next.fact)
          ?.columns.find((c) => c.columnName === next.column)?.dataType ?? null;
      setForm((f) => ({
        ...f,
        expression: `${next.agg}(${next.fact}.${next.column})`,
        datatype: deriveDatatype(next.agg!, columnType),
      }));
    }
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
      closeForm();
    } else {
      setMsg(result.message || t("metricsPage.saveFailed"));
    }
  };

  const handleDelete = async () => {
    if (!deleteTarget) return;
    const result = await deleteMetric(deleteTarget);
    setDeleteTarget(null);
    if (expanded === deleteTarget) setExpanded(null);
    if (!result.success) setMsg(result.message || t("metricsPage.deleteFailed"));
  };

  const formCard = (
    <MetricFormCard
      editingName={editingName}
      form={form}
      setForm={setForm}
      builder={builder}
      applyBuilder={applyBuilder}
      factTables={factTables}
      saving={saving}
      msg={msg}
      onSave={handleSave}
      onCancel={closeForm}
    />
  );

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
      {msg && !creating && editingName === null && (
        <Alert color="red" mb="sm" withCloseButton onClose={() => setMsg("")}>
          {msg}
        </Alert>
      )}

      {creating && (
        <Paper withBorder p="md" mb="md" data-testid="metric-create-card">
          <Title order={5} mb="sm">
            {t("metricsPage.createTitle")}
          </Title>
          {formCard}
        </Paper>
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
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {metrics.map((m) => {
              const isExpanded = expanded === m.name;
              const isEditing = editingName === m.name;
              return (
                <React.Fragment key={m.name}>
                  <Table.Tr
                    data-testid={`metrics-row-${m.name}`}
                    onClick={() => {
                      setExpanded(isExpanded ? null : m.name);
                      if (isEditing && isExpanded) closeForm();
                    }}
                    style={{
                      cursor: "pointer",
                      background: isExpanded ? "var(--surface)" : undefined,
                    }}
                  >
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
                  </Table.Tr>
                  {isExpanded && (
                    <Table.Tr key={`${m.name}-detail`}>
                      <Table.Td
                        colSpan={5}
                        style={{
                          padding: "0.75rem 1rem",
                          background: "var(--bg)",
                          borderTop: "1px solid var(--border)",
                        }}
                        onClick={(e) => e.stopPropagation()}
                      >
                        {isEditing ? (
                          formCard
                        ) : (
                          <MetricDetailPanel
                            m={m}
                            dependentViews={dependentViews(m.name)}
                            onEdit={() => openEdit(m)}
                            onDelete={() => setDeleteTarget(m.name)}
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
      )}

      {/* Delete confirm */}
      <Modal
        opened={deleteTarget !== null}
        onClose={() => setDeleteTarget(null)}
        title={t("metricsPage.deleteTitle")}
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
