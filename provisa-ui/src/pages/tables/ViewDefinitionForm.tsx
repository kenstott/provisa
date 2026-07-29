// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4b2e6a-1c7f-4a3d-b8e5-0f2a6c9d3e71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1318: the Views page definition-mode toggle — SQL editor vs metric/dimension
// picker. SQL mode routes to the existing SQL editor unchanged; Metrics mode is a
// declarative picker that registers through the real viewMetrics input (the server
// generates the view SQL — the UI never emits joins or aggregation, REQ-1321).
// Editing a metric view opens in Metrics mode prefilled; a free-hand view opens in
// SQL mode. Switching modes happens only through this control — never silently.

import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  SegmentedControl,
  Select,
  Stack,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { MultiSelect } from "../../components/MultiSelect";
import { useMetrics } from "../../hooks/useAdminQueries";
import { metricDimensionTables } from "../sql/sqlHelpers";
import type { MutationResult, RegisteredTable, Relationship } from "../../types/admin";
import { buildTableUpdateInput } from "./helpers";

type DefinitionMode = "sql" | "metrics";

interface ViewDefinitionFormProps {
  /** Non-null = editing this registered view's definition; null = creating a view. */
  editing: RegisteredTable | null;
  tables: RegisteredTable[];
  relationships: Relationship[];
  domainHints: string[];
  registerTable: (input: Record<string, unknown>) => Promise<MutationResult>;
  updateTable: (input: Record<string, unknown>) => Promise<MutationResult>;
  onSuccess: () => void;
  onCancel: () => void;
}

export function ViewDefinitionForm({
  editing,
  tables,
  relationships,
  domainHints,
  registerTable,
  updateTable,
  onSuccess,
  onCancel,
}: ViewDefinitionFormProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { metrics } = useMetrics();

  // A metric view opens in Metrics mode prefilled; a free-hand view opens in SQL mode.
  const [mode, setMode] = useState<DefinitionMode>(editing?.viewMetrics ? "metrics" : "sql");
  const [name, setName] = useState(editing ? editing.alias || editing.tableName : "");
  const [domainId, setDomainId] = useState(editing?.domainId ?? "");
  const [description, setDescription] = useState(editing?.description ?? "");
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(
    editing?.viewMetrics?.metrics ?? [],
  );
  const [selectedDims, setSelectedDims] = useState<string[]>(
    editing?.viewMetrics?.dimensions ?? [],
  );
  const [filtersText, setFiltersText] = useState((editing?.viewMetrics?.filters ?? []).join("\n"));
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");

  // Valid dimensions: columns of tables referenced by the chosen metrics' expressions,
  // plus tables one registered-relationship hop away (mirrors the server's resolution).
  const dimensionOptions = useMemo(() => {
    const seen = new Map<string, string>(); // bare column name → "table.column" label
    for (const metricName of selectedMetrics) {
      const metric = metrics.find((m) => m.name === metricName);
      if (!metric) continue;
      for (const tbl of metricDimensionTables(metric, tables, relationships)) {
        for (const col of tbl.columns) {
          if (!seen.has(col.columnName))
            seen.set(col.columnName, `${tbl.tableName}.${col.columnName}`);
        }
      }
    }
    // Keep already-selected dimensions selectable when editing an existing spec.
    for (const dim of selectedDims) if (!seen.has(dim)) seen.set(dim, dim);
    return [...seen.entries()].map(([id, label]) => ({ id, label }));
  }, [selectedMetrics, selectedDims, metrics, tables, relationships]);

  const handleSaveMetrics = async () => {
    setMsg("");
    setSaving(true);
    try {
      const spec = {
        metrics: selectedMetrics,
        dimensions: selectedDims,
        filters: filtersText
          .split("\n")
          .map((f) => f.trim())
          .filter(Boolean),
      };
      let result: MutationResult;
      if (editing) {
        // Explicit user action: the definition becomes metric-composed. viewSql is
        // omitted — the server compiles it from the spec (mutually exclusive inputs).
        result = await updateTable({
          ...buildTableUpdateInput(editing),
          viewSql: undefined,
          viewMetrics: spec,
        });
      } else {
        result = await registerTable({
          sourceId: "__provisa__",
          domainId: domainId.trim(),
          schemaName: "views",
          tableName: name.trim(),
          alias: name.trim(),
          description: description.trim() || undefined,
          viewMetrics: spec,
          // View output columns = the spec dimensions plus one column per metric.
          columns: [...selectedDims, ...selectedMetrics].map((n) => ({
            name: n,
            visibleTo: ["*"],
          })),
        });
      }
      if (result.success) onSuccess();
      else setMsg(result.message || t("viewDefinitionForm.saveFailed"));
    } catch (e) {
      setMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const metricsSaveDisabled =
    selectedMetrics.length === 0 || (!editing && (!name.trim() || !domainId.trim()));

  return (
    <Stack gap="sm" data-testid="view-definition-form">
      <SegmentedControl
        value={mode}
        onChange={(v) => setMode(v as DefinitionMode)}
        data={[
          { label: t("viewDefinitionForm.modeSql"), value: "sql" },
          { label: t("viewDefinitionForm.modeMetrics"), value: "metrics" },
        ]}
        data-testid="view-definition-mode"
      />

      {mode === "sql" ? (
        <Stack gap="sm" data-testid="view-definition-sql-panel">
          <Text size="sm" c="dimmed">
            {editing
              ? t("viewDefinitionForm.sqlEditHint")
              : t("viewDefinitionForm.sqlCreateHint")}
          </Text>
          <Group gap="sm">
            <Button
              variant="default"
              data-testid="view-definition-open-sql"
              onClick={() =>
                editing
                  ? navigate("/sql", { state: { sql: editing.viewSql, viewTable: editing } })
                  : navigate("/sql")
              }
            >
              {editing
                ? t("viewDefinitionForm.editInSqlEditor")
                : t("viewDefinitionForm.openSqlEditor")}
            </Button>
            <Button variant="subtle" onClick={onCancel} data-testid="view-definition-cancel">
              {t("viewDefinitionForm.cancel")}
            </Button>
          </Group>
        </Stack>
      ) : (
        <Stack gap="sm" data-testid="view-definition-metrics-panel">
          {!editing && (
            <Group grow>
              <TextInput
                label={t("viewDefinitionForm.fieldName")}
                required
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder={t("viewDefinitionForm.fieldNamePlaceholder")}
                data-testid="view-definition-name"
              />
              <Select
                label={t("viewDefinitionForm.fieldDomain")}
                required
                searchable
                data={domainHints}
                value={domainId || null}
                onChange={(v) => setDomainId(v ?? "")}
                data-testid="view-definition-domain"
              />
            </Group>
          )}
          {!editing && (
            <Textarea
              label={t("viewDefinitionForm.fieldDescription")}
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={2}
              data-testid="view-definition-description"
            />
          )}
          <MultiSelect
            label={t("viewDefinitionForm.fieldMetrics")}
            options={metrics.map((m) => ({ id: m.name, label: m.name }))}
            value={selectedMetrics}
            onChange={setSelectedMetrics}
            placeholder={t("viewDefinitionForm.fieldMetricsPlaceholder")}
          />
          <MultiSelect
            label={t("viewDefinitionForm.fieldDimensions")}
            options={dimensionOptions}
            value={selectedDims}
            onChange={setSelectedDims}
            placeholder={t("viewDefinitionForm.fieldDimensionsPlaceholder")}
          />
          <Textarea
            label={t("viewDefinitionForm.fieldFilters")}
            description={t("viewDefinitionForm.fieldFiltersHelp")}
            value={filtersText}
            onChange={(e) => setFiltersText(e.target.value)}
            rows={2}
            styles={{ input: { fontFamily: "monospace" } }}
            data-testid="view-definition-filters"
          />
          {msg && (
            <Alert color="red" data-testid="view-definition-error">
              {msg}
            </Alert>
          )}
          <Group justify="flex-end" gap="sm">
            <Button variant="default" onClick={onCancel} data-testid="view-definition-cancel">
              {t("viewDefinitionForm.cancel")}
            </Button>
            <Button
              onClick={handleSaveMetrics}
              loading={saving}
              disabled={metricsSaveDisabled}
              data-testid="view-definition-save"
            >
              {t("viewDefinitionForm.save")}
            </Button>
          </Group>
        </Stack>
      )}
    </Stack>
  );
}
