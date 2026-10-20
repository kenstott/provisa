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
import { usePanelDirty } from "../../components/list/panelLeave";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
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
import { DERIVED_SOURCE_ID } from "../../types/admin";
import { buildTableUpdateInput } from "./helpers";
import {
  FormField,
  FormFooter,
  FormIdentifiers,
  FormLayout,
} from "../../components/form/FormLayout";
import type { Registration } from "./draftTable";

type DefinitionMode = "sql" | "metrics";

interface ViewDefinitionFormProps {
  /** Non-null = editing this registered view's definition; null = creating a view. */
  editing: RegisteredTable | null;
  tables: RegisteredTable[];
  relationships: Relationship[];
  domainHints: string[];
  updateTable: (input: Record<string, unknown>) => Promise<MutationResult>;
  /**
   * REQ-2109 (settled): a new view's definition is chosen here; every setting of it follows in
   * the same sections as its editor, and Save there registers it.
   */
  onContinue: (registration: Registration) => void;
  /** An edited definition was saved. */
  onSuccess: () => void;
  onCancel: () => void;
}

export function ViewDefinitionForm({
  editing,
  tables,
  relationships,
  domainHints,
  updateTable,
  onContinue,
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
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(
    editing?.viewMetrics?.metrics ?? [],
  );
  const [selectedDims, setSelectedDims] = useState<string[]>(
    editing?.viewMetrics?.dimensions ?? [],
  );
  const [filtersText, setFiltersText] = useState((editing?.viewMetrics?.filters ?? []).join("\n"));
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  // Leaving the panel with something changed asks first (the panel's one mechanism).
  const state = JSON.stringify([mode, name, domainId, selectedMetrics, selectedDims, filtersText]);
  const [openedWith] = useState(state);
  const [openedAs] = useState(name);
  usePanelDirty(state !== openedWith, openedAs || name);

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
      if (!editing) {
        onContinue({
          input: {
            sourceId: DERIVED_SOURCE_ID,
            domainId: domainId.trim(),
            schemaName: "views",
            tableName: name.trim(),
            alias: name.trim(),
            viewMetrics: spec,
            // View output columns = the spec dimensions plus one column per metric.
            columns: [...selectedDims, ...selectedMetrics].map((n) => ({
              name: n,
              visibleTo: ["*"],
            })),
          },
          pagination: null,
          pagingKind: null,
          pagingCeilingRows: null,
          rowRules: [],
        });
        return;
      }
      // Explicit user action: the definition becomes metric-composed. viewSql is
      // omitted — the server compiles it from the spec (mutually exclusive inputs).
      const result = await updateTable({
        ...buildTableUpdateInput(editing),
        viewSql: undefined,
        viewMetrics: spec,
      });
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
        <FormLayout data-testid="view-definition-sql-panel">
          <FormField size="full">
            <Text size="sm" c="dimmed">
              {editing
                ? t("viewDefinitionForm.sqlEditHint")
                : t("viewDefinitionForm.sqlCreateHint")}
            </Text>
          </FormField>
          {/* Opening the editor is not a save: it keeps its word, first, and Cancel follows. */}
          <FormFooter
            panel
            onCancel={onCancel}
            cancelProps={{ "data-testid": "view-definition-cancel" }}
            action={
              <Button
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
            }
          />
        </FormLayout>
      ) : (
        <FormLayout data-testid="view-definition-metrics-panel">
          {!editing && (
            <FormIdentifiers>
              <FormField size="medium">
                <TextInput
                  label={t("viewDefinitionForm.fieldName")}
                  required
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder={t("viewDefinitionForm.fieldNamePlaceholder")}
                  data-testid="view-definition-name"
                />
              </FormField>
              <FormField size="medium">
                <Select
                  label={t("viewDefinitionForm.fieldDomain")}
                  required
                  searchable
                  data={domainHints}
                  value={domainId || null}
                  onChange={(v) => setDomainId(v ?? "")}
                  data-testid="view-definition-domain"
                />
              </FormField>
            </FormIdentifiers>
          )}
          {/* Creating asks only what a view needs in order to exist: its name, where it
              belongs and its definition. Its description is set in the edit it opens in. */}
          <FormField size="long">
            <MultiSelect
              label={t("viewDefinitionForm.fieldMetrics")}
              options={metrics.map((m) => ({ id: m.name, label: m.name }))}
              value={selectedMetrics}
              onChange={setSelectedMetrics}
              placeholder={t("viewDefinitionForm.fieldMetricsPlaceholder")}
            />
          </FormField>
          <FormField size="long">
            <MultiSelect
              label={t("viewDefinitionForm.fieldDimensions")}
              options={dimensionOptions}
              value={selectedDims}
              onChange={setSelectedDims}
              placeholder={t("viewDefinitionForm.fieldDimensionsPlaceholder")}
            />
          </FormField>
          <FormField size="full" hint={t("viewDefinitionForm.fieldFiltersHelp")}>
            <Textarea
              label={t("viewDefinitionForm.fieldFilters")}
              value={filtersText}
              onChange={(e) => setFiltersText(e.target.value)}
              rows={2}
              styles={{ input: { fontFamily: "monospace" } }}
              data-testid="view-definition-filters"
            />
          </FormField>
          {msg && (
            <FormField size="full">
              <Alert color="red" data-testid="view-definition-error">
                {msg}
              </Alert>
            </FormField>
          )}
          <FormFooter
            panel
            onCancel={onCancel}
            cancelProps={{ "data-testid": "view-definition-cancel" }}
            {...(editing
              ? {
                  onSave: handleSaveMetrics,
                  saving,
                  saveProps: {
                    "data-testid": "view-definition-save",
                    disabled: metricsSaveDisabled,
                  },
                }
              : {
                  // Choosing the definition is not a save: every setting of the view follows.
                  action: (
                    <Button
                      onClick={handleSaveMetrics}
                      disabled={metricsSaveDisabled}
                      data-testid="view-definition-save"
                    >
                      {t("registerTableForm.continue")}
                    </Button>
                  ),
                })}
          />
        </FormLayout>
      )}
    </Stack>
  );
}
