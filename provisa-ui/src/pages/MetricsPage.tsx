// Copyright (c) 2026 Kenneth Stott
// Canary: 76700681-50aa-4a71-a438-bf2c3d3e423e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Gauge } from "lucide-react";
import { PageTitle } from "../components/PageTitle";
import React, { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Group,
  Select,
  Table,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../components/form/FormLayout";
import {
  useMetrics,
  useTables,
  useUpsertMetric,
  useDeleteMetric,
  useDeleteMetrics,
} from "../hooks/useAdminQueries";
import { useListSelection } from "../components/list/listSelection";
import { ListBulkBar } from "../components/list/ListBulkBar";

import type { Metric, ListedTable } from "../types/admin";
import { OssieInterchangePanel } from "./metrics/OssieInterchangePanel";
import { MetricDetailPanel } from "./metrics/MetricDetailPanel";
import { SplitPane } from "../components/list/SplitPane";
import { useRememberedDetail } from "../components/list/useDetailParam";
import { detailListOf, searchNarrowing, shownBy } from "../components/list/leftOut";
import { useUrlFilter } from "../components/list/useUrlFilter";
import { FilterInput } from "../components/admin/FilterInput";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../components/list/useListSortGroup";
import { PageLoading } from "../components/PageLoading";
import { listGlossaryTerms } from "../api/glossary";
import { PageActions } from "../components/PageActions";

interface MetricForm {
  name: string;
  expression: string;
  datatype: string;
  description: string;
  aiContext: string;
  /** REQ-2243: the glossary term the metric names. */
  term: string | null;
}

const EMPTY_FORM: MetricForm = {
  name: "",
  expression: "",
  datatype: "",
  description: "",
  aiContext: "",
  term: null,
};

// The expression-builder shape: AGG(fact.column). A hand-edited expression that
// matches prefills the pickers on edit; anything else leaves them blank.
const AGGREGATES = ["SUM", "AVG", "COUNT", "MIN", "MAX"] as const;
const BUILDER_EXPR_RE =
  /^\s*(SUM|AVG|COUNT|MIN|MAX)\s*\(\s*([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s*\)\s*$/i;

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
  factTables: ListedTable[];
  /** REQ-2243: the glossary terms a metric may name. */
  termNames: string[];
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
  termNames,
  saving,
  msg,
  onSave,
  onCancel,
}: MetricFormCardProps) {
  const { t } = useTranslation();
  return (
    <FormLayout data-testid="metric-form">
      <FormIdentifiers>
        <FormField size="medium">
          <TextInput
            label={t("metricsPage.fieldName")}
            required
            value={form.name}
            disabled={editingName !== null}
            onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
            placeholder={t("metricsPage.fieldNamePlaceholder")}
            data-testid="metric-name-input"
          />
        </FormField>
      </FormIdentifiers>
      {/* REQ-1320/1324: facts are the metric sources — pick fact + measure + aggregate
          to compose the expression; the textarea below stays the escape hatch. Always
          visible so the fact-sourced path is discoverable even before any fact exists. */}
      <FormField
        size="medium"
        hint={factTables.length === 0 ? t("metricsPage.builderNoFacts") : undefined}
      >
        <Select
          label={t("metricsPage.builderFact")}
          value={builder.fact}
          onChange={(v) => applyBuilder({ ...builder, fact: v, column: null })}
          data={factTables.map((tbl) => tbl.tableName)}
          disabled={factTables.length === 0}
          clearable
          searchable
          data-testid="metric-builder-fact"
        />
      </FormField>
      <FormField size="medium">
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
      </FormField>
      <FormField size="short">
        <Select
          label={t("metricsPage.builderAgg")}
          value={builder.agg}
          onChange={(v) => applyBuilder({ ...builder, agg: v })}
          data={[...AGGREGATES]}
          disabled={factTables.length === 0}
          clearable
          data-testid="metric-builder-agg"
        />
      </FormField>
      <FormField size="full">
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
      </FormField>
      <FormField size="medium" hint={t("metricsPage.fieldDatatypeDerived")}>
        <TextInput
          label={t("metricsPage.fieldDatatype")}
          value={form.datatype}
          onChange={(e) => setForm((f) => ({ ...f, datatype: e.target.value }))}
          placeholder={t("metricsPage.fieldDatatypePlaceholder")}
          data-testid="metric-datatype-input"
        />
      </FormField>
      {/* What a metric does not need in order to exist is edited once it does. (Its datatype
          stays in the first step: the builder derives it from the aggregate, in sight.) */}
      {editingName !== null && (
        <>
          <FormField size="full">
            <Textarea
              label={t("metricsPage.fieldDescription")}
              value={form.description}
              onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
              rows={2}
              data-testid="metric-description-input"
            />
          </FormField>
          <FormField size="full">
            <Textarea
              label={t("metricsPage.fieldAiContext")}
              value={form.aiContext}
              onChange={(e) => setForm((f) => ({ ...f, aiContext: e.target.value }))}
              placeholder={t("metricsPage.fieldAiContextPlaceholder")}
              rows={2}
              data-testid="metric-ai-context-input"
            />
          </FormField>
          {/* REQ-2243: the term whose definition and calculation method the metric carries. */}
          <FormField size="medium" hint={t("metricsPage.fieldTermHint")}>
            <Select
              label={t("metricsPage.fieldTerm")}
              value={form.term}
              onChange={(term) => setForm((f) => ({ ...f, term }))}
              data={termNames}
              clearable
              searchable
              data-testid="metric-term-input"
            />
          </FormField>
        </>
      )}
      <FormFooter
        panel
        problem={msg || undefined}
        onSave={onSave}
        saving={saving}
        onCancel={onCancel}
        saveProps={{
          "data-testid": "metric-save-button",
          disabled: !form.name.trim() || !form.expression.trim(),
        }}
        cancelProps={{ "data-testid": "metric-cancel-button" }}
      />
    </FormLayout>
  );
}

// REQ-1317: registered-metrics management page (list / create / edit / delete).
// REQ-1323: detail-then-edit — row click expands the detail panel; Edit/Delete
// live inside it and edit swaps the panel for the inline form (no modals).
/** REQ-2259: ``readOnly`` is the Context group's Metrics page -- the metrics as this page shows
 * them, with no control that writes; editing stays here under Derived. */
export function MetricsPage({ readOnly = false }: { readOnly?: boolean }) {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { t } = useTranslation();
  const { metrics, loading, error, refetch: refetchMetrics } = useMetrics();
  const { tables } = useTables();
  const { upsertMetric, loading: saving } = useUpsertMetric();
  const { deleteMetric } = useDeleteMetric();
  const [creating, setCreating] = useState(false);
  const [editingName, setEditingName] = useState<string | null>(null);
  const [form, setForm] = useState<MetricForm>({ ...EMPTY_FORM });
  const [builder, setBuilder] = useState<BuilderState>({ ...EMPTY_BUILDER });
  // REQ-2241: `?metric=<name>` is the open metric's panel.
  // REQ-2260: and the one left open is open again on coming back.
  // REQ-2277: the list's filter, in the URL. REQ-2277 rule 9: a metric is open only while the
  // list, under it, shows its row.
  const [search, setSearch] = useUrlFilter();
  const searchNarrowed = useMemo(
    () =>
      searchNarrowing<Metric>(
        t,
        search,
        () => setSearch(""),
        // Every text a metric's row shows, and the glossary term it names.
        (m) => [
          m.name,
          m.fromFact ? t("metricsPage.fromFact") : "",
          m.expression,
          m.datatype,
          m.description,
          m.aiContext,
          m.term,
        ],
      ),
    [t, search, setSearch],
  );
  const detail = useRememberedDetail(
    "metrics",
    detailListOf(
      t,
      loading ? null : metrics,
      (m) => m.name,
      (m) => m.name,
      [searchNarrowed],
    ),
  );
  const expanded = detail.id;
  const setExpanded = (name: string | null) => (name === null ? detail.close() : detail.open(name));
  const [msg, setMsg] = useState("");
  // REQ-2243: the glossary terms a metric may name, for the editor's term picker.
  const [termNames, setTermNames] = useState<string[]>([]);
  useEffect(() => {
    // The terms are the editor's picker; the read-only page has no editor.
    if (readOnly) return;
    let live = true;
    listGlossaryTerms("", false).then(
      // REQ-1591: a metric names its term by name; same-named terms of other domains are one
      // option here, and the server refuses a name several terms share.
      (terms) => live && setTermNames([...new Set(terms.map((term) => term.name))]),
      (e: Error) => live && setMsg(e.message),
    );
    return () => {
      live = false;
    };
  }, [readOnly]);

  // REQ-1320: facts are the metric sources — the builder's source picker.
  const factTables = tables.filter((tbl) => tbl.modelingRole === "fact");

  const dependentViews = (name: string) =>
    tables.filter((tbl) => tbl.viewMetrics?.metrics.includes(name)).map((tbl) => tbl.tableName);

  const openCreate = () => {
    // One thing in the panel at a time: the form replaces whatever metric was open.
    setExpanded(null);
    setEditingName(null);
    setForm({ ...EMPTY_FORM });
    setBuilder({ ...EMPTY_BUILDER });
    setMsg("");
    setCreating(true);
  };

  const formOf = (m: Metric): MetricForm => ({
    name: m.name,
    expression: m.expression,
    datatype: m.datatype ?? "",
    description: m.description ?? "",
    aiContext: m.aiContext ?? "",
    term: m.term,
  });
  const builderOf = (m: Metric): BuilderState => {
    const match = BUILDER_EXPR_RE.exec(m.expression);
    return match
      ? { agg: match[1].toUpperCase(), fact: match[2], column: match[3] }
      : { ...EMPTY_BUILDER };
  };

  const openEdit = (m: Metric) => {
    setCreating(false);
    setEditingName(m.name);
    setExpanded(m.name);
    setForm(formOf(m));
    setBuilder(builderOf(m));
    setMsg("");
  };

  // Duplicate: the create form, filled with the metric's definition under a new name. An
  // existing name is refused by the create on save.
  const openDuplicate = (m: Metric) => {
    openCreate();
    setForm({ ...formOf(m), name: `${m.name}_copy` });
    setBuilder(builderOf(m));
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
    const result = await upsertMetric(
      {
        name: form.name.trim(),
        expression: form.expression.trim(),
        datatype: form.datatype.trim() || null,
        description: form.description.trim() || null,
        aiContext: form.aiContext.trim() || null,
        term: form.term,
      },
      // The new-metric form creates; a metric's own detail updates it.
      creating,
    );
    if (result.success && creating) {
      // The metric exists now, with everything else at its defaults: it is selected in the list
      // and its detail opens in edit, where its description and AI context are filled in.
      setCreating(false);
      setExpanded(form.name.trim());
      setEditingName(form.name.trim());
    } else if (result.success) {
      closeForm();
    } else {
      setMsg(result.message || t("metricsPage.saveFailed"));
    }
  };

  // REQ-2198: the panel's Delete (PanelDelete) shows the delete's progress and says why one
  // failed; it resolves once the list is read again without the metric.
  const handleDelete = async (name: string) => {
    const result = await deleteMetric(name);
    if (refusal.refused(result, name)) return false;
    if (!result.success) throw new Error(result.message || t("metricsPage.deleteFailed"));
    await refetchMetrics();
  };

  const formCard = (
    <MetricFormCard
      editingName={editingName}
      form={form}
      setForm={setForm}
      builder={builder}
      applyBuilder={applyBuilder}
      factTables={factTables}
      termNames={termNames}
      saving={saving}
      msg={msg}
      onSave={handleSave}
      onCancel={closeForm}
    />
  );

  // REQ-1940: sort and group are the shared list mechanism.
  const metricColumns = useMemo<ListColumn<Metric>[]>(
    () => [
      { key: "name", label: t("metricsPage.colName"), sortValue: (m) => m.name },
      { key: "expression", label: t("metricsPage.colExpression"), sortValue: (m) => m.expression },
      {
        key: "datatype",
        label: t("metricsPage.colDatatype"),
        sortValue: (m) => m.datatype ?? "",
        groupValue: (m) => m.datatype ?? "",
      },
      {
        key: "description",
        label: t("metricsPage.colDescription"),
        sortValue: (m) => m.description ?? "",
      },
      {
        key: "aiContext",
        label: t("metricsPage.colAiContext"),
        sortValue: (m) => m.aiContext ?? "",
      },
    ],
    [t],
  );
  const shownMetrics = useMemo(() => shownBy(metrics, [searchNarrowed]), [metrics, searchNarrowed]);
  const sortGroup = useListSortGroup(shownMetrics, metricColumns, "metrics");
  // REQ-2240: metrics selected and deleted together.
  const shownMetricNames = useMemo(() => shownMetrics.map((m) => m.name), [shownMetrics]);
  const selection = useListSelection(shownMetricNames);
  const deleteMetrics = useDeleteMetrics();

  // The right-hand panel: the form that creates a metric while it is open, otherwise the selected
  // metric -- its detail, or the form that edits it. Nothing selected: no panel.
  const closePanel = () => {
    closeForm();
    setExpanded(null);
  };
  const selectedMetric =
    expanded === null ? null : (metrics.find((m) => m.name === expanded) ?? null);
  const panelTitle = creating
    ? t("metricsPage.createTitle")
    : selectedMetric
      ? selectedMetric.name
      : "";
  const panelBody = creating ? (
    <div data-testid="metric-create-card">{formCard}</div>
  ) : selectedMetric ? (
    editingName === selectedMetric.name ? (
      // While the form is open the panel's head is the form's: its Save and Cancel.
      formCard
    ) : (
      <MetricDetailPanel
        readOnly={readOnly}
        m={selectedMetric}
        dependentViews={dependentViews(selectedMetric.name)}
        onEdit={() => openEdit(selectedMetric)}
        onDuplicate={() => openDuplicate(selectedMetric)}
        onDelete={() => handleDelete(selectedMetric.name)}
      />
    )
  ) : null;

  return (
    // The list and its panel share the height the heading leaves; each scrolls by itself.
    <div className="page page-sticky-head">
      <div className="page-header">
        <PageTitle icon={Gauge}>{t("metricsPage.title")}</PageTitle>
        <FilterInput
          value={search}
          onChange={setSearch}
          placeholder={t("metricsPage.filterPlaceholder")}
          testId="metrics-filter"
        />
        <PageActions>
          {/* The opener is hidden while a metric form is open (the form ends in Save, Cancel). */}
          {!readOnly && !creating && editingName === null && (
            <Button onClick={openCreate} data-testid="metrics-new-button">
              {t("metricsPage.newMetric")}
            </Button>
          )}
        </PageActions>
      </div>

      {/* REQ-1316: semantic interchange (Ossie) — export endpoint/download + import review. */}
      {/* An import under review can be long: it scrolls in its own box, the list keeps its place. */}
      {!readOnly && (
        <div style={{ maxHeight: "50vh", overflow: "auto" }}>
          <OssieInterchangePanel />
        </div>
      )}

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

      {loading && metrics.length === 0 ? (
        <PageLoading message={t("metricsPage.loading")} />
      ) : (
        <SplitPane
          outOfList={detail.outOfList}
          testId="metrics-split"
          storageKey="provisa.metrics.detailWidth"
          title={panelTitle}
          onClose={closePanel}
          // Escape closes a metric that is only being looked at; it never drops what is typed.
          closeOnEscape={!creating && editingName === null}
          labels={{
            panel: t("metricsPage.detailPanel"),
            resize: t("tablesPage.resizeDetail"),
            expand: t("tablesPage.expandDetail"),
            restore: t("tablesPage.restoreDetail"),
            close: t("tablesPage.closeDetail"),
            backToList: t("tablesPage.backToList"),
          }}
          list={
            <>
              {!readOnly && (
                <ListBulkBar
                  selection={selection}
                  nameOf={(name) => name}
                  onDelete={deleteMetrics}
                  testId="metrics-bulk"
                />
              )}
              <ListTable testId="metrics-table" selection={readOnly ? undefined : selection}>
                <ListHead
                  sortGroup={sortGroup}
                  columns={[
                    { col: "name" },
                    { col: "expression" },
                    { col: "datatype" },
                    { col: "description" },
                    { col: "aiContext" },
                  ]}
                />
                <Table.Tbody>
                  {shownMetrics.length === 0 && (
                    <ListEmpty colSpan={5} testId="metrics-empty">
                      {/* A list a filter emptied says so, never that nothing is registered. */}
                      {metrics.length === 0 ? t("metricsPage.empty") : t("metricsPage.noMatch")}
                    </ListEmpty>
                  )}
                  <ListItems
                    state={sortGroup}
                    colSpan={5}
                    rowKey={(m) => m.name}
                    render={(m) => {
                      const isExpanded = expanded === m.name;
                      return (
                        <React.Fragment key={m.name}>
                          <ListRow
                            testId={`metrics-row-${m.name}`}
                            // The row whose detail the right-hand panel is showing.
                            selected={isExpanded}
                            onClick={() => {
                              // The row opens its detail in the panel, swaps it, or closes it.
                              setExpanded(isExpanded ? null : m.name);
                              closeForm();
                            }}
                          >
                            <Table.Td>
                              <Group gap="0.35rem" wrap="nowrap">
                                <Text size="sm" fw={600} ff="monospace">
                                  {m.name}
                                </Text>
                                {m.fromFact && (
                                  <Badge
                                    size="xs"
                                    variant="light"
                                    data-testid={`metrics-from-fact-${m.name}`}
                                  >
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
                          </ListRow>
                        </React.Fragment>
                      );
                    }}
                  />
                </Table.Tbody>
              </ListTable>
            </>
          }
        >
          {panelBody}
        </SplitPane>
      )}

      {refusal.dialog}
    </div>
  );
}
