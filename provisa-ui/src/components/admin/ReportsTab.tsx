// Copyright (c) 2026 Kenneth Stott
// Canary: 8159e9f3-f071-45bc-aa4b-592d1481a773
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386: admin Reports viewer. A report IS an ops-domain registered table:
// the seeded management views appear automatically, and a custom report is a
// governed Provisa view registered in the ops domain (so it is cataloged,
// exportable, and access-controlled like any table). Rows load through the one
// governed SQL pipeline; sort/filter/group happen client-side in ResultsGrid.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Stack, Table, Text, TextInput, Textarea } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { FileBarChart, X } from "lucide-react";
import { useDeleteTable, useRegisterTable, useTables } from "../../hooks/useAdminQueries";
import { DERIVED_SOURCE_ID } from "../../types/admin";
import type { ListedTable } from "../../types/admin";
import { GovernedTableViewer } from "../GovernedTableViewer";
import { reportCellHref } from "../../lib/reportCellHref";
import { useAuth } from "../../context/AuthContext";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import { CapabilityGate } from "../CapabilityGate";
import { PageTitle } from "../PageTitle";
import { ClampedDetail } from "../../pages/data-products/ClampedDetail";
import { ListRow } from "../list/ListTable";
import { PanelDelete } from "../list/PanelDelete";
import { SortGroupTable } from "../list/SortGroupTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { useRememberedDetail } from "../list/useDetailParam";
import { detailListOf, searchNarrowing, shownBy } from "../list/leftOut";
import { useUrlFilter } from "../list/useUrlFilter";
import { OntologyDriftPanel } from "./OntologyDriftPanel";
import { FilterInput } from "./FilterInput";

/** REQ-1386: drift is computed from the model's history and shown with this report. */
const ONTOLOGY_QUALITY_REPORT = "ontology_quality";

const DESCRIPTION_LINES = 2;

const nameOf = (r: ListedTable) => r.alias || r.tableName;

/**
 * REQ-2277: the object management anatomy -- the header (title, filter, Add), the reports listed,
 * and a row opening the report's panel (?report=<table name>), where its rows are read and a custom
 * report is deleted. Adding a report opens its form in the same panel.
 */
export function ReportsTab({ title }: { title: string }) {
  const { t } = useTranslation();
  // REQ-1620: a report is read as the selected roles, the "Role: All" set included.
  const { actingRoles } = useAuth();
  const { tables, loading: tablesLoading, refetch: refetchTables } = useTables({ inPage: true });
  const { registerTable } = useRegisterTable();
  const { deleteTable } = useDeleteTable();

  const reports = useMemo(
    () =>
      tables
        .filter((tbl) => tbl.domainId === "ops")
        // Alphabetical by the name the row shows, case and accents ignored.
        .sort((a, b) => nameOf(a).localeCompare(nameOf(b), undefined, { sensitivity: "base" })),
    [tables],
  );
  const loaded = !(tablesLoading && reports.length === 0);

  // The open report is named by its table name (`?report=<table name>`), as links into a report
  // have always named it; the viewer remounts per report (key) and owns fetching and grid state.
  // The list is narrowed by name or description. REQ-2277 rule 9: a report is open only while the
  // list shows its row.
  const [filter, setFilter] = useUrlFilter();
  const kindOf = (r: ListedTable) =>
    r.sourceId === DERIVED_SOURCE_ID ? t("reportsTab.kindCustom") : t("reportsTab.kindBuiltIn");
  const narrowings = [
    searchNarrowing<ListedTable>(
      t,
      filter,
      () => setFilter(""),
      // Every text a report's row shows, and the table it reads.
      (r) => [nameOf(r), r.tableName, r.description, kindOf(r)],
    ),
  ];
  const detail = useRememberedDetail(
    "reports",
    detailListOf(t, loaded ? reports : null, (r) => r.tableName, nameOf, narrowings),
  );
  const selected = reports.find((r) => r.tableName === detail.id) ?? null;
  const visibleReports = shownBy(reports, narrowings);

  const [addOpen, setAddOpen] = useState(false);
  const [addName, setAddName] = useState("");
  const [addDescription, setAddDescription] = useState("");
  const [addSql, setAddSql] = useState("");
  const [addSaving, setAddSaving] = useState(false);
  const [addError, setAddError] = useState("");
  const required = useRequiredFields({ name: addName, sql: addSql });

  const openAdd = () => {
    setAddName("");
    setAddDescription("");
    setAddSql("");
    setAddError("");
    setAddOpen(true);
  };

  const handleAdd = async () => {
    if (!required.validate()) return;
    const name = addName.trim();
    setAddSaving(true);
    setAddError("");
    try {
      await registerTable({
        sourceId: DERIVED_SOURCE_ID,
        domainId: "ops",
        schemaName: "views",
        tableName: name,
        alias: name,
        description: addDescription.trim() || undefined,
        viewSql: addSql.trim(),
        columns: [],
      });
      notifications.show({ color: "green", message: t("reportsTab.added", { name }) });
      setAddOpen(false);
      refetchTables();
      detail.open(name);
    } catch (e) {
      setAddError(e instanceof Error ? e.message : String(e));
    } finally {
      setAddSaving(false);
    }
  };

  const handleDelete = async (report: ListedTable) => {
    const result = await deleteTable(report.id);
    if (!result.success) throw new Error(result.message);
    notifications.show({
      color: "green",
      message: t("reportsTab.deleted", { name: nameOf(report) }),
    });
    detail.close({ only: report.tableName });
    refetchTables();
  };

  const addForm = (
    <FormLayout data-testid="report-add-form">
      <FormField size="full">
        <Text size="xs" c="dimmed">
          {t("reportsTab.addHint")}
        </Text>
      </FormField>
      <FormIdentifiers>
        <FormField size="medium">
          <TextInput
            label={t("reportsTab.nameLabel")}
            value={addName}
            onChange={(e) => setAddName(e.currentTarget.value)}
            data-testid="report-name-input"
            {...required.field("name")}
          />
        </FormField>
      </FormIdentifiers>
      <FormField size="long">
        <TextInput
          label={t("reportsTab.descriptionLabel")}
          value={addDescription}
          onChange={(e) => setAddDescription(e.currentTarget.value)}
          data-testid="report-description-input"
        />
      </FormField>
      <FormField size="full">
        <Textarea
          label={t("reportsTab.sqlLabel")}
          value={addSql}
          onChange={(e) => setAddSql(e.currentTarget.value)}
          autosize
          minRows={5}
          styles={{ input: { fontFamily: "monospace" } }}
          data-testid="report-sql-input"
          {...required.field("sql")}
        />
      </FormField>
      {addError && (
        <FormField size="full">
          <Alert color="red" data-testid="report-add-error">
            {addError}
          </Alert>
        </FormField>
      )}
      <FormFooter
        panel
        onSave={() => void handleAdd()}
        saving={addSaving}
        onCancel={() => setAddOpen(false)}
        saveProps={{ "data-testid": "report-save-btn" }}
      />
    </FormLayout>
  );

  const reportView = (report: ListedTable) => (
    <Stack
      gap="sm"
      style={{ minWidth: 0 }}
      data-testid="report-pane"
      data-report={report.tableName}
    >
      {report.sourceId === DERIVED_SOURCE_ID && (
        <CapabilityGate capability="table_registration">
          <PanelActions>
            <fieldset className="panel-action-set">
              <PanelDelete
                itemKey={report.tableName}
                title={t("reportsTab.delete")}
                consequence={t("reportsTab.deleteConfirm", { name: nameOf(report) })}
                ariaLabel={t("reportsTab.delete")}
                testId={`report-delete-${report.tableName}`}
                onDelete={() => handleDelete(report)}
              />
            </fieldset>
          </PanelActions>
        </CapabilityGate>
      )}
      <GovernedTableViewer
        key={report.id}
        table={report}
        role={actingRoles}
        showTitle
        cellHref={reportCellHref}
      />
      {report.tableName === ONTOLOGY_QUALITY_REPORT && <OntologyDriftPanel />}
    </Stack>
  );

  return (
    <>
      <div className="page-header">
        <PageTitle icon={FileBarChart}>{title}</PageTitle>
        <FilterInput
          value={filter}
          onChange={setFilter}
          placeholder={t("reportsTab.filterPlaceholder")}
        />
        <div className="page-actions">
          {/* A report is a view registered in the ops domain, so adding one asks what
              registering a table asks; reading the reports is the ops domain's right. */}
          <CapabilityGate capability="table_registration">
            <Button
              variant={addOpen ? "outline" : "filled"}
              onClick={() => (addOpen ? setAddOpen(false) : openAdd())}
              aria-label={addOpen ? t("reportsTab.closeForm") : undefined}
              data-testid="add-report-btn"
            >
              {addOpen ? <X size={14} /> : t("reportsTab.addTitle")}
            </Button>
          </CapabilityGate>
        </div>
      </div>
      <SplitPane
        outOfList={detail.outOfList}
        testId="reports-split"
        storageKey="provisa.reportsTab.detailWidth"
        title={addOpen ? t("reportsTab.addTitle") : selected !== null ? nameOf(selected) : ""}
        onClose={() => {
          setAddOpen(false);
          detail.close();
        }}
        closeOnEscape={!addOpen}
        labels={{
          panel: t("reportsTab.panelLabel"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          <div data-testid="reports-list">
            <SortGroupTable
              testPrefix="reports"
              rows={loaded ? visibleReports : []}
              columns={[
                { key: "name", label: t("reportsTab.nameLabel"), sortValue: nameOf },
                {
                  key: "description",
                  label: t("reportsTab.descriptionLabel"),
                  sortValue: (r) => r.description ?? "",
                },
                {
                  key: "kind",
                  label: t("reportsTab.kindColumn"),
                  sortValue: kindOf,
                  groupValue: kindOf,
                },
              ]}
              headers={[{ col: "name" }, { col: "description" }, { col: "kind" }]}
              colSpan={3}
              rowKey={(r) => r.tableName}
              empty={
                !loaded
                  ? t("reportsTab.loading")
                  : reports.length === 0
                    ? t("reportsTab.empty")
                    : t("reportsTab.noMatch")
              }
              emptyTestId={loaded && reports.length > 0 ? "reports-no-match" : "reports-empty"}
              render={(report) => (
                <ListRow
                  key={report.id}
                  testId={`report-item-${report.tableName}`}
                  selected={!addOpen && report.tableName === detail.id}
                  onClick={() => {
                    setAddOpen(false);
                    if (detail.id === report.tableName) detail.close();
                    else detail.open(report.tableName);
                  }}
                >
                  <Table.Td>{nameOf(report)}</Table.Td>
                  <Table.Td>
                    {report.description ? (
                      // Held to two lines; a click on the text opens or closes it and stops
                      // there, so it does not open the report (a click elsewhere on the row does).
                      <ClampedDetail
                        testId={`report-description-${report.tableName}`}
                        lines={DESCRIPTION_LINES}
                        // The row's own type, not the browser's button type.
                        buttonStyle={{ font: "inherit", color: "inherit" }}
                        onToggle={(e) => e.stopPropagation()}
                      >
                        {report.description}
                      </ClampedDetail>
                    ) : null}
                  </Table.Td>
                  <Table.Td>{kindOf(report)}</Table.Td>
                </ListRow>
              )}
            />
          </div>
        }
      >
        {addOpen ? addForm : selected !== null ? reportView(selected) : null}
      </SplitPane>
    </>
  );
}
