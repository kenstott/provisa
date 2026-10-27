// Copyright (c) 2026 Kenneth Stott
// Canary: f1cb8af4-3963-453c-959e-e5a4d5503201
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Checkbox, Select, Table, Text, TextInput } from "@mantine/core";
import { Tip } from "../../components/Tip";
import { GrantPicker } from "../../components/GrantPicker";
import { MaskListEditor } from "../../components/MaskListEditor";
import type { RegisteredTable, Source, TableColumn } from "../../types/admin";
import type { Role } from "../../types/auth";
import { fetchIrTypes } from "../../api/admin";
import { IR_TYPES_FALLBACK, shownType, toIrType } from "../../irTypes";
import { NumericSizeInputs } from "./NumericSizeInputs";
import { TestDataColumns } from "./testdata/TestDataColumns";
import { DescriptionField } from "./DescriptionField";
import { ColumnNameCell } from "./ColumnNameCell";
import { ColumnTabs } from "./ColumnTabs";
import { COLUMN_TABS, useColumnTab, type ColumnTab } from "./columnTopics";
import type { ColumnDefinitionLine } from "./ColumnGlossaryHover";
import { VectorModelCell } from "./VectorModelCell";
import { useVectorModels } from "./useVectorModels";
import {
  governanceWithheld,
  masksSummary,
  shownGrants,
  shownGrantsLabel,
  shownMasks,
} from "./helpers";
import type { UpdateEditCol } from "./tableEditTypes";

/** The Columns section of a table's form: the same when it is created and when it is edited. */
export function TableColumnsSection({
  editingTable,
  updateEditCol,
  editSource,
  roles,
  hidingOnly,
  describeColumn,
}: {
  editingTable: RegisteredTable;
  updateEditCol: UpdateEditCol;
  editSource: Source | undefined;
  roles: Role[];
  hidingOnly: boolean;
  /** Writes a column's description from the saved table; absent while it is being created. */
  describeColumn?: (column: string) => Promise<string>;
}) {
  const { t } = useTranslation();
  const roleOptions = roles.map((r) => ({ id: r.id, label: r.id }));
  const [generatingColDesc, setGeneratingColDesc] = useState<string | null>(null);
  // The columns whose mask list is open below their row.
  const [masksOpen, setMasksOpen] = useState<Set<string>>(new Set());
  const toggleMasks = (column: string) =>
    setMasksOpen((open) => {
      const next = new Set(open);
      if (next.has(column)) next.delete(column);
      else next.add(column);
      return next;
    });
  // REQ-846: canonical IR type vocabulary for the per-column Data Type override dropdown, fetched
  // from the backend (authoritative list in provisa/core/ir_types.py); fallback if the fetch fails.
  const [irTypes, setIrTypes] = useState<string[]>(IR_TYPES_FALLBACK);
  useEffect(() => {
    fetchIrTypes()
      .then((types) => {
        if (types.length > 0) setIrTypes(types);
      })
      .catch(() => setIrTypes(IR_TYPES_FALLBACK));
  }, []);
  // REQ-2000: a Qdrant collection's vectors, with the registered models each may name.
  const vectorModels = useVectorModels(
    editingTable.sourceId,
    editingTable.tableName,
    editSource?.type === "qdrant" && !editingTable.queryTemplate,
  );
  const governanceHidden = governanceWithheld(editingTable);
  // The column editor shows a topic at a time. A caller not shown grants is not offered Access;
  // one who may change only how columns are hidden is not offered Testing (their Save writes
  // nothing of a column's test data).
  const shownTabs: ColumnTab[] = COLUMN_TABS.filter(
    (topic) => !(topic === "access" && governanceHidden) && !(topic === "testing" && hidingOnly),
  );
  const [tab, setTab] = useColumnTab(shownTabs);
  // A column's full definition for the hover on its name: every attribute, whichever tab edits it.
  const definitionOf = (c: TableColumn): ColumnDefinitionLine[] => [
    { label: t("tableEditForm.columnHeader"), value: c.columnName },
    { label: t("tableEditForm.sqlAliasHeader"), value: c.alias || c.computedSqlAlias || "" },
    { label: t("tableEditForm.descriptionHeader"), value: c.description || "" },
    ...(editSource?.type === "ingest"
      ? [{ label: t("tableEditForm.jsonPathHeader"), value: c.path || "" }]
      : []),
    { label: t("tableEditForm.dataTypeHeader"), value: shownType(c.dataType, c.subtype) },
    { label: t("tableEditForm.pkHeader"), value: c.isPrimaryKey ? "✓" : "" },
    { label: t("tableEditForm.scopeHeader"), value: c.scope || "domain" },
    ...(governanceHidden
      ? []
      : [
          { label: t("tableEditForm.visibleToHeader"), value: shownGrantsLabel(c.visibleTo, t) },
          { label: t("tableEditForm.maskingHeader"), value: masksSummary(c.masks, t) },
        ]),
  ];
  return (
    <div data-testid="table-section-columns-panel">
      {/* One tab bar for everything of a column: its own attributes, and its test data. */}
      <ColumnTabs value={tab} onChange={setTab} shown={shownTabs} nested />
      {tab === "testing" ? (
        <TestDataColumns table={editingTable} updateEditCol={updateEditCol} />
      ) : (
        <Table.ScrollContainer minWidth={640}>
          <Table className="data-table" style={{ margin: "0 0 0.5rem" }}>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("tableEditForm.columnHeader")}</Table.Th>
                {tab === "naming" && (
                  <>
                    <Table.Th>{t("tableEditForm.sqlAliasHeader")}</Table.Th>
                    <Table.Th w="60%">{t("tableEditForm.descriptionHeader")}</Table.Th>
                    {editSource?.type === "ingest" && (
                      <Table.Th>{t("tableEditForm.jsonPathHeader")}</Table.Th>
                    )}
                  </>
                )}
                {tab === "type" && (
                  <>
                    <Table.Th>{t("tableEditForm.dataTypeHeader")}</Table.Th>
                    <Table.Th>{t("tableEditForm.pkHeader")}</Table.Th>
                    <Table.Th>{t("tableEditForm.scopeHeader")}</Table.Th>
                    {editSource?.type === "qdrant" && (
                      <Table.Th>{t("vectorModelCell.header")}</Table.Th>
                    )}
                  </>
                )}
                {/* REQ-1958: a caller who is not shown a column's grant lists and mask edits
                  the table without them: the Access tab is not offered, the save names none
                  and the store keeps them. */}
                {tab === "access" && (
                  <>
                    <Table.Th>{t("tableEditForm.visibleToHeader")}</Table.Th>
                    <Table.Th>{t("tableEditForm.maskingHeader")}</Table.Th>
                  </>
                )}
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {editingTable.columns.map((c, i) => (
                <Fragment key={c.id}>
                  <Table.Tr data-testid={`column-row-${c.columnName}`}>
                    <ColumnNameCell table={editingTable} c={c} definition={definitionOf(c)} />
                    {tab === "naming" && (
                      <>
                        <Table.Td>
                          <TextInput
                            aria-label={t("tableEditForm.sqlAliasHeader")}
                            value={c.alias || c.computedSqlAlias}
                            onChange={(e) => updateEditCol(i, "alias", e.target.value)}
                            disabled={hidingOnly}
                          />
                        </Table.Td>
                        <Table.Td>
                          {hidingOnly ? (
                            <Text size="sm">{c.description || ""}</Text>
                          ) : (
                            <DescriptionField
                              value={c.description || ""}
                              onChange={(v) => updateEditCol(i, "description", v)}
                              placeholder={t("tableEditForm.descriptionHeader")}
                              rows={1}
                              generating={generatingColDesc === c.columnName}
                              onGenerate={
                                describeColumn &&
                                (async () => {
                                  setGeneratingColDesc(c.columnName);
                                  try {
                                    const desc = await describeColumn(c.columnName);
                                    if (desc) updateEditCol(i, "description", desc);
                                  } catch (err) {
                                    console.error("generateColumnDescription failed:", err);
                                  } finally {
                                    setGeneratingColDesc(null);
                                  }
                                })
                              }
                            />
                          )}
                        </Table.Td>
                        {editSource?.type === "ingest" && (
                          <Table.Td>
                            <TextInput
                              aria-label={t("tableEditForm.jsonPathHeader")}
                              value={c.path || ""}
                              onChange={(e) => updateEditCol(i, "path", e.target.value)}
                              disabled={hidingOnly}
                              placeholder="payload.order_id"
                              data-testid={`table-edit-col-path-${c.columnName}`}
                            />
                          </Table.Td>
                        )}
                      </>
                    )}
                    {tab === "type" && (
                      <>
                        <Table.Td>
                          <Select
                            aria-label={t("tableEditForm.dataTypeHeader")}
                            placeholder={t("tableEditForm.dataTypePlaceholder")}
                            data={Array.from(
                              new Set([...(c.dataType ? [toIrType(c.dataType)] : []), ...irTypes]),
                            ).map((v) => ({ value: v, label: v }))}
                            value={c.dataType ? toIrType(c.dataType) : null}
                            onChange={(v) => updateEditCol(i, "dataType", v ?? "")}
                            disabled={hidingOnly}
                            searchable
                            comboboxProps={{ withinPortal: true }}
                            styles={{ input: { fontFamily: "monospace" } }}
                          />
                          {c.subtype && (
                            // REQ-2171: the subtype the column was registered as, beside its base.
                            <Text
                              size="xs"
                              ff="monospace"
                              c="dimmed"
                              data-testid={`edit-table-col-subtype-${c.columnName}`}
                            >
                              {shownType(c.dataType, c.subtype)}
                            </Text>
                          )}
                          {c.dataType && toIrType(c.dataType) === "numeric" && (
                            <NumericSizeInputs
                              key={c.columnName}
                              dataType={c.dataType}
                              onChange={(v) => updateEditCol(i, "dataType", v)}
                              disabled={hidingOnly}
                              testId={`edit-table-col-numeric-size-${c.columnName}`}
                            />
                          )}
                        </Table.Td>
                        <Table.Td style={{ textAlign: "center" }}>
                          <Tip text={t("tableEditForm.primaryKeyAria")}>
                            <Checkbox
                              aria-label={t("tableEditForm.primaryKeyAria")}
                              checked={c.isPrimaryKey || false}
                              disabled={hidingOnly}
                              onChange={(e) =>
                                updateEditCol(i, "isPrimaryKey", e.currentTarget.checked)
                              }
                            />
                          </Tip>
                        </Table.Td>
                        <Table.Td>
                          <Select
                            aria-label={t("tableEditForm.scopeHeader")}
                            title={t("tableEditForm.scopeHelp")}
                            data={[
                              { value: "domain", label: t("tableEditForm.scopeDomain") },
                              { value: "public", label: t("tableEditForm.scopePublic") },
                              {
                                value: "restricted",
                                label: t("tableEditForm.scopeRestricted"),
                              },
                            ]}
                            value={c.scope || "domain"}
                            onChange={(v) => updateEditCol(i, "scope", v ?? "domain")}
                            disabled={hidingOnly}
                            comboboxProps={{ withinPortal: true }}
                            allowDeselect={false}
                          />
                        </Table.Td>
                        {editSource?.type === "qdrant" && (
                          <Table.Td>
                            <VectorModelCell
                              column={c.columnName}
                              path={c.path}
                              value={c.embeddingModel}
                              models={vectorModels}
                              onChange={(m) => updateEditCol(i, "embeddingModel", m ?? "")}
                            />
                          </Table.Td>
                        )}
                      </>
                    )}
                    {tab === "access" && (
                      <>
                        <Table.Td>
                          <GrantPicker
                            options={roleOptions}
                            value={shownGrants(c.visibleTo)}
                            onChange={(selected) => updateEditCol(i, "visibleTo", selected)}
                            ariaLabel={t("tableEditForm.visibleToHeader")}
                          />
                        </Table.Td>
                        <Table.Td>
                          <Button
                            variant="subtle"
                            size="xs"
                            aria-expanded={masksOpen.has(c.columnName)}
                            onClick={() => toggleMasks(c.columnName)}
                            data-testid={`mask-toggle-${c.columnName}`}
                          >
                            {masksSummary(c.masks, t)}
                          </Button>
                        </Table.Td>
                      </>
                    )}
                  </Table.Tr>
                  {tab === "access" && masksOpen.has(c.columnName) && (
                    <Table.Tr>
                      <Table.Td colSpan={4} style={{ paddingInlineStart: "1.5rem" }}>
                        <MaskListEditor
                          value={shownMasks(c.masks)}
                          onChange={(next) => updateEditCol(i, "masks", next)}
                          roleOptions={roleOptions}
                          testId={`mask-list-${c.columnName}`}
                        />
                      </Table.Td>
                    </Table.Tr>
                  )}
                </Fragment>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </div>
  );
}
