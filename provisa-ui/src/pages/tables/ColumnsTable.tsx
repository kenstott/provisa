// Copyright (c) 2026 Kenneth Stott
// Canary: 5c1e8126-83d6-4b4d-afb2-d8812e3b10fb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The per-column editor table of the Register Table form, extracted so RegisterTableForm stays
// within its line budget (REQ-788 wiring needed the room). Pure presentation over the form's
// column state; updateCol mutates one column's one field.

import { Tip } from "../../components/Tip";
import { Fragment, useState } from "react";
import { Button, Checkbox, Select, Table, Text, TextInput } from "@mantine/core";
import { MultiSelect } from "../../components/MultiSelect";
import { useTranslation } from "react-i18next";

import { shownType, toIrType } from "../../irTypes";
import { NumericSizeInputs } from "./NumericSizeInputs";
import type { Role } from "../../types/auth";
import type { ColumnForm } from "./types";
import { MaskListEditor } from "../../components/MaskListEditor";
import type { MaskEntry } from "../../lib/masks";
import { masksSummary } from "./helpers";
import { ColumnGlossaryHover, type ColumnDefinitionLine } from "./ColumnGlossaryHover";
import { ColumnTabs } from "./ColumnTabs";
import { ColumnBadge } from "./ColumnBadge";
import { METADATA_TABS, useColumnTab } from "./columnTopics";

export function ColumnsTable({
  columns,
  updateCol,
  roles,
  irTypes,
  sourceType,
  readOnly = false,
  tableId,
  testId,
}: {
  columns: ColumnForm[];
  /** Absent in read-only mode: nothing is edited. */
  updateCol?: (
    i: number,
    key: keyof ColumnForm,
    value: string | boolean | string[] | MaskEntry[],
  ) => void;
  roles: Role[];
  irTypes: string[];
  sourceType: string;
  /** REQ-2239 (amended 2026-11-11): the same table, read-only -- a column of a data product's
   *  contract looks as it does here, its mask a badge beside its name. */
  readOnly?: boolean;
  /** The registered table the columns are of, for their glossary hover. */
  tableId?: number;
  testId?: string;
}) {
  if (readOnly) {
    return (
      <ReadOnlyColumns
        columns={columns}
        sourceType={sourceType}
        tableId={tableId}
        testId={testId}
      />
    );
  }
  if (updateCol === undefined) throw new Error("ColumnsTable: updateCol is required to edit");
  return (
    <EditableColumns
      columns={columns}
      updateCol={updateCol}
      roles={roles}
      irTypes={irTypes}
      sourceType={sourceType}
    />
  );
}

/** A column's every attribute, as the hover lists it. */
function useDefinitionOf(sourceType: string) {
  const { t } = useTranslation();
  return (col: ColumnForm): ColumnDefinitionLine[] => [
    { label: t("registerTableForm.colHeaderColumn"), value: col.name },
    { label: t("registerTableForm.colHeaderAlias"), value: col.alias || "" },
    { label: t("registerTableForm.colHeaderDescription"), value: col.description },
    ...(sourceType === "ingest"
      ? [{ label: t("registerTableForm.colHeaderJsonPath"), value: col.path ?? "" }]
      : []),
    {
      label: t("registerTableForm.colHeaderDataType"),
      value: shownType(col.dataType, col.subtype),
    },
    {
      label: t("registerTableForm.colHeaderPk"),
      value: col.isPrimaryKey ? "✓" : "",
    },
    { label: t("registerTableForm.colHeaderScope"), value: col.scope },
    { label: t("registerTableForm.colHeaderVisibleTo"), value: col.visibleTo.join(", ") },
    { label: t("registerTableForm.colHeaderMasking"), value: masksSummary(col.masks, t) },
  ];
}

/** The kinds of a column's masks, in words; empty when it is unmasked. */
function maskKinds(col: ColumnForm, t: (k: string) => string): string {
  return col.masks
    .map((m) => (m.mask ? t(`maskList.${m.mask.type}`) : t("maskList.unmasked")))
    .join(", ");
}

function ReadOnlyColumns({
  columns,
  sourceType,
  tableId,
  testId,
}: {
  columns: ColumnForm[];
  sourceType: string;
  tableId?: number;
  testId?: string;
}) {
  const { t } = useTranslation();
  const [tab, setTab] = useColumnTab(METADATA_TABS);
  const definitionOf = useDefinitionOf(sourceType);
  return (
    <>
      <ColumnTabs value={tab} onChange={setTab} shown={METADATA_TABS} nested />
      <Table.ScrollContainer minWidth={480}>
        <Table striped withTableBorder verticalSpacing="xs" data-testid={testId}>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("registerTableForm.colHeaderColumn")}</Table.Th>
              {tab === "naming" && (
                <>
                  <Table.Th>{t("registerTableForm.colHeaderAlias")}</Table.Th>
                  <Table.Th w="60%">{t("registerTableForm.colHeaderDescription")}</Table.Th>
                </>
              )}
              {tab === "type" && (
                <>
                  <Table.Th>{t("registerTableForm.colHeaderDataType")}</Table.Th>
                  <Table.Th ta="center">{t("registerTableForm.colHeaderPk")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderScope")}</Table.Th>
                </>
              )}
              {tab === "access" && (
                <>
                  <Table.Th>{t("registerTableForm.colHeaderVisibleTo")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderMasking")}</Table.Th>
                </>
              )}
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {columns.map((col) => (
              <Table.Tr key={col.name} data-testid={testId && `${testId}-col-${col.name}`}>
                <Table.Td ff="monospace" fz="sm">
                  <ColumnGlossaryHover
                    tableId={tableId}
                    columnName={col.name}
                    definition={definitionOf(col)}
                  >
                    <Text
                      ff="monospace"
                      fz="sm"
                      truncate="end"
                      maw={220}
                      component="span"
                      style={{ display: "inline-block", verticalAlign: "bottom" }}
                    >
                      {col.name}
                    </Text>
                  </ColumnGlossaryHover>
                  {col.masks.some((m) => m.mask) && (
                    <ColumnBadge color="orange">{maskKinds(col, t)}</ColumnBadge>
                  )}
                </Table.Td>
                {tab === "naming" && (
                  <>
                    <Table.Td fz="sm">{col.alias}</Table.Td>
                    <Table.Td fz="sm">{col.description}</Table.Td>
                  </>
                )}
                {tab === "type" && (
                  <>
                    <Table.Td fz="sm" ff="monospace">
                      {shownType(col.dataType, col.subtype)}
                    </Table.Td>
                    <Table.Td ta="center">{col.isPrimaryKey ? "✓" : ""}</Table.Td>
                    <Table.Td fz="sm">{col.scope}</Table.Td>
                  </>
                )}
                {tab === "access" && (
                  <>
                    <Table.Td fz="sm">{col.visibleTo.join(", ")}</Table.Td>
                    <Table.Td fz="sm">{maskKinds(col, t) || t("maskList.unmasked")}</Table.Td>
                  </>
                )}
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </>
  );
}

function EditableColumns({
  columns,
  updateCol,
  roles,
  irTypes,
  sourceType,
}: {
  columns: ColumnForm[];
  updateCol: (
    i: number,
    key: keyof ColumnForm,
    value: string | boolean | string[] | MaskEntry[],
  ) => void;
  roles: Role[];
  irTypes: string[];
  sourceType: string;
}) {
  const { t } = useTranslation();
  // A table not registered yet has no test data: its columns' own attributes only.
  const [tab, setTab] = useColumnTab(METADATA_TABS);
  // The columns whose mask list is open below their row.
  const [masksOpen, setMasksOpen] = useState<Set<string>>(new Set());
  const toggleMasks = (column: string) =>
    setMasksOpen((open) => {
      const next = new Set(open);
      if (next.has(column)) next.delete(column);
      else next.add(column);
      return next;
    });
  // What Save will refuse, per tab: a selected column with no data type.
  const errors = { type: columns.filter((c) => c.selected && !c.dataType).length };
  const definitionOf = useDefinitionOf(sourceType);
  return (
    <>
      <ColumnTabs value={tab} onChange={setTab} shown={METADATA_TABS} errors={errors} />
      <Table.ScrollContainer minWidth={640}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th></Table.Th>
              <Table.Th>{t("registerTableForm.colHeaderColumn")}</Table.Th>
              {tab === "naming" && (
                <>
                  <Table.Th>{t("registerTableForm.colHeaderAlias")}</Table.Th>
                  <Table.Th w="60%">{t("registerTableForm.colHeaderDescription")}</Table.Th>
                  {sourceType === "ingest" && (
                    <Table.Th>{t("registerTableForm.colHeaderJsonPath")}</Table.Th>
                  )}
                </>
              )}
              {tab === "type" && (
                <>
                  <Table.Th>{t("registerTableForm.colHeaderDataType")}</Table.Th>
                  <Table.Th ta="center">{t("registerTableForm.colHeaderPk")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderScope")}</Table.Th>
                </>
              )}
              {tab === "access" && (
                <>
                  <Table.Th>{t("registerTableForm.colHeaderVisibleTo")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderMasking")}</Table.Th>
                </>
              )}
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {columns.map((col, i) => (
              <Fragment key={col.name}>
                <Table.Tr>
                  <Table.Td>
                    <Checkbox
                      checked={col.selected}
                      onChange={(e) => updateCol(i, "selected", e.currentTarget.checked)}
                      aria-label={t("registerTableForm.includeColumnAriaLabel", {
                        name: col.name,
                      })}
                      data-testid={`register-table-col-selected-${col.name}`}
                    />
                  </Table.Td>
                  <Table.Td ff="monospace" fz="sm">
                    <ColumnGlossaryHover columnName={col.name} definition={definitionOf(col)}>
                      {/* A raw source column name can run to 60+ characters: it is cut with an
                      ellipsis here, and the hover shows it whole with the rest of the column. */}
                      <Text
                        ff="monospace"
                        fz="sm"
                        truncate="end"
                        maw={220}
                        component="span"
                        style={{ display: "inline-block", verticalAlign: "bottom" }}
                      >
                        {col.name}
                      </Text>
                    </ColumnGlossaryHover>
                  </Table.Td>
                  {tab === "naming" && (
                    <>
                      <Table.Td>
                        <TextInput
                          aria-label={t("registerTableForm.colHeaderAlias")}
                          value={col.alias || ""}
                          onChange={(e) => updateCol(i, "alias", e.currentTarget.value)}
                        />
                      </Table.Td>
                      <Table.Td>
                        <TextInput
                          aria-label={t("registerTableForm.colHeaderDescription")}
                          value={col.description}
                          onChange={(e) => updateCol(i, "description", e.currentTarget.value)}
                          placeholder={t("registerTableForm.descriptionColPlaceholder")}
                        />
                      </Table.Td>
                      {sourceType === "ingest" && (
                        <Table.Td>
                          <TextInput
                            aria-label={t("registerTableForm.colHeaderJsonPath")}
                            value={col.path ?? ""}
                            onChange={(e) => updateCol(i, "path", e.currentTarget.value)}
                            placeholder="payload.order_id"
                            data-testid={`register-table-col-path-${col.name}`}
                          />
                        </Table.Td>
                      )}
                    </>
                  )}
                  {tab === "type" && (
                    <>
                      <Table.Td>
                        <Select
                          aria-label={t("registerTableForm.colHeaderDataType")}
                          placeholder={t("registerTableForm.dataTypePlaceholder")}
                          data={Array.from(
                            new Set([
                              ...(col.dataType ? [toIrType(col.dataType)] : []),
                              ...irTypes,
                            ]),
                          )}
                          value={col.dataType ? toIrType(col.dataType) : null}
                          onChange={(v) => updateCol(i, "dataType", v ?? "")}
                          error={col.selected && !col.dataType}
                          searchable
                          allowDeselect={false}
                          data-testid={`register-table-col-datatype-${col.name}`}
                        />
                        {col.subtype && (
                          // REQ-2171: the subtype the column registers as, beside its base.
                          <Text
                            size="xs"
                            ff="monospace"
                            c="dimmed"
                            data-testid={`register-table-col-subtype-${col.name}`}
                          >
                            {shownType(col.dataType, col.subtype)}
                          </Text>
                        )}
                        {col.dataType && toIrType(col.dataType) === "numeric" && (
                          <NumericSizeInputs
                            key={col.name}
                            dataType={col.dataType}
                            onChange={(v) => updateCol(i, "dataType", v)}
                            testId={`register-table-col-numeric-size-${col.name}`}
                          />
                        )}
                      </Table.Td>
                      <Table.Td ta="center">
                        <Tip text={t("registerTableForm.primaryKeyTitle")}>
                          <Checkbox
                            checked={col.isPrimaryKey}
                            onChange={(e) => updateCol(i, "isPrimaryKey", e.currentTarget.checked)}
                            aria-label={t("registerTableForm.primaryKeyAriaLabel", {
                              name: col.name,
                            })}
                            data-testid={`register-table-col-pk-${col.name}`}
                          />
                        </Tip>
                      </Table.Td>
                      <Table.Td>
                        <Select
                          aria-label={t("registerTableForm.colHeaderScope")}
                          title={t("registerTableForm.scopeHelp")}
                          data={[
                            { value: "domain", label: t("registerTableForm.scopeDomain") },
                            { value: "listed", label: t("registerTableForm.scopeListed") },
                            { value: "public", label: t("registerTableForm.scopePublic") },
                            {
                              value: "own-domain",
                              label: t("registerTableForm.scopeOwnDomain"),
                            },
                          ]}
                          value={col.scope}
                          onChange={(v) => updateCol(i, "scope", v ?? "domain")}
                          allowDeselect={false}
                        />
                      </Table.Td>
                    </>
                  )}
                  {tab === "access" && (
                    <>
                      <Table.Td>
                        <MultiSelect
                          options={roles.map((r) => ({ id: r.id, label: r.id }))}
                          value={col.visibleTo}
                          onChange={(selected) => updateCol(i, "visibleTo", selected)}
                          ariaLabel={t("registerTableForm.colHeaderVisibleTo")}
                        />
                      </Table.Td>
                      <Table.Td>
                        <Button
                          variant="subtle"
                          size="xs"
                          aria-expanded={masksOpen.has(col.name)}
                          onClick={() => toggleMasks(col.name)}
                          data-testid={`register-mask-toggle-${col.name}`}
                        >
                          {masksSummary(col.masks, t)}
                        </Button>
                      </Table.Td>
                    </>
                  )}
                </Table.Tr>
                {tab === "access" && masksOpen.has(col.name) && (
                  <Table.Tr>
                    <Table.Td></Table.Td>
                    <Table.Td colSpan={4}>
                      <MaskListEditor
                        value={col.masks}
                        onChange={(next) => updateCol(i, "masks", next)}
                        roleOptions={roles.map((r) => ({ id: r.id, label: r.id }))}
                        testId={`register-mask-list-${col.name}`}
                      />
                    </Table.Td>
                  </Table.Tr>
                )}
              </Fragment>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </>
  );
}
