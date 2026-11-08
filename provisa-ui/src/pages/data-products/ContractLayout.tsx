// Copyright (c) 2026 Kenneth Stott
// Canary: 5f2c8e14-7a3b-4d96-b0e1-c94d6a27f381
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The layout of a contract's panels, shared by every view of a contract. Three tiers, top to bottom:
// small key-value panels in a grid, medium tables in two flowing columns, large tables across the
// full width. A panel draws what is declared on the shared label and value grid and in the shared
// list table; what is not declared folds into one quiet line, which is all an empty panel shows.

import { type CSSProperties, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Table, Text } from "@mantine/core";
import type { OdcsRecord } from "../../api/dataProductContracts";
import { DetailFields } from "../../components/detail/DetailFields";
import { ListHead, ListRow, ListTable } from "../../components/list/ListTable";
import { DetailCard } from "./DetailCard";
import "./ContractLayout.css";

/** A value as one line of text: a list joined, a record as its fields. */
function shown(value: unknown): string {
  if (Array.isArray(value)) return value.map(shown).join(", ");
  if (value !== null && typeof value === "object") return record(value as OdcsRecord);
  return String(value);
}

/** A record as its set fields, in the order ODCS gives them. */
function record(r: OdcsRecord): string {
  return Object.entries(r)
    .filter(([, v]) => v !== null && v !== undefined)
    .map(([k, v]) => `${k}: ${shown(v)}`)
    .join(" · ");
}

function present(value: unknown): boolean {
  return !(value === null || value === undefined || (Array.isArray(value) && value.length === 0));
}

/** One element of a panel: its ODCS name and its value, undeclared when it has none. */
export interface PanelField {
  name: string;
  value: unknown;
}

/** A column of a panel's table, named by its ODCS name unless it has a `label` to show instead. */
export interface PanelColumn {
  name: string;
  label?: string;
  /** The row's fields this column draws, where they are not just `name`. */
  reads?: readonly string[];
  /** A value read whole (a name, an address): never wrapped or cut, whatever room the table has. */
  whole?: boolean;
  /** The cell's value, where it is not the row's `name`. */
  value?: (row: OdcsRecord) => unknown;
  /** The cell's drawing, where it is more than the value as text. */
  cell?: (row: OdcsRecord) => ReactNode;
}

/** A table in a panel: one row per record. A field no column reads is a further column. */
export interface PanelTable {
  name: string;
  rows: readonly OdcsRecord[];
  columns: readonly PanelColumn[];
  testId?: string;
  rowTestId?: (row: OdcsRecord) => string;
}

// A schema's table holds more wrapped columns than a page list does: each may narrow further, so a
// wide contract stays inside its panel.
const TABLE_STYLE = { "--list-wrap-min": "14ch" } as CSSProperties;

const columnValue = (column: PanelColumn, row: OdcsRecord) =>
  column.value ? column.value(row) : row[column.name];

/** The table's columns, its own first then any further field its rows carry. */
function allColumns({ rows, columns }: PanelTable): PanelColumn[] {
  const read = new Set(columns.flatMap((c) => c.reads ?? [c.name]));
  const further = [...new Set(rows.flatMap((r) => Object.keys(r)))].filter((k) => !read.has(k));
  return [...columns, ...further.map((name) => ({ name }))];
}

const columnDeclared = (column: PanelColumn, rows: readonly OdcsRecord[]) =>
  rows.some((r) => present(columnValue(column, r)));

function RecordTable({ table, named }: { table: PanelTable; named: boolean }) {
  const columns = allColumns(table).filter((c) => columnDeclared(c, table.rows));
  const { t } = useTranslation();
  return (
    <div className="contract-panel-table">
      {named && (
        <Text size="sm" c="dimmed" fw={500} mb={4}>
          {table.name}
        </Text>
      )}
      <ListTable testId={table.testId} style={TABLE_STYLE}>
        <ListHead columns={columns.map((c) => c.label ?? c.name)} />
        <Table.Tbody>
          {table.rows.map((row, i) => (
            <ListRow key={i} testId={table.rowTestId?.(row)}>
              {columns.map((c) => {
                const value = columnValue(c, row);
                const text = present(value) ? shown(value) : t("detailFields.empty");
                return (
                  <Table.Td key={c.name}>
                    {c.cell ? (
                      c.cell(row)
                    ) : c.whole ? (
                      <div className="contract-cell-whole">{text}</div>
                    ) : (
                      text
                    )}
                  </Table.Td>
                );
              })}
            </ListRow>
          ))}
        </Table.Tbody>
      </ListTable>
    </div>
  );
}

/** The one quiet line that names what is not declared. */
export function NotDeclared({ names, testId }: { names: readonly string[]; testId?: string }) {
  const { t } = useTranslation();
  return (
    <Text size="xs" c="dimmed" className="contract-not-declared" data-testid={testId}>
      {t("dataProductsTab.contract.notDeclaredList", { names: names.join(", ") })}
    </Text>
  );
}

/** A panel's content: its declared fields and tables, then what is not declared in one line. */
function PanelBody({
  fields,
  tables,
  testId,
}: {
  fields: readonly PanelField[];
  tables: readonly PanelTable[];
  testId: string;
}) {
  const declared = fields.filter((f) => present(f.value));
  const filled = tables.filter((table) => table.rows.length > 0);
  const undeclared = [
    ...fields.filter((f) => !present(f.value)).map((f) => f.name),
    ...tables.flatMap((table) =>
      table.rows.length === 0
        ? [table.name]
        : allColumns(table)
            .filter((c) => !columnDeclared(c, table.rows))
            .map((c) => c.label ?? c.name),
    ),
  ];
  return (
    <div className="contract-panel-body">
      {declared.length > 0 && (
        <DetailFields fields={declared.map((f) => ({ label: f.name, value: shown(f.value) }))} />
      )}
      {filled.map((table) => (
        <RecordTable key={table.name} table={table} named={filled.length > 1} />
      ))}
      {undeclared.length > 0 && (
        <NotDeclared names={[...new Set(undeclared)]} testId={`${testId}-not-declared`} />
      )}
    </div>
  );
}

/** One panel of a contract, in a tier or across the width (`wide`). */
export function ContractPanel({
  section,
  label,
  testId,
  wide,
  fields = [],
  tables = [],
  children,
}: {
  section: string;
  label: string;
  testId: string;
  wide?: boolean;
  fields?: readonly PanelField[];
  tables?: readonly PanelTable[];
  /** A panel that draws its own content, instead of `fields` and `tables`. */
  children?: ReactNode;
}) {
  return (
    <DetailCard section={section} label={label} testId={testId} wide={wide}>
      {children ?? <PanelBody fields={fields} tables={tables} testId={testId} />}
    </DetailCard>
  );
}

/** The contract's panels by tier: each tier's panels in its own layout, its large ones last. */
export function ContractLayout({
  small,
  medium,
  large,
  testId,
}: {
  small: ReactNode;
  medium: ReactNode;
  large: ReactNode;
  testId: string;
}) {
  return (
    <div className="contract-layout" data-testid={testId}>
      <div className="contract-tier--small">{small}</div>
      <div className="contract-tier--medium">{medium}</div>
      {large}
    </div>
  );
}
