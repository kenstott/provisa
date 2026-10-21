// Copyright (c) 2026 Kenneth Stott
// Canary: 403ccf4d-f2dd-4751-bcb9-f0bc30113632
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123 (amended 2026-10-21): every table of the profile runs modal sorts and groups as the
// app's other lists do (SortGroupTable), and a table of a run's results has an expand button that
// draws it again in the 95% modal, as the 3D views do (Expandable3D).

import type { ReactNode } from "react";
import { Group, ScrollArea, Table } from "@mantine/core";
import { ListRow } from "../../components/list/ListTable";
import { SortGroupTable } from "../../components/list/SortGroupTable";
import type { ListColumn } from "../../components/list/useListSortGroup";
import { ExpandButton, Expandable3D } from "./profile3d/Expandable3D";
import "./profile3d/profile3d.css";

export interface ResultColumn<T> {
  key: string;
  label: string;
  /** What the column sorts by, and groups by when any of its values is not a number. */
  value: (row: T) => unknown;
  /** Its cell, when it is not the value as text. */
  cell?: (row: T) => ReactNode;
}

const sortable = (v: unknown): string | number =>
  typeof v === "number" ? v : v == null ? "" : String(v);

function listColumns<T>(rows: T[], columns: ResultColumn<T>[]): ListColumn<T>[] {
  return columns.map((c) => {
    const categorical = rows.some((r) => {
      const v = c.value(r);
      return v != null && typeof v !== "number";
    });
    return {
      key: c.key,
      label: c.label,
      sortValue: (r) => sortable(c.value(r)),
      ...(categorical ? { groupValue: (r: T) => String(c.value(r) ?? "") } : {}),
    };
  });
}

export function ProfileResultTable<T>({
  rows,
  columns,
  rowKey,
  testId,
  title,
  height,
  expandable = true,
  onRowClick,
  selected,
  rowTestId,
  rowProps,
}: {
  rows: T[];
  columns: ResultColumn<T>[];
  rowKey: (row: T, index: number) => string | number;
  testId: string;
  /** The expanded copy's title. */
  title: string;
  /** The in-place copy's height; the expanded one fills its modal. */
  height: number;
  expandable?: boolean;
  onRowClick?: (row: T) => void;
  selected?: (row: T) => boolean;
  rowTestId?: (row: T, index: number) => string;
  rowProps?: (row: T) => { fw?: number };
}) {
  const index = new Map(rows.map((r, i) => [r, i]));
  const table = (expanded: boolean) => (
    <SortGroupTable
      rows={rows}
      columns={listColumns(rows, columns)}
      headers={columns.map((c) => ({ col: c.key }))}
      colSpan={columns.length}
      rowKey={(r) => rowKey(r, index.get(r) ?? 0)}
      testPrefix={expanded ? `${testId}-expanded` : testId}
      testId={expanded ? `${testId}-expanded` : testId}
      render={(r) => (
        <ListRow
          key={rowKey(r, index.get(r) ?? 0)}
          onClick={onRowClick ? () => onRowClick(r) : undefined}
          selected={selected?.(r)}
          testId={rowTestId?.(r, index.get(r) ?? 0)}
        >
          {columns.map((c) => (
            <Table.Td key={c.key} fw={rowProps?.(r).fw}>
              {c.cell ? c.cell(r) : String(c.value(r) ?? "")}
            </Table.Td>
          ))}
        </ListRow>
      )}
    />
  );
  if (!expandable) return <ScrollArea h={height}>{table(false)}</ScrollArea>;
  return (
    <Expandable3D
      title={title}
      testId={testId}
      render={(expanded, onExpand) =>
        expanded ? (
          <ScrollArea h="100%">{table(true)}</ScrollArea>
        ) : (
          <>
            <Group justify="flex-end" mb={4}>
              {onExpand && <ExpandButton onClick={onExpand} testId={`${testId}-expand`} />}
            </Group>
            <ScrollArea h={height}>{table(false)}</ScrollArea>
          </>
        )
      }
    />
  );
}
