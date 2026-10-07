// Copyright (c) 2026 Kenneth Stott
// Canary: 91d3b7e4-6c2a-4f18-b5a0-e7c4d9f2a613
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, type ReactNode } from "react";
import { Table } from "@mantine/core";
import {
  ListEmpty,
  ListHead,
  ListItems,
  ListTable,
  type ListLabelCol,
  type ListSortCol,
} from "./ListTable";
import { pageItems, useListSortGroup, type ListColumn } from "./useListSortGroup";

/**
 * REQ-1940: a complete list — header with sort/group controls, grouped rows, empty state — for
 * pages that hand over their rows and per-row markup and own nothing else. It holds the sort and
 * group state, so it can sit anywhere in a page's JSX.
 */
export interface SortGroupTableProps<T> {
  rows: T[];
  columns: ListColumn<T>[];
  /** One entry per `<th>`: `{ col }` for a declared column, a node for a static header. */
  headers: Array<ReactNode | ListSortCol | ListLabelCol>;
  colSpan: number;
  rowKey: (row: T) => string | number;
  /** One item row (and its detail row, if any). */
  render: (row: T) => ReactNode;
  testPrefix: string;
  testId?: string;
  minWidth?: number;
  empty?: ReactNode;
  emptyTestId?: string;
  /** Zero-based page and its size; paging applies only while ungrouped. */
  page?: { index: number; size: number };
  initialSortCol?: string;
  /** Lets the page hide its pager while grouped. */
  onGroupedChange?: (grouped: boolean) => void;
}

export function SortGroupTable<T>({
  rows,
  columns,
  headers,
  colSpan,
  rowKey,
  render,
  testPrefix,
  testId,
  minWidth,
  empty,
  emptyTestId,
  page,
  initialSortCol,
  onGroupedChange,
}: SortGroupTableProps<T>) {
  const state = useListSortGroup(rows, columns, testPrefix, initialSortCol ?? null);
  const grouped = state.groupBy.length > 0;
  useEffect(() => {
    onGroupedChange?.(grouped);
  }, [grouped, onGroupedChange]);
  return (
    <ListTable testId={testId} minWidth={minWidth}>
      <ListHead sortGroup={state} columns={headers} />
      <Table.Tbody>
        {rows.length === 0 && empty !== undefined && (
          <ListEmpty colSpan={colSpan} testId={emptyTestId}>
            {empty}
          </ListEmpty>
        )}
        <ListItems
          state={state}
          items={page ? pageItems(state, page.index, page.size) : undefined}
          colSpan={colSpan}
          rowKey={rowKey}
          render={render}
        />
      </Table.Tbody>
    </ListTable>
  );
}
