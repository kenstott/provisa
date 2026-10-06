// Copyright (c) 2026 Kenneth Stott
// Canary: 6b1e5c0a-3d52-4f7e-9a41-8c2f7d0b19e3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CSSProperties, HTMLAttributes, ReactNode } from "react";
import { Table } from "@mantine/core";

/**
 * REQ-1940: the one list style, taken from the Register Tables page. Every admin page that lists
 * items renders through these parts; a page supplies only its columns, cells and actions.
 *
 * `.table-scroll` + `.data-table` (App.css) own the header row, row height, spacing, typography,
 * clickable-row hover and the pinned header (REQ-1587).
 */
export interface ListTableProps {
  children: ReactNode;
  testId?: string;
  minWidth?: number;
  style?: CSSProperties;
}

export function ListTable({ children, testId, minWidth, style }: ListTableProps) {
  return (
    <div className="table-scroll">
      <Table
        className="data-table"
        data-list-table=""
        data-testid={testId}
        miw={minWidth}
        style={style}
      >
        {children}
      </Table>
    </div>
  );
}

/** Header row: one `<th>` per entry; pass `""` for an unlabelled actions column. */
export function ListHead({ columns }: { columns: ReactNode[] }) {
  return (
    <Table.Thead>
      <Table.Tr>
        {columns.map((c, i) => (
          <Table.Th key={i}>{c}</Table.Th>
        ))}
      </Table.Tr>
    </Table.Thead>
  );
}

export interface ListRowProps
  extends Omit<HTMLAttributes<HTMLTableRowElement>, "className" | "style" | "onClick"> {
  children: ReactNode;
  testId?: string;
  /** Click toggles the row's expansion (or whatever the page binds); adds the pointer/hover style. */
  onClick?: () => void;
  selected?: boolean;
}

export function ListRow({ children, testId, onClick, selected, ...aria }: ListRowProps) {
  // list-row: the item rows App.css stripes (every even item row, detail rows excluded).
  const cls = ["list-row", onClick ? "clickable" : "", selected ? "row-selected" : ""]
    .filter(Boolean)
    .join(" ");
  return (
    <Table.Tr {...aria} data-testid={testId} className={cls} onClick={onClick}>
      {children}
    </Table.Tr>
  );
}

/** Full-width row shown beneath an expanded row. */
export function ListExpandRow({
  colSpan,
  children,
  testId,
  contained,
}: {
  colSpan: number;
  children: ReactNode;
  testId?: string;
  /**
   * Keeps flex-wrap content in the detail from widening the table's auto-layout columns, so the
   * panels reflow with the viewport.
   */
  contained?: boolean;
}) {
  return (
    <Table.Tr data-testid={testId} className="list-expand">
      <Table.Td colSpan={colSpan} style={{ padding: 0, maxWidth: contained ? 0 : undefined }}>
        {children}
      </Table.Td>
    </Table.Tr>
  );
}

/** Empty state: a single muted centred row spanning every column. */
export function ListEmpty({
  colSpan,
  children,
  testId,
}: {
  colSpan: number;
  children: ReactNode;
  testId?: string;
}) {
  return (
    <Table.Tr data-testid={testId}>
      <Table.Td colSpan={colSpan} ta="center" c="dimmed">
        {children}
      </Table.Td>
    </Table.Tr>
  );
}

/** In-table loading state: same row as the empty state, for pages whose header is already drawn. */
export function ListLoadingRow({
  colSpan,
  children,
  testId,
}: {
  colSpan: number;
  children: ReactNode;
  testId?: string;
}) {
  return (
    <ListEmpty colSpan={colSpan} testId={testId}>
      {children}
    </ListEmpty>
  );
}

/** Padded panel for simple expanded-row content (rich panels such as TableReadView bring their own). */
export function ListDetail({ children }: { children: ReactNode }) {
  return <div className="list-detail">{children}</div>;
}

/** Loading state: the page-level text the Register Tables page shows while its list loads. */
export function ListLoading({ message, testId }: { message: ReactNode; testId?: string }) {
  return (
    <div className="page" data-testid={testId}>
      {message}
    </div>
  );
}
