// Copyright (c) 2026 Kenneth Stott
// Canary: 6b1e5c0a-3d52-4f7e-9a41-8c2f7d0b19e3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment, type CSSProperties, type HTMLAttributes, type ReactNode } from "react";
import { ActionIcon, Group, Table, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { ArrowDown, ArrowUp, ArrowUpDown, Layers } from "lucide-react";
import type { ListItem, ListSortGroup } from "./useListSortGroup";

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

/** A header cell bound to a column the page declared to useListSortGroup. */
export interface ListSortCol {
  col: string;
  /** Fixed column width, for tables with `table-layout: fixed`. */
  width?: string;
}

/** A static header cell that needs a width. */
export interface ListLabelCol {
  label: ReactNode;
  width?: string;
}

function isLabelCol(c: ReactNode | ListSortCol | ListLabelCol): c is ListLabelCol {
  return typeof c === "object" && c !== null && "label" in c && !("props" in c);
}

function isSortCol(c: ReactNode | ListSortCol | ListLabelCol): c is ListSortCol {
  return (
    typeof c === "object" && c !== null && "col" in c && typeof (c as ListSortCol).col === "string"
  );
}

const SORT_BUTTON_STYLE: CSSProperties = {
  cursor: "pointer",
  userSelect: "none",
  background: "none",
  border: "none",
  padding: 0,
  display: "inline-flex",
  alignItems: "center",
  gap: "0.25rem",
  font: "inherit",
  color: "inherit",
};

function ListSortCell<T>({ state, col }: { state: ListSortGroup<T>; col: string }) {
  const { t } = useTranslation();
  const def = state.columns.find((c) => c.key === col);
  if (!def) throw new Error(`list column "${col}" is not declared`);
  const label = def.label;
  const sortActive = state.sortCol === col;
  const sortLabel = sortActive
    ? state.sortDir === "asc"
      ? t("list.sortAscending")
      : t("list.sortDescending")
    : t("list.sortNone");
  const groupLevel = state.groupBy.indexOf(col);
  const isGrouped = groupLevel !== -1;
  const groupLabel = isGrouped
    ? t("list.ungroupLevel", { level: groupLevel + 1 })
    : t("list.groupBy", { label });
  return (
    <Group gap={4} wrap="nowrap" component="span">
      {def.sortValue ? (
        <button
          type="button"
          data-testid={`${state.testPrefix}-sort-${col}`}
          onClick={() => state.toggleSort(col)}
          aria-label={`${label}, ${sortLabel}`}
          style={SORT_BUTTON_STYLE}
        >
          {label}
          {sortActive ? (
            state.sortDir === "asc" ? (
              <ArrowUp size={11} color="var(--text-muted)" aria-hidden="true" />
            ) : (
              <ArrowDown size={11} color="var(--text-muted)" aria-hidden="true" />
            )
          ) : (
            <ArrowUpDown size={11} color="var(--text-muted)" aria-hidden="true" />
          )}
        </button>
      ) : (
        label
      )}
      {def.groupValue && (
        <ActionIcon
          variant="transparent"
          size="xs"
          data-testid={`${state.testPrefix}-group-${col}`}
          aria-label={groupLabel}
          title={groupLabel}
          onClick={() => state.toggleGroup(col)}
          style={{ opacity: isGrouped ? 1 : 0.35 }}
        >
          <Layers
            size={11}
            color={isGrouped ? "var(--primary, #6366f1)" : undefined}
            aria-hidden="true"
          />
        </ActionIcon>
      )}
      {def.groupValue && isGrouped && (
        <Text span fz="0.65rem" c="var(--primary, #6366f1)">
          {groupLevel + 1}
        </Text>
      )}
    </Group>
  );
}

/**
 * Header row: one `<th>` per entry. A plain node is a static header; `{ col }` binds the cell to a
 * column declared to useListSortGroup, which draws its sort and group controls.
 */
export function ListHead<T = never>({
  columns,
  sortGroup,
}: {
  columns: Array<ReactNode | ListSortCol | ListLabelCol>;
  sortGroup?: ListSortGroup<T>;
}) {
  return (
    <Table.Thead>
      <Table.Tr>
        {columns.map((c, i) => {
          if (isSortCol(c)) {
            if (!sortGroup) throw new Error("ListHead: a { col } header needs sortGroup");
            return (
              <Table.Th key={i} style={{ whiteSpace: "nowrap", width: c.width }}>
                <ListSortCell state={sortGroup} col={c.col} />
              </Table.Th>
            );
          }
          if (isLabelCol(c)) {
            return (
              <Table.Th key={i} style={{ width: c.width }}>
                {c.label}
              </Table.Th>
            );
          }
          return <Table.Th key={i}>{c}</Table.Th>;
        })}
      </Table.Tr>
    </Table.Thead>
  );
}

/** Collapsible group header row, level 1 outermost. */
export function ListGroupRow({
  colSpan,
  level,
  label,
  count,
  collapsed,
  onToggle,
}: {
  colSpan: number;
  level: number;
  label: string;
  count: number;
  collapsed: boolean;
  onToggle: () => void;
}) {
  const isL1 = level === 1;
  return (
    <Table.Tr>
      <Table.Td
        colSpan={colSpan}
        role="button"
        tabIndex={0}
        aria-expanded={!collapsed}
        onClick={onToggle}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggle();
          }
        }}
        style={{
          fontWeight: isL1 ? 600 : 500,
          fontSize: isL1 ? "0.8rem" : "0.75rem",
          padding: isL1 ? "0.35rem 0.75rem" : `0.25rem ${0.75 * level}rem`,
          color: "var(--text-muted)",
          background: isL1 ? "var(--surface)" : "var(--surface-raised, var(--surface))",
          borderTop: isL1 ? "2px solid var(--border)" : "1px solid var(--border)",
          cursor: "pointer",
          userSelect: "none",
        }}
      >
        {collapsed ? "▶" : "▼"} {label}{" "}
        <span style={{ fontWeight: "normal", opacity: 0.7 }}>({count})</span>
      </Table.Td>
    </Table.Tr>
  );
}

/** The sorted, grouped items: group headers, and `render(row)` for each item row. */
export function ListItems<T>({
  state,
  items,
  colSpan,
  rowKey,
  render,
}: {
  state: ListSortGroup<T>;
  /** Defaults to every item; pass `pageItems(...)` to page. */
  items?: ListItem<T>[];
  colSpan: number;
  rowKey: (row: T) => string | number;
  render: (row: T) => ReactNode;
}) {
  return (
    <>
      {(items ?? state.items).map((item) =>
        item.type === "header" ? (
          <ListGroupRow
            key={`grp-${item.key}`}
            colSpan={colSpan}
            level={item.level}
            label={item.label}
            count={item.count}
            collapsed={state.collapsed.has(item.key)}
            onToggle={() => state.toggleCollapsed(item.key)}
          />
        ) : (
          <Fragment key={rowKey(item.row)}>{render(item.row)}</Fragment>
        ),
      )}
    </>
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

/** Padded panel for simple expanded-row content (rich panels such as TableReadView bring their own). */
export function ListDetail({ children }: { children: ReactNode }) {
  return <div className="list-detail">{children}</div>;
}
