// Copyright (c) 2026 Kenneth Stott
// Canary: 6b1e5c0a-3d52-4f7e-9a41-8c2f7d0b19e3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import {
  Children,
  cloneElement,
  createContext,
  Fragment,
  isValidElement,
  useContext,
  useEffect,
  useMemo,
  useState,
  type CSSProperties,
  type HTMLAttributes,
  type ReactElement,
  type ReactNode,
} from "react";
import { ActionIcon, Code, Group, Stack, Table, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { ArrowDown, ArrowUp, ArrowUpDown, Layers } from "lucide-react";
import type { ListItem, ListSortGroup } from "./useListSortGroup";
import { ListCell } from "./ListCell";
import {
  LIST_COLUMN_CAP_PX,
  LIST_CUT_MIN_CHARS,
  LIST_ROW_MAX_LINES,
  LIST_WRAP_MIN_CHARS,
} from "./listCellLimits";
import { Tip } from "../Tip";
import { useLeaveGuard } from "./panelLeave";

/** The caps the list's columns stated for themselves, by position; told by the head, read by rows. */
const ColumnCaps = createContext<{
  caps: ReadonlyArray<number | undefined>;
  tell: (caps: ReadonlyArray<number | undefined>) => void;
} | null>(null);

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
  /**
   * The narrowest the list may be drawn, in pixels: below it the list scrolls sideways instead
   * of crushing its columns. It is a floor, never a width: a list always fills the room it has.
   */
  minWidth?: number;
  style?: CSSProperties;
}

/** A list is at least as wide as its container, and at least its own floor where it has one. */
const listMinWidth = (floor: number | undefined) =>
  floor === undefined ? undefined : `max(100%, ${floor}px)`;

export function ListTable({ children, testId, minWidth, style }: ListTableProps) {
  const [caps, tell] = useState<ReadonlyArray<number | undefined>>([]);
  const columnCaps = useMemo(() => ({ caps, tell }), [caps]);
  return (
    <div className="table-scroll">
      <Table
        className="data-table"
        data-list-table=""
        data-testid={testId}
        miw={listMinWidth(minWidth)}
        style={
          {
            "--list-col-cap": `${LIST_COLUMN_CAP_PX}px`,
            "--list-row-lines": LIST_ROW_MAX_LINES,
            "--list-wrap-min": `${LIST_WRAP_MIN_CHARS}ch`,
            "--list-cut-min": `${LIST_CUT_MIN_CHARS}ch`,
            ...style,
          } as CSSProperties
        }
      >
        <ColumnCaps.Provider value={columnCaps}>{children}</ColumnCaps.Provider>
      </Table>
    </div>
  );
}

/** A header cell bound to a column the page declared to useListSortGroup. */
export interface ListSortCol {
  col: string;
  /** Fixed column width, for tables with `table-layout: fixed`. */
  width?: string;
  /** The most width, in pixels, this column asks for, where the list's own cap does not suit it. */
  cap?: number;
}

/** A static header cell that needs a width. */
export interface ListLabelCol {
  label: ReactNode;
  width?: string;
  /** The most width, in pixels, this column asks for, where the list's own cap does not suit it. */
  cap?: number;
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
        <Tip text={groupLabel} named>
          <ActionIcon
            variant="transparent"
            size="xs"
            data-testid={`${state.testPrefix}-group-${col}`}
            aria-label={groupLabel}
            onClick={() => state.toggleGroup(col)}
            style={{ opacity: isGrouped ? 1 : 0.35 }}
          >
            <Layers
              size={11}
              color={isGrouped ? "var(--primary, #6366f1)" : undefined}
              aria-hidden="true"
            />
          </ActionIcon>
        </Tip>
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
  // The rows learn each column's own cap from here: a cell cannot see its header.
  const told = useContext(ColumnCaps);
  const stated = columns.map((c) => (isSortCol(c) || isLabelCol(c) ? (c.cap ?? "") : "")).join(",");
  useEffect(() => {
    told?.tell(stated.split(",").map((cap) => (cap === "" ? undefined : Number(cap))));
    // eslint-disable-next-line react-hooks/exhaustive-deps -- told once for each set of caps
  }, [stated]);
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
          // The actions column's header is pinned with its cells (ListActions).
          const actions = isValidElement(c) && c.type === ListActionsHead;
          return (
            <Table.Th key={i} className={actions ? "list-actions" : undefined}>
              {c}
            </Table.Th>
          );
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

export interface ListRowProps extends Omit<
  HTMLAttributes<HTMLTableRowElement>,
  "className" | "style" | "onClick"
> {
  children: ReactNode;
  testId?: string;
  /** Click toggles the row's expansion (or whatever the page binds); adds the pointer/hover style. */
  onClick?: () => void;
  selected?: boolean;
}

/**
 * A row's actions: its last cell, pinned at the visible end of the row, so a list wider than its
 * room scrolls under them and they are never cut (App.css .list-actions). Its column's header is
 * `ListActionsHead`.
 */
export function ListActions({ children }: { children: ReactNode }) {
  return <Table.Td className="list-actions">{children}</Table.Td>;
}

/** The header of a row's actions column (`ListActions`): pinned with it. */
export function ListActionsHead({ children }: { children?: ReactNode }) {
  return <>{children}</>;
}

export function ListRow({ children, testId, onClick, selected, ...aria }: ListRowProps) {
  // list-row: the item rows App.css stripes (every even item row, detail rows excluded).
  const cls = ["list-row", onClick ? "clickable" : "", selected ? "row-selected" : ""]
    .filter(Boolean)
    .join(" ");
  // Beside a detail panel whose form has unsaved changes, choosing a row asks first.
  const guard = useLeaveGuard();
  const caps = useContext(ColumnCaps)?.caps ?? [];
  return (
    <Table.Tr
      {...aria}
      data-testid={testId}
      className={cls}
      onClick={onClick ? () => guard(onClick) : undefined}
    >
      {capCells(children, caps, { at: 0 })}
    </Table.Tr>
  );
}

/** Elements that only give text a look: text inside one of them is still a cell of text. */
const TEXT_LOOKS: ReadonlySet<unknown> = new Set([Text, Code, "span", "code", "em", "strong", "b"]);

/**
 * The text of a cell that holds nothing but text -- bare, or inside one element that only gives
 * it a look -- and undefined for a cell that holds anything else (a control, a pill, an icon).
 */
function cellText(children: ReactNode): string | undefined {
  const parts = Children.toArray(children);
  if (parts.length === 0) return undefined;
  if (parts.length === 1 && isValidElement(parts[0])) {
    const only = parts[0] as ReactElement<{ children?: ReactNode; onClick?: unknown }>;
    if (!TEXT_LOOKS.has(only.type) || only.props.onClick !== undefined) return undefined;
    return cellText(only.props.children);
  }
  if (!parts.every((p) => typeof p === "string" || typeof p === "number")) return undefined;
  const text = parts.join("");
  return text.trim() === "" ? undefined : text;
}

/**
 * A row's cells, with every cell of plain text held to its column's cap and the row's maximum
 * height (`ListCell`). A cell that holds a control, a pill or any other element is drawn as the
 * page drew it, at the width it asks for: controls are never cut, and never squeezed.
 */
function capCells(
  children: ReactNode,
  caps: ReadonlyArray<number | undefined>,
  column: { at: number },
): ReactNode {
  return Children.map(children, (child) => {
    if (!isValidElement(child)) return child;
    const cell = child as ReactElement<{
      children?: ReactNode;
      className?: string;
      style?: CSSProperties;
      colSpan?: number;
    }>;
    if (cell.type === Fragment) return <>{capCells(cell.props.children, caps, column)}</>;
    const cap = caps[column.at];
    column.at += cell.props.colSpan ?? 1;
    if (cell.type !== Table.Td) return child;
    const text = cellText(cell.props.children);
    if (text === undefined) {
      // Whatever else the page drew is not cut: a control or a pill keeps its unbreakable width.
      // Text set in a row beside it (a name and its badges) is held like a cell of text.
      return cloneElement(
        cell,
        undefined,
        <div className="list-cell-drawn">{capGroupText(cell.props.children)}</div>,
      );
    }
    return cloneElement(
      cell,
      {
        className: [cell.props.className, "list-capped"].filter(Boolean).join(" "),
        style:
          cap === undefined
            ? cell.props.style
            : ({ ...cell.props.style, "--list-cell-cap": `${cap}px` } as CSSProperties),
      },
      <ListCell text={text}>{cell.props.children}</ListCell>,
    );
  });
}

/**
 * A cell drawn as a row of items (a Mantine Group: a name followed by its badges or controls) or a
 * column of them (a Mantine Stack: a statement above a dimmed line): each item of plain text in it
 * is held to the column's cap and the row's maximum like a cell of text. A row keeps to one line,
 * so the name gives up width and its badges stay beside it.
 */
function capGroupText(children: ReactNode): ReactNode {
  const parts = Children.toArray(children);
  if (parts.length !== 1 || !isValidElement(parts[0])) return children;
  if (parts[0].type !== Group && parts[0].type !== Stack) return children;
  const group = parts[0] as ReactElement<{ children?: ReactNode; wrap?: string }>;
  let capped = false;
  const items = Children.map(group.props.children, (item) => {
    const text = cellText(item);
    if (text === undefined) return item;
    capped = true;
    return <ListCell text={text}>{item}</ListCell>;
  });
  if (!capped) return children;
  return cloneElement(group, group.type === Group ? { wrap: "nowrap" } : undefined, items);
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
