// Copyright (c) 2026 Kenneth Stott
// Canary: 2e9c5a71-8d4b-4f03-a6e1-b7d30c8f4a52
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1940: sort and group are the one shared mechanism on every list.

import { describe, it, expect } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { fireEvent, render, screen } from "../../test-utils/render";
import { Table } from "@mantine/core";
import { useListSortGroup, pageItems, type ListColumn } from "../list/useListSortGroup";
import { SortGroupTable } from "../list/SortGroupTable";
import { ListRow } from "../list/ListTable";

interface Row {
  id: string;
  kind: string;
  team: string;
  n: number;
}

const ROWS: Row[] = [
  { id: "c", kind: "x", team: "t2", n: 3 },
  { id: "a", kind: "y", team: "t1", n: 10 },
  { id: "b", kind: "x", team: "t1", n: 2 },
  { id: "d", kind: "y", team: "t2", n: 1 },
];

const COLUMNS: ListColumn<Row>[] = [
  { key: "id", label: "Id", sortValue: (r) => r.id },
  { key: "kind", label: "Kind", sortValue: (r) => r.kind, groupValue: (r) => r.kind },
  { key: "team", label: "Team", groupValue: (r) => r.team },
  { key: "n", label: "N", sortValue: (r) => r.n },
];

const ids = (items: ReturnType<typeof useListSortGroup<Row>>["items"]) =>
  items.map((i) => (i.type === "row" ? i.row.id : `[${i.label}]`));

describe("useListSortGroup", () => {
  it("keeps input order until a column is sorted", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    expect(ids(result.current.items)).toEqual(["c", "a", "b", "d"]);
  });

  it("cycles ascending, descending, off", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    act(() => result.current.toggleSort("id"));
    expect(ids(result.current.items)).toEqual(["a", "b", "c", "d"]);
    act(() => result.current.toggleSort("id"));
    expect(ids(result.current.items)).toEqual(["d", "c", "b", "a"]);
    act(() => result.current.toggleSort("id"));
    expect(result.current.sortCol).toBeNull();
    expect(ids(result.current.items)).toEqual(["c", "a", "b", "d"]);
  });

  it("sorts numbers numerically, not as text", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    act(() => result.current.toggleSort("n"));
    expect(ids(result.current.items)).toEqual(["d", "b", "c", "a"]);
  });

  it("can open already sorted", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p", "id"));
    expect(ids(result.current.items)).toEqual(["a", "b", "c", "d"]);
  });

  it("groups by a column with ordered group headers and counts", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    act(() => result.current.toggleGroup("kind"));
    expect(ids(result.current.items)).toEqual(["[Kind: x]", "c", "b", "[Kind: y]", "a", "d"]);
    const header = result.current.items[0];
    expect(header).toMatchObject({ type: "header", level: 1, count: 2, key: "x" });
  });

  it("groups multi-level in click order, with composite keys", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    act(() => result.current.toggleGroup("kind"));
    act(() => result.current.toggleGroup("team"));
    expect(ids(result.current.items)).toEqual([
      "[Kind: x]",
      "[Team: t1]",
      "b",
      "[Team: t2]",
      "c",
      "[Kind: y]",
      "[Team: t1]",
      "a",
      "[Team: t2]",
      "d",
    ]);
    expect(result.current.items[1]).toMatchObject({ level: 2, key: "x|t1" });
  });

  it("collapses a group to its header and resets when grouping changes", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    act(() => result.current.toggleGroup("kind"));
    act(() => result.current.toggleCollapsed("x"));
    expect(ids(result.current.items)).toEqual(["[Kind: x]", "[Kind: y]", "a", "d"]);
    act(() => result.current.toggleGroup("team"));
    expect(result.current.collapsed.size).toBe(0);
  });

  it("sorts rows inside their groups", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    act(() => result.current.toggleGroup("kind"));
    act(() => result.current.toggleSort("n"));
    expect(ids(result.current.items)).toEqual(["[Kind: x]", "b", "c", "[Kind: y]", "d", "a"]);
  });

  it("pages only while ungrouped", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    expect(pageItems(result.current, 1, 3)).toHaveLength(1);
    act(() => result.current.toggleGroup("kind"));
    expect(pageItems(result.current, 1, 3)).toHaveLength(6);
  });

  it("refuses to sort or group a column that does not declare it", () => {
    const { result } = renderHook(() => useListSortGroup(ROWS, COLUMNS, "p"));
    expect(() => {
      act(() => result.current.toggleSort("team"));
    }).toThrow(/not sortable/);
  });
});

describe("SortGroupTable", () => {
  const renderTable = () =>
    render(
      <SortGroupTable
        testPrefix="demo"
        rows={ROWS}
        columns={COLUMNS}
        headers={[{ col: "id" }, { col: "kind" }, { col: "team" }, { col: "n" }]}
        colSpan={4}
        rowKey={(r) => r.id}
        empty="nothing"
        render={(r) => (
          <ListRow testId={`row-${r.id}`}>
            <Table.Td>{r.id}</Table.Td>
          </ListRow>
        )}
      />,
    );
  const order = (c: HTMLElement) =>
    Array.from(c.querySelectorAll("tbody tr")).map(
      (tr) => tr.getAttribute("data-testid") ?? tr.textContent,
    );

  it("draws sort controls on sortable columns and group controls on groupable ones", () => {
    renderTable();
    expect(screen.getByTestId("demo-sort-id")).toBeInTheDocument();
    expect(screen.getByTestId("demo-group-kind")).toBeInTheDocument();
    expect(screen.queryByTestId("demo-group-id")).toBeNull();
    expect(screen.queryByTestId("demo-sort-team")).toBeNull();
  });

  it("clicking sort reorders the rows", () => {
    const { container } = renderTable();
    fireEvent.click(screen.getByTestId("demo-sort-id"));
    expect(order(container)).toEqual(["row-a", "row-b", "row-c", "row-d"]);
  });

  it("clicking group adds collapsible group rows and keeps item rows striped", () => {
    const { container } = renderTable();
    fireEvent.click(screen.getByTestId("demo-group-kind"));
    const rows = Array.from(container.querySelectorAll("tbody tr"));
    expect(rows[0].textContent).toContain("Kind: x");
    expect(container.querySelectorAll("tbody tr.list-row")).toHaveLength(4);
    fireEvent.click(rows[0].querySelector("td")!);
    expect(container.querySelectorAll("tbody tr.list-row")).toHaveLength(2);
  });

  it("shows the empty state when there are no rows", () => {
    render(
      <SortGroupTable
        testPrefix="e"
        rows={[] as Row[]}
        columns={COLUMNS}
        headers={[{ col: "id" }]}
        colSpan={1}
        rowKey={(r) => r.id}
        empty="nothing"
        render={() => null}
      />,
    );
    expect(screen.getByText("nothing")).toBeInTheDocument();
  });
});
