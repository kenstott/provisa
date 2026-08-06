// Copyright (c) 2026 Kenneth Stott
// Canary: 21abd302-8c7a-4bf8-a66c-17d526bc7a0c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386: shared results-grid state — filter -> sort -> collapsible multi-level
// group tree (row headers, never aggregates) -> page.

import { describe, it, expect } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { useResultsGrid } from "../useResultsGrid";
import type { GridItem } from "../useResultsGrid";

const ROWS = [
  { domain: "ops", table: "a", n: 1 },
  { domain: "ops", table: "a", n: 2 },
  { domain: "ops", table: "b", n: 3 },
  { domain: "sales", table: "c", n: 4 },
];
const COLS = ["domain", "table", "n"];

function kinds(items: GridItem[]): string[] {
  return items.map((i) => (i.type === "group" ? `g${i.level}:${i.value}(${i.count})` : "row"));
}

describe("useResultsGrid", () => {
  it("passes rows through ungrouped, in order", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    expect(result.current.displayColumns).toEqual(COLS);
    expect(result.current.pagedItems).toHaveLength(4);
    expect(result.current.pagedItems.every((i) => i.type === "row")).toBe(true);
  });

  it("sorts by column with direction toggle", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    act(() => result.current.handleSort("n"));
    act(() => result.current.handleSort("n")); // asc -> desc
    const first = result.current.pagedItems[0];
    expect(first.type === "row" && first.row.n).toBe(4);
  });

  it("filters rows by substring", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    act(() => result.current.setFilters({ domain: "sales" }));
    expect(result.current.displayRows).toHaveLength(1);
  });

  it("builds a multi-level collapsible tree with unchanged columns", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    act(() => result.current.toggleGroupBy("domain"));
    act(() => result.current.toggleGroupBy("table"));
    // Columns are NOT reduced to aggregates — grouping is presentation only.
    expect(result.current.displayColumns).toEqual(COLS);
    expect(kinds(result.current.pagedItems)).toEqual([
      "g0:ops(3)",
      "g1:a(2)",
      "row",
      "row",
      "g1:b(1)",
      "row",
      "g0:sales(1)",
      "g1:c(1)",
      "row",
    ]);
  });

  it("collapsing a group hides its subtree; toggling again restores it", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    act(() => result.current.toggleGroupBy("domain"));
    act(() => result.current.toggleGroupBy("table"));
    const opsHeader = result.current.pagedItems[0];
    if (opsHeader.type !== "group") throw new Error("expected group header");
    act(() => result.current.toggleGroupCollapsed(opsHeader.key));
    expect(kinds(result.current.pagedItems)).toEqual([
      "g0:ops(3)",
      "g0:sales(1)",
      "g1:c(1)",
      "row",
    ]);
    act(() => result.current.toggleGroupCollapsed(opsHeader.key));
    expect(result.current.pagedItems).toHaveLength(9);
  });

  it("ungrouping a level removes it and clears collapse state", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    act(() => result.current.toggleGroupBy("domain"));
    act(() => result.current.toggleGroupBy("domain"));
    expect(result.current.groupBy).toEqual([]);
    expect(result.current.pagedItems).toHaveLength(4);
  });

  it("persists filter/group/sort choices per storageKey and restores them", () => {
    const first = renderHook(() => useResultsGrid(ROWS, COLS, "test:pets"));
    act(() => {
      first.result.current.toggleGroupBy("domain");
      first.result.current.handleSort("n");
      first.result.current.setFilters({ table: "a" });
    });
    first.unmount();

    const second = renderHook(() => useResultsGrid(ROWS, COLS, "test:pets"));
    expect(second.result.current.groupBy).toEqual(["domain"]);
    expect(second.result.current.sorts).toEqual([{ col: "n", dir: "asc" }]);
    expect(second.result.current.filters).toEqual({ table: "a" });

    const other = renderHook(() => useResultsGrid(ROWS, COLS, "test:other"));
    expect(other.result.current.groupBy).toEqual([]);
  });

  it("server-paged mode: no client filter/sort/slice, pager runs on hasMore", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS, undefined, { hasMore: true }));
    act(() => result.current.setFilters({ domain: "sales" }));
    act(() => result.current.handleSort("n"));
    // rows pass through untouched — filtering/sorting happen in the query upstream
    expect(result.current.displayRows).toHaveLength(4);
    const first = result.current.pagedItems[0];
    expect(first.type === "row" && first.row.n).toBe(1);
    expect(result.current.serverPaged).toBe(true);
    expect(result.current.hasMore).toBe(true);
  });

  it("resetGrid clears filters, sorts, grouping and paging", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    act(() => {
      result.current.setFilters({ domain: "ops" });
      result.current.toggleGroupBy("domain");
      result.current.handleSort("n");
    });
    act(() => result.current.resetGrid());
    expect(result.current.groupBy).toEqual([]);
    expect(result.current.sorts).toEqual([]);
    expect(result.current.displayRows).toHaveLength(4);
  });
});
