// Copyright (c) 2026 Kenneth Stott
// Canary: cdaeba8f-3457-4bdc-95f6-b5acd0725ce8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1442: one control clears every column filter. Narrowing several columns until nothing matches
// is easy; undoing it one input at a time is not, and an empty grid gives no clue how many are set.

import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { renderHook, act } from "@testing-library/react";
import { ResultsGrid } from "../ResultsGrid";
import { useResultsGrid } from "../useResultsGrid";

const ROWS = [
  { domain: "ops", table: "a" },
  { domain: "sales", table: "c" },
];
const COLS = ["domain", "table"];

function Harness() {
  const grid = useResultsGrid(ROWS, COLS);
  return <ResultsGrid grid={grid} totalRowCount={ROWS.length} />;
}

describe("ResultsGrid clear filters", () => {
  it("is offered but inert until something is filtered", () => {
    render(<Harness />);
    expect(screen.getByTestId("clear-filters-btn")).toHaveProperty("disabled", true);
  });

  it("restores every row after filters across two columns emptied the grid", () => {
    render(<Harness />);
    fireEvent.change(screen.getByLabelText("Filter rows… domain"), {
      target: { value: "ops" },
    });
    fireEvent.change(screen.getByLabelText("Filter rows… table"), {
      target: { value: "c" },
    });
    expect(screen.getByTestId("results-grid-empty")).toBeTruthy();

    const clear = screen.getByTestId("clear-filters-btn");
    expect(clear).toHaveProperty("disabled", false);
    fireEvent.click(clear);

    expect(screen.queryByTestId("results-grid-empty")).toBeNull();
    expect(screen.getByText("ops")).toBeTruthy();
    expect(screen.getByText("sales")).toBeTruthy();
  });

  it("returns to the first page, since the pages it cleared were numbered under the filter", () => {
    const { result } = renderHook(() => useResultsGrid(ROWS, COLS));
    act(() => result.current.setFilters({ domain: "sales" }));
    act(() => result.current.setPage(3));
    expect(result.current.hasFilters).toBe(true);

    act(() => result.current.clearFilters());
    expect(result.current.hasFilters).toBe(false);
    expect(result.current.page).toBe(0);
    expect(result.current.pagedItems).toHaveLength(2);
  });
});
