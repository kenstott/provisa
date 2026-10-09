// Copyright (c) 2026 Kenneth Stott
// Canary: 4576fb2d-f7c6-459d-99ef-7073b56b6be0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1937: a grid's columns filter by their type; chips name and remove each filter; a grid that
// holds only part of the result says the filter applies to the rows loaded.

import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { ResultsGrid } from "../ResultsGrid";
import { useResultsGrid } from "../useResultsGrid";

const ROWS = [
  { amount: 50, status: "ok" },
  { amount: 150, status: "error" },
  { amount: 500, status: "timeout" },
  { amount: 700, status: "ok" },
];
const COLS = ["amount", "status"];

function Harness({ partial = false }: { partial?: boolean }) {
  const grid = useResultsGrid(ROWS, COLS, undefined, undefined, {
    amount: "bigint",
    status: "varchar",
  });
  return <ResultsGrid grid={grid} totalRowCount={ROWS.length} rowsPartial={partial} />;
}

const body = () => document.querySelector("tbody")?.textContent ?? "";

describe("typed column filters", () => {
  it("types a column that reports no type as text", () => {
    function Untyped() {
      const grid = useResultsGrid(ROWS, COLS);
      return <ResultsGrid grid={grid} totalRowCount={ROWS.length} />;
    }
    render(<Untyped />);
    // ">100" is plain text on a text column: contains, which matches nothing here.
    fireEvent.change(screen.getByLabelText("Filter rows… amount"), { target: { value: ">100" } });
    expect(screen.getByTestId("results-grid-empty")).toBeTruthy();
  });

  it("reads >100 on a numeric column as a comparison", () => {
    render(<Harness />);
    fireEvent.change(screen.getByLabelText("Filter rows… amount"), { target: { value: ">100" } });
    expect(body()).not.toContain("50ok");
    expect(body()).toContain("150");
    expect(body()).toContain("700");
    expect(screen.getByTestId("filter-chip-amount").textContent).toContain("amount");
  });

  it("keeps plain text meaning contains", () => {
    render(<Harness />);
    fireEvent.change(screen.getByLabelText("Filter rows… status"), { target: { value: "ERR" } });
    expect(body()).toContain("error");
    expect(body()).not.toContain("timeout");
  });

  it("filters to the values ticked in the checklist, with counts, and a chip removes it", async () => {
    render(<Harness />);
    fireEvent.click(screen.getByTestId("col-filter-status-btn"));
    fireEvent.click(await screen.findByTestId("col-filter-status-op"));
    fireEvent.click(await screen.findByRole("option", { name: "is one of", hidden: true }));
    // ok appears twice in the rows, and the list says so
    expect(await screen.findByText("ok (2)")).toBeTruthy();
    fireEvent.click(screen.getByTestId("col-filter-status-value-error"));
    fireEvent.click(screen.getByTestId("col-filter-status-value-timeout"));
    fireEvent.click(screen.getByTestId("col-filter-status-apply"));
    expect(body()).toContain("error");
    expect(body()).toContain("timeout");
    expect(body()).not.toContain("700");
    expect(screen.getByTestId("filter-chip-status")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Remove the filter on status" }));
    expect(body()).toContain("700");
  });

  it("combines filters on several columns with AND", () => {
    render(<Harness />);
    fireEvent.change(screen.getByLabelText("Filter rows… amount"), { target: { value: ">100" } });
    fireEvent.change(screen.getByLabelText("Filter rows… status"), { target: { value: "ok" } });
    expect(body()).toContain("700");
    expect(body()).not.toContain("150");
  });

  it("removes a filter from its chip", () => {
    render(<Harness />);
    fireEvent.change(screen.getByLabelText("Filter rows… amount"), { target: { value: ">100" } });
    fireEvent.click(screen.getByRole("button", { name: "Remove the filter on amount" }));
    expect(screen.queryByTestId("filter-chip-amount")).toBeNull();
    expect(body()).toContain("50");
  });

  it("says the filter applies to the rows loaded when the grid holds part of the result", () => {
    render(<Harness partial />);
    expect(screen.queryByTestId("filter-partial-note")).toBeNull();
    fireEvent.change(screen.getByLabelText("Filter rows… amount"), { target: { value: ">100" } });
    expect(screen.getByTestId("filter-partial-note").textContent).toContain("4");
  });
});
