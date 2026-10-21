// Copyright (c) 2026 Kenneth Stott
// Canary: 0a1410a9-e314-43d1-9624-c089e31f62cc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: a results grid's rows may be clicked, one drawn as selected (the profile runs modal
// picks a run, or a measure's history); a grid given neither is drawn as before.

import { describe, expect, it, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { ResultsGrid } from "../ResultsGrid";
import { useResultsGrid } from "../useResultsGrid";

const ROWS = [
  { id: "r1", plan: "basic" },
  { id: "r2", plan: "pro" },
];

function Harness(props: {
  onRowClick?: (row: Record<string, unknown>) => void;
  selectedRow?: (row: Record<string, unknown>) => boolean;
}) {
  const grid = useResultsGrid(ROWS, ["plan"]);
  return <ResultsGrid grid={grid} totalRowCount={ROWS.length} {...props} />;
}

const rowOf = (text: string) => screen.getByText(text).closest("tr") as HTMLTableRowElement;

describe("ResultsGrid row click", () => {
  it("hands the clicked row, with the keys it does not show, and draws the selected one", () => {
    const clicked = vi.fn();
    render(<Harness onRowClick={clicked} selectedRow={(r) => r.id === "r2"} />);
    fireEvent.click(rowOf("basic"));
    expect(clicked).toHaveBeenCalledWith({ id: "r1", plan: "basic" });
    expect(rowOf("pro").className).toContain("row-selected");
    expect(rowOf("pro").getAttribute("aria-selected")).toBe("true");
    expect(rowOf("basic").className).not.toContain("row-selected");
    expect(rowOf("basic").className).toContain("clickable");
  });

  it("draws rows as before for a grid given neither", () => {
    render(<Harness />);
    expect(rowOf("basic").className).toBe("");
    expect(rowOf("basic").getAttribute("aria-selected")).toBeNull();
  });
});
