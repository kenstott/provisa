// Copyright (c) 2026 Kenneth Stott
// Canary: 9a4d2e71-6b3f-4c85-a1d7-5e8f0c2b9d46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The SQL explorer's Results tab reopens the same grid at full size in a 90% modal from its
// expand control, the way the Analyze tab does for a plan.
import { describe, it, expect, vi } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { ResultsPanel } from "../pages/sql/ResultsPanel";
import type { ResultsGridState } from "../pages/sql/useResultsGrid";

vi.mock("../pages/sql/ResultsGrid", () => ({
  ResultsGrid: () => <div data-testid="results-grid" />,
}));

const grid = { profile: [], handleDownloadProfile: vi.fn() } as unknown as ResultsGridState;

function renderResults(rows: Record<string, unknown>[]) {
  return render(
    <ResultsPanel
      resultTab="results"
      setResultTab={vi.fn()}
      running={false}
      resultError=""
      resultRows={rows}
      resultColumns={["id"]}
      grid={grid}
      errors={[]}
      history={[]}
      queryStats={null}
      analyzePlan={null}
      analyzeError=""
      sqlText="SELECT id FROM orders"
      setSqlText={vi.fn()}
      setRole={vi.fn()}
    />,
  );
}

describe("SQL explorer results expand", () => {
  it("reopens the grid at full size in a modal from the expand control", async () => {
    renderResults([{ id: 1 }]);
    expect(screen.getAllByTestId("results-grid")).toHaveLength(1);
    fireEvent.click(screen.getByTestId("results-expand"));
    await waitFor(() => expect(screen.getAllByTestId("results-grid")).toHaveLength(2));
    const dialog = await screen.findByRole("dialog");
    expect(dialog.querySelector('[data-testid="results-grid"]')).not.toBeNull();
    expect(screen.getByText("Results", { selector: "h2" })).toBeTruthy();
  });

  it("offers no expand control when there is nothing to show", () => {
    renderResults([]);
    expect(screen.queryByTestId("results-expand")).toBeNull();
  });
});
