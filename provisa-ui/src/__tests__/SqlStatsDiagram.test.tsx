// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1517: the SQL surface draws the same execution DAG the GraphQL surface draws. Both
// render the shared MermaidDiagram, so a statement's stats view shows the plan that ran
// instead of a bare source table.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import { ResultsPanel } from "../pages/sql/ResultsPanel";
import type { ResultsGridState } from "../pages/sql/useResultsGrid";

const renderChart = vi.fn(async (id: string, chart: string) => ({
  svg: `<svg data-testid="${id}" data-chart="${chart.split("\n")[0]}"></svg>`,
}));

vi.mock("mermaid", () => ({
  default: {
    initialize: vi.fn(),
    render: (id: string, chart: string) => renderChart(id, chart),
  },
}));

vi.mock("../pages/sql/ResultsGrid", () => ({
  ResultsGrid: () => null,
}));

const grid = { profile: [], handleDownloadProfile: vi.fn() } as unknown as ResultsGridState;

function renderStats(queryStats: unknown) {
  return render(
    <ResultsPanel
      resultTab="stats"
      setResultTab={vi.fn()}
      running={false}
      resultError=""
      resultRows={[]}
      resultColumns={[]}
      grid={grid}
      errors={[]}
      history={[]}
      queryStats={queryStats}
      analyzePlan={null}
      analyzeError=""
      sqlText="SELECT 1"
      setSqlText={vi.fn()}
      setRole={vi.fn()}
    />,
  );
}

describe("SQL stats execution diagram", () => {
  beforeEach(() => {
    renderChart.mockClear();
  });

  it("renders the DAG the server sent with the stats", async () => {
    renderStats({
      total_elapsed_ms: 12,
      mermaid: 'flowchart LR\n  n_pg["pg\\npostgresql"] --> route',
      sources: [
        {
          field: "sql",
          source: "engine",
          strategy: "federated:trino",
          elapsed_ms: 12,
          rows: 3,
        },
      ],
    });
    await waitFor(() => expect(renderChart).toHaveBeenCalledTimes(1));
    expect(renderChart.mock.calls[0][1]).toContain("flowchart LR");
    await waitFor(() => expect(document.querySelector(".stats-mermaid svg")).not.toBeNull());
  });

  it("renders one diagram per statement in a batch", async () => {
    renderStats({
      total_elapsed_ms: 20,
      mermaid: "flowchart LR\n  a --> b\n\nflowchart LR\n  c --> d",
      sources: [],
    });
    await waitFor(() => expect(renderChart).toHaveBeenCalledTimes(2));
  });

  it("shows the source table alone when the server sent no diagram", async () => {
    renderStats({
      total_elapsed_ms: 5,
      sources: [
        { field: "sql", source: "pg_main", strategy: "direct:postgresql", elapsed_ms: 5, rows: 1 },
      ],
    });
    await screen.findByText("direct:postgresql");
    expect(renderChart).not.toHaveBeenCalled();
    expect(document.querySelector(".stats-mermaid")).toBeNull();
  });
});
