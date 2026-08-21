// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1519: Analyze shows the engine's plan for the routed statement AND the Provisa rewrites
// that produced it — the optimizations are what the engine plan cannot say.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import { ResultsPanel } from "../pages/sql/ResultsPanel";
import type { ResultsGridState } from "../pages/sql/useResultsGrid";
import type { ExplainResponse } from "../api/admin";

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

const PLAN: ExplainResponse = {
  route: "DIRECT",
  route_reason: "single source",
  dialect: "postgres",
  analyzed: false,
  sources: ["orders"],
  optimizations: ["hot-table inline: currencies"],
  sql: "EXPLAIN (FORMAT JSON) SELECT id FROM orders",
  plan: [
    {
      op: "Hash Join",
      detail: { "Hash Cond": "o.cur = c.code" },
      rows: 120,
      cost: 44.5,
      actual_ms: null,
      children: [
        { op: "Seq Scan orders", detail: {}, rows: 120, cost: 12.5, actual_ms: null, children: [] },
      ],
    },
  ],
  mermaid: 'flowchart TD\n  route{{"DIRECT route"}}\n  n0["Hash Join"] --> route',
};

function renderAnalyze(analyzePlan: ExplainResponse | null, analyzeError = "") {
  return render(
    <ResultsPanel
      resultTab="analyze"
      setResultTab={vi.fn()}
      running={false}
      resultError=""
      resultRows={[]}
      resultColumns={[]}
      grid={grid}
      errors={[]}
      history={[]}
      queryStats={null}
      analyzePlan={analyzePlan}
      analyzeError={analyzeError}
      sqlText="SELECT id FROM orders"
      setSqlText={vi.fn()}
      setRole={vi.fn()}
    />,
  );
}

describe("SQL analyze panel", () => {
  beforeEach(() => {
    renderChart.mockClear();
  });

  it("draws the plan tree and lists every operator under its parent", async () => {
    renderAnalyze(PLAN);
    await waitFor(() => expect(renderChart).toHaveBeenCalledTimes(1));
    expect(renderChart.mock.calls[0][1]).toContain("flowchart TD");
    const panes = await screen.findByTestId("analyze-panes");
    const [listPane, diagramPane] = Array.from(panes.children);
    expect(diagramPane.querySelector(".stats-mermaid")).not.toBeNull();
    expect(listPane.querySelector("table")).not.toBeNull();
    await screen.findByText("Hash Join");
    await screen.findByText("Seq Scan orders");
    expect(screen.getByText("44.5")).not.toBeNull();
  });

  it("names the Provisa optimizations the engine plan cannot show", async () => {
    renderAnalyze(PLAN);
    await screen.findByText("hot-table inline: currencies");
  });

  it("shows measured timings instead of cost once the statement was run", async () => {
    renderAnalyze({
      ...PLAN,
      analyzed: true,
      plan: [
        { op: "Seq Scan orders", detail: {}, rows: 120, cost: 12.5, actual_ms: 3.5, children: [] },
      ],
    });
    await screen.findByText("3.5");
    expect(screen.queryByText("12.5")).toBeNull();
  });

  it("shows the refusal when the server would not describe the statement", async () => {
    renderAnalyze(null, "EXPLAIN is only supported for read statements");
    await screen.findByTestId("analyze-error");
    expect(renderChart).not.toHaveBeenCalled();
  });
});
