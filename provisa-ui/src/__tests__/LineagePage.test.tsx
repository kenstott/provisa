// Copyright (c) 2026 Kenneth Stott
// Canary: 5a9c1e08-3b74-4d62-8f09-2e6d7a0c4b95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1160/REQ-1161: lineage explorer page — build a statement graph, render cycles characterization.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import type { LineageGraphData } from "../api/lineage";

const graph: LineageGraphData = {
  nodes: [
    { id: "orders.amount", column: "amount", relation: "orders", kind: "source", materialized: false },
    { id: "total", column: "total", relation: null, kind: "derived", materialized: false },
  ],
  edges: [{ source: "orders.amount", target: "total", transform: "orders.amount", ops: [{ name: "amount", kind: "identity" }] }],
  outputs: ["total"],
  cycles: [{ nodes: ["x.c", "y.c"], has_materialization_boundary: false, classification: "error" }],
};

const fetchLineageGraph = vi.fn(async () => graph);
const fetchFederationGraph = vi.fn(async () => graph);

vi.mock("../api/lineage", () => ({
  fetchLineageGraph: (...a: unknown[]) => fetchLineageGraph(...a),
  fetchFederationGraph: (...a: unknown[]) => fetchFederationGraph(...a),
}));

// Stub the cytoscape-backed DAG (cytoscape needs a real layout engine, unavailable in jsdom).
vi.mock("../components/lineage/LineageDag", () => ({
  LineageDag: () => <div data-testid="lineage-dag-stub" />,
}));

import { LineagePage } from "../pages/LineagePage";

describe("LineagePage — REQ-1160/1161", () => {
  beforeEach(() => {
    fetchLineageGraph.mockClear();
    fetchFederationGraph.mockClear();
  });

  it("builds a statement graph and renders the DAG", async () => {
    render(<MemoryRouter><LineagePage /></MemoryRouter>);
    fireEvent.click(screen.getByTestId("lineage-build"));
    await waitFor(() => expect(fetchLineageGraph).toHaveBeenCalled());
    expect(await screen.findByTestId("lineage-dag-stub")).toBeInTheDocument();
  });

  it("characterizes a boundary-less cycle as an error", async () => {
    render(<MemoryRouter><LineagePage /></MemoryRouter>);
    fireEvent.click(screen.getByTestId("lineage-build"));
    expect(await screen.findByText(/no materialization boundary/i)).toBeInTheDocument();
    expect(screen.getByText("error")).toBeInTheDocument();
  });

  it("loads the federation graph on demand", async () => {
    render(<MemoryRouter><LineagePage /></MemoryRouter>);
    fireEvent.click(screen.getByTestId("lineage-federation"));
    await waitFor(() => expect(fetchFederationGraph).toHaveBeenCalled());
  });

  it("auto-builds from a ?sql= deep link (the show-lineage entry point)", async () => {
    render(
      <MemoryRouter initialEntries={["/lineage?sql=SELECT%20a%20FROM%20t"]}>
        <LineagePage />
      </MemoryRouter>,
    );
    await waitFor(() => expect(fetchLineageGraph).toHaveBeenCalledWith("SELECT a FROM t"));
    expect(await screen.findByTestId("lineage-dag-stub")).toBeInTheDocument();
  });

  it("auto-loads the federation graph focused from a ?focus= deep link", async () => {
    render(
      <MemoryRouter initialEntries={["/lineage?focus=mv_daily.total"]}>
        <LineagePage />
      </MemoryRouter>,
    );
    await waitFor(() =>
      expect(fetchFederationGraph).toHaveBeenCalledWith({ focus: "mv_daily.total" }),
    );
  });
});
