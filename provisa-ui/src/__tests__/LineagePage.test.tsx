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
    {
      id: "orders.amount",
      column: "amount",
      relation: "orders",
      kind: "source",
      materialized: false,
    },
    { id: "total", column: "total", relation: null, kind: "derived", materialized: false },
  ],
  edges: [
    {
      source: "orders.amount",
      target: "total",
      transform: "orders.amount",
      ops: [{ name: "amount", kind: "identity" }],
    },
  ],
  outputs: ["total"],
  cycles: [{ nodes: ["x.c", "y.c"], has_materialization_boundary: false, classification: "error" }],
};

const fetchLineageGraph = vi.fn(async (..._a: unknown[]) => graph);
const fetchFederationGraph = vi.fn(async (..._a: unknown[]) => graph);

vi.mock("../api/lineage", () => ({
  fetchLineageGraph: (...a: unknown[]) => fetchLineageGraph(...a),
  fetchFederationGraph: (...a: unknown[]) => fetchFederationGraph(...a),
}));

const auth = {
  selectedRoles: [{ id: "analyst" }],
  activeOrgId: "acme",
  capabilities: ["view_governance"],
  demonstrated: [],
  loading: false,
};
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

// The lineage role picker lists EVERY role in the org, not only the ones the user holds (REQ-1628).
const fetchOrgRoles = vi.fn(async () => [
  { id: "analyst", capabilities: [], demonstrated: [], domain_access: [] },
  { id: "auditor", capabilities: [], demonstrated: [], domain_access: [] },
  { id: "vet", capabilities: [], demonstrated: [], domain_access: [] },
]);
vi.mock("../api/admin", () => ({ fetchOrgRoles: () => fetchOrgRoles() }));

// Stub the cytoscape-backed DAG (cytoscape needs a real layout engine, unavailable in jsdom).
// The stub surfaces the collapse/expand and modal callbacks as buttons so the page's REQ-1627
// wiring is testable without a layout engine.
vi.mock("../components/lineage/LineageDag", () => ({
  LineageDag: (props: {
    collapsedRelations?: ReadonlySet<string>;
    onCollapseAll?: () => void;
    onExpandAll?: () => void;
    onOpenModal?: () => void;
  }) => (
    <div data-testid="lineage-dag-stub">
      <span data-testid="collapsed-list">{[...(props.collapsedRelations ?? [])].join(",")}</span>
      {props.onCollapseAll && (
        <button data-testid="stub-collapse-all" onClick={props.onCollapseAll} />
      )}
      {props.onExpandAll && <button data-testid="stub-expand-all" onClick={props.onExpandAll} />}
      {props.onOpenModal && <button data-testid="stub-open-modal" onClick={props.onOpenModal} />}
    </div>
  ),
}));

import { LineagePage } from "../pages/LineagePage";

describe("LineagePage — REQ-1160/1161", () => {
  beforeEach(() => {
    fetchLineageGraph.mockClear();
    fetchFederationGraph.mockClear();
    sessionStorage.clear();
  });

  it("builds a statement graph and renders the DAG", async () => {
    render(
      <MemoryRouter>
        <LineagePage />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId("lineage-build"));
    await waitFor(() => expect(fetchLineageGraph).toHaveBeenCalled());
    expect(await screen.findByTestId("lineage-dag-stub")).toBeInTheDocument();
  });

  it("characterizes a boundary-less cycle as an error", async () => {
    render(
      <MemoryRouter>
        <LineagePage />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId("lineage-build"));
    expect(await screen.findByText(/no materialization boundary/i)).toBeInTheDocument();
    expect(screen.getByText("error")).toBeInTheDocument();
  });

  it("loads the federation graph on demand, through the picked role", async () => {
    render(
      <MemoryRouter>
        <LineagePage />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId("lineage-federation"));
    // REQ-1625: the role is the analytic lens, so it travels with the request. It opens on the
    // user's own role and is changed with the page's own picker.
    await waitFor(() =>
      expect(fetchFederationGraph).toHaveBeenCalledWith(
        expect.objectContaining({ roles: ["analyst"] }),
      ),
    );
  });

  it("offers every role in the org as a lineage perspective, not only the held ones", async () => {
    render(
      <MemoryRouter>
        <LineagePage />
      </MemoryRouter>,
    );
    await waitFor(() => expect(fetchOrgRoles).toHaveBeenCalledWith("acme"));
    fireEvent.click(screen.getByTestId("lineage-roles"));
    // REQ-1628: "vet" is a role the user does not hold; analysing its lineage is the point.
    expect(await screen.findByText("vet")).toBeInTheDocument();
  });

  it("withholds the perspective controls from a caller without view_governance", async () => {
    auth.capabilities = [];
    try {
      render(
        <MemoryRouter>
          <LineagePage />
        </MemoryRouter>,
      );
      expect(screen.queryByTestId("lineage-roles")).not.toBeInTheDocument();
      expect(screen.queryByTestId("lineage-federation")).not.toBeInTheDocument();
    } finally {
      auth.capabilities = ["view_governance"];
    }
  });

  it("collapses every relation when the federation loads, and expands on demand", async () => {
    render(
      <MemoryRouter>
        <LineagePage />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId("lineage-federation"));
    // REQ-1627: a federation arrives collapsed — one node per relation, not one per column.
    expect(await screen.findByText("orders")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("stub-expand-all"));
    await waitFor(() => expect(screen.getByTestId("collapsed-list")).toHaveTextContent(""));
    fireEvent.click(screen.getByTestId("stub-collapse-all"));
    await waitFor(() => expect(screen.getByTestId("collapsed-list")).toHaveTextContent("orders"));
  });

  it("opens the near-fullscreen view over the same graph", async () => {
    render(
      <MemoryRouter>
        <LineagePage />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTestId("lineage-build"));
    await screen.findByTestId("lineage-dag-stub");
    expect(screen.getAllByTestId("lineage-dag-stub")).toHaveLength(1);
    fireEvent.click(screen.getByTestId("stub-open-modal"));
    // REQ-1627: the modal hosts a second view of the same graph.
    await waitFor(() => expect(screen.getAllByTestId("lineage-dag-stub")).toHaveLength(2));
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
