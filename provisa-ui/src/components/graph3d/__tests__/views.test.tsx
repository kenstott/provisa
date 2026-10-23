// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1d3f85-0e2b-4c7a-9d64-b8f2e5c1a039
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the Knowledge Graph panel, the full graph it expands into and the Product map entry
// point. jsdom has no WebGL, so the three.js scene is replaced by a recorder.

import { act } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import type { GraphData } from "../graphModel";
import { DATA } from "./fixture";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
// Each scene's calls, and what it reports to the view (a node picked on the canvas).
const made = vi.hoisted(() => ({
  scenes: [] as Calls[],
  callbacks: [] as { onSelect: (h: { kind: string; id: string }) => void }[],
}));
vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor(
      _canvas: unknown,
      _labels: unknown,
      callbacks: { onSelect: (h: { kind: string; id: string }) => void },
    ) {
      made.callbacks.push(callbacks);
      const calls: Calls = {};
      for (const m of [
        "setTheme",
        "setRotate",
        "setGrid",
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFrontal",
      ])
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));

// Each observed element, so a test can scroll one into view.
const observed = vi.hoisted(() => ({ fire: [] as (() => void)[] }));
class FakeObserver {
  private cb: IntersectionObserverCallback;
  constructor(cb: IntersectionObserverCallback) {
    this.cb = cb;
  }
  observe() {
    observed.fire.push(() =>
      this.cb(
        [{ isIntersecting: true } as IntersectionObserverEntry],
        this as unknown as IntersectionObserver,
      ),
    );
  }
  disconnect() {}
}
vi.stubGlobal("IntersectionObserver", FakeObserver);

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));

// The meta-domain read: what each call asked for, answered with the fixture.
const reads = vi.hoisted(() => ({
  calls: [] as [string | null, boolean][],
  data: null as unknown,
}));
// The view's scoped read (REQ-2136) answers with the same fixture: what its filters keep is
// productGraphData's query, tested there.
const scopes = vi.hoisted(() => ({ asked: [] as unknown[] }));
vi.mock("../useProductGraph", () => ({
  useProductGraph: (product: string | null, enabled: boolean) => {
    reads.calls.push([product, enabled]);
    return {
      data: enabled ? reads.data : null,
      error: null,
      scoped: enabled ? { data: reads.data, query: "query DataProductGraph { x }" } : null,
      onScope: (s: unknown) => scopes.asked.push(s),
    };
  },
}));

import { KnowledgeGraphPanel, ProductMapButton } from "../KnowledgeGraphPanel";

const last = () => made.scenes[made.scenes.length - 1];
/** Turns a layer on or off in the expanded graph's Layers menu. */
const toggleLayer = (layer: string) => {
  const id = `graph3d-layers-menu-option-${layer}`;
  if (!screen.queryByTestId(id)) fireEvent.click(screen.getByTestId("graph3d-layers-menu"));
  fireEvent.click(screen.getByTestId(id));
};
const drawn = (call: unknown[]) => call[0] as GraphData;
const nodeIds = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const kinds = (g: GraphData) => [...new Set(g.edges.map((e) => e.kind))].sort();

beforeEach(() => {
  made.scenes.length = 0;
  made.callbacks.length = 0;
  observed.fire.length = 0;
  reads.calls.length = 0;
  reads.data = DATA;
});

function panel() {
  render(<KnowledgeGraphPanel productId="c360" productName="customer_360" />);
  // Scrolled into view: the product is read and three.js loads.
  act(() => observed.fire.forEach((f) => f()));
}

describe("the Knowledge Graph panel", () => {
  it("loads three.js only once it scrolls into view, and draws the graph the expanded view draws", async () => {
    render(<KnowledgeGraphPanel productId="c360" productName="customer_360" />);
    expect(made.scenes).toHaveLength(0);
    expect(reads.calls.every(([, enabled]) => !enabled)).toBe(true);
    act(() => observed.fire.forEach((f) => f()));
    expect(reads.calls.at(-1)).toEqual(["c360", true]);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const inPlace = drawn(made.scenes[0].setData.mock.calls[0]);
    expect(nodeIds(inPlace)).toEqual(expect.arrayContaining(["p:c360", "t:customers", "t:orders"]));
    // The Ontology View's controls: menus beside the filters, no rows of layer chips.
    expect(screen.getByTestId("kg-panel-layers-menu")).toBeInTheDocument();
    expect(screen.queryByTestId("kg-panel-layers")).not.toBeInTheDocument();
    expect(screen.getByTestId("kg-panel-open-as-query")).toBeInTheDocument();
    // Expanded, it is the same graph under the same settings.
    fireEvent.click(screen.getByTestId("kg-panel-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    expect(nodeIds(drawn(last().setData.mock.calls[0]))).toEqual(nodeIds(inPlace));
  });

  it("lists in its legend only what it draws", async () => {
    panel();
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const entries = (testId: string) =>
      [
        ...(
          screen.getByTestId(testId).querySelector(".p3d-legend") as HTMLElement
        ).querySelectorAll("span"),
      ].map((e) => e.textContent);
    const inPlace = entries("kg-panel-stage");
    expect(inPlace).toEqual(
      expect.arrayContaining(["data product", "member table", "command", "term"]),
    );
    // Each entry keys its shape: a table is a cylinder here as in every graph.
    expect(
      screen.getByTestId("kg-panel-stage").querySelector('[data-testid="legend-table:plain"] i'),
    ).toHaveClass("g3d-shape-cylinder");
    // The same graph expanded: the same entries.
    fireEvent.click(screen.getByTestId("kg-panel-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    expect(entries("graph3d-stage")).toEqual(inPlace);
  });

  it("drops a layer's entry from the full graph's legend as the layer goes", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    const legend = () =>
      [
        ...(
          screen.getByTestId("graph3d-stage").querySelector(".p3d-legend") as HTMLElement
        ).querySelectorAll("span"),
      ].map((e) => e.textContent);
    expect(legend()).toContain("tag");
    toggleLayer("tags");
    await waitFor(() => expect(legend()).not.toContain("tag"));
  });

  it("expands to the full graph on Knowledge graph, focused on the product", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    expect(await screen.findByTestId("graph3d")).toBeInTheDocument();
    // The panel's scene, then the expanded graph's.
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    expect(
      within(screen.getByTestId("graph3d")).getByRole("radio", { name: "Knowledge graph" }),
    ).toBeChecked();
    const g = drawn(last().setData.mock.calls[0]);
    expect(kinds(g)).not.toContain("relationship_outside");
    expect(nodeIds(g)).toContain("g:gold");
    // A linked product is drawn, but nothing is reached through it.
    expect(nodeIds(g)).toContain("p:billing");
    expect(nodeIds(g)).not.toContain("d:finance");
    expect(screen.getByTestId("graph3d-detail")).toHaveTextContent("customer_360");
  });

  it("links the detail's title and each connection to its own page", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    expect(screen.getByTestId("graph3d-title-link")).toHaveAttribute(
      "href",
      "/data-products?product=c360",
    );
    const rows = screen
      .getByTestId("graph3d-detail")
      .querySelectorAll<HTMLElement>('[data-testid^="graph3d-link-"]');
    expect(rows.length).toBeGreaterThan(0);
    for (const row of rows) expect(row.tagName).toBe("A");
    // The product's command and its tag, each on its own page.
    const hrefs = [...rows].map((r) => r.getAttribute("href"));
    expect(hrefs).toContain("/commands?command=refresh");
    expect(hrefs).toContain("/admin/tags?tag=gold");
  });

  it("opens as its query under its own product's focus, and not under one the reader picks", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    // Its filters are its query's: the scope it reports, every filter open at first.
    expect(scopes.asked.at(-1)).toMatchObject({ domains: [], tags: [], sensitivities: [] });
    fireEvent.click(screen.getByTestId("graph3d-open-as-query"));
    expect(await screen.findByTestId("graph3d-open-as-graphql")).not.toBeDisabled();
    await act(async () => {
      fireEvent.keyDown(document.activeElement ?? document.body, { key: "Escape" });
    });
    // A focus of the reader's own: a neighbourhood the query does not state.
    fireEvent.click(screen.getByTestId("graph3d-filter-focus"));
    fireEvent.click(await screen.findByTestId("graph3d-filter-focus-option-term:customer"));
    fireEvent.click(screen.getByTestId("graph3d-open-as-query"));
    await waitFor(() => expect(screen.getByTestId("graph3d-open-as-graphql")).toBeDisabled());
  });

  it("removes only the tags when the tags layer goes, and the Lineage preset draws lineage alone", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    // The panel's scene, then the expanded graph's.
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    const before = drawn(last().setData.mock.calls[0]);
    toggleLayer("tags");
    await waitFor(() => expect(last().setData).toHaveBeenCalledTimes(2));
    const after = drawn(last().setData.mock.calls[1]);
    expect(nodeIds(before).filter((id) => !nodeIds(after).includes(id))).toEqual(["g:gold"]);
    // A layer change keeps the camera where it is.
    expect(last().setData.mock.calls[1][2]).toBe(false);

    fireEvent.click(within(screen.getByTestId("graph3d")).getByRole("radio", { name: "Lineage" }));
    await waitFor(() => expect(last().setData).toHaveBeenCalledTimes(3));
    const [lineage, at, reset] = last().setData.mock.calls[2];
    expect(kinds(lineage)).toEqual(["lineage"]);
    expect(at.get("x:raw_orders")[0]).toBeLessThan(at.get("t:orders")[0]);
    expect(reset).toBe(true);
  });

  it("offers no layer the data has nothing in", async () => {
    reads.data = { ...DATA, edges: DATA.edges.filter((e) => e.kind !== "lineage") };
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    fireEvent.click(await screen.findByTestId("graph3d-layers-menu"));
    expect(await screen.findByTestId("graph3d-layers-menu-option-tags")).toBeInTheDocument();
    expect(screen.queryByTestId("graph3d-layers-menu-option-lineage")).not.toBeInTheDocument();
  });

  it("shows a selected table's columns on asking", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    // The panel's scene, then the expanded graph's.
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    // The table picked on the canvas (a connection row is a link to the table's page).
    act(() => made.callbacks.at(-1)?.onSelect({ kind: "node", id: "t:customers" }));
    fireEvent.click(screen.getByTestId("graph3d-columns"));
    await waitFor(() => expect(last().setData).toHaveBeenCalledTimes(2));
    expect(nodeIds(drawn(last().setData.mock.calls[1]))).toContain("c:customers.email");
  });
});

describe("the Product map entry point", () => {
  it("opens the full graph on the Product map preset, every product shown", async () => {
    render(<ProductMapButton />);
    expect(reads.calls.every(([, enabled]) => !enabled)).toBe(true);
    fireEvent.click(screen.getByTestId("product-map-open"));
    expect(reads.calls.at(-1)).toEqual([null, true]);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(screen.getByRole("radio", { name: "Product map" })).toBeChecked();
    expect(nodeIds(drawn(last().setData.mock.calls[0]))).toEqual([
      "d:finance",
      "d:sales",
      "p:billing",
      "p:c360",
    ]);
  });
});
