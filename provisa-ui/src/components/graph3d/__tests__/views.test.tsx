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
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import type { GraphData } from "../graphModel";
import { DATA } from "./fixture";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
const made = vi.hoisted(() => ({ scenes: [] as Calls[] }));
vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor() {
      const calls: Calls = {};
      for (const m of [
        "setTheme",
        "setRotate",
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

// The meta-domain read: what each call asked for, answered with the fixture.
const reads = vi.hoisted(() => ({
  calls: [] as [string | null, boolean][],
  data: null as unknown,
}));
vi.mock("../useProductGraph", () => ({
  useProductGraph: (product: string | null, enabled: boolean) => {
    reads.calls.push([product, enabled]);
    return { data: enabled ? reads.data : null, error: null };
  },
}));

import { KnowledgeGraphPanel, ProductMapButton } from "../KnowledgeGraphPanel";

const last = () => made.scenes[made.scenes.length - 1];
const drawn = (call: unknown[]) => call[0] as GraphData;
const nodeIds = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const kinds = (g: GraphData) => [...new Set(g.edges.map((e) => e.kind))].sort();

beforeEach(() => {
  made.scenes.length = 0;
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
  it("loads three.js only once it scrolls into view, and draws the product's core", async () => {
    render(<KnowledgeGraphPanel productId="c360" productName="customer_360" />);
    expect(made.scenes).toHaveLength(0);
    expect(reads.calls.every(([, enabled]) => !enabled)).toBe(true);
    act(() => observed.fire.forEach((f) => f()));
    expect(reads.calls.at(-1)).toEqual(["c360", true]);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const g = drawn(last().setData.mock.calls[0]);
    expect(nodeIds(g)).toEqual(["f:refresh", "p:c360", "t:customers", "t:orders", "term:customer"]);
    // No layer controls in the panel.
    expect(screen.queryByTestId("graph3d-layers")).not.toBeInTheDocument();
  });

  it("lists in its legend only what it draws", async () => {
    panel();
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const legend = screen.getByTestId("kg-panel-stage").querySelector(".p3d-legend") as HTMLElement;
    expect([...legend.querySelectorAll("span")].map((e) => e.textContent)).toEqual([
      "data product",
      "member table",
      "command",
      // A term's entry names its variant (nodeLook).
      "concrete term",
    ]);
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
    fireEvent.click(screen.getByTestId("graph3d-layer-tags"));
    await waitFor(() => expect(legend()).not.toContain("tag"));
  });

  it("expands to the full graph on Knowledge graph, focused on the product", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    expect(await screen.findByTestId("graph3d")).toBeInTheDocument();
    // The panel's scene, then the expanded graph's.
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    expect(screen.getByRole("radio", { name: "Knowledge graph" })).toBeChecked();
    const g = drawn(last().setData.mock.calls[0]);
    expect(kinds(g)).not.toContain("relationship_outside");
    expect(nodeIds(g)).toContain("g:gold");
    // A linked product is drawn, but nothing is reached through it.
    expect(nodeIds(g)).toContain("p:billing");
    expect(nodeIds(g)).not.toContain("d:finance");
    expect(screen.getByTestId("graph3d-detail")).toHaveTextContent("customer_360");
  });

  it("removes only the tags when the tags layer goes, and the Lineage preset draws lineage alone", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    // The panel's scene, then the expanded graph's.
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    const before = drawn(last().setData.mock.calls[0]);
    fireEvent.click(screen.getByTestId("graph3d-layer-tags"));
    await waitFor(() => expect(last().setData).toHaveBeenCalledTimes(2));
    const after = drawn(last().setData.mock.calls[1]);
    expect(nodeIds(before).filter((id) => !nodeIds(after).includes(id))).toEqual(["g:gold"]);
    // A layer change keeps the camera where it is.
    expect(last().setData.mock.calls[1][2]).toBe(false);

    fireEvent.click(screen.getByRole("radio", { name: "Lineage" }));
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
    expect(await screen.findByTestId("graph3d-layer-tags")).toBeInTheDocument();
    expect(screen.queryByTestId("graph3d-layer-lineage")).not.toBeInTheDocument();
  });

  it("shows a selected table's columns on asking", async () => {
    panel();
    fireEvent.click(await screen.findByTestId("kg-panel-expand"));
    // The panel's scene, then the expanded graph's.
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    fireEvent.click(screen.getByTestId("graph3d-link-member:p:c360>t:customers"));
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
