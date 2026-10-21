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

import { KnowledgeGraphPanel, ProductMapButton } from "../KnowledgeGraphPanel";

const last = () => made.scenes[made.scenes.length - 1];
const drawn = (call: unknown[]) => call[0] as GraphData;
const nodeIds = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const kinds = (g: GraphData) => [...new Set(g.edges.map((e) => e.kind))].sort();
const ZERO = ["t:customers", "t:orders"];

beforeEach(() => {
  made.scenes.length = 0;
  observed.fire.length = 0;
});

function panel() {
  render(
    <KnowledgeGraphPanel data={DATA} product="p:c360" productName="customer_360" zero={ZERO} />,
  );
}

describe("the Knowledge Graph panel", () => {
  it("loads three.js only once it scrolls into view, and draws the product's core", async () => {
    panel();
    expect(made.scenes).toHaveLength(0);
    act(() => observed.fire.forEach((f) => f()));
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const g = drawn(last().setData.mock.calls[0]);
    expect(nodeIds(g)).toEqual(["f:refresh", "p:c360", "t:customers", "t:orders", "term:customer"]);
    // No layer controls in the panel.
    expect(screen.queryByTestId("graph3d-layers")).not.toBeInTheDocument();
  });

  it("expands to the full graph on Knowledge graph, focused on the product", async () => {
    panel();
    fireEvent.click(screen.getByTestId("kg-panel-expand"));
    expect(await screen.findByTestId("graph3d")).toBeInTheDocument();
    await waitFor(() => expect(made.scenes).toHaveLength(1));
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
    fireEvent.click(screen.getByTestId("kg-panel-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(1));
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
    const noLineage = { ...DATA, edges: DATA.edges.filter((e) => e.kind !== "lineage") };
    render(
      <KnowledgeGraphPanel
        data={noLineage}
        product="p:c360"
        productName="customer_360"
        zero={ZERO}
      />,
    );
    fireEvent.click(screen.getByTestId("kg-panel-expand"));
    expect(await screen.findByTestId("graph3d-layer-tags")).toBeInTheDocument();
    expect(screen.queryByTestId("graph3d-layer-lineage")).not.toBeInTheDocument();
  });

  it("shows a selected table's columns on asking", async () => {
    panel();
    fireEvent.click(screen.getByTestId("kg-panel-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("graph3d-link-member:p:c360>t:customers"));
    fireEvent.click(screen.getByTestId("graph3d-columns"));
    await waitFor(() => expect(last().setData).toHaveBeenCalledTimes(2));
    expect(nodeIds(drawn(last().setData.mock.calls[1]))).toContain("c:customers.email");
  });
});

describe("the Product map entry point", () => {
  it("opens the full graph on the Product map preset, every product shown", async () => {
    render(<ProductMapButton data={DATA} />);
    fireEvent.click(screen.getByTestId("product-map-open"));
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
