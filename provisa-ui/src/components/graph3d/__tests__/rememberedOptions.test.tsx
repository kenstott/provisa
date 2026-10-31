// Copyright (c) 2026 Kenneth Stott
// Canary: 6677f29f-be8e-40c7-bdcd-87fe59186c05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Everything a reader sets on the Ontology graph is there again after a reload: each option is
// changed through its control, the graph is unmounted and mounted afresh, and the control and the
// scene read as they were left. Nothing saved, a graph opens Force, whatever its edges fit.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import { setStored } from "../../../lib/browserStorage";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
const made = vi.hoisted(() => ({ scenes: [] as Calls[] }));
vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor() {
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
        "setFlat",
        "setArcs",
      ])
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));
// A 2D graph is drawn by cytoscape, which jsdom cannot draw; what matters here is that it is chosen.
vi.mock("../LaneStage", () => ({ LaneStage: () => <div data-testid="x-lanes" /> }));

import { GraphExplorer } from "../GraphExplorer";
import { GLOSSARY_SCOPE, type GraphData } from "../graphModel";

// One rank class, no cycle: the shape the opening layout used to open Layered.
const DATA: GraphData = {
  nodes: [
    { id: "term:1", type: "term", label: "Customer", domains: ["sales"], status: "approved" },
    { id: "term:2", type: "term", label: "Supplier", domains: ["procurement"], status: "draft" },
    { id: "term:3", type: "term", label: "Invoice", domains: ["finance"], status: "approved" },
    { id: "d:sales", type: "domain", label: "sales" },
    { id: "c:1", type: "column", label: "id" },
  ],
  edges: [
    { id: "e1", a: "term:2", b: "term:1", kind: "term_edge", label: "KIND_OF" },
    { id: "e2", a: "term:3", b: "term:1", kind: "term_edge", label: "PART_OF" },
    { id: "e3", a: "term:1", b: "d:sales", kind: "in_domain" },
    { id: "e4", a: "term:1", b: "c:1", kind: "term_binding" },
  ],
};

const explorer = () => (
  <GraphExplorer
    data={DATA}
    scope={GLOSSARY_SCOPE}
    preset="ontology"
    settingsKey={{ kind: "ontology", subject: null }}
    exportName="ontology"
    testId="x"
  />
);

/** The last scene made, once the graph has one and has drawn. */
async function scene(): Promise<Calls> {
  await waitFor(() => expect(made.scenes.length).toBeGreaterThan(0));
  const s = made.scenes[made.scenes.length - 1];
  await waitFor(() => expect(s.setData).toHaveBeenCalled());
  return s;
}

/** Leave the page and come back: a remount reads only what was saved. */
function reload(view: ReturnType<typeof render>) {
  view.unmount();
  made.scenes.length = 0;
  return render(explorer());
}

async function pick(menu: string, value: string) {
  fireEvent.click(screen.getByTestId(menu));
  fireEvent.click(await screen.findByTestId(`${menu}-option-${value}`));
  fireEvent.click(screen.getByTestId(menu));
}

afterEach(() => {
  made.scenes.length = 0;
});

describe("the layout a graph opens in", () => {
  it("is Force with nothing saved, where Layered would fit", async () => {
    render(explorer());
    const s = await scene();
    expect(screen.getByRole("radio", { name: "Force" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "Layered" })).not.toBeDisabled();
    expect(s.setArcs).toHaveBeenLastCalledWith(false);
  });

  it("is a saved Layered, still", async () => {
    setStored("graph3d.ontology.org", JSON.stringify({ layout: "layered" }));
    render(explorer());
    const s = await scene();
    expect(screen.getByRole("radio", { name: "Layered" })).toBeChecked();
    await waitFor(() => expect(s.setArcs).toHaveBeenLastCalledWith(true));
  });

  it("is the Taxonomy preset's own Layered, until the reader chooses Force, and that is kept", async () => {
    const view = render(explorer());
    await scene();
    fireEvent.click(screen.getByRole("radio", { name: "Taxonomy" }));
    await waitFor(() => expect(screen.getByRole("radio", { name: "Layered" })).toBeChecked());
    fireEvent.click(screen.getByRole("radio", { name: "Force" }));
    reload(view);
    await scene();
    expect(screen.getByRole("radio", { name: "Taxonomy" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "Force" })).toBeChecked();
  });
});

describe("what the reader sets on the graph, after a reload", () => {
  it("keeps Layered", async () => {
    const view = render(explorer());
    await scene();
    fireEvent.click(screen.getByRole("radio", { name: "Layered" }));
    reload(view);
    const s = await scene();
    expect(screen.getByRole("radio", { name: "Layered" })).toBeChecked();
    await waitFor(() => expect(s.setArcs).toHaveBeenLastCalledWith(true));
  });

  it("keeps Force over a preset that opens Layered", async () => {
    setStored("graph3d.ontology.org", JSON.stringify({ preset: "taxonomy", layout: "force" }));
    render(explorer());
    await scene();
    expect(screen.getByRole("radio", { name: "Force" })).toBeChecked();
  });

  it("keeps the preset", async () => {
    const view = render(explorer());
    await scene();
    fireEvent.click(screen.getByRole("radio", { name: "Taxonomy" }));
    reload(view);
    await scene();
    expect(screen.getByRole("radio", { name: "Taxonomy" })).toBeChecked();
  });

  it("keeps 2D, and 3D's choices underneath it", async () => {
    const view = render(explorer());
    await scene();
    fireEvent.click(screen.getByRole("radio", { name: "Layered" }));
    fireEvent.click(screen.getByRole("radio", { name: "2D" }));
    expect(await screen.findByTestId("x-lanes")).toBeInTheDocument();
    const again = reload(view);
    expect(await screen.findByTestId("x-lanes")).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "2D" })).toBeChecked();
    fireEvent.click(screen.getByRole("radio", { name: "3D" }));
    await scene();
    expect(screen.getByRole("radio", { name: "Layered" })).toBeChecked();
    reload(again);
    await scene();
    expect(screen.getByRole("radio", { name: "3D" })).toBeChecked();
  });

  it("keeps Rotate", async () => {
    const view = render(explorer());
    await scene();
    fireEvent.click(screen.getByRole("checkbox", { name: "Rotate" }));
    reload(view);
    const s = await scene();
    expect(screen.getByRole("checkbox", { name: "Rotate" })).toBeChecked();
    await waitFor(() => expect(s.setRotate).toHaveBeenLastCalledWith(true));
  });

  it("keeps Grid off", async () => {
    const view = render(explorer());
    await scene();
    fireEvent.click(screen.getByRole("checkbox", { name: "Grid" }));
    reload(view);
    const s = await scene();
    expect(screen.getByRole("checkbox", { name: "Grid" })).not.toBeChecked();
    await waitFor(() => expect(s.setGrid).toHaveBeenLastCalledWith(false));
  });

  it("keeps the tightness slider", async () => {
    const view = render(explorer());
    await scene();
    const before = Number(screen.getByRole("slider").getAttribute("aria-valuenow"));
    fireEvent.keyDown(screen.getByRole("slider"), { key: "ArrowRight" });
    const set = Number(screen.getByRole("slider").getAttribute("aria-valuenow"));
    expect(set).toBeGreaterThan(before);
    reload(view);
    await scene();
    expect(Number(screen.getByRole("slider").getAttribute("aria-valuenow"))).toBe(set);
  });

  it("keeps the Layers chosen", async () => {
    const view = render(explorer());
    await scene();
    await pick("x-layers-menu", "domains");
    const said = screen.getByTestId("x-layers-menu").textContent;
    reload(view);
    await scene();
    expect(screen.getByTestId("x-layers-menu").textContent).toBe(said);
    expect(said).not.toContain("Domains");
  });

  it("keeps the Terms chosen, by status and Unconnected", async () => {
    const view = render(explorer());
    await scene();
    await pick("x-terms-menu", "draft");
    await pick("x-terms-menu", "unconnected");
    const said = screen.getByTestId("x-terms-menu").textContent;
    reload(view);
    const s = await scene();
    expect(screen.getByTestId("x-terms-menu").textContent).toBe(said);
    const nodes = (s.setData.mock.lastCall?.[0] as GraphData).nodes.map((n) => n.id);
    expect(nodes).not.toContain("term:2");
  });

  it("keeps the edge types chosen", async () => {
    const view = render(explorer());
    await scene();
    await pick("x-filter-edge-type", "PART_OF");
    expect(screen.getByTestId("x-filter-edge-type")).toHaveTextContent("KIND_OF");
    reload(view);
    await scene();
    expect(screen.getByTestId("x-filter-edge-type")).toHaveTextContent("KIND_OF");
    expect(screen.getByTestId("x-filter-edge-type")).not.toHaveTextContent("PART_OF");
  });

  it("drops an edge type the graph no longer has", async () => {
    setStored("graph3d.ontology.org", JSON.stringify({ edgeTypes: ["KIND_OF", "GONE"] }));
    render(explorer());
    await scene();
    expect(screen.getByTestId("x-filter-edge-type")).toHaveTextContent("KIND_OF");
    expect(screen.getByTestId("x-filter-edge-type")).not.toHaveTextContent("GONE");
  });

  it("keeps the domains chosen", async () => {
    const view = render(explorer());
    await scene();
    await pick("x-filter-domain", "procurement");
    const said = screen.getByTestId("x-filter-domain").textContent;
    reload(view);
    await scene();
    expect(screen.getByTestId("x-filter-domain").textContent).toBe(said);
    expect(said).not.toContain("procurement");
  });
});
